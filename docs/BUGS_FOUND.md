# Planted & Discovered Concurrency Bugs Report

This document records the concurrency and reliability bugs investigated, reproduced, and remediated in `minilab`. Each entry includes authentic reproduction output, root cause breakdown, fix mechanics, regression tests, and architectural lessons.

---

## Bug 1: Lock-Order Deadlock Between Contending Actuators

**Symptom:**
When two arms attempt to access overlapping physical zones in opposing order, the orchestrator hangs indefinitely. The watchdog timeout fires and reports circular wait:
```text
DEADLOCK suspected: workflow did not finish in 1.0s
Zone State: {'holders': {'A': 'deadlock:step1', 'B': 'deadlock:step2'}, 'waiting': {'deadlock:step1': 'B', 'deadlock:step2': 'A'}}
Deadlock cycle detected: ['deadlock:step1', 'deadlock:step2', 'deadlock:step1']
```

**How to reproduce:**
Run `deadlock-naive` scenario:
```bash
minilab run --scenario deadlock-naive --timeout 1.0
```
or run regression test:
```bash
pytest -v tests/test_bugs.py::test_bug1_repro_naive_zone_order_deadlocks
```

**Root cause:**
`step1` requests zones `["A", "B"]`, while `step2` requests `["B", "A"]`.
With `zone_ordered=False` and a minor execution delay (`zone_step_delay=0.01`):
1. `step1` acquires zone `A`.
2. `step2` acquires zone `B`.
3. `step1` suspends waiting for zone `B` (held by `step2`).
4. `step2` suspends waiting for zone `A` (held by `step1`).

All four Coffman conditions are met:
- Mutual exclusion: spatial zones can only be held by one owner.
- Hold and wait: tasks retain acquired locks while waiting for subsequent locks.
- No preemption: locks cannot be forcibly revoked from holding tasks.
- Circular wait: `step1 -> B (step2) -> A (step1)`.

**Fix:**
Implemented two-tier remediation in `minilab/host/zones.py`:
1. **Global Canonical Ordering (`ordered=True`)**: In `ZoneManager.acquire()`, requested zone identifiers are sorted lexicographically (`order = sorted(set(zones))`). Both tasks must acquire `A` before acquiring `B`, breaking the *circular wait* condition.
2. **Watchdog Fallback (`zone_timeout`)**: `ZoneManager.acquire()` wraps acquisitions in `async with asyncio.timeout(timeout):`. If contention exceeds the timeout threshold, a `ZoneTimeout` is raised and all partially acquired locks are safely released in the `except BaseException` block.

**Regression tests:**
- `tests/test_bugs.py::test_bug1_fix_ordered_zones_complete`
- `tests/test_bugs.py::test_bug1_fix_zone_timeout_breaks_deadlock`

**Lesson:**
In cooperative async runtimes, uncontended `asyncio.Lock.acquire()` does not yield execution to the event loop. Deadlocks only manifest across suspension points (such as physical motion delays), making canonical resource ordering essential.

---

## Bug 2: Command Retry Causes Double Execution on Non-Idempotent Device

**Symptom:**
Under transport packet loss where an acknowledgement (ACK) frame is dropped, the host times out and resends the command. A device simulator lacking deduplication executes the operation twice:
```text
Workflow 'dispense'  status=SUCCESS  duration=0.51s
Hardware Device State:
  pump1 : primed=False, dispensed_total=10.0, stopped=False
```
For a 5.0 mL dispense command, the device dispensed 10.0 mL—a double dose.

**How to reproduce:**
Run `double-dispense` scenario:
```bash
minilab run --scenario double-dispense
```
or run regression test:
```bash
pytest -v tests/test_bugs.py::test_bug2_repro_no_dedup_double_executes
```

**Root cause:**
The host protocol employs at-least-once transmission semantics. When the host sends `cmd id=1`, the device receives it, dispatches execution, and sends an `ack id=1`. The network drops the `ack`. When the host's `ack_timeout` expires, it retransmits `cmd id=1 attempt=1`. Without device-side deduplication, the simulator treats the retry as an independent command and executes a second dispensing action.

**Fix:**
Implemented device-side message tracking in `minilab/devices/base.py`:
1. Each device maintains `self._replies: dict[int, Message | None]`.
2. When a command arrives, if `msg.msg_id in self._replies`:
   - Immediately re-transmit the `ack` frame.
   - If execution has completed (`reply is not None`), replay the cached `done` or `error` frame.
   - Return immediately without queueing a second execution task.

**Regression tests:**
- `tests/test_devices.py::test_device_dedup`
- `tests/test_client.py::test_retry_on_dropped_ack_does_not_double_execute`
- `tests/test_bugs.py::test_bug2_fix_dedup_executes_once`

**Lesson:**
At-least-once transport delivery plus idempotent receiver caching guarantees effectively-once execution semantics.

---

## Bug 3: Synchronous Blocking Call Stalls Event Loop and Degrades Latency

**Symptom:**
Simulating a 20 ms computational workload (e.g. image processing or CV calibration) using standard blocking `time.sleep(0.02)` causes massive latency spikes across unrelated background tasks:
```text
Baseline (blocker=none):
  p50: 0.07 ms | p90: 0.09 ms | p99: 0.21 ms | Max: 0.66 ms

Synchronous Blocker (blocker=sync):
  p50: 0.07 ms | p90: 0.09 ms | p99: 0.20 ms | Max: 20.51 ms
```
The maximum command response latency jumped from 0.66 ms to over 20 ms. On a physical machine, such a stall would unacceptably delay an emergency stop command.

**How to reproduce:**
Run latency benchmarks:
```bash
minilab bench --blocker none --out results/bench_none.json
minilab bench --blocker sync --out results/bench_sync.json
minilab bench --blocker thread --out results/bench_thread.json
python scripts/plot_latency.py results/bench_sync.json results/bench_thread.json --out docs/img/latency.png
```

**Root cause:**
Python `asyncio` is a cooperative single-threaded event loop. When a coroutine invokes a blocking synchronous call like `time.sleep()`, the OS thread is halted. The event loop cannot process I/O events, schedule pending tasks, or process incoming protocol frames until the synchronous call returns.

**Fix:**
Offloaded blocking operations from the main event loop thread to Python's background thread pool via `await asyncio.to_thread(time.sleep, 0.02)`. The main event loop remains unblocked and immediately responsive to protocol messages and e-stop commands:
```text
Thread Offloaded (blocker=thread):
  p50: 0.07 ms | p90: 0.09 ms | p99: 0.20 ms | Max: 6.87 ms
```

**Evidence:**
Generated overlaid histogram: `docs/img/latency.png`.

**Lesson:**
In a cooperative event-driven architecture, a single blocking call anywhere in the process introduces unacceptable latency jitter for all concurrent subsystems.

