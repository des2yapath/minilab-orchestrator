# Mini Lab Controller (`minilab`): Codebase Walkthrough & Interview Guide

This guide is designed for the human owner to thoroughly understand the architecture, concurrency patterns, and design decisions of `minilab` over a single weekend.

---

## 1. Suggested Reading Order

1. [`minilab/messages.py`](file:///C:/Users/divya/OneDrive/Desktop/Codebase/minilab-controller/minilab/messages.py): CRC32-framed message schema and serialization.
2. [`minilab/transport.py`](file:///C:/Users/divya/OneDrive/Desktop/Codebase/minilab-controller/minilab/transport.py): In-memory channels with deterministic, seeded fault injection.
3. [`minilab/devices/base.py`](file:///C:/Users/divya/OneDrive/Desktop/Codebase/minilab-controller/minilab/devices/base.py): Device simulator base class with deduplication and e-stop handling.
4. [`minilab/host/client.py`](file:///C:/Users/divya/OneDrive/Desktop/Codebase/minilab-controller/minilab/host/client.py): Reliable host client with ACK wait, retries, and nudge mechanisms.
5. [`minilab/host/zones.py`](file:///C:/Users/divya/OneDrive/Desktop/Codebase/minilab-controller/minilab/host/zones.py): Spatial zone manager with canonical ordering and cycle detection.
6. [`minilab/host/runner.py`](file:///C:/Users/divya/OneDrive/Desktop/Codebase/minilab-controller/minilab/host/runner.py): Concurrent DAG workflow execution engine using asyncio tasks and events.
7. [`minilab/host/orchestrator.py`](file:///C:/Users/divya/OneDrive/Desktop/Codebase/minilab-controller/minilab/host/orchestrator.py): High-level lifecycle coordinator, emergency stop, and reset.
8. [`tests/test_bugs.py`](file:///C:/Users/divya/OneDrive/Desktop/Codebase/minilab-controller/tests/test_bugs.py): Planted concurrency bug reproductions and regression test suite.

---

## 2. Asynchronous Concurrency Primer

### Event Loop and Tasks
Python's `asyncio` runs a single-threaded cooperative event loop. An `asyncio.Task` wraps a coroutine and registers it with the loop for execution:
```python
task = asyncio.create_task(coro())
```

### `await` as a Suspension Point
An `await` expression is an explicit cooperative yield point. The currently executing task yields control back to the event loop, allowing other tasks to run:
```python
await asyncio.sleep(0.01)  # Gives other tasks an opportunity to execute
```

### `asyncio.Lock`
Provides mutual exclusion between coroutines. Crucially, an *uncontended* lock acquisition does not yield control to the loop:
```python
async with lock:
    # Critical section
```

### Futures: `asyncio.wait` vs `wait_for`
`asyncio.wait_for(fut, timeout)` automatically cancels `fut` when the timeout fires. For shared or persistent futures (such as ACK waiting), cancelling the future prevents subsequent retries from ever succeeding. Instead, we use `asyncio.wait({fut}, timeout=t)`:
```python
done, _ = await asyncio.wait({p.ack_fut}, timeout=ack_timeout)
if p.ack_fut.done():
    # Succeeded
```

### Task Cancellation & Cleanup
Cancellation injects an `asyncio.CancelledError` at the current suspension point. It is vital to never swallow this error; clean up in `finally` and let it propagate:
```python
try:
    await do_work()
finally:
    release_resources()
```

### Idempotency
An operation is idempotent if executing it multiple times produces the identical side-effect as executing it once:
$$f(f(x)) = f(x)$$
In `minilab`, every command carries a persistent `msg_id`. Retries reuse this ID, allowing devices to return cached results rather than re-executing actions.

### Deadlock & The Coffman Conditions
A deadlock occurs when all four Coffman conditions hold simultaneously:
1. **Mutual Exclusion:** Resources cannot be shared.
2. **Hold and Wait:** A task holds resources while waiting for more.
3. **No Preemption:** Resources cannot be forcibly taken away.
4. **Circular Wait:** Task A waits for Task B, which waits for Task A.
Breaking any one condition prevents deadlocks. `minilab` breaks circular wait by ordering lock acquisition lexicographically.

### Percentiles (p50, p90, p99)
- **p50 (Median):** Typical response time.
- **p99 (99th Percentile):** Latency tail experienced by 1 in 100 commands.
Tail latency reveals event loop stalls, GC pauses, and lock contention that are invisible in average/mean numbers.

### Real-Time vs In-Process Simulation
- **Hard Real-Time:** Missing a deadline is a catastrophic system failure (e.g., flight controls, pacemaker). Requires hardware timers, RTOS kernel preemption, and memory locking (`mlockall`).
- **Simulated Hardware (`minilab`):** Soft latency monitoring in an event-driven user-space process on a general-purpose operating system.

---

## 3. Tracing a Command Through the System

### Scenario A: Happy Path (`pump1.dispense(5.0)`)
1. **Host Request:** `runner` calls `client.call("dispense", {"ml": 5.0})`.
2. **Phase 1 (ACK):** Client generates `msg_id=7`, registers `Pending` futures, and transmits frame `Message("cmd", msg_id=7, ...)` over the link.
3. **Transport Delivery:** `FaultyChannel` computes random numbers, finds no faults, and delivers the encoded frame.
4. **Device Receipt:** `pump1` reader decodes frame, verifies CRC, logs `device_cmd_recv`, sends `Message("ack", msg_id=7)`, and spawns an `_execute` task.
5. **ACK Receipt:** Client reader loop receives `ack`, records `t_ack`, and resolves `p.ack_fut`.
6. **Execution:** `pump1` dispenses fluid for simulated duration ($0.5\text{ s}$), increments `dispensed_total`, and transmits `Message("done", msg_id=7, result=...)`.
7. **Phase 2 (DONE):** Client reader loop receives `done`, resolves `p.done_fut`, records round-trip latency, and returns `CallResult`.

### Scenario B: Dropped ACK with Retry Recovery
1. **Host Request:** Client sends `cmd id=7 attempt=0`.
2. **Device Processing:** Device receives command, sends `ack id=7`, and begins execution.
3. **Transport Drop:** `FaultyChannel` injects a drop fault on the `to_host` link. The `ack` frame is discarded.
4. **Host Timeout & Retry:** Client's `ack_timeout` expires without receiving an ACK. Client increments retry counter and retransmits `cmd id=7 attempt=1` with the same `msg_id`.
5. **Idempotent Device Handling:** Device receives `cmd id=7 attempt=1`. It checks `_replies[7]`. Because `msg_id=7` has already been seen, it **does not execute twice**. It immediately transmits another `ack id=7`.
6. **Completion:** When the original execution finishes, device sends `done id=7`. Client receives it, records metrics, and completes successfully without double dosing.

---

## 4. Systems Interview Q&A

### Q1: How do you handle a lost message between host and device?
**Answer:** The protocol operates on at-least-once delivery with two-phase waiting. In Phase 1, the client sends a command and waits for an immediate ACK. If the ACK is lost or dropped, the client times out (`ack_timeout`) and retries the transmission using the identical `msg_id` and an incremented `attempt` counter. In Phase 2, the client waits for DONE; if the response is delayed or lost, the client periodically sends a "nudge" re-transmission to request a replay of the cached completion frame.

### Q2: Why are command retries safe? How do you prevent duplicate actuation?
**Answer:** Retries are safe because every device simulator enforces strict idempotency keyed by unique `msg_id`. The device maintains an in-memory reply cache (`_replies`). When a duplicate command frame arrives, the device immediately re-sends an ACK and replays any cached DONE/ERROR response, completely bypassing the physical actuation lock (`_exec_lock`). This prevents catastrophic bugs such as double dosing a reagent or repeating an arm translation.

### Q3: How did you identify, reproduce, and fix the actuator deadlock?
**Answer:** When two arms attempted to acquire zones `A` and `B` in reverse order (`A->B` vs `B->A`), the workflow hung. We reproduced this under the `--scenario deadlock-naive` flag with a simulated motion delay. Inspection of the runtime wait-for graph confirmed circular wait. We implemented two fixes: (1) canonical global ordering where all zone acquisition requests are sorted alphabetically before taking locks, and (2) a watchdog timeout (`zone_timeout`) that breaks hung acquisitions and guarantees lock release in `finally` blocks.

### Q4: What happens during an emergency stop (e-stop)?
**Answer:** Safety is prioritized over software task completion. The orchestrator immediately latches `estop_state.active = True` synchronously. It concurrently dispatches dedicated `estop` command frames to all connected devices regardless of whether they were previously marked offline. Next, it cancels all running software step tasks (releasing physical zone locks via `finally` clauses). Devices enter their safe states (arms freeze, heaters revert target temperatures to ambient, pumps record partial volume dispensed), and the system remains latched until an explicit `reset()` is performed.

### Q5: How does the system degrade gracefully when a hardware device fails?
**Answer:** When a device times out or disconnects, the host marks that device `online = False`. The failed step is marked `FAILED`, and any downstream steps that depend on it are immediately marked `SKIPPED`. Independent execution branches (such as other arms or the thermal controller) continue unhindered. Furthermore, subsequent commands to the offline device fail fast (`DeviceOffline` in $<20\text{ ms}$) rather than repeatedly burning retry timeouts.

### Q6: How do you prove that fault injection and test scenarios are reproducible?
**Answer:** All pseudo-randomness in `transport.py` is initialized using deterministic string seeds (`random.Random(f"{seed}:{name}")`), avoiding Python's process-dependent string hash seeding. Additionally, on every transmission, all random numbers (drop, corrupt, duplicate, delay) are drawn in a fixed, invariant sequence prior to branch evaluation. Using `minilab replay <log>`, we verify that fault decisions match the original execution identically.

### Q7: What did your latency benchmark reveal about asynchronous event loop health?
**Answer:** Under our 1,000-command baseline benchmark, command round-trip latency was tight ($p50 = 0.07\text{ ms}$, $p99 = 0.20\text{ ms}$, $\text{Max} = 0.66\text{ ms}$). When a 20 ms synchronous blocking task (`time.sleep(0.02)`) was introduced into the loop, peak latency spiked to $20.51\text{ ms}$ because the cooperative scheduler was starved. By offloading the blocking operation to a worker thread via `asyncio.to_thread`, peak latency dropped back down to $6.87\text{ ms}$, preserving responsiveness.

### Q8: What are the architectural limitations of this project?
**Answer:** This project is an in-process simulation running inside a single Python `asyncio` event loop on a general-purpose operating system. It does not provide hard real-time guarantees, does not use RTOS microcontrollers, and transfers frames across in-memory queues rather than physical serial/CAN buses.

### Q9: How would you port this architecture to physical robotic hardware?
**Answer:**
1. Replace `Link` and `FaultyChannel` with physical transport drivers (CANOpen via `python-can`, serial RS-485 via `pyserial-asyncio`, or EtherCAT).
2. Replace device simulation tasks with embedded microcontroller firmware (C/C++ on FreeRTOS or Zephyr) running on STM32/ESP32 nodes.
3. Move the low-level motion control loop to a compiled C++ service utilizing Linux RT-PREEMPT kernel patches, CPU core pinning, and `SCHED_FIFO` real-time scheduling.
4. Integrate hardware watchdog timers (e.g. heartbeat lines) that trigger physical relay cutoffs if host communication ceases.

### Q10: What would you test next if you had another week?
**Answer:**
1. **Property-Based Protocol Fuzzing:** Use Hypothesis to test arbitrary permutations of corrupted, out-of-order, and truncated frames against the wire decoder.
2. **Long-Term Soak Testing:** Run 100,000 continuous commands under random fault injection to monitor memory consumption and verify zero task or lock leakage over hours.
3. **Multi-Drop Bus Contention:** Simulate multi-drop bus collisions (e.g. CAN bus arbitration and CSMA/CD backoff) on shared physical links.

---

## 5. Resume Project Summary & Bullets

### Project Summary
**Mini Lab Controller (`minilab`)** — *Simulated Robotic Workcell Orchestrator*
Designed and implemented an asynchronous orchestration engine coordinating two robotic positioning arms, a fluidic dispensing pump, and a thermal reaction block over a CRC32-framed protocol with fault injection, zone mutual exclusion, and emergency stop guarantees (**simulated hardware**).

### Tailored Resume Bullets
- Built an `asyncio`-based orchestrator coordinating simulated robotic arms, a fluid pump, and a thermal controller over a CRC32-framed, ACK/retry protocol with idempotent replay, shared-zone locking, and e-stop propagation (**simulated hardware**).
- Implemented seeded fault injection (packet drops, delays, duplicates, corruption, disconnections) and diagnosed/resolved an actuator lock-order deadlock and a command retry double-actuation bug with complete regression suites.
- Measured command latency over 1,000 commands ($p50 = 0.07\text{ ms}$, $p99 = 0.20\text{ ms}$) and cut peak response latency under a 20 ms blocking workload from $20.51\text{ ms}$ to $6.87\text{ ms}$ by offloading blocking computation via `asyncio.to_thread`.
- Fuzzed the orchestrator across 10 random seeds under heavy fault injection verifying zero leaked tasks or locks; engineered a 59-test `pytest` suite integrated with GitHub Actions CI.

