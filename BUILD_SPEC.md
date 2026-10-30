# Mini Lab Controller (`minilab`): Build Specification for a Coding Agent

> **Audience:** an autonomous coding agent (Claude Code, Cursor, Codex, etc.).
> **Human owner:** a student building a resume project for an *AIT (Assembly, Integration & Testing) Software Intern* role at a robotics / medical-device startup. The real machine has 2 six-DOF arms, 25+ linear actuators, 10+ pumps, heaters/chillers, lasers, cameras and GPUs. Its software must be concurrent, low-latency, reliable and debuggable.
> **Constraint:** no hardware. Everything is simulated. The human must be able to **understand the whole codebase in a weekend**, so simplicity beats cleverness everywhere.

---

## 0. How to use this document

1. **Read the entire document before writing any code.**
2. Build in the phases of **§20**, in order. After every phase: run `make test`, fix failures, then `git commit` with a clear message.
3. This spec wins over your preferences. Where it is silent, pick the **simplest** option and record the choice in `docs/DESIGN.md` under "Decisions".
4. **Never fabricate numbers.** Every latency figure, test count or "bugs found" statement in the docs must come from output you actually produced by running the code. Paste real output.
5. Priority labels: **P0** = required for Definition of Done (§22), **P1** = should do, **P2** = stretch (only after everything else is green).
6. Write code the human can read: small functions, type hints, and a short docstring on every class and public function explaining **why** it exists, not just what it does.
7. If something in this spec turns out to be wrong or impossible, fix it sensibly, then note the deviation in `docs/DESIGN.md`. Do not silently diverge.

---

## 1. Project summary

**`minilab`** is a Python `asyncio` program that orchestrates a **simulated robotic lab machine** made of independent device simulators (two arms, a pump, a heater). The orchestrator talks to each device over a **CRC-framed, JSON message protocol** with acknowledgements, timeouts, retries and idempotent replay. The transport can **inject faults** (drops, delays, duplicates, corruption, disconnects) from a **seed**, so every failure is reproducible.

Workflows are **dependency graphs of steps** that run in parallel where possible, with **shared-zone locking** (two arms can't be in the same zone), **e-stop propagation**, and **graceful degradation** (one failed device doesn't crash the rest). Everything is logged as **structured JSON lines**, command **latency percentiles** are measured, and a **test suite** covers the happy path, fault handling, and deliberately planted concurrency bugs.

