"""Tests for transport fault injection, determinism, and endpoint behavior."""

from __future__ import annotations

import asyncio
import random

from minilab.logs import EventLog
from minilab.messages import Message
from minilab.transport import FaultConfig, FaultyChannel, Link, LinkFaults


async def test_same_seed_same_fault_pattern() -> None:
    """Verify that identical seeds produce identical drop decisions."""
    cfg = FaultConfig(drop_prob=0.3)
    log1 = EventLog()
    log2 = EventLog()
    log3 = EventLog()

    link1 = Link("test", LinkFaults.both(cfg), seed=123, log=log1)
    link2 = Link("test", LinkFaults.both(cfg), seed=123, log=log2)
    link3 = Link("test", LinkFaults.both(cfg), seed=999, log=log3)

    c1 = FaultyChannel("c1", cfg, random.Random("123:test:c1"), link1, log1)
    c2 = FaultyChannel("c2", cfg, random.Random("123:test:c1"), link2, log2)
    c3 = FaultyChannel("c3", cfg, random.Random("999:test:c1"), link3, log3)

    for i in range(200):
        await c1.send(f"msg_{i}")
        await c2.send(f"msg_{i}")
        await c3.send(f"msg_{i}")

    drops1 = [e["n"] for e in log1.find(event="fault_drop")]
    drops2 = [e["n"] for e in log2.find(event="fault_drop")]
    drops3 = [e["n"] for e in log3.find(event="fault_drop")]

    assert drops1 == drops2
    assert drops1 != drops3
    await link1.close()
    await link2.close()
    await link3.close()


async def test_probability_changes_do_not_shift_stream() -> None:
    """Verify that changing delay_prob does not shift drop decisions under fixed seed."""
    log1 = EventLog()
    log2 = EventLog()
    link1 = Link("test", LinkFaults(), seed=42, log=log1)
    link2 = Link("test", LinkFaults(), seed=42, log=log2)

    cfg1 = FaultConfig(drop_prob=0.25, delay_prob=0.0)
    cfg2 = FaultConfig(drop_prob=0.25, delay_prob=0.5, delay_range=(0.001, 0.005))

    c1 = FaultyChannel("c", cfg1, random.Random("42:test:c"), link1, log1)
    c2 = FaultyChannel("c", cfg2, random.Random("42:test:c"), link2, log2)

    for i in range(100):
        await c1.send(f"msg_{i}")
        await c2.send(f"msg_{i}")

    drops1 = [e["n"] for e in log1.find(event="fault_drop")]
    drops2 = [e["n"] for e in log2.find(event="fault_drop")]
    assert drops1 == drops2

    await c1.close()
    await c2.close()
    await link1.close()
    await link2.close()


async def test_drop_first_n_and_drop_indices() -> None:
    """Verify deterministic dropping by message index and prefix count."""
    log = EventLog()
    link = Link("test", LinkFaults(), seed=1, log=log)
    cfg = FaultConfig(drop_first_n=2, drop_indices=(4, 5))
    c = FaultyChannel("c", cfg, random.Random("1:test:c"), link, log)

    for i in range(6):
        await c.send(f"m_{i + 1}")

    drops = [e["n"] for e in log.find(event="fault_drop")]
    assert drops == [1, 2, 4, 5]

    delivered = []
    while not c._q.empty():
        delivered.append(await c.recv())
    assert delivered == ["m_3", "m_6"]

    await c.close()
    await link.close()


async def test_duplicate_delivers_twice() -> None:
    """Verify that dup_prob=1.0 queues two copies of each message."""
    log = EventLog()
    link = Link("test", LinkFaults(), seed=1, log=log)
    cfg = FaultConfig(dup_prob=1.0)
    c = FaultyChannel("c", cfg, random.Random("1:test:c"), link, log)

    await c.send("payload")
    # Small sleep to allow delayed duplicate task to deliver
    await asyncio.sleep(0.02)

    msg1 = await c.recv()
    msg2 = await c.recv()
    assert msg1 == "payload"
    assert msg2 == "payload"

    await c.close()
    await link.close()


async def test_disconnect_after_kills_both_directions() -> None:
    """Verify disconnect_after disables further message transmission."""
    log = EventLog()
    cfg = FaultConfig(disconnect_after=2)
    link = Link("arm1", LinkFaults(to_device=cfg), seed=1, log=log)

    await link.host.send(Message(type="cmd", msg_id=1, device="arm1"))
    await link.host.send(Message(type="cmd", msg_id=2, device="arm1"))
    assert link.connected is True

    # 3rd message triggers disconnection
    await link.host.send(Message(type="cmd", msg_id=3, device="arm1"))
    assert link.connected is False

    # Disconnect also stops the reverse direction
    await link.device.send(Message(type="ack", msg_id=1, device="arm1"))
    disconn_events = log.find(event="fault_disconnect")
    assert len(disconn_events) >= 2

    await link.close()


async def test_corrupt_is_caught_by_endpoint() -> None:
    """Verify corrupted frame is dropped, logged, and reader receives subsequent valid frame."""
    log = EventLog()
    # First message corrupted, second clean
    cfg = FaultConfig(corrupt_prob=1.0)
    link = Link("test", LinkFaults(to_device=cfg), seed=1, log=log)

    msg1 = Message(type="cmd", msg_id=1, device="test", action="ping")
    msg2 = Message(type="cmd", msg_id=2, device="test", action="status")

    # Send corrupted msg1
    await link.host.send(msg1)
    # Turn off corruption for msg2
    link._a.cfg.corrupt_prob = 0.0
    await link.host.send(msg2)

    # Receiver should skip corrupted msg1 and return msg2
    received = await link.device.recv()
    assert received.msg_id == 2
    assert received.action == "status"

    errs = log.find(event="frame_error")
    assert len(errs) == 1

    await link.close()
