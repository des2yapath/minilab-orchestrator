"""Transport layer with deterministic fault injection.

Implements bidirectional queues between host and devices with simulated drops,
delays, duplicates, corruption, and disconnects driven by seeded pseudo-randomness.
"""

from __future__ import annotations

import asyncio
import random
from dataclasses import dataclass, field

from minilab.errors import FrameError
from minilab.logs import EventLog
from minilab.messages import Message, decode, encode


def corrupt(raw: str) -> str:
    """Corrupt a single character in a frame to invalidate its CRC."""
    if len(raw) > 9:
        replacement = "X" if raw[9] != "X" else "Y"
        return raw[:9] + replacement + raw[10:]
    return raw + "X"


@dataclass
class FaultConfig:
    """Configuration for unidirectional transport fault injection."""

    drop_prob: float = 0.0
    dup_prob: float = 0.0
    corrupt_prob: float = 0.0
    delay_prob: float = 0.0
    delay_range: tuple[float, float] = (0.0, 0.0)  # real seconds
    drop_first_n: int = 0
    drop_indices: tuple[int, ...] = ()
    disconnect_after: int | None = None


@dataclass
class LinkFaults:
    """Pair of fault configurations for both directions of a communication link."""

    to_device: FaultConfig = field(default_factory=FaultConfig)
    to_host: FaultConfig = field(default_factory=FaultConfig)

    @classmethod
    def both(cls, cfg: FaultConfig) -> LinkFaults:
        """Create symmetrical faults for both link directions."""
        return cls(to_device=cfg, to_host=cfg)


FAULT_PRESETS: dict[str, LinkFaults] = {
    "none": LinkFaults(),
    "light": LinkFaults.both(
        FaultConfig(drop_prob=0.03, delay_prob=0.10, delay_range=(0.0, 0.02))
    ),
    "heavy": LinkFaults.both(
        FaultConfig(
            drop_prob=0.15,
            dup_prob=0.10,
            corrupt_prob=0.05,
            delay_prob=0.30,
            delay_range=(0.0, 0.05),
        )
    ),
}


class FaultyChannel:
    """Unidirectional in-memory queue with deterministic fault injection."""

    def __init__(
        self,
        name: str,
        cfg: FaultConfig,
        rng: random.Random,
        link: Link,
        log: EventLog,
    ) -> None:
        self.name = name
        self.cfg = cfg
        self._rng = rng
        self.link = link
        self._log = log
        self._q: asyncio.Queue[str] = asyncio.Queue()
        self._count = 0
        self._tasks: set[asyncio.Task[None]] = set()

    @property
    def _comp(self) -> str:
        dev = self.name.split(":")[0]
        return f"link:{dev}"

    async def send(self, raw: str) -> None:
        """Inject faults and queue raw frame for delivery."""
        self._count += 1
        n = self._count

        # 1. Fixed-order random draws to guarantee reproducibility
        r_drop = self._rng.random()
        r_corrupt = self._rng.random()
        r_dup = self._rng.random()
        r_delay = self._rng.random()
        delay = self._rng.uniform(*self.cfg.delay_range)
        dup_gap = self._rng.uniform(0.0, 0.01)

        # 2. Link-level disconnection checks
        if not self.link.connected:
            self._log.emit(
                self._comp, "fault_disconnect", channel=self.name, n=n, reason="already_down"
            )
            return

        if self.cfg.disconnect_after is not None and n > self.cfg.disconnect_after:
            self.link.connected = False
            self._log.emit(
                self._comp, "fault_disconnect", channel=self.name, n=n, reason="threshold_reached"
            )
            return

        # 3. Message-level drop check
        if n <= self.cfg.drop_first_n or n in self.cfg.drop_indices or r_drop < self.cfg.drop_prob:
            self._log.emit(self._comp, "fault_drop", channel=self.name, n=n)
            return

        # 4. Message-level corruption
        if r_corrupt < self.cfg.corrupt_prob:
            raw = corrupt(raw)
            self._log.emit(self._comp, "fault_corrupt", channel=self.name, n=n)

        # 5. Delay and delivery
        d = delay if (r_delay < self.cfg.delay_prob and delay > 0.0) else 0.0
        self._deliver(raw, d, n, is_dup=False)

        # 6. Duplication
        if r_dup < self.cfg.dup_prob:
            self._deliver(raw, d + dup_gap, n, is_dup=True)

    def _deliver(self, raw: str, delay: float, n: int, is_dup: bool = False) -> None:
        if is_dup:
            self._log.emit(self._comp, "fault_dup", channel=self.name, n=n)
        elif delay > 0:
            self._log.emit(self._comp, "fault_delay", channel=self.name, n=n, delay=round(delay, 6))

        if delay <= 0:
            self._q.put_nowait(raw)
            return

        async def later() -> None:
            await asyncio.sleep(delay)
            self._q.put_nowait(raw)

        t = asyncio.create_task(later())
        self._tasks.add(t)
        t.add_done_callback(self._tasks.discard)

    async def recv(self) -> str:
        """Receive the next raw frame."""
        return await self._q.get()

    async def close(self) -> None:
        """Cancel and await all pending delivery tasks."""
        for t in list(self._tasks):
            t.cancel()
        if self._tasks:
            await asyncio.gather(*self._tasks, return_exceptions=True)
        self._tasks.clear()


class Endpoint:
    """Network endpoint performing message framing, serialization, and CRC validation."""

    def __init__(
        self,
        tx: FaultyChannel,
        rx: FaultyChannel,
        log: EventLog,
        channel_name: str,
    ) -> None:
        self._tx = tx
        self._rx = rx
        self._log = log
        self._channel_name = channel_name

    async def send(self, msg: Message) -> None:
        """Serialize and send a message through the transmit channel."""
        await self._tx.send(encode(msg))

    async def recv(self) -> Message:
        """Read and decode frames, dropping and logging any FrameErrors."""
        dev = self._channel_name.split(":")[0]
        while True:
            raw = await self._rx.recv()
            try:
                return decode(raw)
            except FrameError as e:
                self._log.emit(
                    f"link:{dev}", "frame_error", channel=self._channel_name, error=str(e)
                )
                continue


class Link:
    """Bidirectional communication link connecting host and device endpoints."""

    def __init__(self, name: str, faults: LinkFaults, seed: int, log: EventLog) -> None:
        self.name = name
        self.connected = True
        a = FaultyChannel(
            f"{name}:to_device",
            faults.to_device,
            random.Random(f"{seed}:{name}:to_device"),
            self,
            log,
        )
        b = FaultyChannel(
            f"{name}:to_host",
            faults.to_host,
            random.Random(f"{seed}:{name}:to_host"),
            self,
            log,
        )
        self.host = Endpoint(tx=a, rx=b, log=log, channel_name=f"{name}:to_host")
        self.device = Endpoint(tx=b, rx=a, log=log, channel_name=f"{name}:to_device")
        self._a = a
        self._b = b

    def disconnect(self) -> None:
        """Simulate hardware cable disconnect."""
        self.connected = False

    def reconnect(self) -> None:
        """Simulate hardware cable reconnection."""
        self.connected = True

    async def close(self) -> None:
        """Close both directional channels."""
        await asyncio.gather(self._a.close(), self._b.close(), return_exceptions=True)
