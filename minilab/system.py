"""System assembly, configuration, and life-cycle management.

Wires together communication links, device simulators, host clients, zone locks,
and central orchestrator from a unified declarative SystemConfig.
"""

from __future__ import annotations

import itertools
from dataclasses import asdict, dataclass, field
from typing import Any, Self

from minilab.devices import Device, make_device
from minilab.host.client import DeviceClient, EStopState
from minilab.host.orchestrator import Orchestrator
from minilab.host.workflow import Workflow, WorkflowResult
from minilab.host.zones import ZoneManager
from minilab.logs import EventLog
from minilab.metrics import Metrics
from minilab.transport import FaultConfig, Link, LinkFaults


@dataclass
class SystemConfig:
    """Declarative specification for configuring the simulated lab workcell."""

    seed: int = 0
    time_scale: float = 1.0
    ack_timeout: float = 0.25
    done_timeout: float = 5.0
    max_retries: int = 3
    devices: tuple[str, ...] = ("arm1", "arm2", "pump1", "heater1")
    zones: tuple[str, ...] = ("A", "B", "C")
    faults: dict[str, LinkFaults] = field(default_factory=dict)
    device_opts: dict[str, dict[str, Any]] = field(default_factory=dict)
    zone_ordered: bool = True
    zone_step_delay: float = 0.0
    zone_timeout: float | None = None
    log_path: str | None = None

    def to_dict(self) -> dict[str, Any]:
        """Convert configuration to a JSON-serializable dictionary."""
        return asdict(self)

    @classmethod
    def from_dict(cls, d: dict[str, Any]) -> SystemConfig:
        """Reconstruct SystemConfig from dictionary, restoring nested fault configs."""
        raw_faults = d.get("faults", {})
        rebuilt_faults: dict[str, LinkFaults] = {}
        for dev_name, lf in raw_faults.items():
            if isinstance(lf, LinkFaults):
                rebuilt_faults[dev_name] = lf
            elif isinstance(lf, dict):
                to_dev = FaultConfig(**lf.get("to_device", {}))
                to_host = FaultConfig(**lf.get("to_host", {}))
                rebuilt_faults[dev_name] = LinkFaults(to_device=to_dev, to_host=to_host)

        return cls(
            seed=d.get("seed", 0),
            time_scale=d.get("time_scale", 1.0),
            ack_timeout=d.get("ack_timeout", 0.25),
            done_timeout=d.get("done_timeout", 5.0),
            max_retries=d.get("max_retries", 3),
            devices=tuple(d.get("devices", ("arm1", "arm2", "pump1", "heater1"))),
            zones=tuple(d.get("zones", ("A", "B", "C"))),
            faults=rebuilt_faults,
            device_opts=dict(d.get("device_opts", {})),
            zone_ordered=d.get("zone_ordered", True),
            zone_step_delay=d.get("zone_step_delay", 0.0),
            zone_timeout=d.get("zone_timeout"),
            log_path=d.get("log_path"),
        )


class System:
    """Running instance of the lab system."""

    def __init__(self, config: SystemConfig, scenario: str = "custom") -> None:
        self.config = config
        self.scenario = scenario
        self.log = EventLog(config.log_path)
        self.metrics = Metrics()
        self.estop_state = EStopState()
        self._id_gen = itertools.count(1)

        # Build links
        self.links: dict[str, Link] = {}
        for dev_name in config.devices:
            fault = config.faults.get(dev_name, LinkFaults())
            self.links[dev_name] = Link(dev_name, fault, config.seed, self.log)

        # Build simulated devices
        self.devices: dict[str, Device] = {}
        for dev_name in config.devices:
            opts = dict(config.device_opts.get(dev_name, {}))
            self.devices[dev_name] = make_device(
                dev_name,
                self.links[dev_name].device,
                seed=config.seed,
                time_scale=config.time_scale,
                log=self.log,
                **opts,
            )

        # Build host clients
        self.clients: dict[str, DeviceClient] = {}
        for dev_name in config.devices:
            self.clients[dev_name] = DeviceClient(
                dev_name,
                self.links[dev_name].host,
                ids=self._id_gen,
                estop_state=self.estop_state,
                log=self.log,
                metrics=self.metrics,
                ack_timeout=config.ack_timeout,
                done_timeout=config.done_timeout,
                max_retries=config.max_retries,
            )

        self.zones = ZoneManager(
            names=config.zones,
            ordered=config.zone_ordered,
            step_delay=config.zone_step_delay,
            log=self.log,
        )
        self.orchestrator = Orchestrator(
            clients=self.clients,
            zones=self.zones,
            estop_state=self.estop_state,
            log=self.log,
            metrics=self.metrics,
            zone_timeout=config.zone_timeout,
        )

    async def run(self, wf: Workflow) -> WorkflowResult:
        """Execute a workflow DAG through the orchestrator."""
        return await self.orchestrator.run(wf)

    async def __aenter__(self) -> Self:
        # Emit run_config as the first event in the log
        self.log.emit(
            "run",
            "run_config",
            scenario=self.scenario,
            config=self.config.to_dict(),
        )

        # Start devices and clients
        for dev in self.devices.values():
            await dev.start()
        for client in self.clients.values():
            await client.start()

        return self

    async def __aexit__(self, *exc: object) -> None:
        # Teardown in strict order, each step fault-tolerant
        try:
            await self.orchestrator.shutdown()
        except Exception as e:  # noqa: BLE001
            self.log.emit("system", "teardown_error", component="orchestrator", error=str(e))

        for client in self.clients.values():
            try:
                await client.close()
            except Exception as e:  # noqa: BLE001
                self.log.emit(
                    "system", "teardown_error", component=f"client:{client.name}", error=str(e)
                )

        for dev in self.devices.values():
            try:
                await dev.stop()
            except Exception as e:  # noqa: BLE001
                self.log.emit(
                    "system", "teardown_error", component=f"device:{dev.name}", error=str(e)
                )

        for link in self.links.values():
            try:
                await link.close()
            except Exception as e:  # noqa: BLE001
                self.log.emit(
                    "system", "teardown_error", component=f"link:{link.name}", error=str(e)
                )

        self.log.close()


def build_system(scenario: str = "custom", **overrides: Any) -> System:
    """Factory helper to construct a configured System instance."""
    cfg = SystemConfig(**overrides)
    return System(cfg, scenario=scenario)
