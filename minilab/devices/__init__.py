"""Device factory and registrations."""

from __future__ import annotations

import re
from typing import Any

from minilab.devices.arm import Arm
from minilab.devices.base import Device
from minilab.devices.heater import Heater
from minilab.devices.pump import Pump
from minilab.transport import Endpoint

DEVICE_CLASSES: dict[str, type[Device]] = {
    "arm": Arm,
    "pump": Pump,
    "heater": Heater,
}


def make_device(name: str, endpoint: Endpoint, **kw: Any) -> Device:
    """Instantiate a concrete device simulator based on name prefix."""
    match = re.match(r"^([a-zA-Z]+)", name)
    if not match:
        raise ValueError(f"Cannot determine device type from name: {name!r}")
    prefix = match.group(1).lower()
    cls = DEVICE_CLASSES.get(prefix)
    if cls is None:
        raise ValueError(f"No device class registered for prefix {prefix!r}")
    return cls(name, endpoint, **kw)


__all__ = ["DEVICE_CLASSES", "Arm", "Device", "Heater", "Pump", "make_device"]
