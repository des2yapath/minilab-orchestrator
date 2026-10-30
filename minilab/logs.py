"""Structured JSON-lines event logging.

Captures timestamped event dictionaries for offline analysis, deterministic replay,
and execution timeline verification. Avoids stdlib logging for zero-overhead,
exact-schema, and thread/asyncio safety.
"""

from __future__ import annotations

import json
import time
from pathlib import Path
from typing import Any, Self


class EventLog:
    """In-memory and file-backed structured event log."""

    def __init__(self, path: str | None = None) -> None:
        """Initialize the event log and optional output file."""
        self.t0 = time.perf_counter()
        self.events: list[dict[str, Any]] = []
        self._path = path
        self._fh = None

        if path:
            file_path = Path(path)
            file_path.parent.mkdir(parents=True, exist_ok=True)
            self._fh = open(file_path, "w", buffering=1, encoding="utf-8")  # noqa: SIM115

    def emit(self, component: str, event: str, **fields: Any) -> None:
        """Emit a structured event with monotonic timestamp and context fields."""
        rec: dict[str, Any] = {
            "t": round(time.perf_counter() - self.t0, 6),
            "component": component,
            "event": event,
            **fields,
        }
        self.events.append(rec)
        if self._fh:
            self._fh.write(json.dumps(rec, default=str) + "\n")

    def find(
        self, event: str | None = None, component: str | None = None
    ) -> list[dict[str, Any]]:
        """Filter logged events by event name and/or component prefix."""
        matches: list[dict[str, Any]] = []
        for e in self.events:
            if event is not None and e.get("event") != event:
                continue
            if component is not None:
                comp = e.get("component", "")
                if component.endswith("*"):
                    prefix = component[:-1]
                    if not comp.startswith(prefix):
                        continue
                elif comp != component:
                    continue
            matches.append(e)
        return matches

    def close(self) -> None:
        """Close the file handle if open."""
        if self._fh:
            self._fh.close()
            self._fh = None

    def __enter__(self) -> Self:
        return self

    def __exit__(self, *args: object) -> None:
        self.close()
