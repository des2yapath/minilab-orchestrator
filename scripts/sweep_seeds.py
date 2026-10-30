"""Sweep random seeds under heavy fault injection to analyze degradation rates."""

from __future__ import annotations

import argparse
import asyncio
from collections import Counter

from minilab.scenarios import basic_workflow
from minilab.system import build_system
from minilab.transport import FAULT_PRESETS


async def run_sweep(count: int) -> None:
    """Run sequential seed evaluations under heavy fault injection."""
    print(f"Sweeping {count} seeds under heavy fault injection...")
    faults = {d: FAULT_PRESETS["heavy"] for d in ("arm1", "arm2", "pump1", "heater1")}
    outcomes: Counter[str] = Counter()

    for seed in range(count):
        params = {
            "seed": seed,
            "faults": faults,
            "time_scale": 0.02,
            "ack_timeout": 0.05,
            "done_timeout": 2.0,
            "max_retries": 3,
        }
        async with build_system(**params) as sys:
            try:
                res = await asyncio.wait_for(sys.run(basic_workflow()), timeout=5.0)
                outcomes[res.status] += 1
            except Exception as e:  # noqa: BLE001
                outcomes[f"EXCEPTION:{type(e).__name__}"] += 1

    print("\n--- Seed Sweep Results ---")
    for status, cnt in sorted(outcomes.items()):
        pct = (cnt / count) * 100.0
        print(f"  {status:<15}: {cnt:>4}/{count} ({pct:.1f}%)")


def main() -> None:
    """CLI entry point for seed sweep."""
    parser = argparse.ArgumentParser(description="Seed sweep testing")
    parser.add_argument("--n", type=int, default=50, help="Number of seeds to evaluate")
    args = parser.parse_args()
    asyncio.run(run_sweep(args.n))


if __name__ == "__main__":
    main()

