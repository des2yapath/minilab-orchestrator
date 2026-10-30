"""Plot latency histograms from benchmark metrics dumps.

Generates overlaid latency distribution histograms comparing baseline, blocked,
and thread-offloaded event loop runs.
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path


def main() -> None:
    """Entry point for latency plotting script."""
    parser = argparse.ArgumentParser(description="Plot latency histograms from benchmark JSON")
    parser.add_argument("files", nargs="+", help="Paths to benchmark JSON dump files")
    parser.add_argument("--out", default="docs/img/latency.png", help="Output image file path")
    args = parser.parse_args()

    try:
        import matplotlib.pyplot as plt
        import numpy as np
    except ImportError:
        print(
            "Notice: matplotlib and numpy are required for plotting. Install with 'pip install -e \".[plot]\"'",
            file=sys.stderr,
        )
        sys.exit(0)

    _fig, ax = plt.subplots(figsize=(10, 6))

    all_vals = []
    datasets = []

    for path_str in args.files:
        p = Path(path_str)
        if not p.exists():
            print(f"Warning: File not found: {path_str}", file=sys.stderr)
            continue

        with open(p, encoding="utf-8") as f:
            data = json.load(f)

        ack_samples = []
        for dev_samples in data.get("samples", {}).get("ack_rtt", {}).values():
            ack_samples.extend([v * 1000.0 for v in dev_samples])  # convert s to ms

        if ack_samples:
            datasets.append((p.stem, ack_samples))
            all_vals.extend(ack_samples)

    if not datasets or not all_vals:
        print("No sample data found in input files.", file=sys.stderr)
        sys.exit(1)

    min_val = max(min(all_vals), 0.01)
    max_val = max(all_vals)
    bins = np.logspace(np.log10(min_val), np.log10(max_val * 1.1), 50)

    for label, samples in datasets:
        ax.hist(samples, bins=bins, alpha=0.5, label=label, edgecolor="black", linewidth=0.5)

    ax.set_xscale("log")
    ax.set_xlabel("ACK Round-Trip Time (ms) [Log Scale]", fontsize=12)
    ax.set_ylabel("Frequency (Count)", fontsize=12)
    ax.set_title("Command Latency Distribution: Loop Blocker Impact & Remediation", fontsize=14)
    ax.grid(True, which="both", ls="--", alpha=0.5)
    ax.legend(fontsize=11)

    out_path = Path(args.out)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    plt.tight_layout()
    plt.savefig(out_path, dpi=200)
    plt.close()
    print(f"Saved latency histogram plot to {out_path}")


if __name__ == "__main__":
    main()
