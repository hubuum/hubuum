#!/usr/bin/env python3
"""Partition the pinned rust-pr-bench discovery into independent build lanes."""

import sys

if sys.version_info < (3, 11):
    raise SystemExit(
        "Hubuum tooling requires Python 3.11 or newer; found "
        + sys.version.split()[0]
        + ". Install Python 3.11+ and ensure python3 on PATH selects it."
    )

import argparse
import importlib.util
import json
from pathlib import Path


def partition(benchmarks):
    lanes = {"standard": [], "postgres": []}
    for benchmark in benchmarks:
        features = set(benchmark.get("required_features", []))
        features.update(benchmark.get("base", {}).get("required_features", []))
        lane = "postgres" if "postgres-bench" in features else "standard"
        lanes[lane].append(benchmark)
    return {"include": [{"name": name, "benchmarks": specs}
                        for name, specs in lanes.items() if specs]}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--action-path", type=Path, required=True)
    parser.add_argument("--head", type=Path, required=True)
    parser.add_argument("--base", type=Path)
    args = parser.parse_args()

    # Reuse discovery and move matching from the exact action revision used to
    # build and measure these targets; do not maintain a second bench inventory.
    scripts = args.action_path.resolve() / "scripts"
    sys.path.insert(0, str(scripts))
    spec = importlib.util.spec_from_file_location("expand_matrix", scripts / "expand_matrix.py")
    discovery = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(discovery)
    benchmarks = discovery.discover_benchmarks(args.head.resolve(), ".", "all")
    if args.base:
        base = discovery.discover_benchmarks(args.base.resolve(), ".", "all")
        benchmarks = discovery.pair_moved_benchmarks(benchmarks, base)
    print(json.dumps(partition(benchmarks), separators=(",", ":")))


if __name__ == "__main__":
    main()
