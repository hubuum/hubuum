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
from fnmatch import fnmatchcase
import importlib.util
import json
from pathlib import Path
import re
import shlex


def partition(benchmarks):
    lanes = {"micro": [], "postgres": []}
    for benchmark in benchmarks:
        features = set(benchmark.get("required_features", []))
        features.update(benchmark.get("base", {}).get("required_features", []))
        lane = "postgres" if "postgres-bench" in features else "micro"
        lanes[lane].append(benchmark)
    return {"include": [{"name": name, "benchmarks": specs}
                        for name, specs in lanes.items() if specs]}


def configure_profiles(benchmark, policy, base_root=None):
    if benchmark["backend"] != "criterion":
        return
    budgets = {}
    base = benchmark.get("base", benchmark)
    if base_root:
        source = base_root / base.get("repo_crate", ".") / "benches" / f"{base['bench']}.rs"
        if source.is_file():
            # Transitional base revisions can override CLI budgets inside groups.
            # Match their literal overrides on both sides until those bases age out.
            for block in re.findall(r"\bgroup\s*((?:\.(?:sample_size|warm_up_time|measurement_time)\([^;]+));", source.read_text()):
                for method, flag in (("sample_size", "--sample-size"), ("warm_up_time", "--warm-up-time"),
                                     ("measurement_time", "--measurement-time")):
                    match = re.search(r"\." + method + r"\((?:Duration::from_secs\()?(\d+)", block)
                    if match:
                        if flag in budgets and budgets[flag] != match[1]:
                            raise ValueError(f"Incompatible legacy per-group budgets in {source}")
                        budgets[flag] = match[1]
    benchmark["profiles"] = {}
    for name, profile in policy["profiles"].items():
        args = shlex.split(profile["criterion_args"])
        for flag, value in budgets.items():
            if flag in args:
                args[args.index(flag) + 1] = value
            else:
                args.extend((flag, value))
        if name == "quick" and benchmark["bench"] in policy.get("quick_filters", {}):
            args.append(policy["quick_filters"][benchmark["bench"]])
        benchmark["profiles"][name] = {"criterion_args": shlex.join(args)}
    if budgets:
        benchmark["profile_notes"] = ["Legacy base group budgets are applied equally to both revisions: "
                                      + shlex.join([part for pair in budgets.items() for part in pair])]


def select_lanes(benchmarks, selection, policy, base_root=None):
    """Keep full per-scope catalogs for warm-cache lookup; measure selected IDs."""
    available = set()
    selected_ids = set()
    result = []
    whole_scopes = set(selection.get("whole_scopes", policy["scopes"]))
    def matches_input(path, benchmark):
        for spec in (benchmark, benchmark.get("base", {})):
            if not spec.get("bench"):
                continue
            root = Path(spec.get("repo_crate", ".")) / "benches" / spec["bench"]
            if path == str(root.with_suffix(".rs")) or path.startswith(str(root) + "/"):
                return True
        return False
    if any(not any(matches_input(path, benchmark) for benchmark in benchmarks)
           for path in selection.get("benchmark_paths", [])):
        # Unknown shared or removed benchmark inputs must not select nothing.
        whole_scopes.update(("micro", "postgres"))
    for lane in partition(benchmarks)["include"]:
        selected = []
        for benchmark in lane["benchmarks"]:
            configure_profiles(benchmark, policy, base_root)
            tags = set(benchmark.get("tags", []))
            for rule in policy["target_tags"]:
                if fnmatchcase(benchmark["name"], rule["pattern"]):
                    tags.update(rule["tags"])
            benchmark["tags"] = sorted(tags)
            identity = (f"{benchmark['backend']}:{benchmark.get('manifest_path', 'Cargo.toml')}:"
                        f"{benchmark['bench']}")
            available.add(identity)
            changed = any(matches_input(path, benchmark) for path in selection.get("benchmark_paths", []))
            if (lane["name"] in whole_scopes or tags.intersection(selection.get("tags", []))
                    or identity in selection.get("targets", []) or changed):
                selected.append(identity)
        if selected:
            selected_ids.update(selected)
            result.append({
                **lane, "selected_targets": selected,
                "cache_namespace": "hubuum-benchmarks-" + ("standard" if lane["name"] == "micro" else "postgres"),
            })
    unknown = set(selection.get("targets", [])) - available
    if unknown:
        raise ValueError(f"Unknown benchmark targets: {', '.join(sorted(unknown))}")
    if selection.get("explicit") and selection.get("tags") and not selected_ids:
        if set(selection.get("scopes", [])) & {"micro", "postgres"}:
            raise ValueError("Requested benchmark tags match no discovered targets")
    return {"include": result}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--action-path", type=Path, required=True)
    parser.add_argument("--head", type=Path, required=True)
    parser.add_argument("--base", type=Path)
    parser.add_argument("--selection-json", default="")
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
    policy = json.loads((args.head / ".github/benchmark-policy.json").read_text())
    selection = json.loads(args.selection_json) if args.selection_json else {"whole_scopes": policy["scopes"]}
    matrix = select_lanes(benchmarks, selection, policy, args.base)
    print(json.dumps(matrix, separators=(",", ":")))


if __name__ == "__main__":
    main()
