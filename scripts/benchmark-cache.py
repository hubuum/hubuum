#!/usr/bin/env python3
"""Inventory and bounded cleanup of Hubuum-owned benchmark caches."""

import sys

if sys.version_info < (3, 11):
    sys.exit(
        "Hubuum tooling requires Python 3.11 or newer; found "
        + sys.version.split()[0]
        + ". Install Python 3.11+ and ensure python3 on PATH selects it."
    )

import argparse
from collections import defaultdict
from datetime import datetime, timedelta, timezone
import hashlib
import json
import os
from pathlib import Path
import re

from benchmark_github import api, pages


NAMESPACES = ("hubuum-benchmarks-standard", "hubuum-benchmarks-postgres")


def namespace_prefix(namespace):
    digest = hashlib.sha256(json.dumps(namespace).encode()).hexdigest()[:24]
    return f"rust-pr-bench-v1-{digest}-"


def candidates(caches, closed_prs, *, now=None, max_per_pr=8, keep_main=2):
    now = now or datetime.now(timezone.utc)
    cutoff = now - timedelta(days=7)
    grouped = defaultdict(list)
    for cache in caches:
        prefix = next((value for name in NAMESPACES
                       if cache["key"].startswith(value := namespace_prefix(name))), None)
        if prefix is None:
            continue
        # Only manage the shared action's documented cache kinds in our namespaces.
        kind = cache["key"][len(prefix):].split("-", 1)[0]
        if kind not in {"build", "executables", "downloads", "runner"}:
            continue
        grouped[(cache["ref"], prefix, kind)].append(cache)
    result = []
    for (ref, _, _), entries in grouped.items():
        entries.sort(key=lambda item: item["last_accessed_at"], reverse=True)
        pr = re.fullmatch(r"refs/pull/(\d+)/merge", ref)
        for index, cache in enumerate(entries):
            old = datetime.fromisoformat(cache["last_accessed_at"].replace("Z", "+00:00")) < cutoff
            if pr and (int(pr[1]) in closed_prs or index >= max_per_pr or old):
                result.append(cache)
            elif ref == "refs/heads/main" and index >= keep_main and old:
                result.append(cache)
    return sorted(result, key=lambda item: item["last_accessed_at"])


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--apply", action="store_true", help="default is inventory only")
    parser.add_argument("--max-delete", type=int, default=20)
    parser.add_argument("--output", type=Path, default=Path("benchmark-cache-inventory.json"))
    args = parser.parse_args()
    if not 0 <= args.max_delete <= 100:
        parser.error("--max-delete must be between 0 and 100")
    base = f"repos/{os.environ['GITHUB_REPOSITORY']}"
    caches = pages(f"{base}/actions/caches", "actions_caches")
    relevant = [cache for cache in caches if any(cache["key"].startswith(namespace_prefix(name)) for name in NAMESPACES)]
    prs = {int(match[1]) for cache in relevant
           if (match := re.fullmatch(r"refs/pull/(\d+)/merge", cache["ref"]))}
    closed = {number for number in prs if api(f"{base}/pulls/{number}")["state"] == "closed"}
    selected = candidates(relevant, closed)[:args.max_delete]
    report = {"version": 1, "apply": args.apply, "repository_cache_bytes": sum(cache["size_in_bytes"] for cache in caches),
              "benchmark_caches": relevant, "deletion_candidates": selected, "deleted": []}
    # Preserve the concrete inventory even if an API mutation later fails.
    args.output.write_text(json.dumps(report, indent=2) + "\n")
    for cache in selected if args.apply else []:
        api(f"{base}/actions/caches/{int(cache['id'])}", method="DELETE")
        report["deleted"].append(cache["id"])
        args.output.write_text(json.dumps(report, indent=2) + "\n")
    print(f"Benchmark caches: {len(relevant)}; candidates: {len(selected)}; deleted: {len(report['deleted'])}")


if __name__ == "__main__":
    main()
