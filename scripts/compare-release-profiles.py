#!/usr/bin/env python3
"""Measure opt-level=z versus 3 with identical features and deterministic corpora.

Requires Python 3.11+, GNU time, strip, Cargo, and optionally Docker/Podman.
Output contains measurements and build logs, never environment dumps.
"""

from __future__ import annotations

import sys

if sys.version_info < (3, 11):
    sys.exit(
        "Hubuum tooling requires Python 3.11 or newer; found "
        + sys.version.split()[0]
        + ". Install Python 3.11+ and ensure python3 on PATH selects it."
    )

import argparse
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import platform
import shutil
import subprocess

ROOT = Path(__file__).resolve().parent.parent
TARGETS = [f"release_{name}_criterion" for name in ("serialization", "schema", "search", "template")]
FEATURES = "tls-rustls,tls-openssl,embedded-migrations"
BINARIES = ("hubuum-server", "hubuum-admin", "hubuum-template-worker")
WORKLOADS = {"release_serialization/encode", "release_serialization/decode",
             "release_schema/validate", "release_search/unified_parse",
             "release_search/filter_parse", "release_template/render"}


def command_output(command):
    return subprocess.check_output(command, cwd=ROOT, text=True).strip()


def timed(command, log, env=None):
    """GNU time reports maximum individual-child RSS, not simultaneous tree RSS."""
    metrics = log.with_suffix(".time")
    with log.open("w") as output:
        subprocess.run(
            ["/usr/bin/time", "-f", "%e %U %S %M", "-o", str(metrics), *command],
            cwd=ROOT, env=env, stdout=output, stderr=subprocess.STDOUT, check=True,
        )
    wall, user, system, rss = map(float, metrics.read_text().split())
    return {"wall_seconds": wall, "user_seconds": user, "system_seconds": system,
            "max_child_rss_bytes": int(rss * 1024)}


def estimates(directory):
    result = {}
    for path in sorted(directory.glob("release_*/**/new/estimates.json")):
        benchmark = json.loads(path.with_name("benchmark.json").read_text())
        # Criterion sanitizes slash-containing single-function names on disk.
        # Its metadata retains the original ID for both groups and functions.
        name = benchmark["full_id"]
        if name in result:
            raise ValueError(f"duplicate workload measurement: {name}")
        values = json.loads(path.read_text())
        result[name] = {
            "mean_ns": values["mean"]["point_estimate"],
            "median_ns": values["median"]["point_estimate"],
            "mean_confidence_interval_ns": values["mean"]["confidence_interval"],
        }
        throughput = benchmark.get("throughput")
        if throughput:
            unit, count = next(iter(throughput.items()))
            result[name]["throughput"] = {
                "unit": unit, "per_second": count * 1e9 / result[name]["mean_ns"]}
    if result.keys() != WORKLOADS:
        raise ValueError(f"expected six specific workload measurements, found {sorted(result)}")
    return result


def comparison(profiles):
    baseline, speed = profiles["z"]["workloads"], profiles["3"]["workloads"]
    if baseline.keys() != speed.keys():
        raise ValueError("workload sets differ")
    return {name: {"latency_change_percent":
                   (speed[name]["mean_ns"] / value["mean_ns"] - 1) * 100}
            for name, value in baseline.items()}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--jobs", type=int, default=4)
    parser.add_argument("--container-engine", default="docker")
    parser.add_argument("--skip-containers", action="store_true",
                        help="local iteration only; final comparison requires image sizes")
    args = parser.parse_args()
    if args.jobs < 1:
        parser.error("--jobs must be positive")
    output = args.output.resolve()
    output.mkdir(parents=True, exist_ok=True)
    report = {"schema_version": 1, "started_at": datetime.now(timezone.utc).isoformat(),
              "compiler": command_output(["rustc", "-Vv"]),
              "cargo": command_output(["cargo", "-V"]), "platform": platform.platform(),
              "cpu": command_output(["lscpu", "--json"]), "jobs": args.jobs,
              "features": FEATURES, "commit": command_output(["git", "rev-parse", "HEAD"]),
              "dirty": bool(command_output(["git", "status", "--porcelain"])),
              "corpus_sha256": {name: hashlib.sha256((ROOT / "benches" / f"{name}.rs").read_bytes()).hexdigest()
                                for name in TARGETS}, "profiles": {}}
    for level in ("z", "3"):
        destination = output / level
        destination.mkdir(exist_ok=True)
        target = destination / "target"
        # Explicitly disclose reused build caches; do not label a warm build cold.
        cached = target.exists()
        env = os.environ.copy()
        env.update(CARGO_TARGET_DIR=str(target), CARGO_PROFILE_RELEASE_OPT_LEVEL=level,
                   CARGO_BUILD_JOBS=str(args.jobs))
        common = ["--locked", "--release", "--features", FEATURES]
        build = ["cargo", "build", *common]
        for binary in BINARIES:
            build += ["--bin", binary]
        profile = {"target_cache_existed": cached, "build": timed(build, destination / "build.log", env),
                   "executables": {}}
        for binary in BINARIES:
            source = target / "release" / binary
            stripped = destination / binary
            shutil.copy2(source, stripped)
            subprocess.run(["strip", str(stripped)], check=True)
            profile["executables"][binary] = {
                "unstripped_bytes": source.stat().st_size,
                "stripped_bytes": stripped.stat().st_size}
        executables = {}
        profile["benchmark_builds"] = {}
        for name in TARGETS:
            # Capture an unambiguous executable and build measurement per target.
            # Cargo may also prepare application binaries as bench dependencies.
            log = destination / f"{name}-build.log"
            profile["benchmark_builds"][name] = timed(
                ["cargo", "rustc", *common, "--bench", name, "--message-format=json"], log, env)
            for line in log.read_text().splitlines():
                if not line.startswith("{"):
                    continue
                message = json.loads(line)
                if (message.get("reason") == "compiler-artifact" and message.get("executable")
                        and message["target"]["name"] == name):
                    executables[name] = message["executable"]
        if set(executables) != set(TARGETS):
            raise ValueError("Cargo did not report every benchmark executable")
        profile["benchmark_processes"] = {}
        for name, executable in sorted(executables.items()):
            profile["benchmark_processes"][name] = timed(
                [executable, "--bench", "--sample-size", "50", "--warm-up-time", "2",
                 "--measurement-time", "5", "--noplot"], destination / f"{name}.log", env)
        profile["workloads"] = estimates(target / "criterion")
        if not args.skip_containers:
            tag = f"hubuum-server:opt-{level}-verify"
            flags = f"-F tls-rustls -F tls-openssl --locked --release --config profile.release.opt-level=\"{level}\""
            # opt-level=3 is a TOML integer, while z is a TOML string.
            if level == "3":
                flags = flags.replace('opt-level="3"', 'opt-level=3')
            profile["container_build"] = timed(
                [args.container_engine, "build", "--build-arg", f"CARGO_BUILD_FLAGS={flags}",
                 "--tag", tag, "."], destination / "container-build.log")
            image = json.loads(command_output([args.container_engine, "image", "inspect", tag]))[0]
            profile["container_uncompressed_bytes"] = image["Size"]
            profile["container_id"] = image["Id"]
        report["profiles"][level] = profile
        (output / "report.json").write_text(json.dumps(report, indent=2) + "\n")
    report["comparison"] = comparison(report["profiles"])
    (output / "report.json").write_text(json.dumps(report, indent=2) + "\n")
    print(output / "report.json")


if __name__ == "__main__":
    main()
