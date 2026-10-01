#!/usr/bin/env python3
"""Record bounded operator-job results for node exporter's textfile collector."""
import sys

if sys.version_info < (3, 11):
    sys.exit(
        "Hubuum tooling requires Python 3.11 or newer; found "
        + sys.version.split()[0]
        + ". Install Python 3.11+ and ensure python3 on PATH selects it."
    )

import argparse
import json
import os
from pathlib import Path
import re
import subprocess
import tempfile
import time


def write_metrics(path, labels, maximum_age, result=None):
    previous = path.read_text() if path.exists() else ""
    match = re.search(r'^hubuum_operator_job_last_success_timestamp_seconds\{[^}]+\} ([0-9.]+)$', previous, re.M)
    success_time = match[1] if match else None
    values = {"expected": 1, "max_age_seconds": maximum_age}
    if result is not None:
        code, duration, finished = result
        values.update(success=int(code == 0), duration_seconds=duration, last_run_timestamp_seconds=finished)
        if code == 0:
            success_time = finished
    if success_time is not None:
        values["last_success_timestamp_seconds"] = success_time
    lines = []
    for name, value in values.items():
        metric = "hubuum_operator_job_" + name
        lines += [f"# TYPE {metric} gauge", f"{metric}{{{labels}}} {value}"]
    # Rename in the same filesystem prevents the collector seeing partial data.
    with tempfile.NamedTemporaryFile(mode="w", dir=path.parent, delete=False) as temporary:
        temporary.write("\n".join(lines) + "\n")
    os.chmod(temporary.name, 0o644)
    os.replace(temporary.name, path)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--directory", type=Path, required=True)
    parser.add_argument("--deployment", required=True)
    parser.add_argument("--operation", choices=("restore_verify", "event_archive", "retention"), required=True)
    parser.add_argument("--max-age-seconds", type=int, default=86400)
    parser.add_argument("--init", action="store_true", help="declare an expected job before its first run")
    parser.add_argument("command", nargs=argparse.REMAINDER)
    args = parser.parse_args()
    if not re.fullmatch(r"[a-zA-Z0-9_.-]+", args.deployment) or args.max_age_seconds <= 0:
        parser.error("deployment must be a stable identifier and maximum age must be positive")
    command = args.command[1:] if args.command[:1] == ["--"] else args.command
    if args.init == bool(command):
        parser.error("choose either --init or a command after --")
    args.directory.mkdir(parents=True, exist_ok=True)
    path = args.directory / f"hubuum-{args.deployment}-{args.operation}.prom"
    labels = f"deployment={json.dumps(args.deployment)},operation={json.dumps(args.operation)}"
    if args.init:
        if not path.exists():
            write_metrics(path, labels, args.max_age_seconds)
        return
    started = time.monotonic()
    try:
        code = subprocess.run(command, check=False).returncode
    except OSError as error:
        print(f"Operator job could not start: {error}", file=sys.stderr)
        code = 127
    write_metrics(path, labels, args.max_age_seconds, (code, time.monotonic() - started, time.time()))
    sys.exit(code if code >= 0 else 128 - code)


if __name__ == "__main__":
    main()
