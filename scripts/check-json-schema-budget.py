#!/usr/bin/env python3
"""Build and run JSON Schema adversarial evidence with process resource limits."""

import sys

if sys.version_info < (3, 11):
    sys.exit(
        "Hubuum tooling requires Python 3.11 or newer; found "
        + sys.version.split()[0]
        + ". Install Python 3.11+ and ensure python3 on PATH selects it."
    )

import argparse
import json
from pathlib import Path
import subprocess


def process_limits():
    """Linux adds CPU/address-space caps to the portable wall-time cap."""
    import resource

    resource.setrlimit(resource.RLIMIT_CPU, (5, 5))
    resource.setrlimit(resource.RLIMIT_AS, (512 * 1024 * 1024,) * 2)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--release", action="store_true")
    args = parser.parse_args()
    root = Path(__file__).resolve().parents[1]
    command = ["cargo", "test", "--locked", "-p", "hubuum-domain", "--lib",
               "--no-run", "--message-format=json"]
    if args.release:
        command.append("--release")
    build = subprocess.run(command, cwd=root, check=True, text=True,
                           stdout=subprocess.PIPE)
    executables = [message["executable"] for line in build.stdout.splitlines()
                   if (message := json.loads(line)).get("reason") == "compiler-artifact"
                   and message.get("executable")]
    if len(executables) != 1:
        raise RuntimeError(f"Expected one domain test binary, found {len(executables)}")
    probe = subprocess.run(
        [executables[0], "--exact", "json_schema::tests::schema_budget_resource_probe",
         "--ignored", "--nocapture", "--test-threads=1"],
        cwd=root, text=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
        timeout=6, preexec_fn=process_limits if sys.platform.startswith("linux") else None,
    )
    sys.stderr.write(probe.stderr)
    if probe.returncode:
        sys.stderr.write(probe.stdout)
        probe.check_returncode()
    evidence = [json.loads(line.split("SCHEMA_BUDGET_EVIDENCE ", 1)[1])
                for line in probe.stdout.splitlines()
                if "SCHEMA_BUDGET_EVIDENCE " in line]
    if len(evidence) != 10:
        sys.stderr.write(probe.stdout)
        raise RuntimeError("The resource-bounded schema probe did not run")
    for record in evidence:
        print(json.dumps(record, sort_keys=True))


if __name__ == "__main__":
    main()
