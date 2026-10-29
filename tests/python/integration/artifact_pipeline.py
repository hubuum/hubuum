"""Measure real artifact producers in fresh, concurrent worker processes."""

import argparse
import json
import os
from pathlib import Path
import platform
import subprocess
import tempfile
import time
from urllib.parse import quote
import uuid

from support import ROOT

PROBE = "tasks::tests::artifact_pipeline::full_pipeline_resource_probe"


def build(release):
    flags = ["--release"] if release else []
    subprocess.run(["cargo", "build", "--locked", "--features", "embedded-migrations",
                    "--bin", "hubuum-admin", "--bin", "hubuum-template-worker", *flags], cwd=ROOT, check=True)
    result = subprocess.run(["cargo", "test", "--lib", "--no-run", "--locked",
                             "--features", "integration-test-support,embedded-migrations",
                             "--message-format=json", *flags], cwd=ROOT, check=True,
                            stdout=subprocess.PIPE, text=True)
    binaries = [record["executable"] for line in result.stdout.splitlines()
                if (record := json.loads(line)).get("reason") == "compiler-artifact"
                and record.get("executable") and record.get("target", {}).get("name") == "hubuum"]
    if len(binaries) != 1:
        raise RuntimeError("Expected one application test executable")
    return binaries[0], ROOT / "target" / ("release" if release else "debug") / "hubuum-admin"


def connection_environment(database):
    env = os.environ.copy()
    for key, fallback in (("HOST", "localhost"), ("PORT", "5432"), ("USER", "postgres"), ("PASSWORD", "")):
        env["PG" + key] = env.get("HUBUUM_TEST_DB_" + key, fallback)
    env["PGDATABASE"] = database
    return env


def run_pair(binary, admin, args, sample):
    database = "hubuum_artifact_probe_" + uuid.uuid4().hex
    admin_env = connection_environment("postgres")
    subprocess.run(["psql", "-v", "ON_ERROR_STOP=1", "-c", f"CREATE DATABASE {database}"],
                   env=admin_env, check=True, stdout=subprocess.DEVNULL)
    env = connection_environment(database)
    url = f"postgres://{quote(env['PGUSER'], safe='')}:{quote(env['PGPASSWORD'], safe='')}@{env['PGHOST']}:{env['PGPORT']}/{database}"
    env.update({"HUBUUM_DATABASE_URL": url, "HUBUUM_MIGRATION_DATABASE_URL": url,
                "HUBUUM_DATABASE_ROLE_MODE": "single", "HUBUUM_DATABASE_PRIVILEGE_MODE": "warn",
                "HUBUUM_PROBE_ROWS": str(args.rows), "HUBUUM_PROBE_ROW_BYTES": str(args.row_bytes),
                "HUBUUM_PROBE_SLOTS": str(args.slots), "HUBUUM_PROBE_RATE": str(args.rate),
                "HUBUUM_MAX_PAGE_LIMIT": str(max(args.rows, 250)),
                "HUBUUM_EXPORT_MAX_OUTPUT_BYTES": str(args.output_limit),
                "HUBUUM_BACKUP_MAX_OUTPUT_BYTES": str(args.output_limit),
                "HUBUUM_ARTIFACT_MAX_SPOOL_BYTES": str(args.output_limit),
                "HUBUUM_EXPORT_STAGE_TIMEOUT_MS": "3600000",
                "HUBUUM_EXPORT_TEMPLATE_FUEL": "10000000",
                "HUBUUM_EXPORT_TEMPLATE_MAX_OBJECTS": str(max(args.rows * 2, 10000)),
                "HUBUUM_DATABASE_ROLE_TESTS": "false"})
    try:
        subprocess.run([str(admin), "--migrate"], env=env, check=True, stdout=subprocess.DEVNULL)
        with tempfile.TemporaryDirectory(prefix="hubuum-artifact-probe-") as directory:
            processes = []
            started = time.monotonic()
            for replica, mode in enumerate(("export", "backup")):
                path = Path(directory) / f"worker-{replica}.log"
                with path.open("w") as log:
                    process = subprocess.Popen([binary, "--exact", PROBE, "--ignored", "--nocapture", "--test-threads=1"],
                                               env={**env, "HUBUUM_PROBE_MODE": mode}, stdout=log, stderr=subprocess.STDOUT)
                processes.append((process, path))
            records = []
            try:
                for process, path in processes:
                    process.wait(timeout=7200)
                    lines = path.read_text().splitlines()
                    if process.returncode:
                        print("\n".join(lines[-80:]), flush=True)
                        raise RuntimeError(f"Artifact probe worker exited {process.returncode}")
                    evidence = [json.loads(line.split("ARTIFACT_PIPELINE_EVIDENCE ", 1)[1]) for line in lines if "ARTIFACT_PIPELINE_EVIDENCE " in line]
                    if len(evidence) != 1:
                        raise RuntimeError("Worker did not produce exactly one evidence record")
                    records.extend(evidence)
            finally:
                for process, _ in processes:
                    if process.poll() is None:
                        process.terminate()
                        process.wait(timeout=30)
            return {"profile": "artifact-remote-v2", "sample": sample,
                    "system": platform.platform(), "build": "release" if args.release else "debug",
                    "elapsed_seconds": time.monotonic() - started, "workers": records}
    finally:
        subprocess.run(["psql", "-v", "ON_ERROR_STOP=1", "-c", f"DROP DATABASE {database} WITH (FORCE)"],
                       env=admin_env, check=True, stdout=subprocess.DEVNULL)


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--runs", type=int, default=5)
    parser.add_argument("--warmup", type=int, default=1)
    parser.add_argument("--rows", type=int, default=1024)
    parser.add_argument("--row-bytes", type=int, default=65536)
    parser.add_argument("--slots", type=int, default=2)
    parser.add_argument("--rate", type=int, default=1048576)
    parser.add_argument("--output-limit", type=int, default=134217728)
    parser.add_argument("--release", action="store_true")
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args(argv)
    if min(args.runs, args.rows, args.slots, args.output_limit) < 1 or args.warmup < 0 or args.rate < 0 or not 1024 < args.row_bytes <= 4 * 1024 * 1024:
        parser.error("Invalid probe bounds")
    binary, admin = build(args.release)
    with args.output.open("w") as output:
        for sample in range(-args.warmup, args.runs):
            record = run_pair(binary, admin, args, sample)
            output.write(json.dumps(record, sort_keys=True) + "\n")
            output.flush()
            print(json.dumps(record, sort_keys=True), flush=True)
