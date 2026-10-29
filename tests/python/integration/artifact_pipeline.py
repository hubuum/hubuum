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


def database_environment(env):
    query = "SELECT json_build_object('version',current_setting('server_version'),'shared_buffers',current_setting('shared_buffers'),'work_mem',current_setting('work_mem'),'max_connections',current_setting('max_connections'))"
    timings = []
    for _ in range(5):
        started = time.monotonic()
        result = subprocess.run(["psql", "-XAt", "-v", "ON_ERROR_STOP=1", "-c", query],
                                env=env, check=True, capture_output=True, text=True)
        timings.append((time.monotonic() - started) * 1000)
    return {**json.loads(result.stdout), "connection_and_query_wall_ms": timings,
            "server_cpu_limit": None, "server_memory_limit": None,
            "server_peak_rss": None}


def run_pair(binary, admin, args, sample):
    database = "hubuum_artifact_probe_" + uuid.uuid4().hex
    admin_env = connection_environment("postgres")
    subprocess.run(["psql", "-v", "ON_ERROR_STOP=1", "-c", f"CREATE DATABASE {database}"],
                   env=admin_env, check=True, stdout=subprocess.DEVNULL)
    env = connection_environment(database)
    url = f"postgres://{quote(env['PGUSER'], safe='')}:{quote(env['PGPASSWORD'], safe='')}@{env['PGHOST']}:{env['PGPORT']}/{database}"
    env.update({"HUBUUM_DATABASE_URL": url, "HUBUUM_MIGRATION_DATABASE_URL": url,
                "HUBUUM_DATABASE_ROLE_MODE": "single", "HUBUUM_DATABASE_PRIVILEGE_MODE": "warn",
                "HUBUUM_DB_POOL_SIZE": "8", "HUBUUM_DB_POOL_ACQUIRE_TIMEOUT_MS": "30000",
                "HUBUUM_DB_STATEMENT_TIMEOUT_MS": "30000",
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
        # A fresh benchmark database gets the declared 8 GiB deployment envelope.
        # This is the same persisted policy used by production admission.
        policy = """WITH current AS (SELECT public.hubuum_task_policy_read() AS value),
        tuned AS (SELECT (value->>'revision')::BIGINT AS revision,
        jsonb_set(jsonb_set(jsonb_set(value->'policy','{deployment,resident_bytes}','8589934592'),
        '{principal,resident_bytes}','8589934592'),'{collection,resident_bytes}','8589934592') AS policy FROM current)
        SELECT public.hubuum_task_policy_replace(revision,jsonb_set(policy,'{kinds}',
        (SELECT jsonb_agg(jsonb_set(kind,'{capacity,resident_bytes}','8589934592'))
        FROM jsonb_array_elements(policy->'kinds') AS kind))) FROM tuned"""
        subprocess.run(["psql", "-X", "-v", "ON_ERROR_STOP=1", "-c", policy],
                       env=env, check=True, stdout=subprocess.DEVNULL)
        database_settings = database_environment(env)
        with tempfile.TemporaryDirectory(prefix="hubuum-artifact-probe-") as directory:
            processes = []
            started = time.monotonic()
            for replica, mode in enumerate(args.modes):
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
                    "logical_cpus": os.cpu_count(), "worker_cpu_limit": None,
                    "worker_pool_size": 8, "pool_acquire_timeout_ms": 30000,
                    "statement_timeout_ms": 30000,
                    "worker_memory_limit": None, "rss_acceptance_bytes": 536870912,
                    "deployment_resident_limit": 8589934592, "database": database_settings,
                    "output_limit": args.output_limit, "modes": args.modes,
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
    parser.add_argument("--modes", nargs=2, choices=("export", "backup"), default=("export", "backup"))
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
