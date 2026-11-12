"""Measure isolated real expiry-worker runs under the lifecycle H1/H4 ceilings."""
import argparse
from concurrent.futures import ThreadPoolExecutor, as_completed
import hashlib
import json
from pathlib import Path
import platform
import statistics
import subprocess
from subprocess import SubprocessError
import tarfile
import time

from integration.rust_tests import run_tests
from integration.scheduling_benchmark import Fixture, percentile


TEST = "restore_contract::history_expiry_qualification::qualified_history_expiry_worker"
PUBLISHER_TEST = "services::deletion_recovery::repository::size_tests::maximum_composite_checkpoint_resource_probe"
REPLICA_TEST = "restore_contract::history_expiry_qualification::replica::qualified_history_expiry_replica"
CEILING = 512 * 1024 ** 2


def verify_binary_reuse(changed_paths, original_workflow, current_workflow):
    """Reuse permits only known non-Rust inputs and the unchanged build recipe."""
    allowed = {".github/workflows/history-expiry-qualification.yml",
               "tests/python/integration/history_expiry.py",
               "tests/python/integration/scheduling_benchmark.py",
               "tests/python/unit/deployment/test_history_expiry.py",
               "docs/storage_adapter_sdk.md"}
    if set(changed_paths) - allowed:
        raise ValueError("Binary reuse requires unchanged Rust/build inputs")
    def recipe(workflow):
        build = workflow.index("\n  build:\n")
        start = workflow.index("    runs-on:", build)
        return workflow[start:workflow.index("\n  measure:\n", start)]
    if recipe(original_workflow) != recipe(current_workflow):
        raise ValueError("Binary reuse requires an identical build recipe")


def package_measurements(output, destination):
    """Keep private runtime working directories outside uploaded diagnostics."""
    extras = [output.parent / ("history-expiry-" + name) for name in
              ("build.txt", "runner.txt", "reuse.json")]
    files = [*output.iterdir(), *extras] if output.is_dir() else extras
    with tarfile.open(destination, "w:gz", dereference=False) as archive:
        for path in files:
            if path.is_file() and not path.is_symlink():
                name = "history-expiry-report/" + path.name if path.parent == output else path.name
                archive.add(path, arcname=name, recursive=False)
    destination.chmod(0o644)


def database_failure_log(fixture, destination):
    """Retain both Docker log streams before the disposable database is removed."""
    result = subprocess.run(["docker", "logs", "--tail", "200", fixture.containers[0]],
                            capture_output=True, text=True, timeout=30, check=False)
    diagnostic = (result.stdout + result.stderr).replace(fixture.password, "[fixture password]")
    destination.write_text(diagnostic[-64_000:])


def measured_resources(output, name):
    resources = json.loads((output / f"{name}-container.json").read_text())["resources"]
    count = peak_rss = peak_hwm = first = previous = 0
    with (output / f"{name}-rss.jsonl").open() as trace:
        for line in trace:
            sample = json.loads(line)
            stamp, rss, hwm = (sample[key] for key in ("monotonic_ns", "rss_bytes", "hwm_bytes"))
            if any(type(value) is not int for value in (stamp, rss, hwm)) or not previous < stamp or not 0 < rss <= hwm:
                raise ValueError("Invalid raw process-memory sample")
            first = first or stamp
            previous = stamp
            count += 1
            peak_rss = max(peak_rss, rss)
            peak_hwm = max(peak_hwm, hwm)
    if not count or (count, peak_rss, peak_hwm) != (resources["samples"], resources["peak_rss_bytes"], resources["peak_hwm_bytes"]):
        raise ValueError("Raw process-memory trace differs from its summary")
    return {**resources, "rss_trace_verified": True, "first_sample_ns": first, "last_sample_ns": previous}


def measurement_environment(environment, database_url, report_name, shape, profile):
    """Forward only disposable fixture authority, never the host's credentials."""
    allowed = (
        "HUBUUM_RESTORE_TEST_DATABASE_URL", "HUBUUM_RESTORE_TEST_MIGRATION_DATABASE_URL",
        "HUBUUM_ARCHIVE_TEST_DATABASE_URL", "HUBUUM_ARCHIVE_TEST_MIGRATION_DATABASE_URL",
    )
    return {**{key: environment[key] for key in allowed},
            "HUBUUM_DATABASE_URL": database_url,
            "HUBUUM_MIGRATION_DATABASE_URL": database_url,
            # All processes must hash the fixture's bearer token with the same
            # key. This public test material is never a deployment credential.
            "HUBUUM_TOKEN_HASH_KEY": "history-expiry-qualification-fixture-only",
            "HUBUUM_REQUIRE_STABLE_TOKEN_HASH_KEY": "true",
            "HUBUUM_TEST_HISTORY_QUALIFICATION_SHAPE": shape,
            "HUBUUM_TEST_HISTORY_QUALIFICATION_REPORT": "/reports/" + report_name,
            "HUBUUM_TEST_HISTORY_QUALIFICATION_SLOTS": "1" if profile == "local" else "2",
            "HUBUUM_TEST_HISTORY_QUALIFICATION_REPLICAS": "1" if profile == "local" else "2"}


def assess(report, resources):
    """Incomplete measurements cannot qualify a worker, even with small numbers."""
    h1 = (resources["samples"] > 0
                    and resources.get("rss_trace_verified") is True
                    and 0 < resources["peak_rss_bytes"] <= CEILING
                    and 0 < resources["peak_hwm_bytes"] <= CEILING
                    and 0 < report["kernel_peak_rss_bytes"] <= CEILING
                    and report["kernel_child_peak_rss_bytes"] == 0)
    if report["shape"] == "authority":
        return {"h1_pass": h1, "maximum_composite_pass": report["maximum_composite_verified"]}
    batches = report["batches"]
    checks = {
        "h1_pass": h1,
        "h4_pass": (bool(batches)
                    and all(0 < batch["rows"] <= 1000
                            and 0 < batch["input_bytes"] <= 16 * 1024 ** 2
                            and batch["input_bytes"] == batch["native_input_bytes"]
                            and batch["removed_rows"] == batch["rows"]
                            for batch in batches)
                    and (report["shape"] != "rows" or any(batch["rows"] == 1000 for batch in batches))
                    and (report["shape"] != "bytes" or any(batch["input_bytes"] >= 15 * 1024 ** 2 for batch in batches))
                    and (report["shape"] != "large_row" or any(4 * 1024 ** 2 - 8192 <= batch["largest_claimed_row_bytes"] <= 4 * 1024 ** 2 for batch in batches))
                    and (report["shape"] != "oversized" or report["oversized_deferred_rows"] == report["slots"] * report.get("replicas", 1))),
    }
    if report["shape"] == "oversized":
        checks["oversized_outcome_pass"] = (report["backup_rejected_oversized"]
            and report["oversized_preserved_rows"] == report["oversized_deferred_rows"]
            and report["oversized_reported_rows"] >= report["oversized_deferred_rows"])
    else:
        checks["replay_pass"] = report["replay_preserved_receipts"] and report["quarantine_released"]
    replicas = report.get("replica_reports", [])
    measured = resources.get("replicas", [])
    if replicas and measured:
        checks["replica_topology_pass"] = (
            len(replicas) == len(measured) == report["replicas"]
            and {replica["replica"] for replica in replicas} == set(range(report["replicas"]))
            and all(replica["slots"] == report["slots"] and replica["http_workers"] == 2
                    and replica["initial_claimed_slots"] == report["slots"]
                    and replica["claimed_iterations"] > 0
                    and replica["slow_consumer_blocked"] and replica["slow_consumer_retired"]
                    and 0 < replica["kernel_peak_rss_bytes"] <= CEILING
                    and replica["kernel_child_peak_rss_bytes"] == 0 for replica in replicas)
            and (report["replicas"] == 1 or all(replica["database_round_trip_us"]
                    and percentile(replica["database_round_trip_us"], 95) <= 2000 for replica in replicas))
            and all(process["samples"] > 0
                    and process.get("rss_trace_verified") is True
                    and 0 < process["peak_rss_bytes"] <= CEILING
                    and 0 < process["peak_hwm_bytes"] <= CEILING for process in measured)
            and max(process["first_sample_ns"] for process in measured)
                < min(process["last_sample_ns"] for process in measured))
    else:
        checks["replica_topology_pass"] = False
    return checks


def replica_run(fixture, binary, arguments, name, environment, destination):
    """Provisioning stays outside the measured API/worker processes."""
    root = destination.with_suffix(".runtime")
    replicas = int(environment["HUBUUM_TEST_HISTORY_QUALIFICATION_REPLICAS"])
    names = [name, *(f"{name}-replica-{index}" for index in range(replicas))]
    with ThreadPoolExecutor(max_workers=3) as executor:
        workers = []
        coordinator = executor.submit(fixture.binary, binary, arguments, name,
                                      environment=environment)

        def await_path(path):
            deadline = time.monotonic() + 1800
            while not path.is_file():
                if coordinator.done():
                    coordinator.result()
                    raise RuntimeError("Coordinator exited before its phase marker")
                for worker in workers:
                    if worker.done():
                        worker.result()
                if time.monotonic() >= deadline:
                    raise RuntimeError("Shared qualification phase timed out: " + path.name)
                time.sleep(0.1)

        try:
            await_path(root / "prepared")
            for index in range(replicas):
                replica_env = {**environment, "HUBUUM_TEST_HISTORY_QUALIFICATION_REPLICA": str(index)}
                workers.append(executor.submit(
                    fixture.binary, binary, [REPLICA_TEST, "--exact", "--ignored", "--nocapture"],
                    names[index + 1], worker=True, worker_process=index, environment=replica_env,
                    retain_samples=True))
            for index in range(replicas):
                await_path(root / f"ready-{index}")
            (root / "start").write_bytes(b"")
            outputs = {}
            futures = {coordinator: name, **{worker: names[index + 1] for index, worker in enumerate(workers)}}
            for future in as_completed(futures, timeout=7200):
                outputs[futures[future]] = future.result()
                (destination.parent / f"{futures[future]}.log").write_text(outputs[futures[future]])
            return outputs[name]
        except BaseException:
            # Stop only this run's owned containers before fixture DB teardown.
            pending = [coordinator, *workers]
            for future in pending:
                future.cancel()
            deadline = time.monotonic() + 60
            while True:
                for process in names:
                    container = fixture.name + "-" + process
                    if container in fixture.containers:
                        try:
                            fixture.docker("rm", "--force", "--volumes", container, timeout=10)
                        except RuntimeError:
                            # A returning peer may already have removed its container.
                            pass
                        if container in fixture.containers:
                            fixture.containers.remove(container)
                if all(future.done() for future in pending):
                    break
                if time.monotonic() >= deadline:
                    raise RuntimeError("Owned qualification processes did not stop after failure")
                # A running launch may create its container after the first stop.
                time.sleep(0.1)
            raise


def run(args):
    args.output.mkdir(parents=True, exist_ok=False)
    def digest(path):
        with path.open("rb") as source:
            return hashlib.file_digest(source, "sha256").hexdigest()

    manifest = {
        "profile": "lifecycle-" + args.profile + "-v1",
        "topology": ("one combined API/worker process, one worker slot and two HTTP workers" if args.profile == "local" else
                     "two separate combined API/worker processes, two worker slots and two HTTP workers each"),
        "coordinator": "separate unmeasured fixture/restore process; never part of worker RSS evidence",
        "worker_binary_sha256": digest(args.binary),
        "publisher_binary_sha256": digest(args.publisher_binary),
        "admin_binary_sha256": digest(args.admin_binary),
        "sample_interval_ms": 20, "worker_memory_limit_bytes": CEILING,
        "worker_cpu_limit": 2, "pool_size": 8,
        "warmup_runs_per_shape": 1, "fresh_measured_runs_per_shape": 5,
        "shapes": ["rows", "bytes", "large_row", "oversized", "authority"],
        "p6": "deferred to #561; these finite runs do not establish steady-state expiry lag",
        "process_model": "real production worker pipeline in an isolated integration executable",
    }
    (args.output / "manifest.json").write_text(json.dumps(manifest, indent=2) + "\n")
    samples = []
    failures = []
    with Fixture(args) as fixture:
        # Migrate the distinct primary used only for fixture-target exclusion.
        fixture.binary(args.admin_binary, ["--migrate", "--database-url", fixture.url("template_fixture")], "primary-migrate")
        for shape in manifest["shapes"]:
            for repeat in range(6):
                name = f"{shape}-{repeat}"
                destination = args.output / f"{name}.json"
                binary = args.publisher_binary if shape == "authority" else args.binary
                test = PUBLISHER_TEST if shape == "authority" else TEST

                def execute(command, environment):
                    measured_env = measurement_environment(
                        environment, fixture.url("template_fixture"), destination.name, shape, args.profile)
                    try:
                        if shape != "authority":
                            output = replica_run(fixture, binary, command[1:], name, measured_env, destination)
                        else:
                            output = fixture.binary(binary, command[1:], name, worker=True,
                                                    environment=measured_env, retain_samples=True)
                    except (RuntimeError, OSError, SubprocessError):
                        container = fixture.name + "-" + name
                        if container in fixture.containers:
                            fixture.docker("rm", "--force", "--volumes", container)
                            fixture.containers.remove(container)
                        raise
                    (args.output / f"{name}.log").write_text(output)
                    return 0

                try:
                    run_tests([str(binary), test, "--exact", "--ignored", "--nocapture"],
                              args.admin_binary, fixture.url("template_fixture"), execute=execute)
                    report = json.loads(destination.read_text())
                    if shape != "authority" and (report["replicas"] != (1 if args.profile == "local" else 2)
                                                  or report["slots"] != (1 if args.profile == "local" else 2)):
                        raise ValueError("Measured process topology differs from the selected profile")
                    if "replica_reports" in report:
                        processes = [measured_resources(args.output, f"{name}-replica-{index}")
                                     for index in range(report["replicas"])]
                        resources = {"samples": sum(process["samples"] for process in processes),
                                     "peak_rss_bytes": max(process["peak_rss_bytes"] for process in processes),
                                     "peak_hwm_bytes": max(process["peak_hwm_bytes"] for process in processes),
                                     "rss_trace_verified": all(process["rss_trace_verified"] for process in processes),
                                     "replicas": processes}
                    else:
                        resources = measured_resources(args.output, name)
                    checks = assess(report, resources)
                    sample = {"shape": shape, "repeat": repeat, "warmup": repeat == 0,
                              "checks": checks, "report": report, "resources": resources}
                    samples.append(sample)
                    if not all(checks.values()):
                        failures.append(name)
                        print(f"{name}: failed acceptance checks: {checks}", flush=True)
                except (RuntimeError, OSError, SubprocessError, ValueError, KeyError) as error:
                    failures.append(name)
                    (args.output / f"{name}-failure.txt").write_text(str(error) + "\n")
                    print(f"{name}: {error}", flush=True)
                    try:
                        database_failure_log(fixture, args.output / f"{name}-postgres.log")
                    except (OSError, SubprocessError) as diagnostic_error:
                        print(f"{name}: could not capture database diagnostics: {diagnostic_error}", flush=True)
                (args.output / "samples.json").write_text(json.dumps(samples, indent=2) + "\n")
        fixture.database_evidence("final")
    measured = [sample for sample in samples if not sample["warmup"]]
    expected = 5 * len(manifest["shapes"])
    summary = {"measured_runs": len(measured), "expected_measured_runs": expected,
               "failures": failures, "h1_h4_worker_qualified": len(measured) == expected and not failures,
               "full_shared_topology_qualified": args.profile == "shared" and len(measured) == expected and not failures}
    for shape in manifest["shapes"]:
        field = "publisher_fixture_seconds" if shape == "authority" else "worker_seconds"
        seconds = [sample["report"][field] for sample in measured if sample["shape"] == shape]
        summary[shape] = {"runs": len(seconds), "timed_stage": field,
                          "seconds_median": statistics.median(seconds) if seconds else None,
                          "seconds_p95": percentile(seconds, 95) if seconds else None}
    (args.output / "summary.json").write_text(json.dumps(summary, indent=2) + "\n")
    return int(not summary["h1_h4_worker_qualified"])


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--profile", choices=("local", "shared"), required=True)
    parser.add_argument("--binary", type=Path, required=True)
    parser.add_argument("--publisher-binary", type=Path, required=True)
    parser.add_argument("--admin-binary", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args(argv)
    if platform.system() != "Linux" or not Path("/proc/self/status").exists():
        parser.error("qualification requires Linux access to the Docker host PID namespace")
    args.unlimited_cpu = False
    args.cpu_affinity = False
    return run(args)
