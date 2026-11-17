"""Measure isolated real expiry-worker runs under the lifecycle H1/H4 ceilings."""
import argparse
from concurrent.futures import ThreadPoolExecutor, as_completed
import hashlib
import json
from pathlib import Path
import platform
import shutil
import statistics
import subprocess
from subprocess import SubprocessError
import tarfile
import tempfile
import time

from integration.rust_tests import run_tests
from integration.scheduling_benchmark import Fixture, database_resources, percentile, timestamp_us


TEST = "restore_contract::history_expiry_qualification::qualified_history_expiry_worker"
PUBLISHER_TEST = "services::deletion_recovery::repository::size_tests::maximum_composite_checkpoint_resource_probe"
REPLICA_TEST = "restore_contract::history_expiry_qualification::replica::qualified_history_expiry_replica"
CEILING = 512 * 1024 ** 2
SHAPES = ("rows", "bytes", "large_row", "oversized", "logical_size", "census", "metadata",
          "policy_count", "hold_count", "pending_count", "authority")


def measurement_job_provenance(jobs):
    """Require every repetition and retain the original four-hour case window."""
    expected = {(profile, shape, repeat):
                (f"warmup ({profile}, {shape})" if repeat == 0 else
                 f"measure ({profile}, {shape}, {repeat})")
                for profile in ("local", "shared") for shape in SHAPES for repeat in range(6)}
    measured = [job for job in jobs if job["name"].startswith(("warmup (", "measure ("))]
    by_name = {job["name"]: job for job in measured}
    if (len(measured) != len(expected) or set(by_name) != set(expected.values())
            or len({job["id"] for job in measured}) != len(expected)
            or any(type(job["id"]) is not int or job["id"] <= 0 for job in measured)
            or any(job["conclusion"] != "success" for job in measured)):
        raise ValueError("Every local/shared warm-up and measured repetition must have succeeded")
    windows = {}
    for profile in ("local", "shared"):
        for shape in SHAPES:
            case = [by_name[expected[profile, shape, repeat]] for repeat in range(6)]
            try:
                starts = [timestamp_us(job["started_at"]) for job in case]
                ends = [timestamp_us(job["completed_at"]) for job in case]
            except (KeyError, TypeError, ValueError) as error:
                raise ValueError("Missing repetition job timing") from error
            seconds = (max(ends) - starts[0]) / 1_000_000
            if (any(end < start for start, end in zip(starts, ends))
                    or any(start < ends[0] for start in starts[1:])
                    or not 0 < seconds <= 240 * 60):
                raise ValueError("Repetitions must follow warm-up and finish within four hours")
            windows[f"{profile}/{shape}"] = seconds
    return {"shape_jobs": {job["name"]: job["id"] for job in measured},
            "case_window_seconds": windows}


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


def container_failure_log(fixture, destination, container):
    """Retain both Docker log streams before an owned container is removed."""
    result = subprocess.run(["docker", "logs", "--tail", "200", container],
                            capture_output=True, text=True, timeout=30, check=False)
    if result.returncode:
        return
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
        return {"h1_pass": h1, "maximum_composite_pass": report["maximum_composite_verified"],
                "bounded_liveness_pass": 0 < report.get("publisher_fixture_seconds", 0) < 1800}
    batches = report["batches"]
    scanned = report.get("max_scanned_source_rows", 0)
    checks = {
        "h1_pass": h1,
        "bounded_liveness_pass": all(0 < report.get(stage, 0) < 1800
                                     for stage in ("fixture_seconds", "worker_seconds", "replay_seconds")),
        "h4_pass": (type(scanned) is int and 0 < scanned <= 1000
                    and bool(batches)
                    and all(0 < batch["rows"] <= 1000
                            and 0 < batch["input_bytes"] <= 16 * 1024 ** 2
                            and batch["input_bytes"] == batch["native_input_bytes"]
                            and batch["removed_rows"] == batch["rows"]
                            for batch in batches)
                    and (report["shape"] != "rows" or scanned == 1000)
                    and (report["shape"] != "bytes" or any(batch["input_bytes"] >= 15 * 1024 ** 2 for batch in batches))
                    and (report["shape"] != "large_row" or any(4 * 1024 ** 2 - 8192 <= batch["largest_claimed_row_bytes"] <= 4 * 1024 ** 2 for batch in batches))
                    and (report["shape"] != "oversized" or report["oversized_deferred_rows"] == report["slots"] * report.get("replicas", 1))),
    }
    if report["shape"] == "logical_size":
        checks["size_selection_pass"] = bool(batches) and all(
            batch.get("size_authorized_rows") == batch["rows"]
            and batch.get("oldest_first") is True for batch in batches)
    if report["shape"] in ("policy_count", "hold_count", "pending_count"):
        population = report.get("authority_population", {})
        maximum = (population.get("policy_revisions") == 4096 if report["shape"] == "policy_count" else
                   population.get("live_holds") == 2048 if report["shape"] == "hold_count" else
                   population.get("max_pending_intents") == population.get("final_intents") == 256
                   and population.get("initial_pending_intents") == 256 - report["slots"] * report["replicas"])
        control_bytes = (population.get("before_control_bytes"), population.get("after_control_bytes"))
        checks["authority_population_pass"] = maximum and all(
            type(size) is int and 0 < size <= 4 * 1024**2 for size in control_bytes) and all(
            population.get(field) is True for field in ("policies_preserved", "holds_preserved", "pending_completed"))
    if report["shape"] in ("census", "metadata"):
        census = report.get("census", {})
        filler = census.get("filler", {})
        exhausted = ((census.get("source_rows", 0) > 100_000
                      and filler.get("rows") == 100800 and filler.get("closed") == 100400)
                     if report["shape"] == "census" else
                     (0 < census.get("source_rows", 0) <= 100_000
                      and filler.get("rows") == 514 and filler.get("closed") == 513
                      and filler.get("metadata_bytes", 0) > 64 * 1024**2
                      and 0 < filler.get("largest_identity_bytes", 0) <= 1024**2))
        checks["census_fallback_pass"] = (
            exhausted
            and 0 < census.get("exposure_rows", 0) <= 1000
            and 0 < census.get("exposure_bytes", 0) <= 1024 ** 2
            and bool(census.get("filler", {}).get("versions"))
            and census.get("fallback_tasks") == census.get("expected_tasks") == report["slots"] * report["replicas"]
            and all(census.get(field) is True for field in (
                "age_only", "pressure_unchanged", "holds_preserved", "filler_preserved", "restored_filler_preserved"))
            and all(batch.get("size_authorized_rows") == 0 for batch in batches))
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
                    and type(replica.get("max_scanned_source_rows")) is int
                    and 0 < replica["max_scanned_source_rows"] <= 1000
                    and 0 < replica["kernel_peak_rss_bytes"] <= CEILING
                    and replica["kernel_child_peak_rss_bytes"] == 0 for replica in replicas)
            and max(replica["max_scanned_source_rows"] for replica in replicas) == scanned
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
                            container_failure_log(
                                fixture, destination.parent / f"{process}-failure.log", container)
                        except (OSError, SubprocessError):
                            pass
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


def sample_resources(output, name, report):
    if "replica_reports" not in report:
        return measured_resources(output, name)
    processes = [measured_resources(output, f"{name}-replica-{index}")
                 for index in range(report["replicas"])]
    return {"samples": sum(process["samples"] for process in processes),
            "peak_rss_bytes": max(process["peak_rss_bytes"] for process in processes),
            "peak_hwm_bytes": max(process["peak_hwm_bytes"] for process in processes),
            "rss_trace_verified": all(process["rss_trace_verified"] for process in processes),
            "replicas": processes}


def summarize(manifest, samples, failures):
    measured = [sample for sample in samples if not sample["warmup"]]
    expected = {(shape, repeat) for shape in manifest["shapes"] for repeat in range(6)}
    observed = [(sample["shape"], sample["repeat"]) for sample in samples]
    passed = (len(observed) == len(expected) and set(observed) == expected and not failures
              and all(sample["warmup"] is (sample["repeat"] == 0)
                      and sample["report"]["shape"] == sample["shape"]
                      and sample["checks"] == assess(sample["report"], sample["resources"])
                      and all(sample["checks"].values()) for sample in samples))
    qualified = passed and manifest["shapes"] == list(SHAPES)
    summary = {"measured_runs": len(measured), "expected_measured_runs": 5 * len(manifest["shapes"]),
               "failures": failures, "selected_shapes_passed": passed,
               "h1_h4_worker_qualified": qualified,
               "full_shared_topology_qualified": manifest["profile"] == "lifecycle-shared-v1" and qualified}
    if "repetition" in manifest:
        repeat = manifest["repetition"]
        summary["fragment_passed"] = (
            type(repeat) is int and repeat in range(6) and len(manifest["shapes"]) == 1
            and len(samples) == 1 and not failures
            and observed == [(manifest["shapes"][0], repeat)]
            and samples[0]["warmup"] is (repeat == 0)
            and samples[0]["report"]["shape"] == samples[0]["shape"]
            and samples[0]["checks"] == assess(samples[0]["report"], samples[0]["resources"])
            and all(samples[0]["checks"].values()))
    for shape in manifest["shapes"]:
        field = "publisher_fixture_seconds" if shape == "authority" else "worker_seconds"
        seconds = [sample["report"][field] for sample in measured if sample["shape"] == shape]
        summary[shape] = {"runs": len(seconds), "timed_stage": field,
                          "seconds_median": statistics.median(seconds) if seconds else None,
                          "seconds_p95": percentile(seconds, 95) if seconds else None}
    return summary


def verify_database_evidence(root, profile):
    cpus = 2 if profile == "lifecycle-local-v1" else 4
    limit = cpus * 1024**3
    limits = json.loads((root / "postgres-container.json").read_text())["limits"]
    if limits["Memory"] != limit or limits["NanoCpus"] != cpus * 1_000_000_000:
        raise ValueError("PostgreSQL container limits differ from the selected profile")
    network = limits.get("NetworkMode")
    if not isinstance(network, str) or not network or network in {"default", "bridge", "host", "none"}:
        raise ValueError("Missing isolated PostgreSQL network identity")
    for phase in ("initial", "final"):
        observed = database_resources((root / f"{phase}-database-resources.txt").read_text(),
                                      limit, None, cpus)
        if observed != json.loads((root / f"{phase}-database-resources.json").read_text()):
            raise ValueError("PostgreSQL resource summary differs from raw kernel evidence")
    return network


def aggregate_repetitions(inputs, output):
    """Join complete, isolated repetitions before the existing shape aggregation."""
    output.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(prefix="history-expiry-cohorts-", dir=output.parent) as temporary:
        groups, networks = {}, set()
        for root in inputs:
            manifest = json.loads((root / "manifest.json").read_text())
            repeat = manifest.get("repetition")
            if (type(repeat) is not int or repeat not in range(6)
                    or len(manifest["shapes"]) != 1 or manifest["shapes"][0] not in SHAPES
                    or manifest["profile"] not in ("lifecycle-local-v1", "lifecycle-shared-v1")
                    or manifest.get("database_isolation") != "fresh instance per repetition"):
                raise ValueError("Expected one isolated repetition of one supported shape")
            shape, = manifest["shapes"]
            group = groups.setdefault(shape, {})
            if repeat in group:
                raise ValueError("Duplicate shape repetition")
            samples = json.loads((root / "samples.json").read_text())
            summary = summarize(manifest, samples, [])
            if not summary["fragment_passed"] or summary != json.loads((root / "summary.json").read_text()):
                raise ValueError("Repetition fragment is incomplete or failed acceptance")
            network = verify_database_evidence(root, manifest["profile"])
            if network in networks:
                raise ValueError("Repetitions must use distinct isolated database instances")
            networks.add(network)
            group[repeat] = (root, manifest, samples[0])
        if set(groups) != set(SHAPES):
            raise ValueError("Every shape is required for complete profile qualification")
        shards = []
        for shape in SHAPES:
            group = groups[shape]
            if set(group) != set(range(6)):
                raise ValueError("Every shape needs one warm-up and five measured repetitions")
            destination = Path(temporary) / shape / "history-expiry-report"
            destination.mkdir(parents=True)
            common = build = None
            samples = []
            for repeat in range(6):
                root, manifest, sample = group[repeat]
                settings = {key: value for key, value in manifest.items() if key != "repetition"}
                source = (root.parent / "history-expiry-build.txt").read_bytes()
                if common is not None and (settings != common or source != build):
                    raise ValueError("Repetition fragments use different settings or builds")
                common, build = settings, source
                samples.append(sample)
                name = f"{shape}-{repeat}"
                for path in root.iterdir():
                    if path.name in {"manifest.json", "samples.json", "summary.json"}:
                        continue
                    if not path.is_file() or path.is_symlink():
                        raise ValueError("Repetition evidence must contain packaged regular files")
                    target = path.name if path.name.startswith(name) else name + "-" + path.name
                    shutil.copyfile(path, destination / target)
                for extra in ("runner.txt", "reuse.json"):
                    path = root.parent / ("history-expiry-" + extra)
                    if path.exists():
                        shutil.copyfile(path, destination / (name + "-" + extra))
            (destination.parent / "history-expiry-build.txt").write_bytes(build)
            for name, value in (("manifest", common), ("samples", samples),
                                ("summary", summarize(common, samples, []))):
                (destination / (name + ".json")).write_text(json.dumps(value, indent=2) + "\n")
            shards.append(destination)
        return aggregate_measurements(shards, output)


def aggregate_measurements(inputs, output):
    """Require every shape with identical provenance before qualifying a profile."""
    output.mkdir(parents=True, exist_ok=False)
    manifests, samples, shapes = [], [], []
    build = None
    for root in inputs:
        manifest = json.loads((root / "manifest.json").read_text())
        shape, = manifest["shapes"]
        if shape not in SHAPES or shape in shapes:
            raise ValueError("Expected exactly one distinct supported shape per shard")
        shapes.append(shape)
        common = {key: value for key, value in manifest.items() if key != "shapes"}
        if manifests and common != manifests[0]:
            raise ValueError("Measurement shards have different provenance or settings")
        manifests.append(common)
        shard_samples = json.loads((root / "samples.json").read_text())
        summary = summarize(manifest, shard_samples, [])
        if not summary["selected_shapes_passed"] or summary != json.loads((root / "summary.json").read_text()):
            raise ValueError("Measurement shard is incomplete or failed acceptance")
        for sample in shard_samples:
            name = f"{shape}-{sample['repeat']}"
            if (sample["report"] != json.loads((root / (name + ".json")).read_text())
                    or sample["resources"] != sample_resources(root, name, sample["report"])):
                raise ValueError("Measurement shard differs from its raw evidence")
        samples.extend(shard_samples)
        source = (root.parent / "history-expiry-build.txt").read_bytes()
        if build is not None and source != build:
            raise ValueError("Measurement shards use different binary builds")
        build = source
        for path in root.iterdir():
            if path.name in {"manifest.json", "samples.json", "summary.json"}:
                continue
            if not path.is_file() or path.is_symlink():
                raise ValueError("Aggregating requires packaged regular measurement files")
            name = path.name if path.name.startswith(shape + "-") else shape + "-" + path.name
            shutil.copyfile(path, output / name)
        for name in ("runner.txt", "reuse.json"):
            path = root.parent / ("history-expiry-" + name)
            if path.exists():
                shutil.copyfile(path, output / (shape + "-" + name))
    if set(shapes) != set(SHAPES):
        raise ValueError("Every shape is required for complete profile qualification")
    manifest = {**manifests[0], "shapes": list(SHAPES), "measurement_shards": list(SHAPES)}
    summary = summarize(manifest, samples, [])
    for name, data in (("manifest", manifest), ("samples", samples), ("summary", summary)):
        (output / (name + ".json")).write_text(json.dumps(data, indent=2) + "\n")
    (output.parent / "history-expiry-build.txt").write_bytes(build)
    return summary


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
        "shapes": [args.shape] if args.shape else list(SHAPES),
        "p6": "deferred to #561; these finite runs do not establish steady-state expiry lag",
        "process_model": "real production worker pipeline in an isolated integration executable",
    }
    if args.repeat is not None:
        manifest.update(repetition=args.repeat, database_isolation="fresh instance per repetition")
    (args.output / "manifest.json").write_text(json.dumps(manifest, indent=2) + "\n")
    samples = []
    failures = []
    with Fixture(args) as fixture:
        # Migrate the distinct primary used only for fixture-target exclusion.
        fixture.binary(args.admin_binary, ["--migrate", "--database-url", fixture.url("template_fixture")], "primary-migrate")
        for shape in manifest["shapes"]:
            for repeat in (range(6) if args.repeat is None else (args.repeat,)):
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
                    resources = sample_resources(args.output, name, report)
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
                        container_failure_log(fixture, args.output / f"{name}-postgres.log",
                                              fixture.containers[0])
                    except (OSError, SubprocessError) as diagnostic_error:
                        print(f"{name}: could not capture database diagnostics: {diagnostic_error}", flush=True)
                (args.output / "samples.json").write_text(json.dumps(samples, indent=2) + "\n")
                if failures:
                    break
            if failures:
                break
        fixture.database_evidence("final")
    summary = summarize(manifest, samples, failures)
    (args.output / "summary.json").write_text(json.dumps(summary, indent=2) + "\n")
    return int(not summary.get("fragment_passed", summary["selected_shapes_passed"]))


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--profile", choices=("local", "shared"), required=True)
    parser.add_argument("--shape", choices=SHAPES, help="Measure one shape; complete qualification requires aggregation")
    parser.add_argument("--repeat", type=int, choices=range(6),
                        help="Run one repetition fragment (0 is warm-up); aggregation still requires all six")
    parser.add_argument("--binary", type=Path, required=True)
    parser.add_argument("--publisher-binary", type=Path, required=True)
    parser.add_argument("--admin-binary", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args(argv)
    if args.repeat is not None and not args.shape:
        parser.error("a repetition fragment requires --shape")
    if platform.system() != "Linux" or not Path("/proc/self/status").exists():
        parser.error("qualification requires Linux access to the Docker host PID namespace")
    args.unlimited_cpu = False
    args.cpu_affinity = False
    return run(args)
