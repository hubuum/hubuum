"""Measure isolated real expiry-worker runs under the lifecycle H1/H4 ceilings."""
import argparse
import hashlib
import json
from pathlib import Path
import platform
import statistics

from integration.rust_tests import run_tests
from integration.scheduling_benchmark import Fixture, percentile


TEST = "restore_contract::history_expiry_qualification::qualified_history_expiry_worker"
CEILING = 512 * 1024 ** 2


def assess(report, resources):
    """Incomplete measurements cannot qualify a worker, even with small numbers."""
    batches = report["batches"]
    return {
        "h1_pass": (resources["samples"] > 0
                    and 0 < resources["peak_rss_bytes"] <= CEILING
                    and 0 < resources["peak_hwm_bytes"] <= CEILING
                    and 0 < report["kernel_peak_rss_bytes"] <= CEILING
                    and report["kernel_child_peak_rss_bytes"] == 0),
        "h4_pass": (bool(batches)
                    and all(0 < batch["rows"] <= 1000
                            and 0 < batch["input_bytes"] <= 16 * 1024 ** 2
                            and batch["input_bytes"] == batch["native_input_bytes"]
                            and batch["removed_rows"] == batch["rows"]
                            for batch in batches)
                    and (report["shape"] != "rows" or batches[0]["rows"] == 1000)
                    and (report["shape"] != "bytes" or batches[0]["input_bytes"] >= 15 * 1024 ** 2)
                    and (report["shape"] != "oversized" or report["oversized_deferred_rows"] == report["slots"])),
        "replay_pass": report["replay_preserved_receipts"] and report["quarantine_released"],
    }


def run(args):
    args.output.mkdir(parents=True, exist_ok=False)
    manifest = {
        "profile": "lifecycle-" + args.profile + "-v1",
        "topology": "one measured worker process; one local slot or two shared slots",
        "full_shared_topology_qualified": False,
        "worker_binary_sha256": hashlib.sha256(args.binary.read_bytes()).hexdigest(),
        "admin_binary_sha256": hashlib.sha256(args.admin_binary.read_bytes()).hexdigest(),
        "sample_interval_ms": 20, "worker_memory_limit_bytes": CEILING,
        "worker_cpu_limit": 2, "pool_size": 8,
        "warmup_runs_per_shape": 1, "fresh_measured_runs_per_shape": 5,
        "shapes": ["rows", "bytes", "oversized"],
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

                def execute(command, environment):
                    allowed = {
                        "HUBUUM_RESTORE_TEST_DATABASE_URL", "HUBUUM_RESTORE_TEST_MIGRATION_DATABASE_URL",
                        "HUBUUM_ARCHIVE_TEST_DATABASE_URL", "HUBUUM_ARCHIVE_TEST_MIGRATION_DATABASE_URL",
                    }
                    measured_env = {key: environment[key] for key in allowed}
                    measured_env.update({
                        "HUBUUM_DATABASE_URL": fixture.url("template_fixture"),
                        "HUBUUM_MIGRATION_DATABASE_URL": fixture.url("template_fixture"),
                        "HUBUUM_HISTORY_QUALIFICATION_SHAPE": shape,
                        "HUBUUM_HISTORY_QUALIFICATION_REPORT": "/reports/" + destination.name,
                        "HUBUUM_HISTORY_QUALIFICATION_SLOTS": "1" if args.profile == "local" else "2",
                    })
                    try:
                        output = fixture.binary(args.binary, command[1:], name, worker=True,
                                                environment=measured_env)
                    except (RuntimeError, OSError):
                        container = fixture.name + "-" + name
                        if container in fixture.containers:
                            fixture.docker("rm", "--force", "--volumes", container)
                            fixture.containers.remove(container)
                        raise
                    (args.output / f"{name}.log").write_text(output)
                    return 0

                try:
                    run_tests([str(args.binary), TEST, "--exact", "--ignored", "--nocapture"],
                              args.admin_binary, fixture.url("template_fixture"), execute=execute)
                    report = json.loads(destination.read_text())
                    resources = json.loads((args.output / f"{name}-container.json").read_text())["resources"]
                    checks = assess(report, resources)
                    sample = {"shape": shape, "repeat": repeat, "warmup": repeat == 0,
                              "checks": checks, "report": report, "resources": resources}
                    samples.append(sample)
                    if not all(checks.values()):
                        failures.append(name)
                except (RuntimeError, OSError, ValueError, KeyError) as error:
                    failures.append(name)
                    (args.output / f"{name}-failure.txt").write_text(str(error) + "\n")
                (args.output / "samples.json").write_text(json.dumps(samples, indent=2) + "\n")
        fixture.database_evidence("final")
    measured = [sample for sample in samples if not sample["warmup"]]
    summary = {"measured_runs": len(measured), "expected_measured_runs": 15,
               "failures": failures, "h1_h4_worker_qualified": len(measured) == 15 and not failures,
               "full_shared_topology_qualified": False}
    for shape in manifest["shapes"]:
        seconds = [sample["report"]["worker_seconds"] for sample in measured if sample["shape"] == shape]
        summary[shape] = {"runs": len(seconds), "worker_seconds_median": statistics.median(seconds) if seconds else None,
                          "worker_seconds_p95": percentile(seconds, 95) if seconds else None}
    (args.output / "summary.json").write_text(json.dumps(summary, indent=2) + "\n")
    return int(not summary["h1_h4_worker_qualified"])


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--profile", choices=("local", "shared"), required=True)
    parser.add_argument("--binary", type=Path, required=True)
    parser.add_argument("--admin-binary", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args(argv)
    if platform.system() != "Linux" or not Path("/proc/self/status").exists():
        parser.error("qualification requires Linux access to the Docker host PID namespace")
    args.unlimited_cpu = False
    args.cpu_affinity = False
    return run(args)
