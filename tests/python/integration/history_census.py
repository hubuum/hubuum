"""Qualify the native accounting component under bounded Linux resources."""
import argparse
import hashlib
import json
from pathlib import Path

from integration.history_expiry import CEILING, measured_resources
from integration.scheduling_benchmark import Fixture

TEST = "backend::history_lifecycle::accounting::qualification::maximum_native_census"
SHAPES = ("supported", "rows", "rows_over", "identity", "identity_over", "metadata", "metadata_over")


def assess(report, resources):
    shape = report["shape"]
    status = report["outcome"]["status"]
    if shape not in SHAPES or status not in ("complete", "unavailable"):
        raise ValueError("Unknown census shape or outcome")
    supported = shape in ("supported", "identity")
    exhausted = shape in ("rows_over", "identity_over", "metadata_over")
    fallback = report.get("fallback") or {}
    return {
        "resource_pass": (resources.get("rss_trace_verified") is True
                          and resources["samples"] > 0
                          and 0 < report["kernel_peak_rss_bytes"] <= CEILING
                          and 0 < resources["peak_rss_bytes"] <= CEILING
                          and 0 < resources["peak_hwm_bytes"] <= CEILING),
        "complete_or_unavailable_pass": (not supported or status == "complete")
                                        and (not exhausted or status == "unavailable"),
        "bounded_liveness_pass": 0 < report["elapsed_us"] < 10_000_000,
        "atomicity_pass": report["control_unchanged"] is True and report["gate_released"] is True,
        "age_fallback_pass": (shape != "rows_over" or (
            fallback["size_only_members"] == 0 and fallback["age_members"] > 0
            and fallback["age_size_witnesses"] == 0 and fallback["pressure_unchanged"] is True
            and 0 < fallback["size_only_us"] < 10_000_000
            and 0 < fallback["age_or_size_us"] < 10_000_000)),
        "retained_scope_pass": (shape != "rows_over" or (
            fallback.get("overlapping_size_sources") == 2
            and fallback.get("held_copies") == 2 and fallback.get("held_closed_copies") == 1
            and fallback.get("pending_size_members") == 1
            and all(fallback.get(key) is True for key in (
                "governing_partition_verified", "pending_preserved",
                "held_and_pending_unselected", "holds_preserved")))),
        "boundary_pass": ((shape != "supported" or report["source_rows"] == 1000)
                          and (shape != "identity" or report["largest_identity_bytes"] == 1024 ** 2)
                          and (shape != "identity_over" or report["largest_identity_bytes"] == 1024 ** 2 + 1)
                          and (shape != "rows" or report["source_rows"] == 100_000)
                          and (shape != "rows_over" or report["source_rows"] == 100_001)
                          and (shape != "metadata" or report["metadata_bytes"] == 64 * 1024 ** 2)
                          and (shape != "metadata_over" or report["metadata_bytes"] > 64 * 1024 ** 2)),
    }


def run(args):
    args.output.mkdir(parents=True, exist_ok=False)
    def digest(path):
        with path.open("rb") as source:
            return hashlib.file_digest(source, "sha256").hexdigest()

    manifest = {"profile": args.profile, "shapes": SHAPES,
                "process_model": "native PostgreSQL accounting component; no HTTP/worker baseline",
                "database_login": "isolated fixture owner; split-role timeout covered by Rust suite",
                "binary_sha256": digest(args.binary), "admin_sha256": digest(args.admin_binary),
                "memory_limit_bytes": CEILING, "cpu_limit": 2, "sample_interval_ms": 20,
                "warmup_runs_per_shape": 1, "measured_runs_per_shape": 5,
                "terminal_sampling_grace_ms": 1000,
                "p6": "deferred; finite component probes do not establish steady-state lag"}
    (args.output / "manifest.json").write_text(json.dumps(manifest, indent=2) + "\n")
    samples = []
    with Fixture(args) as fixture:
        url = fixture.url("template_fixture")
        fixture.binary(args.admin_binary, ["--migrate", "--database-url", url], "migrate")
        for shape in SHAPES:
            for repeat in range(6):
                name = f"{shape}-{repeat}"
                environment = {"HUBUUM_DATABASE_URL": url, "HUBUUM_MIGRATION_DATABASE_URL": url,
                               "HUBUUM_DATABASE_ROLE_TESTS": "false",
                               "HUBUUM_TEST_HISTORY_QUALIFICATION_SHAPE": shape,
                               "HUBUUM_TEST_HISTORY_QUALIFICATION_REPORT": f"/reports/{name}.json"}
                log = fixture.binary(args.binary, [TEST, "--exact", "--ignored", "--nocapture"],
                                     name, worker=True, environment=environment, retain_samples=True)
                (args.output / f"{name}.log").write_text(log)
                report = json.loads((args.output / f"{name}.json").read_text())
                if report["shape"] != shape:
                    raise ValueError("Report does not match the requested census shape")
                resources = measured_resources(args.output, name)
                checks = assess(report, resources)
                samples.append({"shape": shape, "repeat": repeat, "warmup": repeat == 0,
                                "report": report, "resources": resources, "checks": checks})
                (args.output / "samples.json").write_text(json.dumps(samples, indent=2) + "\n")
                if not all(checks.values()):
                    raise ValueError(f"Census acceptance failed: {name}: {checks}")
        fixture.database_evidence("final")
    (args.output / "summary.json").write_text(json.dumps({
        "runs": len(samples), "measured_runs": sum(not row["warmup"] for row in samples),
        "all_passed": all(all(row["checks"].values()) for row in samples),
        "peak_hwm_bytes": max(row["resources"]["peak_hwm_bytes"] for row in samples),
    }, indent=2) + "\n")
    return 0


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--binary", type=Path, required=True)
    parser.add_argument("--admin-binary", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--profile", choices=("local", "shared"), required=True)
    args = parser.parse_args(argv)
    args.unlimited_cpu = False
    return run(args)
