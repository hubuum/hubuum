"""Measure real scheduler claims in disposable, resource-limited containers."""
import argparse
from concurrent.futures import ThreadPoolExecutor
import hashlib
import json
import math
from pathlib import Path
import platform
import subprocess
import time
import uuid

from integration.corpus import POSTGRES_IMAGE
from support import ROOT


def percentile(values, percent):
    values = sorted(values)
    return values[max(0, math.ceil(len(values) * percent / 100) - 1)] if values else 0


def aggregate(reports, *, timed=False):
    samples = sorted((sample for report in reports for sample in report["samples"]),
                     key=lambda sample: sample["sequence"])
    sequences = [sample["sequence"] for sample in samples]
    if len(set(sequences)) != len(sequences):
        raise ValueError("Worker reports contain duplicate committed scheduler turns")
    warmup = sum(report["warmup_claims"] for report in reports)
    measured = samples[warmup:]
    ordinary = {}
    principals = {}
    for sample in measured:
        principals[str(sample["principal"])] = principals.get(str(sample["principal"]), 0) + 1
        if not sample["promoted"]:
            ordinary[sample["kind"]] = ordinary.get(sample["kind"], 0) + 1
    promotions = [sample["sequence"] for sample in samples if sample["promoted"]]
    promotion_spacing = all(b - a >= 8 for a, b in zip(promotions, promotions[1:]))
    blocked = {}
    for report in reports:
        for principal, occupied in report.get("principal_cap_blocked_us", {}).items():
            blocked[principal] = max(blocked.get(principal, 0), occupied)
    result = {
        "principal_cap_blocked_us": blocked,
        "cap_window_aggregation": "per-principal maximum across the process-local database-clock windows",
        "claims": len(measured), "warmup_claims": warmup,
        "claim_p95_us": percentile([sample["claim_us"] for sample in measured], 95),
        "claim_p99_us": percentile([sample["claim_us"] for sample in measured], 99),
        "lock_p95_us": percentile([sample["lock_wait_us"] for sample in measured], 95),
        "max_round_trips": max((sample["round_trips"] for sample in samples), default=0),
        "ordinary_starts": ordinary, "principal_starts": principals,
        "promotions": sum(sample["promoted"] for sample in measured),
        "promotion_spacing_valid": promotion_spacing,
        "failed_polls": [failure for report in reports for failure in report["failed_polls"]],
        "empty_poll_count": sum(len(report["empty_polls"]) for report in reports),
        "occupied_slot_fraction": sum(report["occupied_slot_fraction"] for report in reports),
    }
    result["p3_latency_pass"] = (bool(measured) and result["claim_p95_us"] <= 25_000
                                 and result["claim_p99_us"] <= 100_000
                                 and result["lock_p95_us"] <= 10_000
                                 and result["max_round_trips"] <= 12
                                 and not result["failed_polls"])
    total = sum(ordinary.values())
    result["p4_start_share_pass"] = None if timed or total < 10_000 else (
        promotion_spacing and total > 0
        and abs(ordinary.get("import", 0) / total - 0.75) <= 0.05
        and abs(ordinary.get("export", 0) / total - 0.25) <= 0.05)
    result["p4_utilization_pass"] = result["occupied_slot_fraction"] >= 0.85 if timed else None
    # Candidate-row bounds need the retained PostgreSQL plans, not query counts.
    result["candidate_bound_verified"] = False
    return result


class Fixture:
    def __init__(self, args):
        self.args = args
        self.name = "hubuum-scheduling-" + uuid.uuid4().hex[:12]
        self.containers = []
        self.slots = 1 if args.profile == "local" else 4
        self.pg_cpus = 2 if self.slots == 1 else 4
        self.pg_memory = "2g" if self.slots == 1 else "4g"
        self.password = uuid.uuid4().hex

    def docker(self, *args, timeout=1800):
        result = subprocess.run(["docker", *map(str, args)], text=True,
                                capture_output=True, timeout=timeout)
        if result.returncode:
            raise RuntimeError(result.stderr[-4000:].replace(self.password, "[fixture password]"))
        return result.stdout

    def limits(self, cpus, memory):
        cpu_limit = [] if self.args.unlimited_cpu else ["--cpus", str(cpus)]
        return [*cpu_limit, "--memory", memory]

    def __enter__(self):
        self.docker("network", "create", self.name)
        try:
            self.containers.append(self.name + "-db")
            self.docker("run", "--detach", "--name", self.containers[0],
                        "--network", self.name, "--network-alias", "postgres",
                        *self.limits(self.pg_cpus, self.pg_memory),
                        "--env", "POSTGRES_USER=postgres", "--env", f"POSTGRES_PASSWORD={self.password}",
                        "--env", "POSTGRES_DB=template_fixture", POSTGRES_IMAGE)
            for _ in range(120):
                # The image's temporary initialization server only listens on
                # a Unix socket; workers need the final TCP server to be ready.
                probe = subprocess.run(["docker", "exec", self.containers[0], "pg_isready", "-h", "127.0.0.1", "-U", "postgres"],
                                       capture_output=True, timeout=10)
                if probe.returncode == 0:
                    break
                time.sleep(0.5)
            else:
                raise RuntimeError("Disposable PostgreSQL did not become ready")
            inspected = json.loads(self.docker("inspect", self.containers[0]))[0]
            self.address = inspected["NetworkSettings"]["Networks"][self.name]["IPAddress"]
            self.args.output.joinpath("postgres-container.json").write_text(json.dumps({
                "image": inspected["Image"], "limits": inspected["HostConfig"],
                "mounts": inspected["Mounts"],
            }, indent=2) + "\n")
            config = inspected["HostConfig"]
            if not self.args.unlimited_cpu and config["NanoCpus"] != self.pg_cpus * 1_000_000_000:
                raise RuntimeError("PostgreSQL CPU limit was not applied")
            if config["Memory"] != self.pg_cpus * 1024 ** 3:
                raise RuntimeError("PostgreSQL memory limit was not applied")
            return self
        except BaseException:
            self.__exit__()
            raise

    def __exit__(self, *_):
        for name in reversed(self.containers):
            subprocess.run(["docker", "rm", "--force", "--volumes", name], capture_output=True, timeout=60)
        subprocess.run(["docker", "network", "rm", self.name], capture_output=True, timeout=60)

    def url(self, database):
        return f"postgres://postgres:{self.password}@{self.address}/{database}?sslmode=disable"

    def sql(self, database, statement):
        return self.docker("exec", self.containers[0], "psql", "-U", "postgres", "-d", database,
                           "-v", "ON_ERROR_STOP=1", "-At", "-c", statement)

    def candidate_plan(self, database, name):
        statement = """LOAD 'auto_explain'; SET client_min_messages=log;
            SET auto_explain.log_min_duration=0; SET auto_explain.log_analyze=on;
            SET auto_explain.log_timing=off; SET auto_explain.log_nested_statements=on;
            SET auto_explain.log_format=json; BEGIN;
            SELECT public.hubuum_schedule_task(gen_random_uuid(),
                ARRAY['import','export','backup','reindex','remote_call','schema_validation','schema_profiling'],120000);
            ROLLBACK;"""
        result = subprocess.run(["docker", "exec", self.containers[0], "psql", "-U", "postgres",
                                 "-d", database, "-v", "ON_ERROR_STOP=1", "-c", statement],
                                text=True, capture_output=True, timeout=300)
        self.args.output.joinpath(name + "-plan.log").write_text(result.stdout + result.stderr)
        if result.returncode:
            raise RuntimeError("Candidate plan probe failed; inspect its retained log")

    def binary(self, binary, arguments, name, *, worker=False):
        container = self.name + "-" + name
        self.containers.append(container)
        binary = binary.resolve()
        executable = "/executables/" + binary.name
        command = ["run", "--name", container, "--network", self.name,
                   "--security-opt", "label=disable",
                   "--mount", f"type=bind,src={binary.parent},dst=/executables,readonly",
                   "--mount", f"type=bind,src={ROOT / 'scale-benchmarks'},dst=/fixtures,readonly",
                   "--mount", f"type=bind,src={self.args.output.resolve()},dst=/reports",
                   "--entrypoint", executable]
        if worker:
            command += self.limits(2, "512m")
        output = self.docker(*command, POSTGRES_IMAGE, *arguments, timeout=7200)
        if worker:
            inspected = json.loads(self.docker("inspect", container))[0]
            config = inspected["HostConfig"]
            if not self.args.unlimited_cpu and config["NanoCpus"] != 2_000_000_000:
                raise RuntimeError("Worker CPU limit was not applied")
            if config["Memory"] != 512 * 1024 ** 2:
                raise RuntimeError("Worker memory limit was not applied")
            self.args.output.joinpath(name + "-container.json").write_text(json.dumps({
                "limits": config, "state": inspected["State"],
            }, indent=2) + "\n")
        self.docker("rm", "--volumes", container)
        self.containers.remove(container)
        return output


def run(args):
    args.output.mkdir(parents=True, exist_ok=False)
    manifest = {"profile": args.profile, "constraints_applied": not args.unlimited_cpu,
                "cpu_limits_applied": not args.unlimited_cpu, "memory_limits_applied": True,
                "host": platform.platform(), "postgres_image": POSTGRES_IMAGE,
                "binary_sha256": hashlib.sha256(args.binary.read_bytes()).hexdigest(),
                "admin_binary_sha256": hashlib.sha256(args.admin_binary.read_bytes()).hexdigest(),
                "warmup": "100 excluded claims per process for immediate runs; a separate 30-second warm-up window before timed runs",
                "runs": []}
    def retain():
        args.output.joinpath("manifest.json").write_text(json.dumps(manifest, indent=2) + "\n")
    retain()
    with Fixture(args) as fixture:
        fixture.binary(args.admin_binary, ["--migrate", "--database-url", fixture.url("template_fixture")], "migrate")
        workload = (ROOT / "scale-benchmarks/lifecycle/workload-v1.toml").read_text()
        # Leave at least 10k ordinary selections after bounded promotions.
        plans = [(1000, False, None, 1000), (10000, False, None, 12000),
                 (100000, False, None, 1000), (10000, True, None, 12000),
                 (10000, True, 60, 10000)]
        for plan, (backlog, overdue, seconds, samples) in enumerate(plans):
            spec = f"workload-{backlog}.toml"
            args.output.joinpath(spec).write_text(workload.replace("tasks = 10000", f"tasks = {backlog}"))
            template = f"prepared_{plan}"
            fixture.sql("postgres", f"CREATE DATABASE {template} TEMPLATE template_fixture")
            common = ["scheduling", "--profile-spec", "/fixtures/lifecycle/dataset-v1.toml",
                      "--workload-spec", "/reports/" + spec, "--slots", str(fixture.slots)]
            if overdue:
                common += ["--overdue"]
            if seconds:
                common += ["--timed-seconds", str(seconds)]
            fixture.binary(args.binary, common + ["--database-url", fixture.url(template), "--prepare-only",
                           "--output", f"/reports/p{plan}-preparation.json"], f"prepare-{plan}")
            fixture.candidate_plan(template, f"p{plan}")
            for repeat in range(1, 6):
                name = f"p{plan}-r{repeat}"
                database = f"measure_{plan}_{repeat}"
                fixture.sql("postgres", f"CREATE DATABASE {database} TEMPLATE {template}")
                try:
                    # Each fresh copy starts at the declared age even when prior
                    # repetitions took longer than the promotion threshold.
                    age = 3600 if overdue else 0
                    fixture.sql(database, "UPDATE public.tasks SET created_at="
                                f"(clock_timestamp() AT TIME ZONE 'UTC')-interval '{age} seconds' "
                                "WHERE status='queued'")
                    fixture.sql(database, "VACUUM ANALYZE public.tasks")
                    processes = 1 if fixture.slots == 1 else 2
                    if seconds:
                        # Warm the same claim path and fixture before the measured
                        # window. Only this private fixture's stale worker leases reset.
                        # Preserve the cap policy while warming the real timed
                        # workload. Its claims and duration remain in a separate report.
                        warm = common.copy()
                        warm[warm.index("--timed-seconds") + 1] = "30"
                        fixture.binary(args.binary, warm + ["--database-url", fixture.url(database),
                            "--worker-process-slots", str(fixture.slots), "--samples", "200",
                            "--output", f"/reports/{name}-warmup.json"], name + "-warmup", worker=True)
                        fixture.sql(database, "DELETE FROM hubuum_control.task_workers")
                    def execute(process):
                        report = f"{name}-worker-{process}.json"
                        fixture.binary(args.binary, common + ["--database-url", fixture.url(database),
                            "--worker-process-slots", str(fixture.slots // processes),
                            "--samples", str(samples // processes), "--output", "/reports/" + report],
                            f"{name}-worker-{process}", worker=True)
                        return json.loads(args.output.joinpath(report).read_text())
                    with ThreadPoolExecutor(max_workers=processes) as executor:
                        reports = list(executor.map(execute, range(processes)))
                    record = aggregate(reports, timed=seconds is not None)
                    record.update(name=name, backlog=backlog, overdue=overdue, seconds=seconds)
                    manifest["runs"].append(record)
                    retain()
                    print(json.dumps(record), flush=True)
                finally:
                    fixture.sql("postgres", f"DROP DATABASE {database} WITH (FORCE)")
            fixture.sql("postgres", f"DROP DATABASE {template} WITH (FORCE)")
    # Keep every repeat even when a budget fails. Plans and shrinking-pool
    # evidence are reviewed separately before claiming full P3/P4 acceptance.
    return 0


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--profile", choices=("local", "shared"), required=True)
    parser.add_argument("--binary", type=Path, default=ROOT / "target/release/hubuum-scale-benchmark")
    parser.add_argument("--admin-binary", type=Path, default=ROOT / "target/release/hubuum-admin")
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--unlimited-cpu", "--unconstrained", dest="unlimited_cpu", action="store_true",
                        help="Diagnostic mode: omit CPU limits, retain memory limits; does not establish resource-profile acceptance")
    return run(parser.parse_args(argv))
