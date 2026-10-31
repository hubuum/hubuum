"""Measure real scheduler claims in disposable, resource-limited containers."""
import argparse
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
import hashlib
import json
import math
import os
from pathlib import Path
import platform
import subprocess
import threading
import time
import uuid

from integration.corpus import POSTGRES_IMAGE
from support import ROOT


def affinity_plan(profile):
    available = iter(sorted(os.sched_getaffinity(0)))
    try:
        database = [next(available) for _ in range(2 if profile == "local" else 4)]
        workers = [[next(available), next(available)] for _ in range(1 if profile == "local" else 2)]
    except StopIteration:
        raise ValueError("Not enough distinct allowed CPUs for the scheduling profile") from None
    return {"database": ",".join(map(str, database)),
            "workers": [",".join(map(str, worker)) for worker in workers]}


def cpu_set(mask):
    cpus = set()
    for part in mask.split(","):
        first, separator, last = part.partition("-")
        lower, upper = int(first), int(last) if separator else int(first)
        if lower < 0 or upper < lower or upper - lower > 65536:
            raise ValueError("Invalid kernel CPU mask")
        cpus.update(range(lower, upper + 1))
    return cpus


def process_memory(status):
    if any(line.startswith("State:") and line.partition(":")[2].strip().startswith("Z")
           for line in status.splitlines()):
        raise ProcessLookupError("Measured worker exited")
    memory = {}
    for line in status.splitlines():
        key, _, value = line.partition(":")
        if key in {"VmRSS", "VmHWM"}:
            amount, unit = value.split()
            if unit != "kB" or int(amount) < 0:
                raise ValueError("Invalid kernel process memory value")
            memory[key] = int(amount) * 1024
    if set(memory) != {"VmRSS", "VmHWM"}:
        raise ValueError("Missing kernel process memory evidence")
    return memory


def process_resources(pid, mask):
    status = Path(f"/proc/{pid}/status").read_text(encoding="ascii")
    try:
        memory = process_memory(status)
    except ValueError:
        # Process exit can remove the address space before State becomes zombie.
        # Missing evidence from a still-running executable remains an error.
        try:
            os.readlink(f"/proc/{pid}/exe")
        except FileNotFoundError:
            raise ProcessLookupError("Measured worker exited") from None
        raise
    if mask:
        allowed = cpu_set(mask)
        for thread in Path(f"/proc/{pid}/task").iterdir():
            try:
                if not os.sched_getaffinity(int(thread.name)).issubset(allowed):
                    raise ValueError("Scheduling worker escaped its CPU affinity")
            except ProcessLookupError:
                continue
    return memory


def cpu_limit(raw, cpus):
    quota, period = raw.split()
    period = int(period)
    quota = None if quota == "max" else int(quota)
    if period <= 0 or (quota is not None and quota <= 0):
        raise ValueError("Invalid kernel CPU limit")
    if cpus is not None and quota != cpus * period:
        raise ValueError("Kernel CPU ceiling was not applied")
    return {"quota_us": quota, "period_us": period}


def database_resources(raw, memory_limit, mask, cpus=None):
    fields = {}
    for line in raw.splitlines():
        key, separator, value = line.partition("=")
        if not separator or key not in {"memory_max", "memory_peak", "cpu_mask", "cpu_max",
                                             "memory_event_max", "memory_event_oom", "memory_event_oom_kill"}:
            raise ValueError("Invalid PostgreSQL kernel resource evidence")
        fields.setdefault(key, []).append(value)
    if fields.get("memory_max") != [str(memory_limit)]:
        raise ValueError("PostgreSQL kernel memory ceiling was not applied")
    peaks = fields.get("memory_peak", [])
    if len(peaks) != 1 or not peaks[0].isdigit():
        raise ValueError("Missing PostgreSQL kernel memory peak")
    events = {}
    for event in ("max", "oom", "oom_kill"):
        values = fields.get("memory_event_" + event, [])
        if len(values) != 1 or not values[0].isdigit():
            raise ValueError("Missing PostgreSQL kernel memory events")
        events[event] = int(values[0])
    if events["oom"] or events["oom_kill"]:
        raise ValueError("PostgreSQL encountered an out-of-memory event")
    # memory.max enforcement may briefly overshoot while reclaiming pages.
    # Retain that peak and reclaim events; an OOM is a failed measurement.
    masks = fields.get("cpu_mask", [])
    if not masks or (mask and any(not cpu_set(value).issubset(cpu_set(mask)) for value in masks)):
        raise ValueError("PostgreSQL process escaped its CPU affinity")
    limits = fields.get("cpu_max", [])
    if cpus is not None and len(limits) != 1:
        raise ValueError("Missing PostgreSQL kernel CPU limit")
    return {"memory_max_bytes": memory_limit, "memory_peak_bytes": int(peaks[0]),
            "memory_peak_over_limit_bytes": max(0, int(peaks[0]) - memory_limit), "memory_events": events,
            "cpu_limit": cpu_limit(limits[0], cpus) if limits else None,
            "postgres_thread_cpu_masks": sorted(set(masks)), "threads_observed": len(masks)}


def percentile(values, percent):
    values = sorted(values)
    return values[max(0, math.ceil(len(values) * percent / 100) - 1)] if values else 0


def timestamp_us(value):
    instant = datetime.fromisoformat(value)
    if instant.tzinfo is None:
        instant = instant.replace(tzinfo=timezone.utc)
    delta = instant - datetime(1970, 1, 1, tzinfo=timezone.utc)
    return delta.days * 86_400_000_000 + delta.seconds * 1_000_000 + delta.microseconds


def timed_utilization(reports):
    """Lower-bound occupancy and upper-bound capacity, including clock uncertainty."""
    starts = [timestamp_us(report["measurement_window"]["start"]) for report in reports]
    start = max(base + report["measurement_clock_bound_us"] + 2 for base, report in zip(starts, reports))
    end = min(timestamp_us(report["measurement_window"]["end"]) for report in reports)
    if end <= start:
        raise ValueError("Worker measurement windows do not overlap")
    slots = reports[0]["options"]["slots"]
    capacity = 1 if slots == 1 else slots - 1
    changes = {start: [0, capacity, capacity], end: [0, None, None]}

    def at(instant):
        return changes.setdefault(instant, [0, None, None])

    controllers = [(base, report) for base, report in zip(starts, reports) if report.get("capacity_changes")]
    if len(controllers) > 1:
        raise ValueError("Capacity cycling must have exactly one controller")
    if controllers:
        base, controller = controllers[0]
        cycle = controller["capacity_changes"]
        if [change["configured_slots"] for change in cycle] != [1, slots]:
            raise ValueError("Capacity cycle did not shrink and restore the declared pool")
        bounds = [(base + change["started_us"], base + controller["measurement_clock_bound_us"] + change["committed_us"] + 2)
                  for change in cycle]
        (shrink_first, shrink_last), (grow_first, grow_last) = bounds
        if not start < shrink_first <= shrink_last < grow_first <= grow_last < end:
            raise ValueError("Capacity changes cannot be resolved inside the common measurement window")
        at(shrink_first)[1] = 1
        at(shrink_last)[2] = 1
        at(grow_first)[2] = capacity
        at(grow_last)[1] = capacity

    for base, report in zip(starts, reports):
        for sample in report["samples"]:
            lower = max(start, base + sample["busy_offset_us"] + report["measurement_clock_bound_us"] + 2)
            upper = min(end, base + sample["busy_offset_us"] + sample["busy_us"])
            if upper > lower:
                at(lower)[0] += 1
                at(upper)[0] -= 1
    occupied = 0
    minimum = maximum = capacity
    busy_area = capacity_area = 0
    previous = start
    for instant, (delta, lower, upper) in sorted(changes.items()):
        width = instant - previous
        busy_area += min(occupied, minimum) * width
        capacity_area += maximum * width
        occupied += delta
        if lower is not None:
            minimum = lower
        if upper is not None:
            maximum = upper
        previous = instant
    return {"fraction_lower_bound": busy_area / capacity_area,
            "occupied_slot_us_lower_bound": busy_area, "eligible_slot_us_upper_bound": capacity_area,
            "window_us": end - start, "capacity_cycle": bool(controllers)}


def aggregate(reports, *, timed=False):
    samples = sorted((sample for report in reports for sample in report["samples"]),
                     key=lambda sample: sample["sequence"])
    timed_warmup = [sample for report in reports
                    for sample in (report.get("timed_warmup") or {}).get("samples", [])]
    all_samples = sorted(samples + timed_warmup, key=lambda sample: sample["sequence"])
    sequences = [sample["sequence"] for sample in all_samples]
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
    promotions = [sample["sequence"] for sample in all_samples if sample["promoted"]]
    promotion_spacing = all(b - a >= 8 for a, b in zip(promotions, promotions[1:]))
    blocked = {}
    for report in reports:
        for principal, occupied in report.get("principal_cap_blocked_us", {}).items():
            blocked[principal] = max(blocked.get(principal, 0), occupied)
    result = {
        "principal_cap_blocked_us": blocked,
        "cap_window_aggregation": "per-principal maximum across the process-local database-clock windows",
        "claims": len(measured), "warmup_claims": warmup,
        "timed_warmup_claims": len(timed_warmup),
        "timed_warmup_pass": all(len((report.get("timed_warmup") or {}).get("samples", [])) == 100
                                  for report in reports) if timed else None,
        "claim_p95_us": percentile([sample["claim_us"] for sample in measured], 95),
        "claim_p99_us": percentile([sample["claim_us"] for sample in measured], 99),
        "lock_p95_us": percentile([sample["lock_wait_us"] for sample in measured], 95),
        "max_round_trips": max((sample["round_trips"] for sample in all_samples), default=0),
        "ordinary_starts": ordinary, "principal_starts": principals,
        "promotions": sum(sample["promoted"] for sample in measured),
        "promotion_spacing_valid": promotion_spacing,
        "failed_polls": [failure for report in reports for failure in report["failed_polls"]],
        "database_round_trip_p95_us": max((percentile(report["database_round_trip_us"], 95)
            for report in reports if "database_round_trip_us" in report), default=None),
        "empty_poll_count": sum(len(report["empty_polls"]) for report in reports),
        "occupied_slot_fraction": sum(report["occupied_slot_fraction"] for report in reports),
    }
    result["p3_latency_pass"] = (bool(measured) and result["claim_p95_us"] <= 25_000
                                 and result["claim_p99_us"] <= 100_000
                                 and result["lock_p95_us"] <= 10_000
                                 and result["max_round_trips"] <= 12
                                 and not result["failed_polls"]
                                 and (not timed or result["timed_warmup_pass"]))
    total = sum(ordinary.values())
    result["p4_start_share_pass"] = None if timed or total < 10_000 else (
        promotion_spacing and total > 0
        and abs(ordinary.get("import", 0) / total - 0.75) <= 0.05
        and abs(ordinary.get("export", 0) / total - 0.25) <= 0.05)
    result["p4_utilization_pass"] = result["occupied_slot_fraction"] >= 0.85 if timed else None
    if timed and all(report.get("measurement_window") and "measurement_clock_bound_us" in report for report in reports):
        result["utilization_evidence"] = timed_utilization(reports)
        result["occupied_slot_fraction"] = result["utilization_evidence"]["fraction_lower_bound"]
        result["p4_utilization_pass"] = result["occupied_slot_fraction"] >= 0.85
    # The fixture adds per-call candidate counters; query counts alone cannot qualify P3.
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
        self.cpu_masks = affinity_plan(args.profile) if getattr(args, "cpu_affinity", False) else None

    def docker(self, *args, timeout=1800):
        result = subprocess.run(["docker", *map(str, args)], text=True,
                                capture_output=True, timeout=timeout)
        if result.returncode:
            raise RuntimeError(result.stderr[-4000:].replace(self.password, "[fixture password]"))
        return result.stdout

    def limits(self, cpus, memory):
        cpu_limit = [] if self.args.unlimited_cpu or self.cpu_masks else ["--cpus", str(cpus)]
        return [*cpu_limit, "--memory", memory]

    def __enter__(self):
        self.docker("network", "create", self.name)
        try:
            self.containers.append(self.name + "-db")
            affinity = (["--entrypoint", "/usr/bin/taskset"] if self.cpu_masks else [])
            entrypoint = (["--cpu-list", self.cpu_masks["database"],
                           "/usr/local/bin/docker-entrypoint.sh", "postgres"] if self.cpu_masks else [])
            self.docker("run", "--detach", "--name", self.containers[0],
                        "--network", self.name, "--network-alias", "postgres",
                        *self.limits(self.pg_cpus, self.pg_memory),
                        "--env", "POSTGRES_USER=postgres", "--env", f"POSTGRES_PASSWORD={self.password}",
                        "--env", "POSTGRES_DB=template_fixture", *affinity, POSTGRES_IMAGE, *entrypoint)
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
            if not self.args.unlimited_cpu and not self.cpu_masks and config["NanoCpus"] != self.pg_cpus * 1_000_000_000:
                raise RuntimeError("PostgreSQL CPU limit was not applied")
            if config["Memory"] != self.pg_cpus * 1024 ** 3:
                raise RuntimeError("PostgreSQL memory limit was not applied")
            self.database_evidence("initial")
            return self
        except BaseException:
            self.__exit__()
            raise

    def database_evidence(self, name):
        # Read the container's actual cgroup and every PostgreSQL thread rather
        # than treating the requested OCI settings as enforcement evidence.
        raw = self.docker("exec", self.containers[0], "sh", "-c", r"""
            set -eu
            printf 'memory_max='; cat /sys/fs/cgroup/memory.max
            printf 'memory_peak='; cat /sys/fs/cgroup/memory.peak
            awk '$1 == "max" || $1 == "oom" || $1 == "oom_kill" { print "memory_event_" $1 "=" $2 }' /sys/fs/cgroup/memory.events
            printf 'cpu_max='; cat /sys/fs/cgroup/cpu.max
            for process in /proc/[0-9]*; do
                [ -r "$process/comm" ] || continue
                [ "$(cat "$process/comm")" = postgres ] || continue
                for status in "$process"/task/*/status; do
                    [ -r "$status" ] || continue
                    awk '/^Cpus_allowed_list:/ { print "cpu_mask=" $2 }' "$status"
                done
            done
        """)
        self.args.output.joinpath(name + "-database-resources.txt").write_text(raw)
        evidence = database_resources(raw, self.pg_cpus * 1024 ** 3,
                                      self.cpu_masks["database"] if self.cpu_masks else None,
                                      None if self.cpu_masks or self.args.unlimited_cpu else self.pg_cpus)
        self.args.output.joinpath(name + "-database-resources.json").write_text(json.dumps(evidence, indent=2) + "\n")
        return evidence

    def __exit__(self, *_):
        for name in reversed(self.containers):
            subprocess.run(["docker", "rm", "--force", "--volumes", name], capture_output=True, timeout=60)
        subprocess.run(["docker", "network", "rm", self.name], capture_output=True, timeout=60)

    def url(self, database):
        return f"postgres://postgres:{self.password}@{self.address}/{database}?sslmode=disable"

    def sql(self, database, statement):
        return self.docker("exec", self.containers[0], "psql", "-U", "postgres", "-d", database,
                           "-v", "ON_ERROR_STOP=1", "-At", "-c", statement)

    def instrument_candidates(self, database):
        original = self.sql(database, "SELECT pg_get_functiondef('public.hubuum_schedule_task(uuid,text[],bigint)'::regprocedure)")
        self.sql(database, """CREATE TABLE hubuum_control.benchmark_scheduler_work (
            id bigserial PRIMARY KEY, selected boolean NOT NULL,
            task_entries bigint NOT NULL, task_blocks bigint NOT NULL);
            ALTER FUNCTION public.hubuum_schedule_task(uuid,text[],bigint)
                RENAME TO hubuum_benchmark_schedule_task;
            CREATE FUNCTION public.hubuum_schedule_task(worker uuid,supported text[],lease_ms bigint)
            RETURNS integer LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $measure$
            DECLARE relations oid[]; before_entries bigint; after_entries bigint;
                before_blocks bigint; after_blocks bigint; chosen integer;
            BEGIN
                SELECT array_agg(oid) INTO relations FROM (
                    SELECT 'public.tasks'::regclass::oid AS oid
                    UNION ALL SELECT indexrelid FROM pg_index WHERE indrelid='public.tasks'::regclass
                ) inputs;
                SELECT sum(pg_stat_get_xact_tuples_returned(oid)),sum(pg_stat_get_xact_blocks_fetched(oid))
                    INTO before_entries,before_blocks FROM unnest(relations) oid;
                chosen := public.hubuum_benchmark_schedule_task(worker,supported,lease_ms);
                SELECT sum(pg_stat_get_xact_tuples_returned(oid)),sum(pg_stat_get_xact_blocks_fetched(oid))
                    INTO after_entries,after_blocks FROM unnest(relations) oid;
                INSERT INTO hubuum_control.benchmark_scheduler_work(selected,task_entries,task_blocks)
                    VALUES(chosen IS NOT NULL,after_entries-before_entries,after_blocks-before_blocks);
                RETURN chosen;
            END $measure$;""")
        return {"production_function_sha256": hashlib.sha256(original.encode()).hexdigest(),
                "method": "transaction-local task index entries returned plus sequential rows; repeated visits count again",
                "block_requests": "task table and index requests, including buffer hits, reported separately",
                "overhead": "fixture wrapper and recording insert included in claim latency and lock occupancy",
                "failed_transactions": "roll back counters; any failed poll fails P3"}

    def candidate_work(self, database, name, expected):
        rows = self.sql(database, """SELECT json_build_object('selected',selected,
            'task_entries',task_entries,'task_blocks',task_blocks)
            FROM hubuum_control.benchmark_scheduler_work ORDER BY id""")
        self.args.output.joinpath(name + "-candidate-work.jsonl").write_text(rows)
        return candidate_work_summary([json.loads(line) for line in rows.splitlines()], expected)

    def candidate_plan(self, database, name):
        statement = """LOAD 'auto_explain'; SET client_min_messages=log;
            SET auto_explain.log_min_duration=0; SET auto_explain.log_analyze=on;
            SET auto_explain.log_timing=off; SET auto_explain.log_buffers=on;
            SET auto_explain.log_nested_statements=on; SET plan_cache_mode=force_generic_plan;
            SET auto_explain.log_format=json; BEGIN;
            DO $probe$
            DECLARE relations oid[]; entries_before bigint; entries_after bigint;
                blocks_before bigint; blocks_after bigint; selected integer;
            BEGIN
                SELECT array_agg(oid) INTO relations FROM (
                    SELECT 'public.tasks'::regclass::oid AS oid
                    UNION ALL SELECT indexrelid FROM pg_index WHERE indrelid='public.tasks'::regclass
                ) inputs;
                FOR turn IN 1..8 LOOP
                    SELECT sum(pg_stat_get_xact_tuples_returned(oid)),
                        sum(pg_stat_get_xact_blocks_fetched(oid))
                        INTO entries_before,blocks_before FROM unnest(relations) oid;
                    selected := public.hubuum_schedule_task(gen_random_uuid(),
                        ARRAY['import','export','backup','reindex','remote_call','schema_validation','schema_profiling'],120000);
                    SELECT sum(pg_stat_get_xact_tuples_returned(oid)),
                        sum(pg_stat_get_xact_blocks_fetched(oid))
                        INTO entries_after,blocks_after FROM unnest(relations) oid;
                    RAISE NOTICE 'HUBUUM_SCHEDULER_WORK %', json_build_object(
                        'turn',turn,'selected',selected IS NOT NULL,
                        'task_index_entries_and_sequential_rows',entries_after-entries_before,
                        'task_table_and_index_block_requests',blocks_after-blocks_before);
                END LOOP;
            END $probe$;
            ROLLBACK;"""
        result = subprocess.run(["docker", "exec", self.containers[0], "psql", "-U", "postgres",
                                 "-d", database, "-v", "ON_ERROR_STOP=1", "-c", statement],
                                text=True, capture_output=True, timeout=300)
        self.args.output.joinpath(name + "-plan.log").write_text(result.stdout + result.stderr)
        if result.returncode:
            raise RuntimeError("Candidate plan probe failed; inspect its retained log")

    def binary(self, binary, arguments, name, *, worker=False, worker_process=0):
        container = self.name + "-" + name
        self.containers.append(container)
        binary = binary.resolve()
        executable = "/executables/" + binary.name
        command = ["run", "--name", container, "--network", self.name,
                   "--security-opt", "label=disable",
                   "--mount", f"type=bind,src={binary.parent},dst=/executables,readonly",
                   "--mount", f"type=bind,src={ROOT / 'scale-benchmarks'},dst=/fixtures,readonly",
                   "--mount", f"type=bind,src={self.args.output.resolve()},dst=/reports",
                   "--entrypoint", "/usr/bin/taskset" if worker and self.cpu_masks else executable]
        if worker:
            command += self.limits(2, "512m")
        mask = self.cpu_masks["workers"][worker_process] if worker and self.cpu_masks else None
        prefix = ["--cpu-list", mask, executable] if mask else []
        resources = {"samples": 0, "peak_rss_bytes": 0, "peak_hwm_bytes": 0, "cpu_affinity": mask}
        errors = []
        stop = threading.Event()

        def observe():
            try:
                pid = None
                while not stop.is_set():
                    if pid is None:
                        inspected = subprocess.run(["docker", "inspect", "--format", "{{.State.Pid}}", container],
                                                   text=True, capture_output=True, timeout=10)
                        if inspected.returncode == 0:
                            pid = int(inspected.stdout.strip()) or None
                    if pid:
                        try:
                            # The OCI process can still be taskset before exec.
                            # Begin verification only after the measured binary starts.
                            if Path(os.readlink(f"/proc/{pid}/exe")).name != binary.name:
                                stop.wait(0.02)
                                continue
                            if "kernel_memory_max_bytes" not in resources:
                                ceiling = self.docker("exec", container, "cat", "/sys/fs/cgroup/memory.max", timeout=10).strip()
                                if ceiling != str(512 * 1024 ** 2):
                                    raise ValueError("Worker kernel memory ceiling was not applied")
                                resources["kernel_memory_max_bytes"] = int(ceiling)
                                quota = self.docker("exec", container, "cat", "/sys/fs/cgroup/cpu.max", timeout=10).strip()
                                resources["kernel_cpu_limit"] = cpu_limit(quota, None if mask or self.args.unlimited_cpu else 2)
                            measured = process_resources(pid, mask)
                        except (FileNotFoundError, ProcessLookupError):
                            break
                        resources["samples"] += 1
                        resources["peak_rss_bytes"] = max(resources["peak_rss_bytes"], measured["VmRSS"])
                        resources["peak_hwm_bytes"] = max(resources["peak_hwm_bytes"], measured["VmHWM"])
                    stop.wait(0.02)
            except (OSError, ValueError, subprocess.SubprocessError) as error:
                errors.append(str(error))

        monitor = threading.Thread(target=observe) if worker else None
        if monitor:
            monitor.start()
        failure = None
        try:
            output = self.docker(*command, POSTGRES_IMAGE, *prefix, *arguments, timeout=7200)
        except (RuntimeError, subprocess.TimeoutExpired) as error:
            failure = error
        finally:
            stop.set()
            if monitor:
                monitor.join()
        if worker:
            inspected = json.loads(self.docker("inspect", container))[0]
            config = inspected["HostConfig"]
            if not self.args.unlimited_cpu and not self.cpu_masks and config["NanoCpus"] != 2_000_000_000:
                raise RuntimeError("Worker CPU limit was not applied")
            if config["Memory"] != 512 * 1024 ** 2:
                raise RuntimeError("Worker memory limit was not applied")
            self.args.output.joinpath(name + "-container.json").write_text(json.dumps({
                "limits": config, "state": inspected["State"], "resources": resources, "errors": errors,
            }, indent=2) + "\n")
            if errors or not resources["samples"]:
                raise RuntimeError("Worker resource monitoring failed; inspect its retained evidence")
            if resources["peak_rss_bytes"] > 512 * 1024 ** 2 or resources["peak_hwm_bytes"] > 512 * 1024 ** 2:
                raise RuntimeError("Worker exceeded the 512 MiB RSS/high-water ceiling")
        if failure:
            raise failure
        self.docker("rm", "--volumes", container)
        self.containers.remove(container)
        return output


def candidate_work_summary(rows, expected):
    selected = sum(row["selected"] for row in rows)
    if selected != expected or any(row["task_entries"] < 0 or row["task_blocks"] < 0 for row in rows):
        raise ValueError("Candidate-work evidence does not cover the measured claims")
    maximum = max((row["task_entries"] for row in rows), default=0)
    return {"recorded_calls": len(rows), "successful_claims": selected,
            "empty_calls": len(rows) - selected, "max_task_entries": maximum,
            "max_task_block_requests": max((row["task_blocks"] for row in rows), default=0),
            "candidate_limit_pass": bool(rows) and maximum <= 1024}


def failed_budgets(manifest):
    failures = []
    if not manifest["constraints_applied"]:
        failures.append("CPU resource constraints were not applied")
    expected = 25 if manifest["profile"] == "local" else 30
    if len(manifest["runs"]) != expected:
        failures.append(f"Expected {expected} complete repeats")
    for run in manifest["runs"]:
        checks = ["p3_pass", "promotion_spacing_valid"]
        if run["seconds"]:
            checks.append("p4_utilization_pass")
        elif run["backlog"] == 10000:
            checks.append("p4_start_share_pass")
        for field in checks:
            if run[field] is not True:
                failures.append(f"{run['name']}: {field}")
        if manifest["profile"] == "shared" and (
                run["database_round_trip_p95_us"] is None or run["database_round_trip_p95_us"] > 2000):
            failures.append(f"{run['name']}: database RTT p95 exceeds 2 ms or is missing")
    return failures


def run(args):
    args.output.mkdir(parents=True, exist_ok=False)
    manifest = {"profile": args.profile, "constraints_applied": not args.unlimited_cpu,
                "cpu_limits_applied": not args.unlimited_cpu, "memory_limits_applied": True,
                "cpu_enforcement": "affinity" if args.cpu_affinity else "quota" if not args.unlimited_cpu else "none",
                "host": platform.platform(), "host_logical_cpus": os.cpu_count(),
                "host_memory_bytes": os.sysconf("SC_PHYS_PAGES") * os.sysconf("SC_PAGE_SIZE"),
                "postgres_image": POSTGRES_IMAGE,
                "binary_sha256": hashlib.sha256(args.binary.read_bytes()).hexdigest(),
                "admin_binary_sha256": hashlib.sha256(args.admin_binary.read_bytes()).hexdigest(),
                "warmup": "100 excluded claims per process; timed runs warm the same pools and worker identities without simulated work delays, retaining separate samples",
                "runs": []}
    def retain():
        args.output.joinpath("manifest.json").write_text(json.dumps(manifest, indent=2) + "\n")
    retain()
    with Fixture(args) as fixture:
        manifest["cpu_affinity_plan"] = fixture.cpu_masks
        fixture.binary(args.admin_binary, ["--migrate", "--database-url", fixture.url("template_fixture")], "migrate")
        manifest["candidate_work_measurement"] = fixture.instrument_candidates("template_fixture")
        retain()
        workload = (ROOT / "scale-benchmarks/lifecycle/workload-v1.toml").read_text()
        # Leave at least 10k ordinary selections after bounded promotions.
        plans = [(1000, False, None, 1000), (10000, False, None, 12000),
                 (100000, False, None, 1000), (10000, True, None, 12000),
                 (10000, True, 60, 10000)]
        cycle_plan = len(plans)
        if args.profile == "shared":
            plans.append((10000, True, 90, 10000))
        for plan, (backlog, overdue, seconds, samples) in enumerate(plans):
            capacity_cycle = plan == cycle_plan
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
                    def execute(process):
                        report = f"{name}-worker-{process}.json"
                        cycle = ["--capacity-cycle"] if capacity_cycle and process == 0 else []
                        fixture.binary(args.binary, common + ["--database-url", fixture.url(database),
                            "--worker-process-slots", str(fixture.slots // processes),
                            "--samples", str(samples // processes), "--output", "/reports/" + report, *cycle],
                            f"{name}-worker-{process}", worker=True, worker_process=process)
                        return json.loads(args.output.joinpath(report).read_text())
                    with ThreadPoolExecutor(max_workers=processes) as executor:
                        reports = list(executor.map(execute, range(processes)))
                    resource_evidence = fixture.database_evidence(name)
                    # Probe the post-workload index state as well as the prepared
                    # template. Claim/replenishment churn can expose work that a
                    # pristine queue-head plan does not show.
                    fixture.candidate_plan(database, name)
                    record = aggregate(reports, timed=seconds is not None)
                    record["candidate_work"] = fixture.candidate_work(
                        database, name, sum(len(report["samples"]) +
                            len((report.get("timed_warmup") or {}).get("samples", [])) for report in reports))
                    record["candidate_bound_verified"] = record["candidate_work"]["candidate_limit_pass"]
                    record["p3_pass"] = record["p3_latency_pass"] and record["candidate_bound_verified"]
                    record["database_resources"] = resource_evidence
                    record.update(name=name, backlog=backlog, overdue=overdue, seconds=seconds, capacity_cycle=capacity_cycle)
                    manifest["runs"].append(record)
                    retain()
                    print(json.dumps(record), flush=True)
                finally:
                    fixture.sql("postgres", f"DROP DATABASE {database} WITH (FORCE)")
            fixture.sql("postgres", f"DROP DATABASE {template} WITH (FORCE)")
    # Enforce only after retaining every repeat, including failed budgets.
    failures = failed_budgets(manifest)
    manifest["budget_failures"] = failures
    retain()
    for failure in failures:
        print("Budget failure: " + failure, flush=True)
    return int(bool(failures) and args.check_budgets)


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--profile", choices=("local", "shared"), required=True)
    parser.add_argument("--binary", type=Path, default=ROOT / "target/release/hubuum-scale-benchmark")
    parser.add_argument("--admin-binary", type=Path, default=ROOT / "target/release/hubuum-admin")
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--check-budgets", action="store_true",
                        help="Fail after retaining all repeats if any numerical P3/P4 or profile budget fails")
    cpu = parser.add_mutually_exclusive_group()
    cpu.add_argument("--cpu-affinity", action="store_true", help="Use disjoint Linux CPU masks when CPU quotas are unavailable")
    cpu.add_argument("--unlimited-cpu", "--unconstrained", dest="unlimited_cpu", action="store_true",
                        help="Diagnostic mode: omit CPU limits, retain memory limits; does not establish resource-profile acceptance")
    args = parser.parse_args(argv)
    if platform.system() != "Linux":
        parser.error("Scheduling measurements require Linux with access to the Docker host PID namespace")
    return run(args)
