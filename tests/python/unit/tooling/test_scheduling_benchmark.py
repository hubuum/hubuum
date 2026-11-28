"""Check cross-process aggregation without containers or a database."""
import contextlib
import io
import re
import unittest
from types import SimpleNamespace
from unittest.mock import patch

from integration.scheduling_benchmark import Fixture, affinity_plan, aggregate, candidate_work_summary, cpu_limit, cpu_set, database_resources, failed_budgets, main, process_memory, process_resources, timed_utilization

from support import ROOT


def sample(sequence, *, claim_us=1000, promoted=False, kind="import"):
    return {"sequence": sequence, "claim_us": claim_us, "lock_wait_us": 100,
            "round_trips": 5, "promoted": promoted, "kind": kind, "principal": sequence % 32}


def report(samples, *, warmup=0, utilization=0.0):
    return {"samples": samples, "warmup_claims": warmup, "failed_polls": [],
            "empty_polls": [], "occupied_slot_fraction": utilization}


def timed_report(samples):
    result = report(samples)
    result.update(options={"slots": 4}, measurement_clock_bound_us=0,
                  measurement_window={"start": "2026-10-30T00:00:00", "end": "2026-10-30T00:01:30"},
                  capacity_changes=[{"started_us": 30_000_000, "committed_us": 30_000_000, "configured_slots": 1},
                                    {"started_us": 60_000_000, "committed_us": 60_000_000, "configured_slots": 4}])
    return result


class SchedulingReportTests(unittest.TestCase):
    def test_candidate_limit_includes_unsuccessful_searches(self):
        rows = [{"selected": True, "task_entries": 4, "task_blocks": 10},
                {"selected": False, "task_entries": 1025, "task_blocks": 2000}]
        self.assertFalse(candidate_work_summary(rows, 1)["candidate_limit_pass"])

    def test_candidate_evidence_cannot_omit_a_successful_claim(self):
        with self.assertRaisesRegex(ValueError, "does not cover"):
            candidate_work_summary([{"selected": True, "task_entries": 4, "task_blocks": 10}], 2)

    def test_candidate_exact_limit_is_accepted(self):
        self.assertTrue(candidate_work_summary(
            [{"selected": True, "task_entries": 1024, "task_blocks": 1200}], 1)["candidate_limit_pass"])

    def test_kernel_cpu_quota_matches_the_declared_capacity(self):
        self.assertEqual(cpu_limit("400000 100000", 4), {"quota_us": 400000, "period_us": 100000})

    def test_requested_cpu_capacity_does_not_replace_kernel_enforcement(self):
        for raw in ("max 100000", "200000 100000", "400000 0"):
            with self.subTest(raw=raw), self.assertRaises(ValueError):
                cpu_limit(raw, 4)

    def test_database_cpu_limit_requires_kernel_evidence(self):
        with self.assertRaisesRegex(ValueError, "Missing PostgreSQL kernel CPU limit"):
            database_resources("memory_max=2048\nmemory_peak=1024\nmemory_event_max=0\nmemory_event_oom=0\nmemory_event_oom_kill=0\ncpu_mask=2-3\n", 2048, None, 2)

    def test_non_linux_measurement_fails_before_creating_a_fixture(self):
        error = io.StringIO()
        with patch("integration.scheduling_benchmark.platform.system", return_value="Darwin"), \
                patch("integration.scheduling_benchmark.run") as run, \
                contextlib.redirect_stderr(error), self.assertRaises(SystemExit) as failure:
            main(["--profile", "shared", "--output", "unused-report"])
        self.assertEqual(failure.exception.code, 2)
        self.assertIn("Docker host PID namespace", error.getvalue())
        run.assert_not_called()

    def test_database_latency_evidence_retains_the_slowest_worker(self):
        fast = dict(report([sample(1)]), database_round_trip_us=[100] * 100)
        slow = dict(report([sample(2)]), database_round_trip_us=[3000] * 100)
        self.assertEqual(aggregate([fast, slow])["database_round_trip_p95_us"], 3000)

    def test_capacity_cycle_utilization_caps_overcommitted_busy_time(self):
        measured = timed_report([dict(sample(index), busy_offset_us=0, busy_us=90_000_000)
                                 for index in range(3)])
        result = timed_utilization([measured])
        self.assertLessEqual(result["fraction_lower_bound"], 1)
        self.assertAlmostEqual(result["eligible_slot_us_upper_bound"] / 1_000_000, 210, places=5)

    def test_drain_overcommit_cannot_hide_idle_capacity_after_growth(self):
        measured = timed_report([dict(sample(index), busy_offset_us=0, busy_us=60_000_000)
                                 for index in range(3)])
        self.assertLess(timed_utilization([measured])["fraction_lower_bound"], 0.60)

    def test_capacity_cycle_rejects_competing_controllers(self):
        with self.assertRaisesRegex(ValueError, "one controller"):
            timed_utilization([timed_report([]), timed_report([])])

    def test_clock_uncertainty_cannot_establish_a_nonexistent_common_window(self):
        measured = timed_report([])
        measured["measurement_clock_bound_us"] = 100_000_000
        with self.assertRaisesRegex(ValueError, "do not overlap"):
            timed_utilization([measured])

    def test_database_kernel_limits_and_descendant_affinity_are_verified(self):
        result = database_resources("memory_max=2048\nmemory_peak=1024\nmemory_event_max=0\nmemory_event_oom=0\nmemory_event_oom_kill=0\ncpu_mask=2-3\ncpu_mask=2\n", 2048, "2,3")
        self.assertEqual(result["threads_observed"], 2)

    def test_database_kernel_reclaim_peak_is_retained(self):
        raw = "memory_max=2048\nmemory_peak=3072\ncpu_mask=2\n"
        raw += "memory_event_max=10\nmemory_event_oom=0\nmemory_event_oom_kill=0\n"
        result = database_resources(raw, 2048, "2,3")
        self.assertEqual(result["memory_peak_over_limit_bytes"], 1024)
        self.assertEqual(result["memory_events"]["max"], 10)

    def test_database_out_of_memory_events_fail_verification(self):
        for event in ("oom", "oom_kill"):
            with self.subTest(event=event):
                raw = "memory_max=2048\nmemory_peak=2048\ncpu_mask=2\n"
                raw += "memory_event_max=10\n"
                raw += "".join(f"memory_event_{key}={int(key == event)}\n" for key in ("oom", "oom_kill"))
                with self.assertRaisesRegex(ValueError, "out-of-memory"):
                    database_resources(raw, 2048, "2,3")

    def test_database_missing_memory_events_fail_verification(self):
        with self.assertRaisesRegex(ValueError, "memory events"):
            database_resources("memory_max=2048\nmemory_peak=1024\ncpu_mask=2\n", 2048, "2,3")

    def test_database_requested_memory_without_enforcement_is_rejected(self):
        with self.assertRaisesRegex(ValueError, "ceiling was not applied"):
            database_resources("memory_max=max\nmemory_peak=1024\nmemory_event_max=0\nmemory_event_oom=0\nmemory_event_oom_kill=0\ncpu_mask=2\n", 2048, "2,3")

    def test_database_descendant_cannot_escape_affinity(self):
        with self.assertRaisesRegex(ValueError, "escaped"):
            database_resources("memory_max=2048\nmemory_peak=1024\nmemory_event_max=0\nmemory_event_oom=0\nmemory_event_oom_kill=0\ncpu_mask=2-4\n", 2048, "2,3")

    def test_affinity_plan_uses_distinct_database_and_worker_cpus(self):
        with patch("integration.scheduling_benchmark.os.sched_getaffinity", return_value=set(range(8)), create=True):
            self.assertEqual(affinity_plan("shared"), {"database": "0,1,2,3", "workers": ["4,5", "6,7"]})

    def test_affinity_plan_rejects_insufficient_allowed_cpus(self):
        with patch("integration.scheduling_benchmark.os.sched_getaffinity", return_value={0, 1, 2}, create=True):
            with self.assertRaisesRegex(ValueError, "Not enough"):
                affinity_plan("local")

    def test_affinity_mask_expands_ranges(self):
        self.assertEqual(cpu_set("2-4,7"), {2, 3, 4, 7})

    def test_process_memory_preserves_kernel_high_water(self):
        self.assertEqual(process_memory("VmRSS:\t10 kB\nVmHWM:\t20 kB\n"),
                         {"VmRSS": 10240, "VmHWM": 20480})

    def test_process_memory_rejects_missing_high_water(self):
        with self.assertRaisesRegex(ValueError, "Missing"):
            process_memory("VmRSS:\t10 kB\n")

    def test_exited_process_is_not_invalid_memory_evidence(self):
        with self.assertRaises(ProcessLookupError):
            process_memory("State:\tZ (zombie)\n")

    def test_address_space_exit_race_ends_resource_sampling(self):
        with patch("integration.scheduling_benchmark.Path.read_text", return_value="State:\tR (running)\n"):
            with patch("integration.scheduling_benchmark.os.readlink", side_effect=FileNotFoundError):
                with self.assertRaises(ProcessLookupError):
                    process_resources(123, None)

    def test_live_process_missing_memory_evidence_still_fails(self):
        with patch("integration.scheduling_benchmark.Path.read_text", return_value="State:\tR (running)\n"):
            with patch("integration.scheduling_benchmark.os.readlink", return_value="/executables/worker"):
                with self.assertRaisesRegex(ValueError, "Missing"):
                    process_resources(123, None)

    def test_unlimited_cpu_keeps_the_profile_memory_limit(self):
        fixture = Fixture(SimpleNamespace(profile="shared", unlimited_cpu=True))
        self.assertEqual(fixture.limits(2, "512m"), ["--memory", "512m"])

    def test_global_commit_order_controls_warmup_across_processes(self):
        reports = [report([sample(1, claim_us=900000), sample(4)], warmup=1),
                   report([sample(2, claim_us=900000), sample(3)], warmup=1)]
        result = aggregate(reports)
        self.assertEqual((result["claims"], result["claim_p95_us"]), (2, 1000))

    def test_timed_warmup_is_retained_outside_measured_latency(self):
        current = report([sample(101)])
        current["timed_warmup"] = {"samples": [sample(index, claim_us=900000) for index in range(1, 101)]}
        result = aggregate([current], timed=True)
        self.assertEqual((result["claims"], result["timed_warmup_claims"], result["claim_p95_us"]),
                         (1, 100, 1000))
        self.assertTrue(result["p3_latency_pass"])

    def test_timed_runs_require_complete_warmup_evidence(self):
        self.assertFalse(aggregate([report([sample(1)])], timed=True)["p3_latency_pass"])

    def test_timed_warmup_cannot_hide_promotion_spacing(self):
        current = report([sample(8, promoted=True)])
        current["timed_warmup"] = {"samples": [sample(1, promoted=True)]}
        self.assertFalse(aggregate([current], timed=True)["promotion_spacing_valid"])

    def test_duplicate_committed_turns_are_rejected(self):
        with self.assertRaisesRegex(ValueError, "duplicate"):
            aggregate([report([sample(1)]), report([sample(1)])])

    def test_latency_tail_cannot_be_hidden_by_fast_majority(self):
        samples = [sample(index, claim_us=200000 if index > 90 else 1000)
                   for index in range(1, 101)]
        result = aggregate([report(samples)])
        self.assertFalse(result["p3_latency_pass"])
        self.assertEqual(result["claims"], 100)

    def test_promotions_crossing_process_boundaries_still_obey_spacing(self):
        result = aggregate([report([sample(8, promoted=True)]),
                            report([sample(9, promoted=True)])])
        self.assertFalse(result["promotion_spacing_valid"])

    def test_weighted_ordinary_starts_exclude_promotions(self):
        samples = []
        ordinary = 0
        for sequence in range(1, 12001):
            promoted = sequence % 8 == 0
            kind = "import" if promoted or ordinary % 4 < 3 else "export"
            samples.append(sample(sequence, promoted=promoted, kind=kind))
            ordinary += not promoted
        result = aggregate([report(samples[::2]), report(samples[1::2])])
        self.assertTrue(result["p4_start_share_pass"])
        self.assertFalse(result["candidate_bound_verified"])

    def test_short_samples_cannot_establish_start_share_acceptance(self):
        result = aggregate([report([sample(index) for index in range(100)])])
        self.assertIsNone(result["p4_start_share_pass"])

    def test_promotions_do_not_count_towards_the_ordinary_sample_requirement(self):
        result = aggregate([report([sample(index, promoted=index % 8 == 0)
                                    for index in range(1, 10001)])])
        self.assertIsNone(result["p4_start_share_pass"])

    def test_process_utilization_contributions_use_the_shared_pool_denominator(self):
        result = aggregate([report([sample(1)], utilization=0.5),
                            report([sample(2)], utilization=0.4)], timed=True)
        self.assertTrue(result["p4_utilization_pass"])
        self.assertAlmostEqual(result["occupied_slot_fraction"], 0.9)

    def test_shared_cap_occupancy_is_not_double_counted(self):
        first = report([sample(1)])
        second = report([sample(2)])
        first["principal_cap_blocked_us"] = {"1": 1000, "2": 300}
        second["principal_cap_blocked_us"] = {"1": 1200, "2": 250}
        self.assertEqual(aggregate([first, second])["principal_cap_blocked_us"],
                         {"1": 1200, "2": 300})


class SchedulingBudgetTests(unittest.TestCase):
    def manifest(self):
        run = {"name": "repeat", "seconds": None, "backlog": 10000,
               "p3_pass": True, "promotion_spacing_valid": True,
               "p4_start_share_pass": True, "p4_utilization_pass": None,
               "database_round_trip_p95_us": 1000}
        return {"profile": "shared", "constraints_applied": True,
                "runs": [dict(run, name=f"repeat-{index}") for index in range(30)]}

    def test_complete_passing_profile_has_no_budget_failures(self):
        self.assertEqual(failed_budgets(self.manifest()), [])

    def test_one_failed_repeat_cannot_be_hidden_by_passing_repeats(self):
        manifest = self.manifest()
        manifest["runs"][-1]["p3_pass"] = False
        self.assertEqual(failed_budgets(manifest), ["repeat-29: p3_pass"])

    def test_missing_applicable_share_measurement_fails(self):
        manifest = self.manifest()
        manifest["runs"][0]["p4_start_share_pass"] = None
        self.assertEqual(failed_budgets(manifest), ["repeat-0: p4_start_share_pass"])

    def test_missing_repeats_and_unconstrained_runs_cannot_pass(self):
        manifest = self.manifest()
        manifest.update(constraints_applied=False, runs=[])
        self.assertEqual(len(failed_budgets(manifest)), 2)

    def test_shared_rtt_budget_is_enforced(self):
        manifest = self.manifest()
        manifest["runs"][0]["database_round_trip_p95_us"] = 2001
        self.assertIn("RTT", failed_budgets(manifest)[0])


class SchedulingWorkflowTests(unittest.TestCase):
    def enabled(self, event="pull_request", action="synchronize", labels=(), label=""):
        workflow = (ROOT / ".github/workflows/scheduler-benchmarks.yml").read_text()
        build = workflow.split("  build:\n", 1)[1].split("  measure:\n", 1)[0]
        expression = re.search(r"    if: >-\n(.*?)    runs-on:", build, re.S).group(1)
        for source, target in (("github.event.pull_request.labels.*.name", "labels"),
                               ("github.event.label.name", "label"),
                               ("github.event.action", "action"), ("github.event_name", "event")):
            expression = expression.replace(source, target)
        expression = " ".join(expression.split()).replace("&&", "and").replace("||", "or")
        return eval(expression, {"__builtins__": {}},
                    {"event": event, "action": action, "labels": labels, "label": label,
                     "contains": lambda values, value: value in values})

    def test_normal_prs_and_generic_labels_do_not_start_measurements(self):
        for labels in ((), ("ci:full",), ("ci:benchmarks",), ("ci:scale-large",)):
            with self.subTest(labels=labels):
                self.assertFalse(self.enabled(labels=labels))
        self.assertFalse(self.enabled(event="push", labels=("ci:scheduler",)))

    def test_explicit_label_and_manual_dispatch_start_measurements(self):
        for action in ("opened", "synchronize", "reopened", "labeled"):
            with self.subTest(action=action):
                self.assertTrue(self.enabled(action=action, labels=("ci:scheduler",), label="ci:scheduler"))
        self.assertTrue(self.enabled(event="workflow_dispatch"))

    def test_unrelated_label_changes_do_not_restart_measurements(self):
        for action in ("labeled", "unlabeled"):
            with self.subTest(action=action):
                self.assertFalse(self.enabled(action=action, labels=("ci:scheduler",), label="documentation"))
        self.assertFalse(self.enabled(action="unlabeled", label="ci:scheduler"))

    def test_measurements_require_the_opt_in_build_and_retain_failed_reports(self):
        workflow = (ROOT / ".github/workflows/scheduler-benchmarks.yml").read_text()
        measure = workflow.split("  measure:\n", 1)[1]
        self.assertIn("    needs: build\n", measure)
        self.assertIn("      fail-fast: false\n", measure)
        self.assertIn('sudo "$(command -v python3)" -I -S', measure)
        self.assertIn("--check-budgets", measure)
        self.assertIn("        if: always()\n", measure)
