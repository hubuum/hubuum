"""Check cross-process aggregation without containers or a database."""
import unittest
from types import SimpleNamespace

from integration.scheduling_benchmark import Fixture, aggregate


def sample(sequence, *, claim_us=1000, promoted=False, kind="import"):
    return {"sequence": sequence, "claim_us": claim_us, "lock_wait_us": 100,
            "round_trips": 5, "promoted": promoted, "kind": kind, "principal": sequence % 32}


def report(samples, *, warmup=0, utilization=0.0):
    return {"samples": samples, "warmup_claims": warmup, "failed_polls": [],
            "empty_polls": [], "occupied_slot_fraction": utilization}


class SchedulingReportTests(unittest.TestCase):
    def test_unlimited_cpu_keeps_the_profile_memory_limit(self):
        fixture = Fixture(SimpleNamespace(profile="shared", unlimited_cpu=True))
        self.assertEqual(fixture.limits(2, "512m"), ["--memory", "512m"])

    def test_global_commit_order_controls_warmup_across_processes(self):
        reports = [report([sample(1, claim_us=900000), sample(4)], warmup=1),
                   report([sample(2, claim_us=900000), sample(3)], warmup=1)]
        result = aggregate(reports)
        self.assertEqual((result["claims"], result["claim_p95_us"]), (2, 1000))

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
