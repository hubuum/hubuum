"""Keep feature-heavy PostgreSQL compilation out of the default bench lane."""

import unittest

from support import load_script


lanes = load_script("benchmark-lanes.py")


class BenchmarkLaneTests(unittest.TestCase):
    def test_every_discovered_target_is_assigned_once(self):
        targets = [
            {"name": "query_criterion", "backend": "criterion"},
            {"name": "query_callgrind", "backend": "gungraun"},
            {"name": "renamed_database_criterion", "required_features": ["postgres-bench"]},
        ]
        result = lanes.partition(targets)["include"]
        self.assertEqual(result, [
            {"name": "standard", "benchmarks": targets[:2]},
            {"name": "postgres", "benchmarks": targets[2:]},
        ])

    def test_moved_target_keeps_base_mapping_and_feature_requirements(self):
        target = {"name": "crates/storage/renamed_criterion", "moved": True,
                  "base": {"name": "old_criterion", "required_features": ["postgres-bench"]}}
        self.assertEqual(lanes.partition([target]), {"include": [
            {"name": "postgres", "benchmarks": [target]},
        ]})

    def test_empty_lane_is_not_scheduled(self):
        self.assertEqual(lanes.partition([{"name": "query_criterion"}]), {"include": [
            {"name": "standard", "benchmarks": [{"name": "query_criterion"}]},
        ]})
