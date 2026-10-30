"""Performance policy is independent of required correctness validation."""

import unittest
from pathlib import Path
import tempfile

from support import ROOT, load_script


policy = load_script("benchmark-policy.py")
lanes = load_script("benchmark-lanes.py")


class BenchmarkPolicyTests(unittest.TestCase):
    def test_validation_label_does_not_force_draft_benchmarks(self):
        actual = policy.resolve(["src/models/object.rs"], labels=["ci:validate"], draft=True)
        self.assertEqual(actual["scopes"], [])

    def test_force_all_labels_preserve_scope_and_minimum_effort(self):
        for label in ("ci:full", "ci:benchmarks"):
            with self.subTest(label=label):
                actual = policy.resolve(["README.md"], draft=True,
                                        labels=[label, "ci:bench:profile:quick", "ci:bench:tag:query"])
                self.assertEqual(actual["scopes"], sorted(policy.load_policy()["scopes"]))
                self.assertEqual(actual["profile"], "standard")
                self.assertEqual(actual["tags"], [])

    def test_each_scope_is_independently_requestable(self):
        for scope in policy.load_policy()["scopes"]:
            with self.subTest(scope=scope):
                actual = policy.resolve(["src/unknown.rs"], draft=True,
                                        labels=[f"ci:bench:scope:{scope}"])
                self.assertEqual(actual["scopes"], [scope])

    def test_explicit_tags_and_scopes_are_additive(self):
        actual = policy.resolve(["src/unknown.rs"], labels=["ci:bench:tag:query", "ci:bench:scope:resources"])
        self.assertEqual(actual["scopes"], ["micro", "resources"])

    def test_query_change_avoids_postgres_runtime_and_resources(self):
        actual = policy.resolve(["crates/hubuum-query/src/parse.rs"])
        self.assertEqual(actual["scopes"], ["micro"])
        self.assertEqual(actual["tags"], ["query"])

    def test_unknown_and_shared_inputs_expand_conservatively(self):
        for path in ("src/new_subsystem.rs", "unknown-build-input", "Cargo.lock", ".github/benchmark-policy.json"):
            with self.subTest(path=path):
                self.assertEqual(policy.resolve([path])["scopes"], sorted(policy.load_policy()["scopes"]))

    def test_docs_select_no_performance_work(self):
        actual = policy.resolve(["README.md", "docs/testing.md"])
        self.assertEqual(actual["scopes"], [])

    def test_draft_transition_and_label_removal_restore_default(self):
        paths = ["crates/hubuum-query/src/parser.rs"]
        self.assertEqual(policy.resolve(paths, draft=True)["scopes"], [])
        self.assertEqual(policy.resolve(paths, draft=False)["scopes"], ["micro"])
        self.assertEqual(policy.resolve(paths, draft=True, labels=["ci:bench:profile:thorough"])["scopes"], ["micro"])

    def test_strongest_profile_wins(self):
        actual = policy.resolve([], labels=["ci:bench:profile:quick", "ci:bench:profile:thorough"],
                                manual={"scopes": "postgres"})
        self.assertEqual(actual["profile"], "thorough")
        self.assertEqual(actual["scopes"], ["postgres"])

    def test_unknown_explicit_selectors_fail(self):
        for label in ("ci:bench:scope:standard", "ci:bench:tag:postgres", "ci:bench:profile:turbo"):
            with self.subTest(label=label), self.assertRaisesRegex(ValueError, "Unknown"):
                policy.resolve([], labels=[label])

    def test_workflow_ignores_unrelated_labels_without_cancelling_work(self):
        workflow = (ROOT / ".github/workflows/benchmarks.yml").read_text()
        self.assertIn("!startsWith(github.event.label.name, 'ci:bench:')", workflow)
        self.assertIn("startsWith(github.event.label.name, 'ci:bench:')", workflow)
        self.assertIn("run_resources == 'true'", workflow)
        self.assertIn("has_targets == 'true'", workflow)

    def test_manual_dispatch_preserves_cache_warming_by_default(self):
        workflow = (ROOT / ".github/workflows/benchmarks.yml").read_text()
        warm_input = workflow.split("      warm_only:\n", 1)[1].split("  pull_request:\n", 1)[0]
        self.assertIn("default: true", warm_input)


class TargetSelectionTests(unittest.TestCase):
    def setUp(self):
        self.specs = [
            {"name": "parse_query_parameter_callgrind", "bench": "parse_query_parameter_callgrind", "backend": "gungraun"},
            {"name": "storage_postgres_criterion", "bench": "storage_postgres_criterion", "backend": "criterion", "required_features": ["postgres-bench"]},
        ]

    def test_selected_targets_keep_full_build_catalog(self):
        actual = lanes.select_lanes(self.specs, policy.resolve([], labels=["ci:bench:tag:query"]), policy.load_policy())
        self.assertEqual([item["name"] for item in actual["include"]], ["micro"])
        self.assertEqual(actual["include"][0]["selected_targets"], ["gungraun:Cargo.toml:parse_query_parameter_callgrind"])

    def test_new_untagged_target_runs_when_its_input_changes(self):
        self.specs.append({"name": "new_criterion", "bench": "new_criterion", "backend": "criterion"})
        actual = lanes.select_lanes(self.specs, policy.resolve(["benches/new_criterion.rs"]), policy.load_policy())
        self.assertEqual(actual["include"][0]["selected_targets"], ["criterion:Cargo.toml:new_criterion"])
        self.assertEqual(len(actual["include"][0]["benchmarks"]), 2)

    def test_unknown_shared_benchmark_input_keeps_both_scopes(self):
        actual = lanes.select_lanes(self.specs, policy.resolve(["benches/unknown-support/data.json"]), policy.load_policy())
        self.assertEqual(len(actual["include"]), 2)

    def test_moved_target_can_be_selected_by_old_path(self):
        self.specs[0].update(base={"repo_crate": "crates/old", "bench": "parse_query_parameter_callgrind"}, moved=True)
        selection = policy.resolve(["crates/old/benches/parse_query_parameter_callgrind.rs"])
        actual = lanes.select_lanes(self.specs, selection, policy.load_policy())
        self.assertEqual(len(actual["include"]), 1)

    def test_empty_selection_schedules_no_targets(self):
        self.assertEqual(lanes.select_lanes(self.specs, policy.resolve(["README.md"]), policy.load_policy()), {"include": []})

    def test_legacy_group_budgets_apply_to_both_revisions_with_visible_fallback(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "benches").mkdir()
            (root / "benches/old_criterion.rs").write_text(
                "group.sample_size(30);\ngroup.measurement_time(Duration::from_secs(10));\n")
            benchmark = {"name": "old_criterion", "bench": "old_criterion", "backend": "criterion"}
            lanes.configure_profiles(benchmark, policy.load_policy(), root)
            args = benchmark["profiles"]["quick"]["criterion_args"]
            self.assertIn("--sample-size 30", args)
            self.assertIn("--measurement-time 10", args)
            self.assertIn("Legacy base", benchmark["profile_notes"][0])

    def test_quick_case_filters_are_repository_owned(self):
        benchmark = {"name": "schema_validation_criterion", "bench": "schema_validation_criterion", "backend": "criterion"}
        lanes.configure_profiles(benchmark, policy.load_policy())
        self.assertTrue(benchmark["profiles"]["quick"]["criterion_args"].endswith("16384"))
        self.assertNotIn("16384", benchmark["profiles"]["standard"]["criterion_args"])
