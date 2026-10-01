"""Regression coverage for reviewed enum CHECK widening; standard library only."""

import unittest
import hashlib

from support import load_script


MODULE = load_script('check-migration-check-replacements.py')
BASELINE = "ALTER TABLE tasks ADD CONSTRAINT tasks_kind_check CHECK (kind IN ('import', 'export'));"
CANDIDATE = """ALTER TABLE tasks DROP CONSTRAINT tasks_kind_check; -- hubuum-compat: widen-enum-check
ALTER TABLE tasks ADD CONSTRAINT tasks_kind_check CHECK (kind IN ('import', 'export', 'schema_validation')) NOT VALID;
ALTER TABLE tasks VALIDATE CONSTRAINT tasks_kind_check;
"""


class ReplacementTests(unittest.TestCase):
    def test_widening_preserves_every_baseline_value(self):
        self.assertEqual(MODULE.approved_replacements([BASELINE], CANDIDATE), {"TASKS:TASKS_KIND_CHECK"})

    def test_unrelated_multi_column_additions_preserve_existing_check(self):
        later = "ALTER TABLE tasks ADD COLUMN attempts INT, ADD CONSTRAINT attempts_positive CHECK (attempts >= 0);"
        self.assertEqual(MODULE.approved_replacements([BASELINE, later], CANDIDATE), {"TASKS:TASKS_KIND_CHECK"})

    def test_unsafe_or_unproven_replacements_are_rejected(self):
        cases = {
            "narrowing": CANDIDATE.replace("'import', 'export', ", ""),
            "wrong_column": CANDIDATE.replace("CHECK (kind", "CHECK (name"),
            "no_marker": CANDIDATE.replace(" -- hubuum-compat: widen-enum-check", ""),
            "no_validation": CANDIDATE.replace("ALTER TABLE tasks VALIDATE CONSTRAINT tasks_kind_check;", ""),
            "blocking_add": CANDIDATE.replace(" NOT VALID", ""),
            "arbitrary_expression": CANDIDATE.replace("')) NOT VALID", "') OR true) NOT VALID"),
            "wrong_table": CANDIDATE.replace("TABLE tasks", "TABLE widgets"),
            "unknown_constraint": CANDIDATE.replace("tasks_kind_check", "unknown_check"),
            "commented_out": "/*\n" + CANDIDATE + "*/",
        }
        for name, candidate in cases.items():
            with self.subTest(name=name):
                self.assertEqual(MODULE.approved_replacements([BASELINE], candidate), set())

    def test_unknown_or_changed_baseline_cannot_supply_proof(self):
        cases = {
            "nonliteral": [BASELINE.replace("kind IN ('import', 'export')", "length(kind) > 0")],
            "later_change": [BASELINE, BASELINE.replace("'export'", "'backup'")],
            "later_drop": [BASELINE, "ALTER TABLE tasks DROP CONSTRAINT tasks_kind_check;"],
            "no_baseline": [],
        }
        for name, baseline in cases.items():
            with self.subTest(name=name):
                self.assertEqual(MODULE.approved_replacements(baseline, CANDIDATE), set())


class OfflineReviewTests(unittest.TestCase):
    def review(self, content):
        return {"schema_version": 1, "reviews": [{"migration": "up.sql", "sha256": hashlib.sha256(content.encode()).hexdigest(), "reason": "test", "upgrade_action": "stop writers", "drop_constraints": {"deliveries": ["normal_unique"]}}]}

    def test_exact_review_permits_only_named_constraint(self):
        content = "ALTER TABLE deliveries DROP CONSTRAINT normal_unique;"
        self.assertEqual(MODULE.approved_offline_drops(self.review(content), "up.sql", content), {"OFFLINE:DELIVERIES:NORMAL_UNIQUE"})

    def test_changed_sql_requires_review(self):
        with self.assertRaises(ValueError):
            MODULE.approved_offline_drops(self.review("old"), "up.sql", "new")

    def test_other_migration_has_no_permission(self):
        self.assertEqual(MODULE.approved_offline_drops(self.review("old"), "other.sql", "old"), set())

    def test_operator_action_is_required(self):
        review = self.review("sql")
        review["reviews"][0]["upgrade_action"] = ""
        with self.assertRaises(ValueError):
            MODULE.approved_offline_drops(review, "up.sql", "sql")


if __name__ == "__main__":
    unittest.main()
