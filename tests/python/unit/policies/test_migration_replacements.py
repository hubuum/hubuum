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
INLINE_BASELINE = """CREATE TABLE public.tasks (
    id SERIAL PRIMARY KEY,
    kind VARCHAR NOT NULL CHECK (
        kind IN ('import', 'export')
    ),
    description TEXT
);
"""


class ReplacementTests(unittest.TestCase):
    def test_widening_preserves_every_baseline_value(self):
        self.assertEqual(MODULE.approved_replacements([BASELINE], CANDIDATE), {"TASKS:TASKS_KIND_CHECK"})

    def test_unrelated_multi_column_additions_preserve_existing_check(self):
        later = "ALTER TABLE tasks ADD COLUMN attempts INT, ADD CONSTRAINT attempts_positive CHECK (attempts >= 0);"
        self.assertEqual(MODULE.approved_replacements([BASELINE, later], CANDIDATE), {"TASKS:TASKS_KIND_CHECK"})

    def test_inline_create_table_check_supplies_its_default_name(self):
        self.assertEqual(MODULE.approved_replacements([INLINE_BASELINE], CANDIDATE), {"TASKS:TASKS_KIND_CHECK"})

    def test_inline_check_still_requires_strict_widening(self):
        for candidate in (CANDIDATE.replace("'import', 'export', ", ""), CANDIDATE.replace("CHECK (kind", "CHECK (name")):
            with self.subTest(candidate=candidate):
                self.assertEqual(MODULE.approved_replacements([INLINE_BASELINE], candidate), set())

    def test_ambiguous_or_unsupported_inline_checks_fail_closed(self):
        long_table = "tasks" * 13
        cases = {
            "named": INLINE_BASELINE.replace("NOT NULL CHECK", "NOT NULL CONSTRAINT chosen_name CHECK"),
            "multiple_checks": INLINE_BASELINE.replace("    ),", "    ) CHECK (kind <> 'other'),"),
            "different_column": INLINE_BASELINE.replace("kind IN", "description IN"),
            "arbitrary_expression": INLINE_BASELINE.replace("kind IN ('import', 'export')", "kind IN ('import', 'export') OR true"),
            "explicit_name_collision": INLINE_BASELINE.replace("description TEXT", "description TEXT, CONSTRAINT tasks_kind_check CHECK (id > 0)"),
            "truncated_name": INLINE_BASELINE.replace("tasks", long_table),
        }
        for name, baseline in cases.items():
            with self.subTest(name=name):
                candidate = CANDIDATE.replace("tasks", long_table) if name == "truncated_name" else CANDIDATE
                self.assertEqual(MODULE.approved_replacements([baseline], candidate), set())

    def test_later_schema_changes_invalidate_inline_proof_in_sql_order(self):
        changes = (
            "ALTER TABLE tasks DROP CONSTRAINT tasks_kind_check;",
            BASELINE.replace("'export'", "'backup'"),
            "ALTER TABLE tasks RENAME COLUMN kind TO previous_kind;",
            "DROP TABLE tasks;",
        )
        for change in changes:
            with self.subTest(change=change):
                self.assertEqual(MODULE.approved_replacements([INLINE_BASELINE + change], CANDIDATE), set())
        # A later recreated table owns its new column CHECK, not an earlier one.
        self.assertEqual(MODULE.approved_replacements([BASELINE.replace("'export'", "'backup'") + "\nDROP TABLE tasks;\n" + INLINE_BASELINE], CANDIDATE), {"TASKS:TASKS_KIND_CHECK"})

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
