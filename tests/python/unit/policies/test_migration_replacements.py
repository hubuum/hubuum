"""Regression coverage for reviewed enum CHECK widening; standard library only."""

import unittest
import hashlib
import re
from pathlib import Path

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

    def test_approved_schema_qualification_preserves_table_identity(self):
        for table, expected in (
            ("public.deliveries", "OFFLINE:DELIVERIES:NORMAL_UNIQUE"),
            ("hubuum_control.deliveries", "OFFLINE:HUBUUM_CONTROL.DELIVERIES:NORMAL_UNIQUE"),
        ):
            with self.subTest(table=table):
                content = f"ALTER TABLE {table} DROP CONSTRAINT normal_unique;"
                review = self.review(content)
                review["reviews"][0]["drop_constraints"] = {table: ["normal_unique"]}
                self.assertEqual(MODULE.approved_offline_drops(review, "up.sql", content), {expected})

    def test_arbitrary_schema_and_multipart_identifiers_are_rejected(self):
        for table in ("audit.deliveries", "public.hubuum_control.deliveries", "hubuum_control.deliveries.extra"):
            with self.subTest(table=table):
                content = f"ALTER TABLE {table} DROP CONSTRAINT normal_unique;"
                review = self.review(content)
                review["reviews"][0]["drop_constraints"] = {table: ["normal_unique"]}
                with self.assertRaises(ValueError):
                    MODULE.approved_offline_drops(review, "up.sql", content)

    def test_managed_deletion_format_widening_preserves_every_old_predicate(self):
        migrations = Path(MODULE.__file__).resolve().parents[1] / "crates/hubuum-storage-postgres/migrations"
        original = (migrations / "2026-10-29-000002_deletion_authority/up.sql").read_text()
        replacement = (migrations / "2026-10-29-000005_managed_deletion_cleanup/up.sql").read_text()
        old_check = original.split("material JSONB NOT NULL CHECK ", 1)[1].split(");", 1)[0]
        new_check = replacement.split("ADD CONSTRAINT deletion_authority_material_check CHECK ", 1)[1].split(" NOT VALID;", 1)[0]
        widened = old_check.replace("material->>'format' = '1'", "material->>'format' IN ('1', '2')")
        self.assertEqual(re.sub(r"\s+", "", new_check), re.sub(r"\s+", "", widened))

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
