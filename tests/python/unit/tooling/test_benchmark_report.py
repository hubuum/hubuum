"""Final reports include late/failing work and read artifacts only as data."""

import io
import json
import unittest
from unittest.mock import patch
import zipfile

from support import load_script


report = load_script("benchmark-report.py")
github = load_script("benchmark_github.py")


class BenchmarkReportTests(unittest.TestCase):
    def setUp(self):
        self.run = {"html_url": "https://example.test/run", "head_sha": "abc", "conclusion": "failure",
                    "created_at": "2026-10-01T00:00:00Z", "run_attempt": 1, "run_number": 8}
        self.selection = {"scopes": ["micro", "postgres", "runtime", "resources"], "tags": ["query"],
                          "profile": "standard", "reasons": ["explicit selection"]}

    def job(self, name, conclusion="success", end="2026-10-01T00:01:00Z"):
        return {"name": name, "status": "completed", "conclusion": conclusion,
                "started_at": "2026-10-01T00:00:00Z", "completed_at": end, "steps": []}

    def test_final_report_includes_late_postgres_and_other_groups(self):
        jobs = [self.job("Benchmarks (micro) / benchmark"),
                self.job("Benchmarks (postgres) / benchmark", "failure", "2026-10-01T00:03:00Z"),
                self.job("Runtime behavior validation", "cancelled"),
                self.job("Template and schema resource evidence")]
        body = report.render(self.run, jobs, [("selection.json", self.selection)], [])
        self.assertIn("| postgres | failed | 3.00 min |", body)
        self.assertIn("| runtime | cancelled |", body)
        self.assertIn("| resources | passed |", body)
        self.assertIn("6.00 runner minutes", body)
        self.assertIn("3.00 minutes", body)
        self.assertIn("Queue time: unavailable", body)

    def test_missing_evidence_is_not_a_success(self):
        body = report.render(self.run, [], [], [])
        self.assertIn("Selection evidence unavailable", body)
        self.assertIn("| postgres | unavailable |", body)

    def test_duplicate_artifact_copies_do_not_duplicate_work(self):
        record = {"operation_id": "one", "label": "unique-operation", "compile_seconds": 10}
        records = [("performance.jsonl", record), ("performance.jsonl", dict(record))]
        body = report.render(self.run, [], records, [])
        self.assertEqual(body.count("unique-operation"), 1)

    def test_measurement_names_are_escaped_and_profiles_visible(self):
        result = {"benchmark_name": "<b>|bad", "feature_name": "default [quick]", "backend": "criterion", "delta_pct": 4}
        body = report.render(self.run, [], [("result.json", result)], [])
        self.assertIn("&lt;b&gt;&#124;bad", body)
        self.assertIn("default [quick]", body)

    def test_older_run_cannot_overwrite_newer_or_changed_head(self):
        pr = {"head": {"sha": "abc"}}
        self.assertFalse(report.current_run(self.run, pr, [{**self.run, "run_number": 9, "status": "in_progress"}]))
        self.assertFalse(report.current_run(self.run, {"head": {"sha": "different"}}, []))
        self.assertFalse(report.current_run(self.run, pr, [{**self.run, "run_attempt": 2}]))
        self.assertTrue(report.current_run(self.run, pr, [self.run]))

    def test_archive_is_read_without_extracting_executable_content(self):
        data = io.BytesIO()
        with zipfile.ZipFile(data, "w") as archive:
            archive.writestr("selection.json", json.dumps(self.selection))
            archive.writestr("payload.py", "raise RuntimeError('must not run')")
        self.assertEqual(report.read_evidence(data.getvalue()), [("selection.json", self.selection)])

    def test_ignored_label_run_cannot_replace_or_fence_previous_results(self):
        useful = {**self.run, "id": 8, "status": "completed"}
        ignored = {**useful, "id": 9, "run_number": 9}
        jobs = lambda run: [{"conclusion": "skipped" if run["id"] == 9 else "success"}]
        self.assertEqual(report.latest_participating_run(ignored, [ignored, useful], jobs), useful)

    def test_newer_active_run_defers_the_final_comment(self):
        old = {**self.run, "id": 8, "status": "completed"}
        new = {**old, "id": 9, "run_number": 9, "status": "in_progress"}
        self.assertIsNone(report.latest_participating_run(old, [old, new], lambda run: []))

    def test_legacy_bot_report_is_migrated_without_touching_user_comments(self):
        user = {"id": 1, "user": {"login": "person"}, "body": "<!-- rust-pr-bench -->"}
        legacy = {"id": 2, "user": {"login": "github-actions[bot]"}, "body": "<!-- rust-pr-bench -->"}
        current = {"id": 3, "user": {"login": "github-actions[bot]"}, "body": report.MARKER}
        self.assertEqual(report.report_comments([user, legacy, current]), [current, legacy])

    def test_archive_traversal_is_rejected(self):
        data = io.BytesIO()
        with zipfile.ZipFile(data, "w") as archive:
            archive.writestr("../selection.json", "{}")
        with self.assertRaisesRegex(ValueError, "path"):
            report.read_evidence(data.getvalue())

    def test_paginated_job_metadata_is_complete(self):
        with patch.object(github, "api", side_effect=[{"jobs": [{}] * 100}, {"jobs": [{}]}]) as request:
            self.assertEqual(len(github.pages("jobs?filter=latest", "jobs")), 101)
        self.assertIn("&per_page=100&page=2", request.call_args.args[0])
