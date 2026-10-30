"""Cache maintenance is bounded to benchmark namespaces and obsolete entries."""

from datetime import datetime, timezone
import unittest

from support import ROOT, load_script


cache = load_script("benchmark-cache.py")


class BenchmarkCacheTests(unittest.TestCase):
    def entry(self, identity, ref, *, accessed="2026-10-01T00:00:00Z", namespace=None):
        return {"id": identity, "ref": ref, "last_accessed_at": accessed,
                "key": cache.namespace_prefix(namespace or cache.NAMESPACES[0]) + f"executables-1-{identity}",
                "size_in_bytes": 100}

    def candidates(self, entries, closed=(), **options):
        return cache.candidates(entries, closed, now=datetime(2026, 10, 20, tzinfo=timezone.utc), **options)

    def test_closed_pr_entries_are_removable_but_other_workflows_are_not(self):
        entries = [self.entry(1, "refs/pull/5/merge"), self.entry(2, "refs/pull/5/merge", namespace="other-project")]
        self.assertEqual([entry["id"] for entry in self.candidates(entries, closed=[5])], [1])

    def test_current_main_entries_are_protected(self):
        entries = [self.entry(index, "refs/heads/main") for index in range(6)]
        self.assertEqual(len(self.candidates(entries, keep_main=4)), 2)

    def test_active_pr_retains_only_bounded_recent_entries(self):
        entries = [self.entry(index, "refs/pull/8/merge", accessed="2026-10-19T00:00:00Z") for index in range(5)]
        self.assertEqual(len(self.candidates(entries, max_per_pr=2)), 3)

    def test_unknown_cache_kinds_and_non_pr_branches_are_not_deleted(self):
        unknown = self.entry(1, "refs/heads/main")
        unknown["key"] = cache.namespace_prefix(cache.NAMESPACES[0]) + "unknown-kind"
        self.assertEqual(self.candidates([unknown, self.entry(2, "refs/heads/feature")]), [])

    def test_pr_cache_writers_cannot_displace_shared_main_entries(self):
        for name in ("ci.yml", "benchmarks.yml", "regex-fuzz.yml", "scale-benchmarks.yml"):
            with self.subTest(workflow=name):
                workflow = (ROOT / ".github/workflows" / name).read_text()
                self.assertEqual(workflow.count("uses: Swatinem/rust-cache@"),
                                 workflow.count("save-if: ${{ github.event_name != 'pull_request' }}"))
        workflow = (ROOT / ".github/workflows/ci.yml").read_text()
        self.assertIn("cache-to: ${{ github.event_name != 'pull_request' && 'type=gha,mode=max,scope=container-amd64-production' || '' }}", workflow)
