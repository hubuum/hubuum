#!/usr/bin/env python3
"""Exercise the Git diff used by CI against disposable repositories."""

import os
from pathlib import Path
import subprocess
import tempfile
import unittest

SCRIPT = Path(__file__).resolve().with_name("ci-changed-paths.sh")


class DiffTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.git("init", "-q", "-b", "main")
        self.git("config", "user.email", "ci@example.invalid")
        self.git("config", "user.name", "CI test")
        self.git("config", "commit.gpgSign", "false")
        (self.root / "README.md").write_text("initial\n")
        self.base = self.commit()

    def git(self, *args):
        return subprocess.check_output(  # nosec B603,B607 - fixed git test commands
            ["git", *args], cwd=self.root, text=True
        ).strip()

    def commit(self):
        self.git("add", "--all")
        self.git("-c", "core.hooksPath=/dev/null", "commit", "-qm", "fixture")
        return self.git("rev-parse", "HEAD")

    def test_pull_request_uses_merge_base(self):
        self.git("checkout", "-qb", "docs")
        (self.root / "README.md").write_text("edited\n")
        head = self.commit()
        self.git("checkout", "-q", "main")
        (self.root / "application.rs").write_text("unrelated main change\n")
        base = self.commit()
        event = {"pull_request": {"base": {"sha": base}, "head": {"sha": head}}}
        self.assertEqual(self.paths("pull_request", event), ["README.md"])

    def test_rename_keeps_deleted_code_path(self):
        (self.root / "app.rs").write_text("code\n")
        base = self.commit()
        self.git("mv", "app.rs", "guide.md")
        head = self.commit()
        paths = self.paths("push", {"before": base, "after": head})
        self.assertEqual(paths, ["app.rs", "guide.md"])

    def test_deleted_document_is_docs_only(self):
        (self.root / "README.md").unlink()
        head = self.commit()
        paths = self.paths("push", {"before": self.base, "after": head})
        self.assertEqual(paths, ["README.md"])

    def test_merge_group_uses_group_diff(self):
        (self.root / "README.md").write_text("edited\n")
        head = self.commit()
        event = {"merge_group": {"base_sha": self.base, "head_sha": head}}
        self.assertEqual(self.paths("merge_group", event), ["README.md"])

    def test_initial_push_lists_all_inputs(self):
        self.assertEqual(
            self.paths("push", {"before": "0" * 40, "after": self.base}), ["README.md"]
        )

    def paths(self, event_name, event):
        if event_name == "pull_request":
            base = event["pull_request"]["base"]["sha"]
            head = event["pull_request"]["head"]["sha"]
        elif event_name == "merge_group":
            base = event["merge_group"]["base_sha"]
            head = event["merge_group"]["head_sha"]
        else:
            base, head = event["before"], event["after"]
        output = subprocess.check_output(
            ["bash", str(SCRIPT)],
            cwd=self.root,
            env={
                **os.environ,
                "BASE_SHA": base,
                "HEAD_SHA": head,
                "IS_PULL_REQUEST": str(event_name == "pull_request").lower(),
            },
            stderr=subprocess.PIPE,
        )
        return [os.fsdecode(path) for path in output.split(b"\0") if path]

    def test_failed_diff_cannot_report_docs_only(self):
        with self.assertRaises(subprocess.CalledProcessError):
            self.paths("push", {"before": "missing", "after": self.base})


if __name__ == "__main__":
    unittest.main()
