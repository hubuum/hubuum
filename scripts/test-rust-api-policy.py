#!/usr/bin/env python3
"""Regression tests for the Rust API policy checker."""

from __future__ import annotations

import subprocess
import tempfile
import textwrap
import unittest
from pathlib import Path


SCRIPT = Path(__file__).with_name("check-rust-api-policy.py")


class RustApiPolicyTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary_directory = tempfile.TemporaryDirectory()
        self.root = Path(self.temporary_directory.name)
        (self.root / "crates" / "member").mkdir(parents=True)

    def tearDown(self) -> None:
        self.temporary_directory.cleanup()

    def write_workspace(
        self,
        *,
        root_status: str = "internal-application",
        root_publish: str = "false",
        member_status: str = "workspace-internal",
        member_publish: str = "false",
        member_policy: str = "",
    ) -> None:
        (self.root / "Cargo.toml").write_text(
            textwrap.dedent(
                f"""
                [package]
                name = "root"
                version = "0.0.1"
                edition = "2024"
                publish = {root_publish}

                [package.metadata.hubuum]
                rust-api = "{root_status}"

                [workspace]
                members = ["crates/member"]
                """
            ),
            encoding="utf-8",
        )
        (self.root / "crates" / "member" / "Cargo.toml").write_text(
            textwrap.dedent(
                f"""
                [package]
                name = "member"
                version = "0.0.1"
                edition = "2024"
                publish = {member_publish}

                [package.metadata.hubuum]
                rust-api = "{member_status}"
                {member_policy}
                """
            ),
            encoding="utf-8",
        )

    def run_checker(self, *arguments: str) -> subprocess.CompletedProcess[str]:
        return subprocess.run(
            [str(SCRIPT), "--root", str(self.root), *arguments],
            check=False,
            capture_output=True,
            text=True,
        )

    def test_internal_workspace_is_accepted(self) -> None:
        self.write_workspace()

        result = self.run_checker()

        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn("passed for 2 package(s)", result.stdout)

    def test_internal_package_must_disable_publishing(self) -> None:
        self.write_workspace(member_publish="true")

        result = self.run_checker()

        self.assertNotEqual(result.returncode, 0)
        self.assertIn("must set publish = false", result.stderr)

    def test_public_package_requires_policy_and_is_selected(self) -> None:
        self.write_workspace(
            member_status="stable-public",
            member_publish="true",
            member_policy='policy-document = "docs/member.md"',
        )

        result = self.run_checker("--supported-packages")

        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(result.stdout, "member\n")

    def test_public_package_cannot_disable_publishing(self) -> None:
        self.write_workspace(
            member_status="experimental-public",
            member_publish="false",
            member_policy='policy-document = "docs/member.md"',
        )

        result = self.run_checker()

        self.assertNotEqual(result.returncode, 0)
        self.assertIn("cannot set publish = false", result.stderr)

    def test_unknown_classification_is_rejected(self) -> None:
        self.write_workspace(member_status="accidentally-public")

        result = self.run_checker()

        self.assertNotEqual(result.returncode, 0)
        self.assertIn("must set package.metadata.hubuum.rust-api", result.stderr)

    def test_root_must_remain_internal_application(self) -> None:
        self.write_workspace(
            root_status="workspace-internal",
            root_publish="false",
        )

        result = self.run_checker()

        self.assertNotEqual(result.returncode, 0)
        self.assertIn("root hubuum package must remain", result.stderr)


if __name__ == "__main__":
    unittest.main()
