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
        (self.root / "src").mkdir()
        (self.root / "src" / "lib.rs").write_text("", encoding="utf-8")
        (self.root / "crates" / "member" / "src").mkdir()
        (self.root / "crates" / "member" / "src" / "lib.rs").write_text(
            "", encoding="utf-8"
        )

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

    def write_policy_document(self, path: str = "docs/member.md") -> None:
        policy_path = self.root / path
        policy_path.parent.mkdir(parents=True, exist_ok=True)
        policy_path.write_text("# Member policy\n", encoding="utf-8")

    def test_internal_workspace_is_accepted(self) -> None:
        self.write_workspace()

        result = self.run_checker()

        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn("passed for 2 package(s)", result.stdout)

    def test_internal_package_must_disable_publishing(self) -> None:
        self.write_workspace(member_publish="true")

        result = self.run_checker()

        self.assertNotEqual(result.returncode, 0)
        self.assertIn("must disable publishing", result.stderr)

    def test_implicit_cargo_workspace_member_is_validated(self) -> None:
        self.write_workspace()
        implicit = self.root / "crates" / "implicit"
        (implicit / "src").mkdir(parents=True)
        (implicit / "src" / "lib.rs").write_text("", encoding="utf-8")
        (implicit / "Cargo.toml").write_text(
            textwrap.dedent(
                """
                [package]
                name = "implicit"
                version = "0.0.1"
                edition = "2024"
                publish = false
                """
            ),
            encoding="utf-8",
        )
        with (self.root / "Cargo.toml").open("a", encoding="utf-8") as manifest:
            manifest.write(
                '\n[dependencies]\nimplicit = { path = "crates/implicit" }\n'
            )

        result = self.run_checker()

        self.assertNotEqual(result.returncode, 0)
        self.assertIn("implicit must set package.metadata.hubuum.rust-api", result.stderr)

    def test_public_package_requires_policy_and_is_selected(self) -> None:
        self.write_workspace(
            member_status="stable-public",
            member_publish="true",
            member_policy='policy-document = "docs/member.md"',
        )
        self.write_policy_document()

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
        self.assertIn("must enable publishing", result.stderr)

    def test_public_package_cannot_use_an_empty_publish_allowlist(self) -> None:
        self.write_workspace(
            member_status="experimental-public",
            member_publish="[]",
            member_policy='policy-document = "docs/member.md"',
        )
        self.write_policy_document()

        result = self.run_checker()

        self.assertNotEqual(result.returncode, 0)
        self.assertIn("must enable publishing", result.stderr)

    def test_public_package_accepts_a_non_empty_publish_allowlist(self) -> None:
        self.write_workspace(
            member_status="experimental-public",
            member_publish='["crates-io"]',
            member_policy='policy-document = "docs/member.md"',
        )
        self.write_policy_document()

        result = self.run_checker("--supported-packages")

        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(result.stdout, "member\n")

    def test_public_package_policy_document_must_exist(self) -> None:
        self.write_workspace(
            member_status="stable-public",
            member_publish="true",
            member_policy='policy-document = "docs/missing.md"',
        )

        result = self.run_checker()

        self.assertNotEqual(result.returncode, 0)
        self.assertIn("policy-document does not exist as a file", result.stderr)

    def test_public_package_policy_document_must_be_a_file(self) -> None:
        self.write_workspace(
            member_status="stable-public",
            member_publish="true",
            member_policy='policy-document = "docs/member"',
        )
        (self.root / "docs" / "member").mkdir(parents=True)

        result = self.run_checker()

        self.assertNotEqual(result.returncode, 0)
        self.assertIn("policy-document does not exist as a file", result.stderr)

    def test_public_package_policy_document_must_stay_in_repository(self) -> None:
        outside_path = self.root.parent / f"{self.root.name}-outside.md"
        self.write_workspace(
            member_status="stable-public",
            member_publish="true",
            member_policy=f'policy-document = "../{outside_path.name}"',
        )
        outside_path.write_text("policy\n", encoding="utf-8")
        self.addCleanup(outside_path.unlink, missing_ok=True)

        result = self.run_checker()

        self.assertNotEqual(result.returncode, 0)
        self.assertIn("must remain inside the repository", result.stderr)

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
