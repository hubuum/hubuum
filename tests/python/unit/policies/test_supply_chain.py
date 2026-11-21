"""Regression tests for supply-chain policy validation."""

from __future__ import annotations

from contextlib import redirect_stderr
from pathlib import Path
import hashlib
import io
import os
import subprocess
import tempfile
import unittest

from support import load_script, SCRIPTS


SCRIPT = SCRIPTS / "check-supply-chain-policy.py"
INSTALL_DIESEL = SCRIPTS / "install-diesel-cli.sh"
INSTALL_SEMVER_CHECKS = SCRIPTS / "install-cargo-semver-checks.sh"
POLICY = load_script(SCRIPT.name)
SEMVER_CHECKS_VERSION = POLICY.parse_tool_manifest(
    SCRIPT.parent.parent / ".github" / "supply-chain-tools.env"
)["CARGO_SEMVER_CHECKS_VERSION"]
CARGO_DENY_VERSION = POLICY.parse_tool_manifest(
    SCRIPT.parent.parent / ".github" / "supply-chain-tools.env"
)["CARGO_DENY_VERSION"]

DIGEST = "a" * 64
VALID_VALUES = {
    "CARGO_DENY_VERSION": "0.22.0",
    "CARGO_SEMVER_CHECKS_VERSION": "0.51.0",
    "DIESEL_CLI_VERSION": "2.3.14",
    "POSTGRES_WINDOWS_VERSION": "18.7-2",
    "POSTGRES_WINDOWS_SHA256": DIGEST,
    "SYFT_IMAGE": f"anchore/syft:v1.57.2@sha256:{DIGEST}",
    "TRIVY_IMAGE": f"aquasec/trivy:0.76.2@sha256:{DIGEST}",
    "COSIGN_VERSION": "v3.1.4",
}


class ToolManifestTests(unittest.TestCase):
    def parse(self, content: str) -> dict[str, str]:
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "tools.env"
            path.write_text(content, encoding="utf-8")
            return POLICY.parse_tool_manifest(path)

    def assert_policy_error(self, callback) -> None:
        with redirect_stderr(io.StringIO()), self.assertRaises(SystemExit):
            callback()

    def test_valid_values_are_accepted(self) -> None:
        POLICY.validate_tool_values(VALID_VALUES.copy())

    def test_duplicate_keys_are_rejected(self) -> None:
        content = "COSIGN_VERSION=v3.1.2\nCOSIGN_VERSION=v3.1.4\n"
        self.assert_policy_error(lambda: self.parse(content))

    def test_shell_active_image_value_is_rejected(self) -> None:
        values = VALID_VALUES.copy()
        values["SYFT_IMAGE"] = f"$(id)@sha256:{DIGEST}"
        self.assert_policy_error(lambda: POLICY.validate_tool_values(values))

    def test_manifest_whitespace_is_rejected(self) -> None:
        self.assert_policy_error(lambda: self.parse(" COSIGN_VERSION=v3.1.2\n"))

    def test_invalid_postgres_archive_pins_are_rejected(self) -> None:
        for key, value in (
            ("POSTGRES_WINDOWS_VERSION", "18"),
            ("POSTGRES_WINDOWS_VERSION", "18.6"),
            ("POSTGRES_WINDOWS_VERSION", "18.7-2/../../other"),
            ("POSTGRES_WINDOWS_SHA256", "a" * 63),
            ("POSTGRES_WINDOWS_SHA256", "g" * 64),
        ):
            with self.subTest(key=key, value=value):
                values = VALID_VALUES | {key: value}
                self.assert_policy_error(lambda: POLICY.validate_tool_values(values))


class CargoDenyWorkspaceTests(unittest.TestCase):
    def run_manifest(self, manifest, status=0):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            binary = root / "bin"
            binary.mkdir()
            cargo = binary / "cargo"
            cargo.write_text(
                "#!/bin/sh\n"
                'if [ "$1" = deny ] && [ "$2" = --version ]; then\n'
                f"  echo 'cargo-deny {CARGO_DENY_VERSION}'\n"
                "else\n"
                '  printf "%s\\0" "$@" > "$CARGO_DENY_TEST_ARGUMENTS"\n'
                '  exit "${CARGO_DENY_TEST_STATUS:-0}"\n'
                "fi\n",
                encoding="utf-8",
            )
            cargo.chmod(0o755)
            deny = binary / "cargo-deny"
            deny.write_text("#!/bin/sh\nexit 0\n", encoding="utf-8")
            deny.chmod(0o755)
            arguments = root / "arguments"
            environment = os.environ.copy()
            environment["PATH"] = str(binary) + os.pathsep + environment["PATH"]
            environment["CARGO_DENY_TEST_ARGUMENTS"] = str(arguments)
            environment["CARGO_DENY_TEST_STATUS"] = str(status)
            command = ["bash", str(SCRIPTS / "run-cargo-deny.sh"), str(root / "report")]
            if manifest is not None:
                command.append(str(manifest))
            result = subprocess.run(
                command, cwd=SCRIPTS.parent, env=environment,
                text=True, capture_output=True, check=False,
            )
            return result, arguments.read_bytes().decode().rstrip("\0").split("\0")

    def test_only_the_standalone_fuzz_audit_permits_root_only_exceptions(self):
        for manifest, standalone in (
            (None, False),
            ("Cargo.toml", False),
            (SCRIPTS.parent / "Cargo.toml", False),
            ("fuzz/Cargo.toml", True),
            (SCRIPTS.parent / "fuzz" / "Cargo.toml", True),
            ("crates/hubuum-budget/Cargo.toml", False),
        ):
            with self.subTest(manifest=manifest):
                result, arguments = self.run_manifest(manifest)
                self.assertEqual(result.returncode, 0, result.stderr)
                self.assertEqual("--warn" in arguments, standalone)
                if standalone:
                    self.assertEqual(arguments[-2:], ["--warn", "advisory-not-detected"])

    def test_fuzz_audit_preserves_scanner_failure_status(self):
        result, _ = self.run_manifest("fuzz/Cargo.toml", status=23)
        self.assertEqual(result.returncode, 23)


class DieselVersionTests(unittest.TestCase):
    def parse(self, description: str) -> subprocess.CompletedProcess[str]:
        return subprocess.run(
            [
                "bash",
                "-c",
                'source "$1"; parse_diesel_cli_version',
                "bash",
                str(INSTALL_DIESEL),
            ],
            input=description,
            text=True,
            capture_output=True,
            check=False,
        )

    def test_current_multiline_description_is_parsed(self) -> None:
        result = self.parse(
            "diesel \n Version: 2.3.14\n Supported Backends: postgres\n"
        )

        self.assertEqual(result.returncode, 0)
        self.assertEqual(result.stdout, "2.3.14\n")

    def test_legacy_single_line_description_is_parsed(self) -> None:
        result = self.parse("diesel 2.2.12\n")

        self.assertEqual(result.returncode, 0)
        self.assertEqual(result.stdout, "2.2.12\n")

    def test_ambiguous_description_is_rejected(self) -> None:
        result = self.parse("diesel 2.3.10\n Version: 2.3.14\n")

        self.assertNotEqual(result.returncode, 0)

    def test_missing_version_is_rejected(self) -> None:
        result = self.parse("diesel\n Supported Backends: postgres\n")

        self.assertNotEqual(result.returncode, 0)


class SemverChecksInstallerTests(unittest.TestCase):
    def test_records_installed_version_and_executable_digest(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            binary = root / "bin" / "cargo-semver-checks"
            binary.parent.mkdir()
            binary.write_text(
                "#!/usr/bin/env bash\n"
                f"echo 'cargo-semver-checks {SEMVER_CHECKS_VERSION}'\n",
                encoding="utf-8",
            )
            binary.chmod(0o755)
            summary = root / "summary.md"
            environment = os.environ.copy()
            environment["CARGO_HOME"] = str(root)
            environment["GITHUB_STEP_SUMMARY"] = str(summary)

            result = subprocess.run(
                [str(INSTALL_SEMVER_CHECKS)],
                text=True,
                capture_output=True,
                check=False,
                env=environment,
            )

            digest = hashlib.sha256(binary.read_bytes()).hexdigest()
            self.assertEqual(result.returncode, 0, result.stderr)
            version_label = f"cargo-semver-checks {SEMVER_CHECKS_VERSION}"
            self.assertIn(version_label, result.stdout)
            self.assertIn(f"cargo-semver-checks sha256:{digest}", result.stdout)
            self.assertEqual(
                summary.read_text(encoding="utf-8"),
                f"- cargo-semver-checks: `{version_label}`\n"
                f"- cargo-semver-checks executable: `sha256:{digest}`\n",
            )


class CargoDenyRunnerTests(unittest.TestCase):
    def test_only_standalone_fuzz_graph_warns_about_unused_root_exceptions(self) -> None:
        version = POLICY.parse_tool_manifest(
            SCRIPT.parent.parent / ".github" / "supply-chain-tools.env"
        )["CARGO_DENY_VERSION"]
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            arguments = root / "arguments"
            cargo = root / "cargo"
            cargo.write_text(
                "#!/usr/bin/env bash\n"
                "if [[ $* == 'deny --version' ]]; then\n"
                f"  echo 'cargo-deny {version}'\n"
                "else\n"
                "  printf '%s\\n' \"$@\" > \"$CAPTURE_DENY_ARGS\"\n"
                "fi\n",
                encoding="utf-8",
            )
            cargo.chmod(0o755)
            (root / "cargo-deny").symlink_to(cargo)
            environment = os.environ.copy()
            environment["PATH"] = f"{root}{os.pathsep}{environment['PATH']}"
            environment["CAPTURE_DENY_ARGS"] = str(arguments)
            environment.pop("GITHUB_STEP_SUMMARY", None)
            for manifest in (None, "Cargo.toml", "fuzz/Cargo.toml", str(SCRIPTS.parent / "fuzz/Cargo.toml")):
                with self.subTest(manifest=manifest):
                    command = ["bash", str(SCRIPTS / "run-cargo-deny.sh"), str(root / "report")]
                    if manifest is not None:
                        command.append(manifest)
                    result = subprocess.run(command, env=environment, capture_output=True, text=True)
                    self.assertEqual(result.returncode, 0, result.stderr)
                    values = arguments.read_text(encoding="utf-8").splitlines()
                    self.assertEqual("--warn" in values, manifest is not None and "fuzz/" in manifest)
                    self.assertNotIn("--allow", values)
                    self.assertIn("--locked", values)
                    self.assertIn("--all-features", values)
