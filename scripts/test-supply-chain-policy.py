#!/usr/bin/env python3
"""Regression tests for supply-chain policy validation."""

from __future__ import annotations

import importlib.util
import io
import subprocess
import tempfile
import unittest
from contextlib import redirect_stderr
from pathlib import Path


SCRIPT = Path(__file__).with_name("check-supply-chain-policy.py")
INSTALL_DIESEL = Path(__file__).with_name("install-diesel-cli.sh")
SPEC = importlib.util.spec_from_file_location("supply_chain_policy", SCRIPT)
if SPEC is None or SPEC.loader is None:
    raise RuntimeError(f"could not load {SCRIPT}")
POLICY = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(POLICY)

DIGEST = "a" * 64
VALID_VALUES = {
    "CARGO_DENY_VERSION": "0.20.2",
    "CARGO_SEMVER_CHECKS_VERSION": "0.49.0",
    "DIESEL_CLI_VERSION": "2.3.11",
    "SYFT_IMAGE": f"anchore/syft:v1.50.0@sha256:{DIGEST}",
    "TRIVY_IMAGE": f"aquasec/trivy:0.73.0@sha256:{DIGEST}",
    "COSIGN_VERSION": "v3.1.2",
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
        content = "COSIGN_VERSION=v3.1.2\nCOSIGN_VERSION=v3.1.3\n"
        self.assert_policy_error(lambda: self.parse(content))

    def test_shell_active_image_value_is_rejected(self) -> None:
        values = VALID_VALUES.copy()
        values["SYFT_IMAGE"] = f"$(id)@sha256:{DIGEST}"
        self.assert_policy_error(lambda: POLICY.validate_tool_values(values))

    def test_manifest_whitespace_is_rejected(self) -> None:
        self.assert_policy_error(lambda: self.parse(" COSIGN_VERSION=v3.1.2\n"))


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
            "diesel \n Version: 2.3.11\n Supported Backends: postgres\n"
        )

        self.assertEqual(result.returncode, 0)
        self.assertEqual(result.stdout, "2.3.11\n")

    def test_legacy_single_line_description_is_parsed(self) -> None:
        result = self.parse("diesel 2.2.12\n")

        self.assertEqual(result.returncode, 0)
        self.assertEqual(result.stdout, "2.2.12\n")

    def test_ambiguous_description_is_rejected(self) -> None:
        result = self.parse("diesel 2.3.10\n Version: 2.3.11\n")

        self.assertNotEqual(result.returncode, 0)

    def test_missing_version_is_rejected(self) -> None:
        result = self.parse("diesel\n Supported Backends: postgres\n")

        self.assertNotEqual(result.returncode, 0)


if __name__ == "__main__":
    unittest.main()
