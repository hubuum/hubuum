"""Exercise CI tier selection and protect the main publication gate."""

import os
from pathlib import Path
import re
import subprocess
import tempfile
import textwrap
import unittest

from support import ROOT


WORKFLOW = (ROOT / ".github/workflows/ci.yml").read_text()


def job(name):
    return re.split(r"\n  [\w-]+:\n", WORKFLOW.split(f"\n  {name}:\n", 1)[1], 1)[0]


class CiTierTests(unittest.TestCase):
    def policy(self, **changes):
        script = job("changes").split("      - name: Select CI tier\n", 1)[1]
        script = textwrap.dedent(script.split("        run: |\n", 1)[1])
        variables = dict.fromkeys(("CODE_CHANGED", "RUST_CHANGED", "CONTAINER_CHANGED",
                                  "OPENAPI_CHANGED", "OPERATIONAL_CONTRACT_CHANGED",
                                  "MARKDOWN_CHANGED", "IS_DRAFT", "FORCE_FULL"), "false")
        variables.update(IS_PULL_REQUEST="true", **changes)
        with tempfile.TemporaryDirectory() as directory:
            output = Path(directory) / "outputs"
            subprocess.run(["bash", "-euo", "pipefail", "-c", script], check=True,
                           env={**os.environ, **variables, "GITHUB_OUTPUT": str(output)})
            return dict(line.split("=", 1) for line in output.read_text().splitlines())

    def test_deployment_edit_keeps_container_and_script_validation(self):
        result = self.policy(CODE_CHANGED="true", CONTAINER_CHANGED="true")
        self.assertEqual({key: result[key] for key in (
            "run_full_ci", "run_container", "run_rust_ci", "run_default_suite", "run_openapi",
        )}, {"run_full_ci": "true", "run_container": "true", "run_rust_ci": "false",
            "run_default_suite": "false", "run_openapi": "false"})

    def test_rust_edit_runs_full_matrix(self):
        result = self.policy(CODE_CHANGED="true", RUST_CHANGED="true")
        self.assertEqual(result["run_rust_ci"], "true")
        self.assertIn("all-features", result["feature_matrix"])

    def test_draft_rust_edit_keeps_default_suite(self):
        result = self.policy(CODE_CHANGED="true", RUST_CHANGED="true", IS_DRAFT="true")
        self.assertEqual(result["run_default_suite"], "true")
        self.assertEqual(result["run_rust_ci"], "false")

    def test_full_label_overrides_path_and_draft_selection(self):
        result = self.policy(FORCE_FULL="true", IS_DRAFT="true")
        for key in ("run_rust_ci", "run_full_ci", "run_container", "run_default_suite"):
            self.assertEqual(result[key], "true", key)


class PublicationGateTests(unittest.TestCase):
    def test_candidates_can_build_before_validation_but_cannot_publish(self):
        for name in ("build-main-linux-artifacts", "build-main-native-artifacts"):
            with self.subTest(job=name):
                block = job(name)
                self.assertNotIn("ci-gate", block)
                self.assertIn("needs.changes.result == 'success'", block)
                self.assertIn("name: candidate-main-", block)
                self.assertNotIn("name: main-", block)
        promotion = job("publish-main-artifacts")
        self.assertIn("needs: [ci-gate, build-main-linux-artifacts, build-main-native-artifacts]", promotion)
        self.assertNotIn("always()", promotion)
        self.assertIn("needs.ci-gate.result == 'success'", promotion)
        self.assertIn("name: main-", promotion)

    def test_gate_still_requires_deployment_and_container_checks(self):
        gate = job("ci-gate")
        self.assertIn("      - deployment-checks\n", gate)
        self.assertIn("      - deployment-build-contracts\n", gate)
        self.assertIn("      - container-build\n", gate)

    def test_deployment_inputs_still_run_rust_container_contracts(self):
        contracts = job("deployment-build-contracts")
        self.assertIn("needs.changes.outputs.run_container == 'true'", contracts)
        self.assertIn("needs.changes.outputs.run_default_suite != 'true'", contracts)
        self.assertIn("cargo test --bin hubuum-server --locked", contracts)
