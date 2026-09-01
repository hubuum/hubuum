#!/usr/bin/env python3
"""Regression tests for the operational-contract compatibility policy."""

from __future__ import annotations

import copy
import hashlib
import json
import os
import subprocess
import tempfile
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parent.parent
CHECKER = ROOT / "scripts" / "check-operational-contract-compatibility.py"


def metric(name: str) -> dict[str, object]:
    return {
        "name": name,
        "kind": "counter",
        "unit": None,
        "labels": [{"name": "outcome", "bounded_by": "enumerated values", "values": ["ok", "error"]}],
        "histogram_buckets": [],
        "scope": "process",
        "description": "fixture",
        "feature": None,
    }


def contract() -> dict[str, object]:
    return {
        "schema_version": 1,
        "release": "1.0.0",
        "metrics": [metric("hubuum_fixture_total")],
        "configuration": [
            {
                "name": "HUBUUM_FIXTURE",
                "owner": "operations",
                "exposure": "public",
                "value_kind": "value",
                "default_is_set": True,
                "default": ["old"],
                "dynamic_default": None,
                "allowed_values": ["old", "new"],
                "minimum": None,
                "maximum": None,
                "runtime_roles": ["all", "api", "worker"],
                "appears_in_running_configuration": True,
                "source": "server",
                "dynamic_prefix": False,
            }
        ],
        "configuration_constraints": [],
        "events": {
            "schema_version": 1,
            "envelope_fields": [{"name": "id", "nullable": False}],
            "provenance_fields": [{"name": "actor", "nullable": False}],
            "sink_payload_fields": [{"name": "id", "nullable": False}],
            "schema_version_semantics": ["positive integer"],
            "actors": ["user"],
            "entities": [{"name": "object", "actions": ["created"]}],
            "redaction_rules": ["redact snapshots"],
            "audit_document_versions": [1],
        },
        "documents": {
            "backup": {
                "version": 1,
                "required_fields": ["backup_version", "state"],
                "optional_fields": ["history"],
                "sections": ["objects"],
                "rejection_policy": "reject unknown versions",
            },
            "import": {
                "version": 1,
                "required_fields": ["version", "graph"],
                "optional_fields": [],
                "sections": ["objects"],
                "rejection_policy": "reject unknown versions",
            },
            "export": {
                "scope_kinds": ["objects_in_class"],
                "content_types": ["application/json"],
                "missing_data_policies": ["strict"],
                "template_kinds": ["export"],
            },
        },
        "cli": [
            {
                "command": "hubuum-server",
                "options": [
                    {
                        "id": "fixture",
                        "long": "fixture",
                        "short": None,
                        "environment": "HUBUUM_FIXTURE",
                        "value_kind": "value",
                        "value_count": {"minimum": 1, "maximum": 1},
                        "default": ["old"],
                        "dynamic_default": None,
                        "allowed_values": ["old", "new"],
                        "required": False,
                        "conflicts_with": [],
                        "requires": [],
                    }
                ],
                "stable_output_modes": [],
                "exit_codes": {"success": 0},
            }
        ],
        "compatibility_policy": {},
    }


class PolicyTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.directory = Path(self.temporary.name)
        self.baseline = self.directory / "baseline.json"
        self.candidate = self.directory / "candidate.json"
        self.metadata = self.directory / "metadata.json"
        self.exceptions = self.directory / "exceptions.json"
        self.changelog = self.directory / "CHANGELOG.md"
        self.report = self.directory / "report"
        self.write_case(contract(), contract())

    def tearDown(self) -> None:
        self.temporary.cleanup()

    def write_json(self, path: Path, value: object) -> None:
        path.write_text(json.dumps(value, indent=2) + "\n", encoding="utf-8")

    def write_case(self, baseline: dict[str, object], candidate: dict[str, object]) -> None:
        self.write_json(self.baseline, baseline)
        self.write_json(self.candidate, candidate)
        self.write_json(
            self.metadata,
            {
                "status": "available",
                "tag": "v1.0.0",
                "source": "fixture",
                "sha256": hashlib.sha256(self.baseline.read_bytes()).hexdigest(),
                "reason": None,
                "candidate_ref": "test",
                "candidate_sha": "test",
            },
        )
        self.write_json(self.exceptions, {"schema_version": 1, "exceptions": []})
        self.changelog.write_text("# Changelog\n\n## [Unreleased]\n\n### Changed\n\n- fixture migration\n", encoding="utf-8")

    def run_checker(self) -> subprocess.CompletedProcess[str]:
        return subprocess.run(
            [
                str(CHECKER),
                str(self.baseline),
                str(self.candidate),
                str(self.metadata),
                str(self.exceptions),
                str(self.changelog),
                str(self.report),
            ],
            cwd=ROOT,
            env={**os.environ, "HUBUUM_OPERATIONAL_CONTRACT_TODAY": "2026-09-01"},
            check=False,
            text=True,
            capture_output=True,
        )

    def report_json(self) -> dict[str, object]:
        return json.loads((self.report / "compatibility.json").read_text(encoding="utf-8"))

    def test_identical_contract_passes(self) -> None:
        result = self.run_checker()
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(self.report_json()["status"], "passed")

    def test_additive_metric_passes(self) -> None:
        candidate = contract()
        candidate["metrics"].append(metric("hubuum_added_total"))
        self.write_case(contract(), candidate)
        result = self.run_checker()
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(self.report_json()["counts"]["additive"], 1)

    def test_removed_metric_fails(self) -> None:
        candidate = contract()
        candidate["metrics"] = []
        self.write_case(contract(), candidate)
        result = self.run_checker()
        self.assertEqual(result.returncode, 1)
        self.assertEqual(self.report_json()["counts"]["unaccepted"], 1)

    def test_changed_default_is_behavioral(self) -> None:
        candidate = contract()
        candidate["configuration"][0]["default"] = ["new"]
        candidate["cli"][0]["options"][0]["default"] = ["new"]
        self.write_case(contract(), candidate)
        result = self.run_checker()
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(self.report_json()["counts"]["behavioral"], 2)

    def test_changed_dynamic_default_is_behavioral(self) -> None:
        candidate = contract()
        dynamic_default = {
            "source": "available_parallelism",
            "divisor": 2,
            "rounding": "ceiling",
            "minimum": 1,
        }
        candidate["configuration"][0]["dynamic_default"] = dynamic_default
        candidate["cli"][0]["options"][0]["dynamic_default"] = dynamic_default
        self.write_case(contract(), candidate)
        result = self.run_checker()
        self.assertEqual(result.returncode, 0, result.stderr)
        kinds = {change["kind"] for change in self.report_json()["changes"]}
        self.assertEqual(
            kinds,
            {
                "configuration-dynamic-default-changed",
                "cli-option-dynamic_default-changed",
            },
        )

    def test_narrowed_configuration_range_fails(self) -> None:
        candidate = contract()
        candidate["configuration"][0]["minimum"] = 1
        self.write_case(contract(), candidate)
        result = self.run_checker()
        self.assertEqual(result.returncode, 1)
        kinds = {change["kind"] for change in self.report_json()["changes"]}
        self.assertIn("configuration-minimum-changed", kinds)

    def test_added_configuration_constraint_fails(self) -> None:
        candidate = contract()
        candidate["configuration_constraints"].append("HUBUUM_FIXTURE requires HUBUUM_OTHER")
        self.write_case(contract(), candidate)
        result = self.run_checker()
        self.assertEqual(result.returncode, 1)
        change = self.report_json()["changes"][0]
        self.assertEqual(change["kind"], "configuration-constraint-added")
        self.assertEqual(change["classification"], "breaking")

    def test_removed_configuration_constraint_is_additive(self) -> None:
        baseline = contract()
        baseline["configuration_constraints"].append("HUBUUM_FIXTURE requires HUBUUM_OTHER")
        self.write_case(baseline, contract())
        result = self.run_checker()
        self.assertEqual(result.returncode, 0, result.stderr)
        change = self.report_json()["changes"][0]
        self.assertEqual(change["kind"], "configuration-constraint-removed")
        self.assertEqual(change["classification"], "additive")

    def test_new_required_cli_option_fails(self) -> None:
        candidate = contract()
        required = copy.deepcopy(candidate["cli"][0]["options"][0])
        required.update(
            {
                "id": "required_fixture",
                "long": "required-fixture",
                "environment": "HUBUUM_REQUIRED_FIXTURE",
                "required": True,
            }
        )
        candidate["cli"][0]["options"].append(required)
        self.write_case(contract(), candidate)
        result = self.run_checker()
        self.assertEqual(result.returncode, 1)
        change = self.report_json()["changes"][0]
        self.assertEqual(change["kind"], "cli.hubuum-server.option-added")
        self.assertEqual(change["classification"], "breaking")

    def test_new_optional_cli_option_is_additive(self) -> None:
        candidate = contract()
        optional = copy.deepcopy(candidate["cli"][0]["options"][0])
        optional.update(
            {
                "id": "optional_fixture",
                "long": "optional-fixture",
                "environment": "HUBUUM_OPTIONAL_FIXTURE",
            }
        )
        candidate["cli"][0]["options"].append(optional)
        self.write_case(contract(), candidate)
        result = self.run_checker()
        self.assertEqual(result.returncode, 0, result.stderr)
        change = self.report_json()["changes"][0]
        self.assertEqual(change["classification"], "additive")

    def test_removed_or_replaced_cli_environment_binding_fails(self) -> None:
        for environment in (None, "HUBUUM_RENAMED_FIXTURE"):
            with self.subTest(environment=environment):
                candidate = contract()
                candidate["cli"][0]["options"][0]["environment"] = environment
                self.write_case(contract(), candidate)
                result = self.run_checker()
                self.assertEqual(result.returncode, 1)
                change = self.report_json()["changes"][0]
                self.assertEqual(change["kind"], "cli-option-environment-changed")
                self.assertEqual(change["classification"], "breaking")

    def test_new_cli_environment_binding_is_additive(self) -> None:
        baseline = contract()
        baseline["cli"][0]["options"][0]["environment"] = None
        self.write_case(baseline, contract())
        result = self.run_checker()
        self.assertEqual(result.returncode, 0, result.stderr)
        change = self.report_json()["changes"][0]
        self.assertEqual(change["kind"], "cli-option-environment-changed")
        self.assertEqual(change["classification"], "additive")

    def test_candidate_prerelease_version_is_valid(self) -> None:
        candidate = contract()
        candidate["release"] = "1.1.0-rc.1"
        self.write_case(contract(), candidate)
        result = self.run_checker()
        self.assertEqual(result.returncode, 0, result.stderr)

    def test_event_shape_requires_version_bump(self) -> None:
        candidate = contract()
        candidate["events"]["envelope_fields"].append({"name": "optional", "nullable": True})
        self.write_case(contract(), candidate)
        result = self.run_checker()
        self.assertEqual(result.returncode, 1)
        kinds = {change["kind"] for change in self.report_json()["changes"]}
        self.assertIn("event-schema-version-not-increased", kinds)

    def test_additive_event_catalog_entry_does_not_require_version_bump(self) -> None:
        candidate = contract()
        candidate["events"]["entities"][0]["actions"].append("updated")
        self.write_case(contract(), candidate)
        result = self.run_checker()
        self.assertEqual(result.returncode, 0, result.stderr)
        kinds = {change["kind"] for change in self.report_json()["changes"]}
        self.assertNotIn("event-schema-version-not-increased", kinds)

    def test_document_shape_passes_with_version_bump(self) -> None:
        candidate = contract()
        candidate["documents"]["backup"]["sections"].append("groups")
        candidate["documents"]["backup"]["version"] = 2
        self.write_case(contract(), candidate)
        result = self.run_checker()
        self.assertEqual(result.returncode, 0, result.stderr)

    def test_narrow_exception_accepts_one_break(self) -> None:
        candidate = contract()
        candidate["metrics"] = []
        self.write_case(contract(), candidate)
        self.assertEqual(self.run_checker().returncode, 1)
        fingerprint = next(
            change["fingerprint"]
            for change in self.report_json()["changes"]
            if change["classification"] == "breaking"
        )
        self.write_json(
            self.exceptions,
            {
                "schema_version": 1,
                "exceptions": [
                    {
                        "id": "fixture.metric-removal",
                        "baseline": "v1.0.0",
                        "expires": "2026-12-01",
                        "reason": "fixture",
                        "migration": "use the replacement metric",
                        "changelog_entry": "fixture migration",
                        "fingerprints": [fingerprint],
                    }
                ],
            },
        )
        result = self.run_checker()
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(self.report_json()["counts"]["accepted"], 1)

    def test_expired_exception_fails(self) -> None:
        candidate = contract()
        candidate["metrics"] = []
        self.write_case(contract(), candidate)
        self.assertEqual(self.run_checker().returncode, 1)
        fingerprint = self.report_json()["changes"][0]["fingerprint"]
        policy = {
            "schema_version": 1,
            "exceptions": [
                {
                    "id": "fixture.expired",
                    "baseline": "v1.0.0",
                    "expires": "2026-08-31",
                    "reason": "fixture",
                    "migration": "fixture",
                    "changelog_entry": "fixture migration",
                    "fingerprints": [fingerprint],
                }
            ],
        }
        self.write_json(self.exceptions, policy)
        self.assertEqual(self.run_checker().returncode, 1)
        self.assertTrue(self.report_json()["exception_errors"])

    def test_missing_baseline_is_explicitly_skipped(self) -> None:
        self.write_json(
            self.metadata,
            {
                "status": "unavailable",
                "tag": None,
                "source": "fixture",
                "sha256": None,
                "reason": "fixture has no baseline",
                "candidate_ref": "test",
                "candidate_sha": "test",
            },
        )
        result = self.run_checker()
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(self.report_json()["status"], "skipped")


if __name__ == "__main__":
    unittest.main()
