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

    def test_open_metric_label_domain_becoming_enumerated_fails(self) -> None:
        baseline = contract()
        baseline_label = baseline["metrics"][0]["labels"][0]
        baseline_label["bounded_by"] = "runtime values"
        baseline_label["values"] = []
        self.write_case(baseline, contract())

        result = self.run_checker()

        self.assertEqual(result.returncode, 1)
        change = self.report_json()["changes"][0]
        self.assertEqual(change["kind"], "metric-label-bound-changed")
        self.assertEqual(change["classification"], "breaking")

    def test_enumerated_metric_label_domain_becoming_open_is_behavioral(self) -> None:
        candidate = contract()
        candidate_label = candidate["metrics"][0]["labels"][0]
        candidate_label["bounded_by"] = "runtime values"
        candidate_label["values"] = []
        self.write_case(contract(), candidate)

        result = self.run_checker()

        self.assertEqual(result.returncode, 0, result.stderr)
        change = self.report_json()["changes"][0]
        self.assertEqual(change["kind"], "metric-label-bound-changed")
        self.assertEqual(change["classification"], "behavioral")

    def test_changed_descriptive_metric_label_bound_fails(self) -> None:
        baseline = contract()
        baseline_label = baseline["metrics"][0]["labels"][0]
        baseline_label["bounded_by"] = "runtime values"
        baseline_label["values"] = []
        candidate = copy.deepcopy(baseline)
        candidate["metrics"][0]["labels"][0]["bounded_by"] = "configured values"
        self.write_case(baseline, candidate)

        result = self.run_checker()

        self.assertEqual(result.returncode, 1)
        change = self.report_json()["changes"][0]
        self.assertEqual(change["kind"], "metric-label-bound-changed")
        self.assertEqual(change["classification"], "breaking")

    def test_unconditional_metric_becoming_feature_gated_fails(self) -> None:
        candidate = contract()
        candidate["metrics"][0]["feature"] = "optional-metrics"
        self.write_case(contract(), candidate)

        result = self.run_checker()

        self.assertEqual(result.returncode, 1)
        change = self.report_json()["changes"][0]
        self.assertEqual(change["kind"], "metric-feature-changed")
        self.assertEqual(change["classification"], "breaking")

    def test_metric_moving_between_features_fails(self) -> None:
        baseline = contract()
        baseline["metrics"][0]["feature"] = "old-metrics"
        candidate = copy.deepcopy(baseline)
        candidate["metrics"][0]["feature"] = "new-metrics"
        self.write_case(baseline, candidate)

        result = self.run_checker()

        self.assertEqual(result.returncode, 1)
        change = self.report_json()["changes"][0]
        self.assertEqual(change["kind"], "metric-feature-changed")
        self.assertEqual(change["classification"], "breaking")

    def test_feature_gated_metric_becoming_unconditional_is_additive(self) -> None:
        baseline = contract()
        baseline["metrics"][0]["feature"] = "optional-metrics"
        self.write_case(baseline, contract())

        result = self.run_checker()

        self.assertEqual(result.returncode, 0, result.stderr)
        change = self.report_json()["changes"][0]
        self.assertEqual(change["kind"], "metric-feature-changed")
        self.assertEqual(change["classification"], "additive")

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

    def test_removed_configuration_runtime_role_fails(self) -> None:
        candidate = contract()
        candidate["configuration"][0]["runtime_roles"].remove("worker")
        self.write_case(contract(), candidate)
        result = self.run_checker()
        self.assertEqual(result.returncode, 1)
        change = self.report_json()["changes"][0]
        self.assertEqual(change["kind"], "configuration-runtime-role-removed")
        self.assertEqual(change["classification"], "breaking")

    def test_added_configuration_runtime_role_is_behavioral(self) -> None:
        baseline = contract()
        baseline["configuration"][0]["runtime_roles"].remove("worker")
        self.write_case(baseline, contract())
        result = self.run_checker()
        self.assertEqual(result.returncode, 0, result.stderr)
        change = self.report_json()["changes"][0]
        self.assertEqual(change["kind"], "configuration-runtime-role-added")
        self.assertEqual(change["classification"], "behavioral")

    def test_removing_running_configuration_visibility_fails(self) -> None:
        candidate = contract()
        candidate["configuration"][0]["appears_in_running_configuration"] = False
        self.write_case(contract(), candidate)
        result = self.run_checker()
        self.assertEqual(result.returncode, 1)
        change = self.report_json()["changes"][0]
        self.assertEqual(change["kind"], "configuration-running-visibility-changed")
        self.assertEqual(change["classification"], "breaking")

    def test_adding_running_configuration_visibility_is_additive(self) -> None:
        baseline = contract()
        baseline["configuration"][0]["appears_in_running_configuration"] = False
        self.write_case(baseline, contract())
        result = self.run_checker()
        self.assertEqual(result.returncode, 0, result.stderr)
        change = self.report_json()["changes"][0]
        self.assertEqual(change["classification"], "additive")

    def test_changed_dynamic_prefix_fails(self) -> None:
        candidate = contract()
        candidate["configuration"][0]["dynamic_prefix"] = True
        self.write_case(contract(), candidate)
        result = self.run_checker()
        self.assertEqual(result.returncode, 1)
        change = self.report_json()["changes"][0]
        self.assertEqual(change["kind"], "configuration-dynamic-prefix-changed")
        self.assertEqual(change["classification"], "breaking")

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

    def test_new_cli_short_alias_is_additive(self) -> None:
        candidate = contract()
        candidate["cli"][0]["options"][0]["short"] = "f"
        self.write_case(contract(), candidate)

        result = self.run_checker()

        self.assertEqual(result.returncode, 0, result.stderr)
        change = self.report_json()["changes"][0]
        self.assertEqual(change["kind"], "cli-option-short-changed")
        self.assertEqual(change["classification"], "additive")

    def test_removed_or_replaced_cli_short_alias_fails(self) -> None:
        for replacement in (None, "r"):
            with self.subTest(replacement=replacement):
                baseline = contract()
                baseline["cli"][0]["options"][0]["short"] = "f"
                candidate = copy.deepcopy(baseline)
                candidate["cli"][0]["options"][0]["short"] = replacement
                self.write_case(baseline, candidate)

                result = self.run_checker()

                self.assertEqual(result.returncode, 1)
                change = self.report_json()["changes"][0]
                self.assertEqual(change["kind"], "cli-option-short-changed")
                self.assertEqual(change["classification"], "breaking")

    def test_relaxed_cli_constraints_are_additive(self) -> None:
        baseline = contract()
        option = baseline["cli"][0]["options"][0]
        option["required"] = True
        option["conflicts_with"] = ["conflict"]
        option["requires"] = ["requirement"]
        candidate = copy.deepcopy(baseline)
        candidate_option = candidate["cli"][0]["options"][0]
        candidate_option["required"] = False
        candidate_option["conflicts_with"] = []
        candidate_option["requires"] = []
        candidate_option["value_count"] = {"minimum": 0, "maximum": 2}
        self.write_case(baseline, candidate)

        result = self.run_checker()

        self.assertEqual(result.returncode, 0, result.stderr)
        changes = self.report_json()["changes"]
        self.assertTrue(changes)
        self.assertTrue(all(change["classification"] == "additive" for change in changes))

    def test_stronger_cli_constraints_are_breaking(self) -> None:
        candidate = contract()
        option = candidate["cli"][0]["options"][0]
        option["required"] = True
        option["conflicts_with"] = ["conflict"]
        option["requires"] = ["requirement"]
        option["value_count"] = {"minimum": 1, "maximum": 1}
        baseline = copy.deepcopy(candidate)
        baseline_option = baseline["cli"][0]["options"][0]
        baseline_option["required"] = False
        baseline_option["conflicts_with"] = []
        baseline_option["requires"] = []
        baseline_option["value_count"] = {"minimum": 0, "maximum": 2}
        self.write_case(baseline, candidate)

        result = self.run_checker()

        self.assertEqual(result.returncode, 1)
        changes = self.report_json()["changes"]
        self.assertTrue(changes)
        self.assertTrue(all(change["classification"] == "breaking" for change in changes))

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

    def test_event_schema_version_decrease_fails_without_shape_change(self) -> None:
        baseline = contract()
        baseline["events"]["schema_version"] = 2
        self.write_case(baseline, contract())
        result = self.run_checker()
        self.assertEqual(result.returncode, 1)
        kinds = {change["kind"] for change in self.report_json()["changes"]}
        self.assertIn("event-schema-version-decreased", kinds)

    def test_added_event_redaction_rule_fails(self) -> None:
        candidate = contract()
        candidate["events"]["redaction_rules"].append("redact actor")
        self.write_case(contract(), candidate)
        result = self.run_checker()
        self.assertEqual(result.returncode, 1)
        change = self.report_json()["changes"][0]
        self.assertEqual(change["kind"], "event-redaction-rule-added")
        self.assertEqual(change["classification"], "breaking")

    def test_document_shape_passes_with_version_bump(self) -> None:
        candidate = contract()
        candidate["documents"]["backup"]["sections"].append("groups")
        candidate["documents"]["backup"]["version"] = 2
        self.write_case(contract(), candidate)
        result = self.run_checker()
        self.assertEqual(result.returncode, 0, result.stderr)

    def test_document_version_decrease_fails_without_shape_change(self) -> None:
        baseline = contract()
        baseline["documents"]["backup"]["version"] = 2
        self.write_case(baseline, contract())
        result = self.run_checker()
        self.assertEqual(result.returncode, 1)
        kinds = {change["kind"] for change in self.report_json()["changes"]}
        self.assertIn("backup-version-decreased", kinds)

    def test_document_rejection_policy_change_fails(self) -> None:
        candidate = contract()
        candidate["documents"]["import"]["rejection_policy"] = "accept unknown versions"
        self.write_case(contract(), candidate)
        result = self.run_checker()
        self.assertEqual(result.returncode, 1)
        kinds = {change["kind"] for change in self.report_json()["changes"]}
        self.assertIn("import-rejection-policy-changed", kinds)

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

    def test_exception_cannot_reuse_a_previous_release_note(self) -> None:
        candidate = contract()
        candidate["metrics"] = []
        self.write_case(contract(), candidate)
        self.assertEqual(self.run_checker().returncode, 1)
        fingerprint = self.report_json()["changes"][0]["fingerprint"]
        self.write_json(
            self.exceptions,
            {
                "schema_version": 1,
                "exceptions": [
                    {
                        "id": "fixture.old-note",
                        "baseline": "v1.0.0",
                        "expires": "2026-12-01",
                        "reason": "fixture",
                        "migration": "use the replacement metric",
                        "changelog_entry": "old migration",
                        "fingerprints": [fingerprint],
                    }
                ],
            },
        )
        self.changelog.write_text(
            "# Changelog\n\n## [Unreleased]\n\n## [1.0.0]\n\n- old migration\n",
            encoding="utf-8",
        )

        result = self.run_checker()

        self.assertEqual(result.returncode, 1)
        self.assertTrue(
            any(
                "no matching changelog migration entry" in error
                for error in self.report_json()["exception_errors"]
            )
        )

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
