"""Validate shared wire inputs, not runtime authentication behavior."""

import json
from pathlib import Path
import re
import unittest


ROOT = Path(__file__).resolve().parents[4]
CONTRACT = ROOT / "docs" / "authentication"


def load_json(path):
    def unique_object(pairs):
        result = {}
        for key, value in pairs:
            if key in result:
                raise ValueError(f"Duplicate JSON key: {key}")
            result[key] = value
        return result

    return json.loads(path.read_text(), object_pairs_hook=unique_object)


def references(value):
    if isinstance(value, dict):
        if "$ref" in value:
            yield value["$ref"]
        for child in value.values():
            yield from references(child)
    elif isinstance(value, list):
        for child in value:
            yield from references(child)


class AuthenticationContractTests(unittest.TestCase):
    def setUp(self):
        self.spec = load_json(CONTRACT / "openapi.proposed.json")
        self.fixtures = load_json(CONTRACT / "fixtures.json")

    def test_references_resolve_to_proposal_or_current_api(self):
        for reference in references(self.spec):
            with self.subTest(reference=reference):
                document, pointer = reference.split("#", 1)
                self.assertIn(document, ("", "../openapi.json"))
                value = self.spec if not document else load_json(ROOT / "docs/openapi.json")
                for segment in pointer.lstrip("/").split("/"):
                    value = value[segment.replace("~1", "/").replace("~0", "~")]

    def test_fixture_operations_and_statuses_exist(self):
        operations = {}
        for path, methods in self.spec["paths"].items():
            for method, operation in methods.items():
                self.assertTrue(path.startswith("/api/v1/"))
                self.assertIn(method, ("get", "post"))
                name = operation["operationId"]
                self.assertNotIn(name, operations)
                operations[name] = operation
        identifiers = set()
        for case in self.fixtures["cases"]:
            with self.subTest(case=case["id"]):
                self.assertNotIn(case["id"], identifiers)
                identifiers.add(case["id"])
                operation = operations[case["operation_id"]]
                response = operation["responses"][str(case["response"]["status"])]
                if "$ref" in response:
                    response = self.spec["components"]["responses"][response["$ref"].rsplit("/", 1)[1]]
                if "body" in case["response"]:
                    self.assertIn("application/json", response["content"])

    def test_required_shared_scenarios_are_retained(self):
        identifiers = {case["id"] for case in self.fixtures["cases"]}
        required = {
            "local_success", "external_success", "remote_browser_cli_success",
            "bff_server_completion", "denial", "expiry", "replay", "cross_client",
            "cross_provider", "wrong_browser", "configuration_changed",
            "stale_group_data", "unsupported_freshness", "lost_completion_cleanup",
            "candidate_connectivity_only", "candidate_browser_verified",
            "runtime_activation", "candidate_revision_conflict",
            "candidate_secret_rotated", "candidate_lock_changed",
            "complete_empty_groups", "missing_optional_groups",
            "local_group_mapping_unsupported", "oauth_large_numeric_subject",
            "treetop_runtime_activation",
        }
        self.assertFalse(required - identifiers, required - identifiers)

    def test_proposal_stays_separate_from_implemented_api(self):
        current = load_json(ROOT / "docs/openapi.json")
        self.assertIn("proposed", self.spec["info"]["version"])
        self.assertEqual(self.fixtures["status"], "proposed")
        self.assertFalse(self.spec["paths"].keys() & current["paths"].keys())

    def test_existing_result_examples_use_current_required_fields_and_approval_syntax(self):
        schemas = load_json(ROOT / "docs/openapi.json")["components"]["schemas"]
        for case in self.fixtures["cases"]:
            body = case["response"].get("body", {})
            with self.subTest(case=case["id"]):
                if "access_token" in body:
                    self.assertTrue(set(schemas["SessionResponse"]["required"]) <= body.keys())
                if "approval" in body:
                    self.assertTrue(set(schemas["CredentialApprovalResponse"]["required"]) <= body.keys())
                    self.assertIsNotNone(re.fullmatch(schemas["CredentialApprovalSecret"]["pattern"], body["approval"]))


if __name__ == "__main__":
    unittest.main()
