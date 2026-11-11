"""Validate shared wire inputs, not runtime authentication behavior."""

from copy import deepcopy
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


def objects(value):
    if isinstance(value, dict):
        yield value
        for child in value.values():
            yield from objects(child)
    elif isinstance(value, list):
        for child in value:
            yield from objects(child)


class AuthenticationContractTests(unittest.TestCase):
    def setUp(self):
        self.spec = load_json(CONTRACT / "openapi.proposed.json")
        self.fixtures = load_json(CONTRACT / "fixtures.json")
        self.current = load_json(ROOT / "docs/openapi.json")
        self.operations = {
            operation["operationId"]: operation
            for methods in self.spec["paths"].values()
            for operation in methods.values()
        }

    def resolve(self, reference, document):
        filename, pointer = reference.split("#", 1)
        self.assertIn(filename, ("", "../openapi.json"))
        document = self.current if filename else document
        value = document
        for segment in pointer.lstrip("/").split("/"):
            value = value[segment.replace("~1", "/").replace("~0", "~")]
        return value, document

    def shape_errors(self, value, schema, document, path="$"):
        """Check fixture structure, not formats, domain rules or authentication."""
        if "$ref" in schema:
            schema, document = self.resolve(schema["$ref"], document)
        if "oneOf" in schema:
            matches = sum(not self.shape_errors(value, branch, document, path)
                          for branch in schema["oneOf"])
            return [] if matches == 1 else [f"{path}: expected exactly one variant"]
        types = {dict: "object", list: "array", str: "string", int: "integer",
                 float: "number", bool: "boolean", type(None): "null"}
        allowed = schema.get("type", list(types.values()))
        allowed = [allowed] if isinstance(allowed, str) else allowed
        if types[type(value)] not in allowed:
            return [f"{path}: expected {allowed}"]
        if "enum" in schema and value not in schema["enum"]:
            return [f"{path}: unexpected enum value"]
        errors = []
        if isinstance(value, dict):
            properties = schema.get("properties", {})
            errors.extend(f"{path}/{key}: missing required property"
                          for key in schema.get("required", []) if key not in value)
            if schema.get("additionalProperties") is False:
                errors.extend(f"{path}/{key}: unexpected property"
                              for key in value.keys() - properties.keys())
            for key in value.keys() & properties.keys():
                errors.extend(self.shape_errors(value[key], properties[key], document,
                                                f"{path}/{key}"))
        if isinstance(value, list) and "items" in schema:
            for index, item in enumerate(value):
                errors.extend(self.shape_errors(item, schema["items"], document,
                                                f"{path}/{index}"))
        return errors

    def test_references_resolve_to_proposal_or_current_api(self):
        for node in objects(self.spec):
            if "$ref" not in node:
                continue
            reference = node["$ref"]
            with self.subTest(reference=reference):
                self.resolve(reference, self.spec)

    def test_discriminators_map_every_variant_tag(self):
        for node in objects(self.spec):
            if "discriminator" not in node:
                continue
            discriminator = node["discriminator"]
            expected = {}
            for branch in node["oneOf"]:
                self.assertIn("$ref", branch, "Discriminators cannot select inline schemas")
                variant, _ = self.resolve(branch["$ref"], self.spec)
                tag = discriminator["propertyName"]
                self.assertIn(tag, variant["required"])
                for value in variant["properties"][tag]["enum"]:
                    self.assertNotIn(value, expected)
                    expected[value] = branch["$ref"]
            self.assertEqual(discriminator.get("mapping"), expected)

    def test_fixture_request_shapes(self):
        for case in self.fixtures["cases"]:
            with self.subTest(case=case["id"]):
                operation = self.operations[case["operation_id"]]
                if "requestBody" in operation:
                    schema = operation["requestBody"]["content"]["application/json"]["schema"]
                else:
                    parameters = [p for p in operation.get("parameters", []) if p["in"] == "query"]
                    schema = {"type": "object", "additionalProperties": False,
                              "properties": {p["name"]: p["schema"] for p in parameters},
                              "required": [p["name"] for p in parameters if p.get("required")]}
                request = deepcopy(case["request"])
                invalid_fields = case.get("invalid_request_fields", [])
                if invalid_fields:
                    self.assertGreaterEqual(case["response"]["status"], 400)
                    self.assertTrue(self.shape_errors(request, schema, self.spec))
                    for pointer in invalid_fields:
                        self.assertTrue(pointer.startswith("/"))
                        parent = request
                        segments = [s.replace("~1", "/").replace("~0", "~")
                                    for s in pointer[1:].split("/")]
                        for segment in segments[:-1]:
                            parent = parent[int(segment)] if isinstance(parent, list) else parent[segment]
                        del parent[segments[-1]]
                self.assertEqual(self.shape_errors(request, schema, self.spec), [])

    def test_fixture_response_shapes(self):
        for case in self.fixtures["cases"]:
            if "body" not in case["response"]:
                continue
            with self.subTest(case=case["id"]):
                operation = self.operations[case["operation_id"]]
                response = operation["responses"][str(case["response"]["status"])]
                if "$ref" in response:
                    response, _ = self.resolve(response["$ref"], self.spec)
                schema = response["content"]["application/json"]["schema"]
                self.assertEqual(self.shape_errors(case["response"]["body"], schema, self.spec), [])

    def test_candidate_provider_id_is_available_for_verification(self):
        self.assertIn("providers", self.spec["components"]["schemas"]["CandidateReport"]["required"])
        cases = {case["id"]: case for case in self.fixtures["cases"]}
        candidate = cases["candidate_connectivity_only"]["response"]["body"]
        begin = cases["candidate_verification_begin"]["request"]
        self.assertEqual(begin["candidate_id"], candidate["id"])
        self.assertEqual(begin["purpose"], "provider_verification")
        provider = next(p for p in candidate["providers"] if p["provider_id"] == begin["provider_id"])
        submitted = cases["candidate_connectivity_only"]["request"]["providers"][0]
        self.assertNotIn("provider_id", submitted)
        self.assertEqual((provider["name"], provider["kind"]), (submitted["name"], submitted["kind"]))
        self.assertEqual(cases["candidate_browser_verified"]["response"]["body"]["provider_id"],
                         provider["provider_id"])

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
            "candidate_connectivity_only", "candidate_verification_begin", "candidate_browser_verified",
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
