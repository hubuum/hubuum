#!/usr/bin/env python3
"""Regression tests for corpus drift detection and isolated harness behavior."""

import sys

if sys.version_info < (3, 11):
    sys.exit(
        "Hubuum tooling requires Python 3.11 or newer; found "
        + sys.version.split()[0]
        + ". Install Python 3.11+ and ensure python3 on PATH selects it."
    )

import copy
import importlib.util
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch


ROOT = Path(__file__).resolve().parent.parent
SPEC = importlib.util.spec_from_file_location("corpus_tool", ROOT / "scripts" / "test-corpus.py")
CORPUS = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(CORPUS)


class CorpusValidationTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.recipe = CORPUS.load_recipe(CORPUS.CORPORA / "recipe.json")
        cls.document = CORPUS.read_json(CORPUS.CORPORA / CORPUS.BACKUP)
        cls.manifest = CORPUS.read_json(CORPUS.CORPORA / CORPUS.MANIFEST)

    def test_committed_corpus_matches_recipe(self):
        CORPUS.inspect_corpus(CORPUS.CORPORA, self.recipe)

    def test_corrupt_bytes_fail_the_checksum(self):
        with tempfile.TemporaryDirectory() as temporary:
            directory = Path(temporary)
            CORPUS.write_json(directory / CORPUS.BACKUP, self.document)
            CORPUS.write_json(directory / CORPUS.MANIFEST, self.manifest)
            with (directory / CORPUS.BACKUP).open("a") as output:
                output.write(" ")
            with self.assertRaisesRegex(ValueError, "checksum"):
                CORPUS.inspect_corpus(directory, self.recipe)

    def test_rehashed_artifacts_still_require_semantic_invariants(self):
        def wrong_policy(document, _):
            cls = document["state"]["sections"]["classes"][0]
            cls["validate_schema"] = not cls["validate_schema"]

        def missing_object(document, _):
            document["state"]["sections"]["objects"].pop()

        def weakened_schema(document, _):
            cls = next(row for row in document["state"]["sections"]["classes"] if row["validate_schema"])
            cls["json_schema"] = {}

        def renamed_object(document, _):
            document["state"]["sections"]["objects"][0]["name"] = "unexpected"

        def missing_history(document, _):
            document["history"] = None

        def rewritten_payload(document, _):
            document["state"]["sections"]["objects"][0]["data"] = "unexpected"

        def incorrect_anchor(_, manifest):
            manifest["anchors"]["updated_object"] = -1

        def enabled_integration(document, manifest):
            document["state"]["sections"]["remote_targets"].append({"enabled": True})
            manifest["state_counts"]["remote_targets"] += 1

        def missing_definition(document, manifest):
            document["state"]["sections"]["computed_field_definitions"].pop()
            manifest["state_counts"]["computed_field_definitions"] -= 1

        def changed_operation(document, _):
            definition = next(row for row in document["state"]["sections"]["computed_field_definitions"]
                              if row["key"] == "monthly_cost")
            definition["operation"]["type"] = "max"

        def changed_personal_owner(document, _):
            definition = next(row for row in document["state"]["sections"]["computed_field_definitions"]
                              if row["visibility"] == "personal")
            definition["owner_principal_id"] = 1

        for mutation, message in (
            (wrong_policy, "Schema policy"),
            (weakened_schema, "Schema document"),
            (missing_object, "3,000 live objects"),
            (renamed_object, "Object name"),
            (rewritten_payload, "Object payload"),
            (missing_history, "retain history"),
            (incorrect_anchor, "History object anchor"),
            (enabled_integration, "enabled external integrations"),
            (missing_definition, "Computed definition or ownership"),
            (changed_operation, "Computed definition or ownership"),
            (changed_personal_owner, "Computed definition or ownership"),
        ):
            with self.subTest(mutation=mutation.__name__), tempfile.TemporaryDirectory() as temporary:
                directory = Path(temporary)
                document = copy.deepcopy(self.document)
                manifest = copy.deepcopy(self.manifest)
                mutation(document, manifest)
                CORPUS.write_json(directory / CORPUS.BACKUP, document)
                manifest["sha256"] = CORPUS.digest(directory / CORPUS.BACKUP)
                manifest["byte_size"] = (directory / CORPUS.BACKUP).stat().st_size
                CORPUS.write_json(directory / CORPUS.MANIFEST, manifest)
                with self.assertRaisesRegex(ValueError, message):
                    CORPUS.inspect_corpus(directory, self.recipe)

    def test_recipe_rejects_missing_schema_configuration(self):
        recipe = copy.deepcopy(self.recipe)
        recipe["classes"][4]["policy"] = "absent"
        with tempfile.TemporaryDirectory() as temporary:
            path = Path(temporary) / "recipe.json"
            CORPUS.write_json(path, recipe)
            with self.assertRaisesRegex(ValueError, "distribution"):
                CORPUS.load_recipe(path)

    def test_recipe_rejects_missing_computed_operation(self):
        recipe = copy.deepcopy(self.recipe)
        definition = next(row for row in recipe["computed"]["shared"]
                          if row["operation"]["type"] == "average")
        definition["operation"]["type"] = "sum"
        with tempfile.TemporaryDirectory() as temporary:
            path = Path(temporary) / "recipe.json"
            CORPUS.write_json(path, recipe)
            with self.assertRaisesRegex(ValueError, "all nine operations"):
                CORPUS.load_recipe(path)


class HarnessTests(unittest.TestCase):
    def test_pagination_rejects_repeated_cursors(self):
        api = CORPUS.Api("http://127.0.0.1:1")
        with patch.object(api, "request", return_value=([{"id": 1}], {"X-Next-Cursor": "repeated"})):
            with self.assertRaisesRegex(ValueError, "Pagination did not terminate"):
                api.pages("/api/v1/classes")

    def test_owned_resources_are_cleaned_after_scenario_failure(self):
        deployment = CORPUS.Deployment("candidate-image", "postgres-image")
        result = type("Result", (), {"returncode": 0, "stdout": "", "stderr": ""})()
        with patch.object(deployment, "docker", return_value=result) as docker:
            with self.assertRaisesRegex(ValueError, "scenario failed"):
                with deployment:
                    raise ValueError("scenario failed")
        self.assertIn(
            (("rm", "--force", "--volumes", deployment.name + "-db"), {"check": False}),
            [(call.args, call.kwargs) for call in docker.call_args_list],
        )
        self.assertEqual(docker.call_args_list[-1].args, ("network", "rm", deployment.name))

    def test_generation_does_not_replace_corpus_when_verification_fails(self):
        with tempfile.TemporaryDirectory() as temporary:
            destination = Path(temporary)
            (destination / CORPUS.BACKUP).write_text("original")
            (destination / CORPUS.MANIFEST).write_text("original manifest")

            def backup(_database, path):
                CORPUS.write_json(path, {
                    "backup_version": 6, "source_version": "test", "state": {"sections": {}},
                })

            with patch.object(CORPUS, "Deployment") as deployment, \
                    patch.object(CORPUS, "seed", return_value={}), \
                    patch.object(CORPUS, "verify", side_effect=ValueError("verification failed")), \
                    patch("builtins.print"):
                deployment.return_value.__enter__.return_value.backup.side_effect = backup
                with self.assertRaisesRegex(ValueError, "verification failed"):
                    CORPUS.generate(destination, CORPUS.CORPORA / "recipe.json", "candidate", "postgres")
            self.assertEqual((destination / CORPUS.BACKUP).read_text(), "original")
            self.assertEqual((destination / CORPUS.MANIFEST).read_text(), "original manifest")


if __name__ == "__main__":
    unittest.main()
