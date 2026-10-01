#!/usr/bin/env python3
"""Check Atlas artifact drift, documented data, and safe corpus generation."""

import sys

if sys.version_info < (3, 11):
    sys.exit(
        "Hubuum tooling requires Python 3.11 or newer; found "
        + sys.version.split()[0]
        + ". Install Python 3.11+ and ensure python3 on PATH selects it."
    )

import copy
import importlib.util
import json
from pathlib import Path
import shutil
import tempfile
import unittest
from unittest.mock import Mock, patch


SPEC = importlib.util.spec_from_file_location(
    "atlas", Path(__file__).with_name("example-corpus.py"),
)
ATLAS = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(ATLAS)


class AtlasTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.document = ATLAS.load_import(ATLAS.DIRECTORY)

    def test_committed_artifacts_and_documented_data_match(self):
        ATLAS.check_backup(ATLAS.DIRECTORY, self.document)
        ATLAS.check_documentation(self.document)

    def test_import_cannot_silently_overwrite_existing_inventory(self):
        document = copy.deepcopy(self.document)
        document["mode"]["collision_policy"] = "overwrite"
        with tempfile.TemporaryDirectory() as temporary:
            directory = Path(temporary)
            ATLAS.write_json(directory / ATLAS.IMPORT, document)
            with self.assertRaisesRegex(ValueError, "abort atomically"):
                ATLAS.load_import(directory)

    def test_dangling_relations_are_rejected(self):
        document = copy.deepcopy(self.document)
        document["graph"]["object_relations"][0]["to_object_ref"] = "object:missing"
        with tempfile.TemporaryDirectory() as temporary:
            directory = Path(temporary)
            ATLAS.write_json(directory / ATLAS.IMPORT, document)
            with self.assertRaisesRegex(ValueError, "Unresolved Atlas ref"):
                ATLAS.load_import(directory)

    def test_checksums_detect_changed_downloads(self):
        with tempfile.TemporaryDirectory() as temporary:
            directory = Path(temporary)
            shutil.copytree(ATLAS.DIRECTORY, directory, dirs_exist_ok=True)
            with (directory / ATLAS.BACKUP).open("a") as output:
                output.write(" ")
            with self.assertRaisesRegex(ValueError, "checksum"):
                ATLAS.check_backup(directory, self.document)

    def test_rehashed_backup_still_must_match_the_canonical_objects(self):
        with tempfile.TemporaryDirectory() as temporary:
            directory = Path(temporary)
            shutil.copytree(ATLAS.DIRECTORY, directory, dirs_exist_ok=True)
            backup = ATLAS.read_json(directory / ATLAS.BACKUP)
            backup["state"]["sections"]["objects"][0]["data"] = {"unexpected": True}
            ATLAS.write_json(directory / ATLAS.BACKUP, backup)
            manifest = ATLAS.read_json(directory / ATLAS.MANIFEST)
            manifest["files"][ATLAS.BACKUP] = {
                "sha256": ATLAS.digest(directory / ATLAS.BACKUP),
                "bytes": (directory / ATLAS.BACKUP).stat().st_size,
            }
            ATLAS.write_json(directory / ATLAS.MANIFEST, manifest)
            with self.assertRaisesRegex(ValueError, "object data drift"):
                ATLAS.check_backup(directory, self.document)

    def test_documentation_must_show_the_actual_data(self):
        with tempfile.TemporaryDirectory() as temporary:
            docs = Path(temporary)
            fence = chr(96) * 3
            (docs / "example.md").write_text(
                "<!-- atlas-data: object:Atlas -->\n" + fence + "json\n"
                + json.dumps({"owner": "outdated example"}) + "\n" + fence,
            )
            with self.assertRaisesRegex(ValueError, "example drift"):
                ATLAS.check_documentation(self.document, docs)

    def test_task_failure_is_reported_without_waiting_for_timeout(self):
        api = Mock()
        api.post.return_value = {"id": 7}
        api.get.side_effect = [{"status": "failed"}, [{"message": "schema rejected"}]]
        with self.assertRaisesRegex(ValueError, "schema rejected"):
            ATLAS.submit_import(api, self.document)

    def test_failed_verification_does_not_replace_downloads(self):
        with tempfile.TemporaryDirectory() as temporary:
            destination = Path(temporary)
            (destination / ATLAS.BACKUP).write_text("previous artifact")

            def backup(_database, path):
                shutil.copyfile(ATLAS.DIRECTORY / ATLAS.BACKUP, path)

            with patch.object(ATLAS.CORPUS, "Deployment") as deployment, \
                    patch.object(ATLAS, "submit_import"), \
                    patch.object(ATLAS, "verify_reads"), \
                    patch.object(ATLAS, "verify", side_effect=ValueError("restore failed")):
                deployment.return_value.__enter__.return_value.backup.side_effect = backup
                with self.assertRaisesRegex(ValueError, "restore failed"):
                    ATLAS.generate(destination, ATLAS.DIRECTORY, "image", "postgres")
            self.assertEqual((destination / ATLAS.BACKUP).read_text(), "previous artifact")
            self.assertFalse((destination / ATLAS.MANIFEST).exists())


if __name__ == "__main__":
    unittest.main()
