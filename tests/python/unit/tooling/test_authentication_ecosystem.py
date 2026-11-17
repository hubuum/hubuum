"""Cross-native qualification refuses source, image and contract drift."""
import hashlib
import importlib
import sys
import json
from pathlib import Path
import subprocess
import tempfile
import unittest
from unittest.mock import patch

from integration import ecosystem


class CheckoutTests(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.root = Path(self.directory.name).resolve()
        self.file = self.root / "consumer.py"
        self.file.write_text("print('public fixture')\n")
        for args in [("init", "--quiet"), ("add", "consumer.py"),
                     ("-c", "user.name=Fixture", "-c", "user.email=fixture@example.invalid", "-c", "core.hooksPath=/dev/null", "commit", "--quiet", "-m", "fixture")]:
            subprocess.run(["git", "-C", str(self.root), *args], check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        self.pin = {"source": ecosystem.git(self.root, "rev-parse", "HEAD"),
                    "tree": ecosystem.git(self.root, "rev-parse", "HEAD^{tree}")}

    def test_clean_accepted_checkout_produces_verified_consumer_input(self):
        checkout = ecosystem.AcceptedCheckout.resolve(self.root, self.pin)
        self.assertEqual(checkout.verified_file("consumer.py", ecosystem.sha256(self.file)), self.file)

    def test_modified_source_is_rejected_before_consumer_execution(self):
        self.file.write_text("print('different executable')\n")
        with self.assertRaisesRegex(RuntimeError, "must be clean"):
            ecosystem.AcceptedCheckout.resolve(self.root, self.pin)

    def test_untracked_executable_is_rejected(self):
        (self.root / "unreviewed.py").write_text("print('unreviewed')\n")
        with self.assertRaisesRegex(RuntimeError, "must be clean"):
            ecosystem.AcceptedCheckout.resolve(self.root, self.pin)

    def test_different_accepted_identity_is_rejected(self):
        for field in ("source", "tree"):
            with self.subTest(field=field), self.assertRaisesRegex(RuntimeError, "accepted source/tree"):
                ecosystem.AcceptedCheckout.resolve(self.root, {**self.pin, field: "0" * 40})

    def test_input_hash_is_checked_even_after_checkout_resolution(self):
        checkout = ecosystem.AcceptedCheckout.resolve(self.root, self.pin)
        expected = ecosystem.sha256(self.file)
        self.file.write_text("print('changed after resolution')\n")
        with self.assertRaisesRegex(RuntimeError, "consumer input differs"):
            checkout.verified_file("consumer.py", expected)


class BoundaryTests(unittest.TestCase):
    def test_image_tag_is_resolved_to_the_qualified_immutable_identity(self):
        pin = {"docker_image_id": "sha256:qualified", "manifest": "sha256:manifest", "config": "sha256:config"}
        with patch.object(ecosystem.subprocess, "check_output", return_value=json.dumps([{"Id": "sha256:qualified"}])):
            image, actual = ecosystem.qualified_image("server:mutable-tag", {"qualified_server_images": [pin]})
        self.assertEqual((image, actual), ("sha256:qualified", pin))

    def test_retagged_unqualified_image_is_rejected(self):
        pin = {"docker_image_id": "sha256:qualified", "manifest": "sha256:manifest", "config": "sha256:config"}
        with patch.object(ecosystem.subprocess, "check_output", return_value=json.dumps([{"Id": "sha256:other"}])):
            with self.assertRaisesRegex(RuntimeError, "qualified immutable"):
                ecosystem.qualified_image("server:mutable-tag", {"qualified_server_images": [pin]})

    def test_fixture_override_cannot_change_the_owning_wire_contract(self):
        original = {"source_openapi_sha256": "spec", "source_fixture_sha256": "corpus"}
        for field in original:
            with self.subTest(field=field), self.assertRaisesRegex(RuntimeError, "different wire contract"):
                ecosystem.compatible_contract(original, {**original, field: "different"})

    def test_partial_consumer_report_cannot_be_counted_as_acceptance(self):
        with self.assertRaisesRegex(RuntimeError, "did not report acceptance"):
            ecosystem.result_count({"passed": False, "checks": ["one successful precondition"]})

    def test_committed_manifest_consumer_profiles_have_owning_drivers(self):
        pins = json.loads(ecosystem.PINS.read_text())
        for name, client in pins["clients"].items():
            with self.subTest(client=name):
                self.assertEqual(len(client["consumer_sha256"]), hashlib.sha256().digest_size * 2)
                self.assertIn("default", client["drivers"])
                if "treetop" in client["profiles"]:
                    self.assertIn("treetop", client["drivers"])


class FixtureImportTests(unittest.TestCase):
    def test_verified_fixture_source_replaces_and_restores_cached_integration(self):
        original = sys.modules["integration"]
        with tempfile.TemporaryDirectory() as temporary:
            server = Path(temporary)
            package = server / "tests/python/integration"
            package.mkdir(parents=True)
            (package / "__init__.py").write_text("")
            (package / "verified_fixture.py").write_text("value = 'accepted server fixture'\n")
            with ecosystem.fixture_imports(server, server / "consumer/driver.py"):
                fixture = importlib.import_module("integration.verified_fixture")
                self.assertEqual(Path(fixture.__file__).parent, package)
                self.assertEqual(fixture.value, "accepted server fixture")
        self.assertIs(sys.modules["integration"], original)
        self.assertNotIn("integration.verified_fixture", sys.modules)
