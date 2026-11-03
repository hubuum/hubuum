"""Pin real S3 fixture identity and enforce executable verification/cleanup."""
import io
from pathlib import Path
import tempfile
import unittest
from unittest import mock

from integration import s3_artifacts as fixture


class S3FixtureTests(unittest.TestCase):
    def test_supported_platforms_have_fixed_release_digests(self):
        for system, machine in fixture.DIGESTS:
            with self.subTest(system=system, machine=machine):
                url, digest = fixture.fixture_identity(system, machine)
                self.assertIn("github.com/minio/minio/releases/download/" + fixture.RELEASE, url)
                self.assertRegex(digest, r"^[a-f0-9]{64}$")

    def test_unsupported_platform_is_explicit(self):
        with self.assertRaisesRegex(ValueError, "requires supported"):
            fixture.fixture_identity("Windows", "AMD64")

    def test_changed_download_is_never_executable(self):
        with tempfile.TemporaryDirectory() as temporary:
            with mock.patch.object(fixture.urllib.request, "urlopen", return_value=io.BytesIO(b"changed binary")):
                with self.assertRaisesRegex(ValueError, "SHA-256 mismatch"):
                    fixture.verified_binary(Path(temporary))
            self.assertFalse((Path(temporary) / "minio").exists())

    def test_fixture_stop_reaps_its_process(self):
        instance = object.__new__(fixture.Fixture)
        process = mock.Mock()
        instance.process = process
        instance.stop()
        process.kill.assert_called_once_with()
        process.wait.assert_called_once_with(timeout=10)
        self.assertIsNone(instance.process)

    def test_fixture_closes_on_acceptance_failure(self):
        instance = mock.Mock()
        instance.run.side_effect = RuntimeError("acceptance failed")
        with mock.patch.object(fixture, "verified_binary"), mock.patch.object(fixture, "Fixture", return_value=instance):
            with self.assertRaisesRegex(RuntimeError, "acceptance failed"):
                fixture.main([])
        instance.close.assert_called_once_with()
