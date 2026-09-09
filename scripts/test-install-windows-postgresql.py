#!/usr/bin/env python3
"""Exercise archive integrity and extraction without downloading or running Windows tools."""

from __future__ import annotations

import hashlib
import importlib.util
from pathlib import Path
import tempfile
import unittest
from zipfile import ZipFile


SCRIPT = Path(__file__).with_name("install-windows-postgresql.py")
SPEC = importlib.util.spec_from_file_location("install_windows_postgresql", SCRIPT)
if SPEC is None or SPEC.loader is None:
    raise RuntimeError(f"could not load {SCRIPT}")
INSTALLER = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(INSTALLER)


class PostgreSQLArchiveTests(unittest.TestCase):
    def setUp(self) -> None:
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name)
        self.archive = self.root / "postgres.zip"
        self.destination = self.root / "installed"

    def fixture(self, *, extra: tuple[str, ...] = (), missing: str = "") -> str:
        names = INSTALLER.REQUIRED_FILES | set(extra)
        with ZipFile(self.archive, "w") as archive:
            for name in sorted(names - {missing}):
                archive.writestr(name, b"fixture")
        return hashlib.sha256(self.archive.read_bytes()).hexdigest()

    def test_extracts_server_dependencies_and_licenses_without_gui(self) -> None:
        digest = self.fixture(extra=(
            "pgsql/bin/runtime.dll",
            "pgsql/include/libpq-fe.h",
            "pgsql/lib/extension.dll",
            "pgsql/share/extension/example.sql",
            "pgsql/server_license.txt",
            "pgsql/pgAdmin 4/application.exe",
            "pgsql/StackBuilder/application.exe",
        ))
        root = INSTALLER.extract_archive(self.archive, self.destination, digest)
        for name in (
            "bin/runtime.dll", "include/libpq-fe.h", "lib/extension.dll",
            "share/extension/example.sql", "server_license.txt",
        ):
            with self.subTest(name=name):
                self.assertEqual((root / name).read_bytes(), b"fixture")
        self.assertFalse((root / "pgAdmin 4").exists())
        self.assertFalse((root / "StackBuilder").exists())

    def test_checksum_mismatch_fails_before_extraction(self) -> None:
        digest = self.fixture()
        with self.archive.open("ab") as output:
            output.write(b"tampered")
        with self.assertRaisesRegex(ValueError, "checksum mismatch"):
            INSTALLER.extract_archive(self.archive, self.destination, digest)
        self.assertFalse(self.destination.exists())

    def test_missing_required_file_fails_before_extraction(self) -> None:
        digest = self.fixture(missing="pgsql/lib/libpq.lib")
        with self.assertRaisesRegex(ValueError, "missing required files"):
            INSTALLER.extract_archive(self.archive, self.destination, digest)
        self.assertFalse(self.destination.exists())

    def test_unsafe_paths_fail_before_extraction(self) -> None:
        for name in (
            "pgsql/bin/../../../escaped", "/absolute", "C:/absolute",
            "pgsql/bin/..\\escaped", "pgsql/bin/file:stream",
        ):
            with self.subTest(name=name):
                digest = self.fixture(extra=(name,))
                with self.assertRaisesRegex(ValueError, "Unsafe PostgreSQL archive path"):
                    INSTALLER.extract_archive(self.archive, self.destination, digest)
                self.assertFalse(self.destination.exists())


if __name__ == "__main__":
    unittest.main()
