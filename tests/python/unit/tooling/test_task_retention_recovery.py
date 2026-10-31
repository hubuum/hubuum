"""The physical drill cannot select unrelated volumes or extract escaping files."""

import io
import json
from pathlib import Path
import tarfile
import tempfile
from types import SimpleNamespace
import unittest

from support import task_retention_recovery as RECOVERY


class TaskRetentionRecoveryTests(unittest.TestCase):
    def test_restart_refreshes_runtime_and_host_database_addresses(self):
        name = "hubuum-corpus-0123456789ab"
        info = {"NetworkSettings": {"Networks": {name: {"IPAddress": "10.89.0.18"}}}}

        def docker(*args):
            if args == ("inspect", name + "-db"):
                return SimpleNamespace(stdout=json.dumps([info]))
            self.assertEqual(args, ("port", name + "-db", "5432/tcp"))
            return SimpleNamespace(stdout="127.0.0.1:32999\n")

        deployment = SimpleNamespace(name=name, containers=[name + "-db"], docker=docker,
                                     password="fixture-secret", database_ip="10.89.0.2",
                                     host_url="postgres://old-endpoint/source")
        RECOVERY._refresh_database_address(deployment)
        self.assertEqual(deployment.database_ip, "10.89.0.18")
        self.assertEqual(deployment.host_url,
                         "postgres://corpus:fixture-secret@127.0.0.1:32999/source?sslmode=disable")

    def test_volume_helper_keeps_selinux_and_matches_only_owned_process_level(self):
        self.assertEqual(RECOVERY._volume_security({}), [])
        self.assertEqual(RECOVERY._volume_security({"ProcessLabel": "system_u:system_r:container_t:s0:c2,c5"}),
                         ["--security-opt", "label=level:s0:c2,c5"])
        with self.assertRaisesRegex(ValueError, "unsupported"):
            RECOVERY._volume_security({"ProcessLabel": "label=disable"})

    def test_privileged_administrator_cannot_inherit_an_external_database(self):
        deployment = SimpleNamespace(
            host_url="postgres://fixture/source?sslmode=disable",
            host_environment=lambda: {"HUBUUM_DATABASE_URL": "postgres://outside/runtime",
                                      "HUBUUM_MIGRATION_DATABASE_URL": "postgres://outside/admin"},
        )
        application = "retention-reapply-" + "a" * 32
        for marker in (None, application):
            with self.subTest(marker=marker):
                environment = RECOVERY._admin_environment(deployment, marker)
                expected = deployment.host_url + ("&application_name=" + marker if marker else "")
                self.assertEqual(environment["HUBUUM_DATABASE_URL"], expected)
                self.assertEqual(environment["HUBUUM_MIGRATION_DATABASE_URL"], expected)

    def test_layout_requires_anonymous_volume_beneath_expected_pgdata(self):
        info = {"Config": {"Env": ["PGDATA=/var/lib/postgresql/18/docker"]},
                "Mounts": [{"Destination": "/var/lib/postgresql", "Type": "volume",
                            "Name": "a" * 64, "RW": True}], "Image": "sha256:fixture"}
        self.assertEqual(RECOVERY._layout(info)["volume"], "a" * 64)
        for kind, name, writable in (("bind", "a" * 64, True), ("volume", "production", True),
                                     ("volume", "a" * 64, False)):
            with self.subTest(kind=kind, name=name, writable=writable):
                info["Mounts"][0].update(Type=kind, Name=name, RW=writable)
                with self.assertRaisesRegex(ValueError, "Refusing"):
                    RECOVERY._layout(info)

    def test_pgdata_cannot_be_volume_root_or_escape_it(self):
        for pgdata in ("/var/lib/postgresql", "/var/lib/postgresql/../other",
                       "/var/lib/postgresql/18/../../other", "/production/pgdata"):
            with self.subTest(pgdata=pgdata), self.assertRaises(ValueError):
                RECOVERY._layout({"Config": {"Env": ["PGDATA=" + pgdata]},
                                  "Mounts": [{"Destination": "/var/lib/postgresql", "Type": "volume",
                                              "Name": "a" * 64, "RW": True}], "Image": "fixture"})

    def test_artifact_requires_owned_private_directory_and_no_symlink(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory).resolve()
            deployment = SimpleNamespace(root=root)
            self.assertEqual(RECOVERY._owned_path(deployment, root / "copy.tar"), root / "copy.tar")
            (root / "link.tar").symlink_to(root / "copy.tar")
            with self.assertRaisesRegex(ValueError, "directly inside"):
                RECOVERY._owned_path(deployment, root / "link.tar")
            root.chmod(0o755)
            try:
                with self.assertRaisesRegex(ValueError, "owner-only"):
                    RECOVERY._owned_path(deployment, root / "copy.tar")
            finally:
                root.chmod(0o700)

    def test_archive_rejects_links_traversal_and_incomplete_backup(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "backup.tar"
            for name, kind in (("../outside", tarfile.REGTYPE), ("link", tarfile.SYMTYPE),
                               ("link", tarfile.LNKTYPE), ("pipe", tarfile.FIFOTYPE)):
                with self.subTest(name=name, kind=kind):
                    with tarfile.open(path, "w") as archive:
                        member = tarfile.TarInfo(name)
                        member.type = kind
                        member.linkname = "/outside"
                        archive.addfile(member)
                    with self.assertRaisesRegex(ValueError, "unsafe"):
                        RECOVERY._validate_archive(path)
            with tarfile.open(path, "w") as archive:
                member = tarfile.TarInfo("PG_VERSION")
                member.size = 2
                archive.addfile(member, io.BytesIO(b"18"))
            with self.assertRaisesRegex(ValueError, "complete"):
                RECOVERY._validate_archive(path)

    def test_generated_plain_base_backup_members_are_accepted(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "backup.tar"
            with tarfile.open(path, "w") as archive:
                for name in ("./PG_VERSION", "./global/pg_control", "./backup_label", "./backup_manifest"):
                    archive.addfile(tarfile.TarInfo(name))
            RECOVERY._validate_archive(path)
            self.assertEqual(len(RECOVERY._digest(path)), 64)
