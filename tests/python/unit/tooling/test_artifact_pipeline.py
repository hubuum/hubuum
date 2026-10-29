"""The live artifact probe always reclaims its isolated database."""

from pathlib import Path
from types import SimpleNamespace
import unittest
from unittest import mock

from integration import artifact_pipeline as probe


class ArtifactPipelineTests(unittest.TestCase):
    def test_database_identity_survives_metadata_collection_and_worker_failure(self):
        for worker_failure in (False, True):
            with self.subTest(worker_failure=worker_failure):
                commands = []
                processes = []

                def run(command, **kwargs):
                    commands.append((command, kwargs.get("env", {})))
                    return SimpleNamespace(stdout='{"version":"18.6"}')

                def spawn(command, **kwargs):
                    process = mock.Mock()
                    process.returncode = 1 if worker_failure else 0
                    process.poll.return_value = process.returncode
                    kwargs["stdout"].write('ARTIFACT_PIPELINE_EVIDENCE {"streams":[]}\n')
                    processes.append(process)
                    return process

                args = SimpleNamespace(rows=1, row_bytes=2048, slots=2, rate=0,
                                       output_limit=1048576, modes=("export", "backup"), release=False)
                with mock.patch.object(probe.subprocess, "run", side_effect=run), \
                        mock.patch.object(probe.subprocess, "Popen", side_effect=spawn), \
                        mock.patch.object(probe.platform, "platform", return_value="test-host"), \
                        mock.patch("builtins.print"):
                    if worker_failure:
                        with self.assertRaisesRegex(RuntimeError, "worker exited"):
                            probe.run_pair("probe", Path("admin"), args, 0)
                    else:
                        result = probe.run_pair("probe", Path("admin"), args, 0)
                        self.assertEqual(result["database"]["version"], "18.6")

                create = next(command[-1] for command, _ in commands
                              if command[-1].startswith("CREATE DATABASE "))
                database = create.removeprefix("CREATE DATABASE ")
                self.assertRegex(database, r"^hubuum_artifact_probe_[0-9a-f]{32}$")
                self.assertEqual(commands[-1][0][-1], f"DROP DATABASE {database} WITH (FORCE)")
                self.assertEqual(commands[1][1]["PGDATABASE"], database)
                self.assertEqual(len(processes), 2)
