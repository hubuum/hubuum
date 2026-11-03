"""Pin real S3 fixture identity and enforce executable verification/cleanup."""
import io
import os
from pathlib import Path
import signal
import socket
import sys
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

    @unittest.skipUnless(os.name == "posix", "native MinIO fixtures require POSIX")
    def test_failure_terminates_acceptance_descendants(self):
        for failure in ("lifecycle", "command", "interruption"):
            with self.subTest(failure=failure), tempfile.TemporaryDirectory() as temporary, socket.socket() as listener:
                directory = Path(temporary)
                listener.bind(("127.0.0.1", 0))
                listener.listen(1)
                listener.settimeout(5)
                pid_file = directory / "child.pid"
                child = (
                    "import os,socket,time; from pathlib import Path; "
                    f"connection=socket.create_connection({listener.getsockname()!r}); "
                    "connection.sendall(b'R'); "
                    f"Path({str(pid_file)!r}).write_text(str(os.getpid())); time.sleep(60)"
                )
                trigger = (
                    f"Path({str(directory / 'request')!r}).write_text('invalid'); time.sleep(60)"
                    if failure == "lifecycle" else "raise SystemExit(7)" if failure == "command" else "time.sleep(60)"
                )
                parent = (
                    "import subprocess,sys,time; from pathlib import Path\n"
                    f"child=subprocess.Popen([sys.executable,'-I','-S','-c',{child!r}])\n"
                    "deadline=time.monotonic()+5\n"
                    f"while not Path({str(pid_file)!r}).exists():\n"
                    "    if child.poll() is not None or time.monotonic()>deadline: raise RuntimeError('child readiness failed')\n"
                    "    time.sleep(0.01)\n"
                    f"{trigger}\n"
                )
                instance = object.__new__(fixture.Fixture)
                instance.directory = directory
                instance.port = 1  # No provider is needed to exercise command cleanup.
                stopped = False
                sleep = fixture.time.sleep

                def wait_or_interrupt(delay):
                    if failure == "interruption" and pid_file.exists():
                        raise KeyboardInterrupt("acceptance interrupted")
                    sleep(delay)

                try:
                    error, message = ((ValueError, "Unknown fixture lifecycle request") if failure == "lifecycle"
                                      else (RuntimeError, "failed with exit 7") if failure == "command"
                                      else (KeyboardInterrupt, "acceptance interrupted"))
                    with mock.patch.object(fixture.time, "sleep", side_effect=wait_or_interrupt), self.assertRaisesRegex(error, message):
                        instance.run([sys.executable, "-I", "-S", "-c", parent])
                    connection, _ = listener.accept()
                    with connection:
                        connection.settimeout(5)
                        self.assertEqual(connection.recv(1), b"R")
                        # EOF proves that the descendant closed its socket even
                        # when its exited parent cannot reap it on our behalf.
                        self.assertEqual(connection.recv(1), b"")
                        stopped = True
                finally:
                    if not stopped and pid_file.exists():
                        try:
                            os.kill(int(pid_file.read_text()), signal.SIGKILL)
                        except ProcessLookupError:
                            pass
