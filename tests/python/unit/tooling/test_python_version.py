"""Exercise Python entrypoints with unsupported versions and no site packages."""

import subprocess
import sys
import unittest

from support import ROOT


class PythonVersionTests(unittest.TestCase):
    def test_every_entrypoint_rejects_unsupported_python_before_running(self):
        for minor in (8, 9, 10):
            for script in [*sorted((ROOT / "scripts").glob("*.py")), ROOT / "tests/python/run.py"]:
                with self.subTest(version=f"3.{minor}", script=script.name):
                    result = subprocess.run(
                        [
                            sys.executable, "-I", "-S", "-c",
                            "import runpy, sys; "
                            f"sys.version_info = (3, {minor}, 0, 'final', 0); "
                            f"sys.version = '3.{minor}.0'; "
                            "runpy.run_path(sys.argv[1], run_name='__main__')",
                            str(script),
                        ],
                        cwd=ROOT, capture_output=True, text=True, check=False,
                        timeout=10,
                    )
                    self.assertEqual(result.returncode, 1, result.stderr)
                    self.assertEqual(result.stdout, "")
                    self.assertIn("requires Python 3.11 or newer", result.stderr)
                    self.assertIn(f"found 3.{minor}.0", result.stderr)
                    self.assertIn("Install Python 3.11+", result.stderr)
                    self.assertIn("python3 on PATH", result.stderr)
                    self.assertNotIn("Traceback", result.stderr)

    def test_repository_checks_work_without_site_packages(self):
        for script, arguments in (
            ("check-rust-api-policy.py", []),
            ("check-supply-chain-policy.py", []),
            ("generate-project-inventory.py", ["--check"]),
            ("observability.py", ["check"]),
            ("observability.py", ["generate", "--check"]),
        ):
            with self.subTest(script=script):
                result = subprocess.run(
                    [sys.executable, "-I", "-S", str(ROOT / "scripts" / script),
                     *arguments],
                    cwd=ROOT, capture_output=True, text=True, check=False,
                    timeout=60,
                )
                self.assertEqual(result.returncode, 0, result.stderr)
