"""Git checkout must preserve the bytes bound by lifecycle capture receipts."""

from pathlib import Path
import os
import subprocess
import tempfile
import unittest

from support import ROOT


FIXTURE = Path("docs/contracts/lifecycle-audit/catchup-output.jsonl")


class LifecycleFixtureCheckoutTests(unittest.TestCase):
    def test_windows_checkout_preserves_capture_output_bytes(self):
        original = (ROOT / FIXTURE).read_bytes()
        self.assertNotIn(b"\r", original, "the capture fixture must have canonical LF bytes")
        git = ["git", "-c", "core.autocrlf=true", "-c",
               f"core.attributesFile={os.devnull}"]
        environment = {**os.environ, "GIT_ATTR_NOSYSTEM": "1"}
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            (root / FIXTURE).parent.mkdir(parents=True)
            (root / FIXTURE).write_bytes(original)
            (root / ".gitattributes").write_bytes((ROOT / ".gitattributes").read_bytes())
            subprocess.run(git + ["init", "--quiet"], cwd=root, env=environment,
                           check=True, capture_output=True)
            subprocess.run(git + ["add", "--", ".gitattributes", FIXTURE.as_posix()],
                           cwd=root, env=environment, check=True, capture_output=True)
            checked_out = subprocess.check_output(
                git + ["cat-file", "--filters", ":" + FIXTURE.as_posix()],
                cwd=root, env=environment,
            )
        self.assertEqual(checked_out, original)
