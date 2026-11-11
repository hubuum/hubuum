"""Exercise the production candidate packaging script from CI."""

import os
from pathlib import Path
import subprocess
import tempfile
import unittest

ROOT = Path(__file__).resolve().parents[4]


class AuthenticationCandidateTests(unittest.TestCase):
    def test_candidate_binds_payload_source_and_tested_commit(self):
        workflow = (ROOT / ".github/workflows/ci.yml").read_text()
        step = workflow.split("      - name: Prepare authentication candidate\n", 1)[1]
        script = step.split("        run: |\n", 1)[1].split("      - name:", 1)[0]
        script = "\n".join(line[10:] for line in script.splitlines() if line.strip())
        with tempfile.TemporaryDirectory() as directory:
            temporary = Path(directory)
            (temporary / "production-container.tar").write_bytes(b"candidate image fixture")
            environment = dict(os.environ, RUNNER_TEMP=directory, SOURCE_SHA="accepted-source",
                               GITHUB_SERVER_URL="https://github.com",
                               GITHUB_REPOSITORY="hubuum/hubuum", GITHUB_RUN_ID="123",
                               GITHUB_RUN_ATTEMPT="2")
            subprocess.run(["bash", "-e", "-o", "pipefail", "-c", script],
                           cwd=ROOT, env=environment, check=True, capture_output=True)
            candidate = temporary / "auth-candidate"
            self.assertEqual((candidate / "SOURCE_COMMIT").read_text(), "accepted-source\n")
            expected = subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=ROOT, text=True)
            self.assertEqual((candidate / "TESTED_COMMIT").read_text(), expected)
            self.assertEqual((candidate / "WORKFLOW_RUN").read_text(),
                             "https://github.com/hubuum/hubuum/actions/runs/123/attempts/2\n")
            subprocess.run(["sha256sum", "--check", "SHA256SUMS"], cwd=candidate,
                           check=True, capture_output=True)
            (candidate / "production-container.tar").write_bytes(b"tampered")
            result = subprocess.run(["sha256sum", "--check", "SHA256SUMS"], cwd=candidate,
                                    capture_output=True)
            self.assertNotEqual(result.returncode, 0)
