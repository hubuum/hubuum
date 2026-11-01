"""Exercise CI tier selection and protect the main publication gate."""

import os
from pathlib import Path
import re
import shlex
import subprocess
import tempfile
import textwrap
import unittest
from unittest.mock import patch

from integration import rust_tests
from support import ROOT


WORKFLOW = (ROOT / ".github/workflows/ci.yml").read_text()


def job(name):
    return re.split(r"\n  [\w-]+:\n", WORKFLOW.split(f"\n  {name}:\n", 1)[1], 1)[0]


class CiTierTests(unittest.TestCase):
    def policy(self, **changes):
        script = job("changes").split("      - name: Select CI tier\n", 1)[1]
        script = textwrap.dedent(script.split("        run: |\n", 1)[1])
        variables = dict.fromkeys(("CODE_CHANGED", "RUST_CHANGED", "CONTAINER_CHANGED",
                                  "OPENAPI_CHANGED", "OPERATIONAL_CONTRACT_CHANGED",
                                  "MARKDOWN_CHANGED", "IS_DRAFT", "FORCE_FULL", "FORCE_TASKS"), "false")
        variables.update({"IS_PULL_REQUEST": "true", **changes})
        with tempfile.TemporaryDirectory() as directory:
            output = Path(directory) / "outputs"
            subprocess.run(["bash", "-euo", "pipefail", "-c", script], check=True,
                           env={**os.environ, **variables, "GITHUB_OUTPUT": str(output)})
            return dict(line.split("=", 1) for line in output.read_text().splitlines())

    def test_deployment_edit_keeps_container_and_script_validation(self):
        result = self.policy(CODE_CHANGED="true", CONTAINER_CHANGED="true")
        self.assertEqual({key: result[key] for key in (
            "run_full_ci", "run_container", "run_rust_ci", "run_default_suite", "run_openapi",
        )}, {"run_full_ci": "true", "run_container": "true", "run_rust_ci": "false",
            "run_default_suite": "false", "run_openapi": "false"})

    def test_rust_edit_runs_full_matrix(self):
        result = self.policy(CODE_CHANGED="true", RUST_CHANGED="true")
        self.assertEqual(result["run_rust_ci"], "true")
        self.assertIn("all-features", result["feature_matrix"])

    def test_draft_rust_edit_keeps_default_suite(self):
        result = self.policy(CODE_CHANGED="true", RUST_CHANGED="true", IS_DRAFT="true")
        self.assertEqual(result["run_default_suite"], "true")
        self.assertEqual(result["run_rust_ci"], "false")

    def test_draft_contract_inputs_wait_for_final_validation(self):
        result = self.policy(IS_DRAFT="true", CODE_CHANGED="true", RUST_CHANGED="true",
                             OPENAPI_CHANGED="true", OPERATIONAL_CONTRACT_CHANGED="true")
        for key in ("full_requested", "run_openapi", "run_operational_contract", "run_full_ci"):
            self.assertEqual(result[key], "false", key)

    def test_ready_contract_only_edits_still_run_contract_checks(self):
        result = self.policy(OPENAPI_CHANGED="true", OPERATIONAL_CONTRACT_CHANGED="true")
        self.assertEqual(result["run_openapi"], "true")
        self.assertEqual(result["run_operational_contract"], "true")

    def test_main_always_selects_final_validation_for_affected_inputs(self):
        result = self.policy(IS_PULL_REQUEST="false", CODE_CHANGED="true", RUST_CHANGED="true",
                             CONTAINER_CHANGED="true")
        for key in ("full_requested", "run_rust_ci", "run_container", "run_openapi"):
            self.assertEqual(result[key], "true", key)

    def test_markdown_does_not_select_python_or_monitoring_jobs(self):
        for name in ("python-tooling", "monitoring-integration"):
            with self.subTest(job=name):
                condition = job(name).split("    if: >-\n", 1)[1].split("    runs-on:", 1)[0]
                self.assertNotIn("outputs.markdown", condition)
                self.assertIn("outputs.run_full_ci == 'true'", condition)

    def test_monitoring_waits_for_final_validation(self):
        condition = job("monitoring-integration").split("    if: >-\n", 1)[1].split("    runs-on:", 1)[0]
        self.assertNotIn("outputs.code", condition)

    def test_expensive_package_checks_wait_for_final_validation(self):
        package_selection = job("rust-api-policy").split("      - name: Select supported Rust packages\n", 1)[1]
        self.assertIn("if: startsWith(github.ref, 'refs/tags/v') || needs.changes.outputs.full_requested == 'true'", package_selection)
        smoke = job("feature-permutations")
        for name in ("Test PostgreSQL adapter-native contracts", "Test workspace package contracts"):
            with self.subTest(step=name):
                step = smoke.split(f"      - name: {name}\n", 1)[1].split("      - name:", 1)[0]
                self.assertIn("needs.changes.outputs.run_rust_ci == 'true'", step)

    def test_external_authorization_waits_for_final_validation(self):
        condition = job("treetop-authorization-conformance").split("    if: >-\n", 1)[1].split("    runs-on:", 1)[0]
        self.assertIn("needs.changes.outputs.full_requested == 'true' &&", condition)

    def test_fuzzing_rechecks_draft_transitions_and_waits_until_ready(self):
        workflow = (ROOT / ".github/workflows/regex-fuzz.yml").read_text()
        self.assertIn("ready_for_review, converted_to_draft", workflow)
        self.assertIn("github.event_name != 'pull_request' || !github.event.pull_request.draft", workflow)

    def test_full_label_overrides_path_and_draft_selection(self):
        result = self.policy(FORCE_FULL="true", IS_DRAFT="true")
        for key in ("run_rust_ci", "run_full_ci", "run_container", "run_default_suite", "run_tasks"):
            self.assertEqual(result[key], "true", key)
        for key in ("run_openapi", "run_operational_contract", "full_requested"):
            self.assertEqual(result[key], "true", key)

    def test_validate_label_requests_full_ci_without_selecting_performance(self):
        changes = job("changes")
        self.assertIn("contains(github.event.pull_request.labels.*.name, 'ci:validate')", changes)
        self.assertIn("github.event.label.name == 'ci:validate'", changes)
        # Both labels feed the existing, already covered correctness-tier resolver.
        self.assertEqual(self.policy(FORCE_FULL="true", IS_DRAFT="true")["run_container"], "true")
        self.assertNotIn("run_benchmarks", changes)

    def test_long_validation_jobs_can_cancel_when_the_run_is_obsolete(self):
        for name in ("dependency-policy", "python-tooling", "monitoring-integration", "rust-api-policy",
                     "openapi-contract", "operational-contract", "treetop-authorization-conformance", "container-build"):
            with self.subTest(job=name):
                condition = job(name).split("    if: >-\n", 1)[1].split("    runs-on:", 1)[0]
                self.assertIn("!cancelled()", condition)
                self.assertNotIn("always()", condition)

    def test_tasks_label_selects_only_the_dedicated_lane_on_a_documentation_draft(self):
        result = self.policy(FORCE_TASKS="true", IS_DRAFT="true")
        self.assertEqual(result["run_tasks"], "true")
        for key in ("run_rust_ci", "run_full_ci", "run_container", "run_default_suite"):
            self.assertEqual(result[key], "false", key)

    def test_ready_rust_changes_do_not_implicitly_select_task_qualification(self):
        self.assertEqual(self.policy(CODE_CHANGED="true", RUST_CHANGED="true")["run_tasks"], "false")

    def test_removing_the_tasks_label_deselects_task_qualification(self):
        self.assertEqual(self.policy(IS_DRAFT="true")["run_tasks"], "false")

    def test_task_label_events_reclassify_and_share_validation_concurrency(self):
        changes = job("changes").split("    runs-on:", 1)[0]
        self.assertIn("github.event.label.name == 'ci:tasks'", changes)
        concurrency = WORKFLOW.split("concurrency:\n", 1)[1].split("\nenv:", 1)[0]
        self.assertIn("github.event.label.name != 'ci:tasks'", concurrency)



class DatabaseInitializationTests(unittest.TestCase):
    def test_every_diesel_setup_finishes_production_backfills(self):
        # Applying versioned SQL alone leaves legacy identity columns empty.
        # Check every provisioning path, including the Windows retry database.
        lines = WORKFLOW.splitlines()
        setups = [index for index, line in enumerate(lines) if ' migration run ' in line]
        self.assertTrue(setups)
        for index in setups:
            with self.subTest(line=index + 1):
                following = '\n'.join(lines[index + 1:index + 4])
                self.assertIn('--bin hubuum-admin -- --migrate', following)
                if lines[index].lstrip().startswith('& '):
                    self.assertIn('Migration backfill failed', '\n'.join(lines[index + 1:index + 5]))

    def test_windows_initial_backfill_uses_the_current_step_database(self):
        setup = job("test").split("      - name: Set up database (Windows)\n", 1)[1]
        setup = setup.split("      - name:", 1)[0]
        # GITHUB_ENV updates only subsequent steps, while this step selected a
        # dynamic PostgreSQL port. The initial migration must use its local URL.
        command = next(line for line in setup.splitlines() if '--bin hubuum-admin' in line)
        self.assertIn('--migration-database-url "$dbUrl"', command)

    def test_application_test_commands_receive_isolated_restore_authority(self):
        smoke = WORKFLOW.split('      - name: Test (${{ matrix.feature.name }})\n', 1)[1].split('\n  valkey-', 1)[0]
        self.assertIn('integration rust-tests', smoke)
        native = job("test").split('      - name: Run tests (non-Windows, production features)\n', 1)[1]
        self.assertIn('integration rust-tests -- cargo test --no-default-features', native)
        self.assertEqual(native.count('integration rust-tests -- cargo @testArgs'), 2)

    def test_feature_compilation_cannot_replace_the_restore_migration_helper(self):
        smoke = job("feature-permutations")
        setup = smoke.split('      - name: Set up database\n', 1)[1].split('      - name:', 1)[0]
        preserve = next(line.strip() for line in setup.splitlines() if line.strip().startswith('cp '))
        test = smoke.split('      - name: Test (${{ matrix.feature.name }})\n', 1)[1]
        command = test.split('        run: >-\n', 1)[1].replace('${{ matrix.feature.args }}', '')
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            binary = root / 'target/release/hubuum-admin'
            binary.parent.mkdir(parents=True)
            binary.write_text('migration enabled')
            subprocess.run(['bash', '-eu', '-c', preserve], cwd=root, check=True,
                           env={**os.environ, 'RUNNER_TEMP': directory})
            binary.write_text('feature matrix without migrations')
            argv = shlex.split(command.replace('$RUNNER_TEMP', directory))
            with patch.object(rust_tests, 'run_tests') as run:
                rust_tests.main(['--database-url', 'postgres://localhost/test',
                                 *argv[argv.index('rust-tests') + 1:]])
            args = run.call_args.args
            self.assertEqual(args[1].read_text(), 'migration enabled')
            self.assertEqual(args[0], ['cargo', 'test', '-F', 'integration-test-support',
                                      '--locked', '--release'])


class PublicationGateTests(unittest.TestCase):
    def test_candidates_can_build_before_validation_but_cannot_publish(self):
        for name in ("build-main-linux-artifacts", "build-main-native-artifacts"):
            with self.subTest(job=name):
                block = job(name)
                self.assertNotIn("ci-gate", block)
                self.assertIn("needs.changes.result == 'success'", block)
                self.assertIn("name: candidate-main-", block)
                self.assertNotIn("name: main-", block)
        promotion = job("publish-main-artifacts")
        self.assertIn("needs: [ci-gate, build-main-linux-artifacts, build-main-native-artifacts]", promotion)
        self.assertNotIn("always()", promotion)
        self.assertIn("needs.ci-gate.result == 'success'", promotion)
        self.assertIn("name: main-", promotion)

    def test_gate_still_requires_deployment_and_container_checks(self):
        gate = job("ci-gate")
        self.assertIn("      - deployment-checks\n", gate)
        self.assertIn("      - deployment-build-contracts\n", gate)
        for name in ("container-build", "container-monitoring", "container-deployment"):
            with self.subTest(job=name):
                self.assertIn(f"      - {name}\n", gate)

    def test_tag_release_requires_all_container_checks(self):
        gate = job("validate-tag-release")
        for name in ("container-build", "container-monitoring", "container-deployment"):
            with self.subTest(job=name):
                self.assertIn(f"      - {name}\n", gate)
                self.assertIn(f"needs.{name}.result == 'success'", gate)

    def test_container_acceptance_consumes_the_same_build_in_parallel(self):
        producer = job("container-build").split(
            "      - name: Share verified production image\n", 1,
        )[1]
        artifact = re.search(r"^          name: (.+)$", producer, re.MULTILINE).group(1)
        for name in ("container-monitoring", "container-deployment"):
            with self.subTest(job=name):
                consumer = job(name)
                self.assertIn("    needs: [changes, container-build]\n", consumer)
                self.assertIn("needs.changes.outputs.run_container == 'true'", consumer)
                self.assertIn("!cancelled() && needs.container-build.result == 'success'", consumer)
                download = consumer.split(
                    "      - name: Download verified production image\n", 1,
                )[1]
                consumed = re.search(r"^          name: (.+)$", download, re.MULTILINE).group(1)
                self.assertEqual(consumed, artifact)
                self.assertIn('docker load --input "$RUNNER_TEMP/production-container.tar"', consumer)

    def test_task_lane_reuses_the_image_and_is_a_required_gate_when_selected(self):
        tasks = job("task-system")
        self.assertIn("needs.changes.outputs.run_tasks == 'true'", tasks)
        self.assertIn("needs.container-build.result == 'success'", tasks)
        self.assertIn("name: production-container", tasks)
        self.assertIn('cargo test --locked --release -p hubuum-storage-memory', tasks)
        self.assertIn('--test task_retention', tasks)
        self.assertIn('--count 100 --profile local --runs 1', tasks)
        self.assertIn('--safety-probes --recovery-probes --physical-recovery-probes', tasks)
        self.assertNotIn('--output-probe', tasks)
        self.assertNotIn('--steady-seconds', tasks)
        self.assertIn('      - task-system\n', job("ci-gate"))
        self.assertIn("needs.changes.outputs.run_tasks == 'true'", job("container-build"))

    def test_failed_native_task_contracts_cannot_be_hidden_by_tee(self):
        tasks = job("task-system")
        block = tasks.split("      - name: Run native task-retention contracts and query budgets\n", 1)[1]
        script = textwrap.dedent(block.split("        run: |\n", 1)[1].split("      - name:", 1)[0])
        with tempfile.TemporaryDirectory() as directory:
            for name, code in (("hubuum-admin", 0), ("cargo", 43)):
                path = Path(directory) / name
                path.write_text(f"#!/bin/sh\nexit {code}\n")
                path.chmod(0o700)
            result = subprocess.run(["bash", "-e", "-c", script], cwd=directory, capture_output=True,
                                    env={**os.environ, "RUNNER_TEMP": directory,
                                         "PATH": directory + os.pathsep + os.environ["PATH"]})
            self.assertEqual(result.returncode, 43)

    def test_task_evidence_gate_rejects_incomplete_or_invalid_measurements(self):
        import json

        block = job("task-system").split("      - name: Verify task-system evidence\n", 1)[1]
        script = textwrap.dedent(block.split("        run: |\n", 1)[1].split("      - name:", 1)[0])
        for invalid in (None, "cleanup", "workload", "measurement", "monitor"):
            with self.subTest(invalid=invalid), tempfile.TemporaryDirectory() as directory:
                root = Path(directory) / "task-system-evidence/run-1"
                root.mkdir(parents=True)
                (root / "cleanup.json").write_text(json.dumps({"completed": invalid != "cleanup"}))
                (root / "resources.json").write_text(json.dumps({
                    "workload_completed": invalid != "workload", "measurement_valid": invalid != "measurement",
                    "monitor_errors": ["missing process"] if invalid == "monitor" else [],
                }))
                result = subprocess.run(["bash", "-euo", "pipefail", "-c", script], cwd=directory,
                                        capture_output=True, text=True)
                self.assertEqual(result.returncode == 0, invalid is None, result.stderr)

    def test_deployment_inputs_still_run_rust_container_contracts(self):
        contracts = job("deployment-build-contracts")
        self.assertIn("needs.changes.outputs.run_container == 'true'", contracts)
        self.assertIn("needs.changes.outputs.run_default_suite != 'true'", contracts)
        self.assertIn("cargo test --bin hubuum-server --locked", contracts)
