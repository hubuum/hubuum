"""Verify discovery, isolation and failure propagation through the public runner."""
import json
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile
import unittest

from support import SCRIPTS, SUITE


class SuiteTests(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.directory = Path(temporary.name)
        self.suite = self.directory / 'repo/tests/python'
        for package in ('support', 'unit', 'integration'):
            (self.suite / package).mkdir(parents=True)
            (self.suite / package / '__init__.py').touch()
        shutil.copy(SUITE / 'run.py', self.suite / 'run.py')
        shutil.copy(SUITE / 'support/__init__.py', self.suite / 'support/__init__.py')

    def invoke(self, *arguments):
        return subprocess.run(
            [sys.executable, '-I', '-S', str(self.suite / 'run.py'), *arguments],
            cwd=self.directory, text=True, capture_output=True, timeout=15,
        )

    def test_default_discovers_new_categories_without_loading_integrations(self):
        category = self.suite / 'unit/new_category'
        category.mkdir()
        (category / '__init__.py').touch()
        (category / 'test_added.py').write_text(
            'import unittest\nclass Added(unittest.TestCase):\n'
            '    def test_discovered(self):\n        self.assertTrue(True)\n'
        )
        (self.suite / 'integration/__init__.py').write_text("raise RuntimeError('must stay opt-in')\n")
        result = self.invoke()
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn('Ran 1 test', result.stderr)

    def test_assertion_failure_exits_unsuccessfully(self):
        (self.suite / 'unit/test_failure.py').write_text(
            'import unittest\nclass Failure(unittest.TestCase):\n'
            '    def test_failure(self):\n        self.fail("expected fixture failure")\n'
        )
        result = self.invoke('unit')
        self.assertEqual(result.returncode, 1)
        self.assertIn('expected fixture failure', result.stderr)

    def test_listing_rejects_an_import_error(self):
        (self.suite / 'unit/test_broken.py').write_text('import nonexistent_suite_fixture_module\n')
        result = self.invoke('unit', '--list')
        self.assertEqual(result.returncode, 1)
        self.assertIn('nonexistent_suite_fixture_module', result.stderr)

    def test_empty_discovery_fails(self):
        result = self.invoke('unit')
        self.assertEqual(result.returncode, 1)
        self.assertIn('No Python tests discovered', result.stderr)

    def test_unknown_selector_fails(self):
        result = self.invoke('unit', 'missing_test')
        self.assertEqual(result.returncode, 1)
        self.assertIn('missing_test', result.stderr)

    def test_module_selection_does_not_load_unselected_tests(self):
        (self.suite / 'unit/test_selected.py').write_text(
            'import unittest\nclass Selected(unittest.TestCase):\n'
            '    def test_selected(self):\n        self.assertTrue(True)\n'
        )
        (self.suite / 'unit/test_unselected.py').write_text("raise RuntimeError('unselected')\n")
        result = self.invoke('unit', 'test_selected', '--list')
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(result.stdout.strip(), 'unit.test_selected.Selected.test_selected')

    def test_integration_forwards_arguments_and_exit_status(self):
        (self.suite / 'integration/monitoring.py').write_text(
            'import json\ndef main(argv):\n    print(json.dumps(argv))\n    return 7\n'
        )
        result = self.invoke('integration', 'monitoring', '--image', 'fixture-image')
        self.assertEqual(result.returncode, 7, result.stderr)
        self.assertEqual(json.loads(result.stdout), ['--image', 'fixture-image'])

    def test_regression_entrypoints_stay_in_the_test_suite(self):
        self.assertEqual(sorted(SCRIPTS.glob('test-*.py')), [])
        self.assertEqual(sorted(SCRIPTS.glob('test_*.py')), [])
