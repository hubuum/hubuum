"""Regression checks for safe live-monitoring failure evidence."""

import json
from unittest.mock import Mock
import unittest

from integration.monitoring import Installation


class FailureEvidenceTests(unittest.TestCase):
    def installation(self, stage):
        installation = Installation.__new__(Installation)
        installation.engine = 'docker'
        installation.report = {'stage': stage}
        installation.values = Mock(return_value={'POSTGRES_PASSWORD': 'fixture-password'})
        container = {
            'Config': {'Labels': {'com.docker.compose.service': 'hubuum-api'},
                       'Env': ['HUBUUM_TOKEN_HASH_KEY=fixture-token-key']},
            'State': {'Status': 'restarting', 'ExitCode': 1, 'OOMKilled': False,
                      'Health': {'Status': 'unhealthy'}},
            'RestartCount': 3,
        }
        installation.run = Mock(return_value=json.dumps([container]))
        return installation

    def test_startup_evidence_redacts_credentials_and_retains_the_failure(self):
        installation = self.installation('start')
        installation.compose = Mock(side_effect=[
            'fixture-container',
            'database rejected fixture-password; key=fixture-token-key; '
            'postgres://user:another-password@localhost/database; fatal startup failure',
        ])
        installation.failure_diagnostics()
        evidence = json.dumps(installation.report)
        for secret in ('fixture-password', 'fixture-token-key', 'another-password'):
            self.assertNotIn(secret, evidence)
        self.assertIn('fatal startup failure', evidence)
        self.assertEqual(installation.report['containers'][0]['exit_code'], 1)

    def test_later_failures_do_not_publish_application_logs(self):
        installation = self.installation('data')
        installation.compose = Mock(return_value='fixture-container')
        installation.failure_diagnostics()
        installation.compose.assert_called_once_with('ps', '-aq')
        installation.values.assert_not_called()
        self.assertNotIn('startup_logs', installation.report)
