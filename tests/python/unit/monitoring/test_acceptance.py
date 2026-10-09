"""Regression checks for safe live-monitoring failure evidence."""

import json
import ipaddress
import os
from pathlib import Path
import subprocess
from unittest.mock import Mock, patch
import unittest

from integration.monitoring import Installation, fixture_environment, network_subnets


class NetworkInspectionTests(unittest.TestCase):
    def test_docker_and_podman_preserve_ipv4_and_ipv6_occupied_subnets(self):
        for networks in (
            [{'IPAM': {'Config': [{'Subnet': '172.31.4.0/24'}, {'Subnet': 'fd00::/64'}]}}],
            [{'subnets': [{'subnet': '172.31.4.0/24'}, {'subnet': 'fd00::/64'}]}],
            [{'IPAM': {'Config': [{'Subnet': '172.31.4.0/24'}]}},
             {'subnets': [{'subnet': 'fd00::/64'}]}],
        ):
            with self.subTest(networks=networks):
                self.assertEqual(list(network_subnets(networks)), [
                    ipaddress.ip_network('172.31.4.0/24'), ipaddress.ip_network('fd00::/64'),
                ])

    def test_networks_without_allocated_subnets(self):
        for networks in ([], [{'IPAM': None}], [{'IPAM': {'Config': None}}],
                         [{'IPAM': {'Config': [{}]}}], [{'subnets': None}], [{'subnets': []}]):
            with self.subTest(networks=networks):
                self.assertEqual(list(network_subnets(networks)), [])

    def test_unknown_or_invalid_network_data_is_not_silently_treated_as_available(self):
        for networks in ([{}], [{'subnets': [{'subnet': 'invalid'}]}]):
            with self.subTest(networks=networks), self.assertRaises(ValueError):
                list(network_subnets(networks))


class FailureEvidenceTests(unittest.TestCase):
    def installation(self, stage):
        installation = Installation.__new__(Installation)
        installation.engine = 'docker'
        installation.report = {'stage': stage}
        installation.values = Mock(return_value={'POSTGRES_PASSWORD': 'fixture-password', 'HUBUUM_TOKEN_RETENTION_DAYS': '30'})
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
            'postgres://user:another-password@localhost/database; timeout=30000; fatal startup failure',
        ])
        installation.failure_diagnostics()
        evidence = json.dumps(installation.report)
        for secret in ('fixture-password', 'fixture-token-key', 'another-password'):
            self.assertNotIn(secret, evidence)
        self.assertIn('fatal startup failure', evidence)
        self.assertIn('timeout=30000', evidence)
        self.assertEqual(installation.report['containers'][0]['exit_code'], 1)

    def test_later_failures_do_not_publish_application_logs(self):
        installation = self.installation('data')
        installation.compose = Mock(return_value='fixture-container')
        installation.failure_diagnostics()
        installation.compose.assert_called_once_with('ps', '-aq')
        installation.values.assert_not_called()
        self.assertNotIn('startup_logs', installation.report)


class EnvironmentIsolationTests(unittest.TestCase):
    def test_fixture_rejects_inherited_deployment_overrides_but_keeps_engine_access(self):
        overrides = {
            'DATABASE_URL': 'host-database',
            'HUBUUM_DATABASE_URL': 'host-database',
            'HUBUUM_MIGRATION_DATABASE_URL': 'host-migrator',
            'POSTGRES_PASSWORD': 'host-password',
            'COMPOSE_FILE': 'another-project.yml',
            'MONITORING_HOST': 'another-host',
            'PROMETHEUS_PASSWORD': 'another-password',
            'GRAFANA_ADMIN_PASSWORD': 'another-password',
        }
        engine = {'PATH': '/tools', 'DOCKER_HOST': 'unix:///fixture-docker.sock'}
        with patch.dict(os.environ, overrides | engine, clear=True):
            environment = fixture_environment('isolated-project')
        self.assertFalse(overrides.keys() & environment.keys())
        for key, value in engine.items():
            self.assertEqual(environment[key], value)
        self.assertEqual(environment['COMPOSE_PROJECT_NAME'], 'isolated-project')

    def test_direct_compose_commands_use_the_isolated_environment(self):
        installation = Installation.__new__(Installation)
        installation.environment = fixture_environment('isolated-project')
        installation.engine = 'docker'
        installation.project = 'isolated-project'
        installation.directory = Path('/fixture')
        with patch('integration.monitoring.subprocess.run',
                   return_value=subprocess.CompletedProcess([], 0, stdout='')) as command:
            installation.compose('up', '-d', 'hubuum-api-standby')
        self.assertEqual(command.call_args.kwargs['env'], installation.environment)


class ComposeReplicaTests(unittest.TestCase):
    def test_exec_selects_deployed_replica_instead_of_one_off_worker(self):
        installation = Installation.__new__(Installation)
        installation.engine = 'docker'
        installation.project = 'isolated-project'
        installation.directory = Path('/fixture')
        installation.run = Mock(return_value='primary')

        result = installation.compose('exec', '-T', 'hubuum-api', 'cat', '/tmp/role')

        self.assertEqual(result, 'primary')
        installation.run.assert_called_once_with(
            'docker', 'compose', '-p', 'isolated-project', '--env-file', '/fixture/.env',
            '-f', '/fixture/compose.yml', 'exec', '--index', '1', '-T',
            'hubuum-api', 'cat', '/tmp/role')
