"""Guard against false positives in deployment event-metric comparisons."""

from copy import deepcopy
import json
from unittest.mock import Mock, patch
import unittest

from integration.monitoring_events import EventScenario, INSTANCES, counts, queue_matches
from integration.monitoring import Installation


class OverlappingAlertTests(unittest.TestCase):
    def test_standby_check_runs_inside_dead_letter_hold_before_replica_recovery(self):
        scenario = Mock(spec=EventScenario)
        scenario.installation = Mock()
        scenario.installation.query.return_value = []
        scenario.worker, scenario.worker_started = 'worker', False
        scenario.caddyfile, scenario.original_caddy = Mock(), 'original'
        scenario.delivery_id = 1
        scenario.report = {'alert': {}}
        scenario.alert_state.side_effect = ['inactive', 'pending', 'firing', 'inactive']
        scenario.sql_counts.return_value = counts(total=2, succeeded=1, failed=1)
        completed = []
        scenario.installation.alert.side_effect = lambda: completed.append('standby recovered')

        def wait(description, predicate, timeout=120):
            if description == 'production dead-letter alert pending':
                self.assertEqual(completed, [])
            if description == 'production dead-letter alert firing':
                self.assertEqual(completed, ['standby recovered'])
                self.assertEqual(timeout, 720)
            self.assertTrue(predicate(), description)

        def snapshot(name, *_):
            if name == 'recovered':
                self.assertEqual(completed, ['standby recovered'])

        scenario.snapshot.side_effect = snapshot
        with patch('integration.monitoring_events.wait_for', side_effect=wait):
            EventScenario.run(scenario)
        scenario.installation.alert.assert_called_once()
        self.assertTrue(scenario.report['alert']['recovered'])

    def test_failed_scrape_alert_still_restarts_standby(self):
        installation = Mock()
        installation.request.return_value = (200, {'data': {'groups': [{'rules': [
            {'name': 'HubuumScrapeUnavailable', 'duration': 300},
        ]}]}})
        with patch('integration.monitoring.wait_for', side_effect=AssertionError('timeout')):
            with self.assertRaisesRegex(AssertionError, 'timeout'):
                Installation.alert(installation)
        self.assertEqual(installation.compose.call_args.args, ('start', 'hubuum-api-standby'))


class EventQueueComparisonTests(unittest.TestCase):
    def setUp(self):
        self.expected = counts(total=2, succeeded=1, dead=1)
        self.rows = [{'metric': {'deployment': 'localhost', 'queue': 'delivery', 'state': state},
                      'value': [0, str(value)]} for state, value in self.expected.items()]

    def test_accepts_complete_recorded_vector_with_zero_states(self):
        self.assertTrue(queue_matches(self.rows, self.expected))

    def test_both_raw_replicas_are_required_independently(self):
        rows = [dict(row, metric=row['metric'] | {'instance': instance})
                for instance in INSTANCES for row in self.rows]
        self.assertTrue(queue_matches(rows, self.expected, replicas=True))
        self.assertFalse(queue_matches(rows[:len(self.rows)], self.expected, replicas=True))
        rows[-1]['value'] = [0, '1']
        self.assertFalse(queue_matches(rows, self.expected, replicas=True))

    def test_rejects_plausible_but_wrong_dashboard_data(self):
        variants = {'empty': [], 'missing_zero': self.rows[:-1], 'duplicate': self.rows + self.rows[:1]}
        for label, change in (
            ('wrong_deployment', lambda row: row['metric'].update(deployment='elsewhere')),
            ('wrong_queue', lambda row: row['metric'].update(queue='fanout')),
            ('sum_instead_of_max', lambda row: row.update(value=[0, str(float(row['value'][1]) * 2)])),
            ('nan', lambda row: row.update(value=[0, 'NaN'])),
            ('raw_instead_of_recorded', lambda row: row['metric'].update(instance='hubuum-api')),
        ):
            rows = deepcopy(self.rows)
            for row in rows:
                change(row)
            variants[label] = rows
        for label, rows in variants.items():
            with self.subTest(label=label):
                self.assertFalse(queue_matches(rows, self.expected))


class EventReceiverEvidenceTests(unittest.TestCase):
    def scenario(self, requests):
        scenario = EventScenario.__new__(EventScenario)
        scenario.event_uuid = 'known-event-uuid'
        scenario.report = {}
        scenario.installation = Mock()
        scenario.installation.compose.return_value = '\n'.join(json.dumps(row) for row in requests)
        return scenario

    def request(self, event='known-event-uuid', status=503):
        return {'request': {'method': 'POST', 'uri': '/retry', 'tls': {'version': 772},
                            'headers': {'Idempotency-Key': [event]}}, 'status': status}

    def test_correlates_real_transport_attempts_with_the_canonical_event(self):
        scenario = self.scenario([self.request(), self.request(status=200)])
        scenario.received([('/retry', 503), ('/retry', 200)])
        self.assertEqual(scenario.report['receiver_requests'], [('/retry', 503), ('/retry', 200)])

    def test_rejects_an_acknowledgement_for_a_different_event(self):
        scenario = self.scenario([self.request(event='unrelated-event')])
        with self.assertRaisesRegex(AssertionError, 'different event UUID'):
            scenario.received([('/retry', 503)])

    def test_rejects_unexpected_duplicate_transport_attempts(self):
        scenario = self.scenario([self.request(), self.request()])
        with self.assertRaisesRegex(AssertionError, 'Unexpected receiver requests'):
            scenario.received([('/retry', 503)])

    def test_rejects_success_when_a_real_503_was_expected(self):
        scenario = self.scenario([self.request(status=200)])
        with self.assertRaisesRegex(AssertionError, 'Unexpected receiver requests'):
            scenario.received([('/retry', 503)])
