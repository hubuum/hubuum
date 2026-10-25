"""Regressions for the live measurement driver's request accounting."""
import unittest
from urllib.parse import parse_qs, urlsplit

from integration.class_field_discovery import Client, percentile


class RecordedClient(Client):
    def __init__(self, pages):
        self.pages = iter(pages)
        self.paths = []

    def get(self, path):
        self.paths.append(path)
        return next(self.pages)


class FieldDiscoveryDriverTests(unittest.TestCase):
    def test_personal_pagination_is_included_before_selecting_computed_objects(self):
        client = RecordedClient([
            ({"id": 7}, None), ({"definitions": []}, None),
            ([{"enabled": False}], "next/token"), ([{"enabled": True}], None),
            ([{"id": 9}], None),
        ])
        self.assertEqual(client.legacy("a/b", 20), [{"id": 9}])
        self.assertEqual(len(client.paths), 5)
        self.assertIn("a%2Fb", client.paths[0])
        self.assertEqual(parse_qs(urlsplit(client.paths[3]).query)["cursor"], ["next/token"])
        self.assertEqual(parse_qs(urlsplit(client.paths[-1]).query)["include"], ["computed"])

    def test_disabled_definitions_use_the_raw_object_route(self):
        client = RecordedClient([
            ({"id": 7}, None), ({"definitions": [{"enabled": False}]}, None),
            ([], None), ([], None),
        ])
        client.legacy("Hosts", 100)
        self.assertEqual(len(client.paths), 4)
        self.assertNotIn("include", parse_qs(urlsplit(client.paths[-1]).query))

    def test_unbounded_personal_pagination_is_rejected(self):
        client = RecordedClient([({"id": 7}, None), ({"definitions": []}, None)] + [([], "again")] * 32)
        with self.assertRaisesRegex(RuntimeError, "pagination exceeded"):
            client.legacy("Hosts", 100)

    def test_report_uses_nearest_rank_percentiles(self):
        self.assertEqual(percentile(list(range(1, 101)), 95), 95)
        self.assertEqual(percentile([5], 50), 5)
