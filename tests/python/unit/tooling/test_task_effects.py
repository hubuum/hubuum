"""The effect fixture exposes dispatch before releasing its synthetic response."""

from concurrent.futures import ThreadPoolExecutor
import json
from pathlib import Path
import tempfile
import time
import unittest
import urllib.request

from support.task_effects import EffectFixture


class TaskEffectFixtureTests(unittest.TestCase):
    def test_counter_is_visible_while_effect_response_is_held(self):
        with tempfile.TemporaryDirectory() as temporary:
            with EffectFixture(Path(temporary) / "effect") as fixture:
                fixture.hold_response("owned")
                request = urllib.request.Request(fixture.counter_url, method="POST",
                                                 data=b'{"probe":"owned"}',
                                                 headers={"Content-Type": "application/json"})

                def send():
                    with urllib.request.urlopen(request, timeout=10) as response:
                        return response.status, json.loads(response.read())

                with ThreadPoolExecutor(max_workers=1) as executor:
                    pending = executor.submit(send)
                    try:
                        deadline = time.monotonic() + 5
                        count = 0
                        while count != 1 and time.monotonic() < deadline:
                            with urllib.request.urlopen(fixture.counter_url + "?probe=owned", timeout=2) as response:
                                count = json.loads(response.read())["count"]
                            if count != 1:
                                time.sleep(0.01)
                        self.assertEqual(count, 1)
                        self.assertFalse(pending.done())
                    finally:
                        fixture.release_response("owned")
                    self.assertEqual(pending.result(timeout=5), (200, {"received": True}))

    def test_response_gates_are_bounded_and_reject_reusing_an_effect(self):
        fixture = EffectFixture(Path("unused"))
        for index in range(8):
            fixture.hold_response(str(index))
        for probe in ("0", "ninth"):
            with self.subTest(probe=probe), self.assertRaisesRegex(ValueError, "unique and bounded"):
                fixture.hold_response(probe)

