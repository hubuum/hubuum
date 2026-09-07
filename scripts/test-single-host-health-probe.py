#!/usr/bin/env python3
"""Regression tests for narrowly scoped, deadline-bound health probe retries."""

import importlib.util
import pathlib
import subprocess
import socketserver
import threading
import unittest
from unittest.mock import patch

SCRIPT_PATH = pathlib.Path(__file__).with_name("single-host-health-probe.py")
SPEC = importlib.util.spec_from_file_location("single_host_health_probe", SCRIPT_PATH)
if SPEC is None or SPEC.loader is None:
    raise RuntimeError(f"could not load {SCRIPT_PATH}")
MODULE = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(MODULE)
URL = "http://127.0.0.1:8080/healthz"


def response(code=0, status="200"):
    return subprocess.CompletedProcess(
        [], code, status, f"curl exit {code}" if code else ""
    )


class HealthProbeTests(unittest.TestCase):
    def test_real_curl_recovers_one_empty_response(self):
        requests = []

        class Handler(socketserver.BaseRequestHandler):
            def handle(self):
                self.request.settimeout(2)
                request = b""
                while b"\r\n\r\n" not in request:
                    chunk = self.request.recv(4096)
                    if not chunk:
                        return
                    request += chunk
                requests.append(request)
                if len(requests) == 1:
                    return
                self.request.sendall(
                    b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\nConnection: close\r\n\r\nok"
                )

        with socketserver.TCPServer(("127.0.0.1", 0), Handler) as server:
            thread = threading.Thread(target=server.serve_forever, daemon=True)
            thread.start()
            try:
                result = MODULE.probe(
                    f"http://127.0.0.1:{server.server_address[1]}/healthz", 7
                )
                self.assertEqual(result, MODULE.ProbeResult("200", 2))
                self.assertTrue(
                    all(request.startswith(b"GET /healthz ") for request in requests)
                )
            finally:
                server.shutdown()
                thread.join(timeout=2)

    def test_retries_only_pre_response_disconnects(self):
        for code in (52, 56):
            with self.subTest(code=code), patch.object(
                MODULE.subprocess,
                "run",
                side_effect=[response(code, "000"), response()],
            ) as run:
                self.assertEqual(MODULE.probe(URL, 7), MODULE.ProbeResult("200", 2))
                self.assertEqual(run.call_count, 2)

    def test_does_not_retry_http_errors(self):
        for status in ("401", "429", "500", "502", "503", "504"):
            with self.subTest(status=status), patch.object(
                MODULE.subprocess, "run", return_value=response(status=status)
            ) as run:
                result = MODULE.probe(URL, 7)
                self.assertEqual(result.status, status)
                self.assertTrue(result.error)
                run.assert_called_once()

    def test_does_not_retry_other_curl_failures(self):
        for code, status in (
            (7, "000"),
            (28, "000"),
            (60, "000"),
            (18, "200"),
            (56, "200"),
        ):
            with self.subTest(code=code, status=status), patch.object(
                MODULE.subprocess, "run", return_value=response(code, status)
            ) as run:
                self.assertTrue(MODULE.probe(URL, 7).error)
                run.assert_called_once()

    def test_repeated_disconnect_still_fails(self):
        with patch.object(
            MODULE.subprocess, "run", return_value=response(52, "000")
        ) as run:
            self.assertTrue(MODULE.probe(URL, 7).error)
        self.assertEqual(run.call_count, 2)

    def test_second_attempt_uses_only_remaining_time(self):
        with patch.object(
            MODULE.time, "monotonic", side_effect=[10, 10, 12]
        ), patch.object(
            MODULE.subprocess, "run", side_effect=[response(52, "000"), response()]
        ) as run:
            self.assertFalse(MODULE.probe(URL, 7).error)
        self.assertEqual(
            [call.kwargs["timeout"] for call in run.call_args_list], [7, 5]
        )

    def test_expired_deadline_does_not_start_second_attempt(self):
        with patch.object(
            MODULE.time, "monotonic", side_effect=[10, 10, 18]
        ), patch.object(
            MODULE.subprocess, "run", return_value=response(52, "000")
        ) as run:
            self.assertIn("deadline expired", MODULE.probe(URL, 7).error)
        run.assert_called_once()

    def test_subprocess_timeout_still_fails(self):
        with patch.object(
            MODULE.subprocess, "run", side_effect=subprocess.TimeoutExpired("curl", 7)
        ) as run:
            self.assertIn("deadline expired", MODULE.probe(URL, 7).error)
        run.assert_called_once()

    def test_rejects_non_health_urls(self):
        for suffix in ("/api/v1/tasks", "/healthz?change=true", "/readyz#fragment"):
            with self.subTest(suffix=suffix), self.assertRaises(ValueError):
                MODULE.probe("http://127.0.0.1:8080" + suffix, 7)

    def test_rejects_invalid_deadlines(self):
        for timeout in (0, -1, float("nan"), float("inf")):
            with self.subTest(timeout=timeout), self.assertRaises(ValueError):
                MODULE.probe(URL, timeout)


if __name__ == "__main__":
    unittest.main()
