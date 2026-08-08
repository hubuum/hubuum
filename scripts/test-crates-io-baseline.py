#!/usr/bin/env python3
"""Regression tests for crates.io baseline detection."""

from __future__ import annotations

import importlib.util
import unittest
from pathlib import Path
from urllib.error import HTTPError, URLError


SCRIPT = Path(__file__).with_name("check-crates-io-baseline.py")
SPEC = importlib.util.spec_from_file_location("check_crates_io_baseline", SCRIPT)
if SPEC is None or SPEC.loader is None:
    raise RuntimeError(f"cannot load {SCRIPT}")
BASELINE = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(BASELINE)


class FakeResponse:
    def __init__(self, status: int) -> None:
        self.status = status

    def __enter__(self) -> FakeResponse:
        return self

    def __exit__(self, *_arguments: object) -> None:
        return None


class CratesIoBaselineTests(unittest.TestCase):
    def test_existing_package_has_a_baseline(self) -> None:
        def opener(request: object, *, timeout: int) -> FakeResponse:
            self.assertEqual(timeout, 30)
            self.assertEqual(
                request.full_url,
                "https://crates.io/api/v1/crates/hubuum-query",
            )
            return FakeResponse(200)

        self.assertTrue(
            BASELINE.registry_baseline_exists("hubuum-query", opener=opener)
        )

    def test_missing_package_is_an_initial_release(self) -> None:
        def opener(request: object, *, timeout: int) -> FakeResponse:
            raise HTTPError(request.full_url, 404, "not found", None, None)

        self.assertFalse(
            BASELINE.registry_baseline_exists("unpublished-package", opener=opener)
        )

    def test_registry_failure_does_not_look_like_an_initial_release(self) -> None:
        def opener(request: object, *, timeout: int) -> FakeResponse:
            raise HTTPError(request.full_url, 503, "unavailable", None, None)

        with self.assertRaisesRegex(BASELINE.BaselineLookupError, "HTTP 503"):
            BASELINE.registry_baseline_exists("hubuum-query", opener=opener)

    def test_network_failure_does_not_look_like_an_initial_release(self) -> None:
        def opener(request: object, *, timeout: int) -> FakeResponse:
            raise URLError("timed out")

        with self.assertRaisesRegex(BASELINE.BaselineLookupError, "timed out"):
            BASELINE.registry_baseline_exists("hubuum-query", opener=opener)


if __name__ == "__main__":
    unittest.main()
