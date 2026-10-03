#!/usr/bin/env python3
"""Serve the Treetop schema before making its policy fixture available."""

from __future__ import annotations

import sys

if sys.version_info < (3, 11):
    sys.exit(
        "Hubuum tooling requires Python 3.11 or newer; found "
        + sys.version.split()[0]
        + ". Install Python 3.11+ and ensure python3 on PATH selects it."
    )

import argparse
import functools
import http.server
import json
import pathlib
import threading
import time
import urllib.error
import urllib.request
from collections.abc import Callable


def schema_is_loaded(treetop_url: str) -> bool:
    try:
        with urllib.request.urlopen(f"{treetop_url}/api/v1/status", timeout=1) as response:
            status = json.load(response)
        return status["policy_configuration"]["schema"]["entries"] > 0
    except urllib.error.HTTPError as error:
        error.close()
        return False
    except (OSError, ValueError, KeyError, TypeError):
        return False


class FixtureHandler(http.server.SimpleHTTPRequestHandler):
    def __init__(
        self,
        *args: object,
        schema_permits: threading.Semaphore,
        schema_ready: Callable[[], bool],
        schema_wait_timeout: float = 30,
        **kwargs: object,
    ) -> None:
        self.schema_permits = schema_permits
        self.schema_ready = schema_ready
        self.schema_wait_timeout = schema_wait_timeout
        super().__init__(*args, **kwargs)

    def _consume_schema_fetch(self) -> bool:
        deadline = time.monotonic() + self.schema_wait_timeout
        if not self.schema_permits.acquire(timeout=self.schema_wait_timeout):
            self.send_error(503, "schema fixture was not fetched first")
            return False
        while time.monotonic() < deadline:
            if self.schema_ready():
                return True
            time.sleep(0.01)
        self.send_error(503, "Treetop did not load the schema fixture")
        return False

    def do_HEAD(self) -> None:  # noqa: N802 - inherited HTTP handler API
        super().do_HEAD()

    def do_GET(self) -> None:  # noqa: N802 - inherited HTTP handler API
        path = self.path.split("?", 1)[0]
        if path == "/test-fixture.cedar" and not self._consume_schema_fetch():
            return
        super().do_GET()
        if path == "/schema.json":
            # A completed response permits a readiness check. Treetop must also
            # finish loading this schema before strict policy validation starts.
            self.schema_permits.release()


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--bind", default="127.0.0.1")
    parser.add_argument("--port", required=True, type=int)
    parser.add_argument("--directory", required=True, type=pathlib.Path)
    parser.add_argument("--treetop-url", required=True)
    args = parser.parse_args()

    for fixture in ("schema.json", "test-fixture.cedar"):
        if not (args.directory / fixture).is_file():
            parser.error(f"missing fixture: {args.directory / fixture}")

    handler = functools.partial(
        FixtureHandler,
        directory=str(args.directory),
        schema_permits=threading.Semaphore(0),
        schema_ready=functools.partial(schema_is_loaded, args.treetop_url),
    )
    server = http.server.ThreadingHTTPServer((args.bind, args.port), handler)
    server.serve_forever()


if __name__ == "__main__":
    main()
