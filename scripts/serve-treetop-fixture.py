#!/usr/bin/env python3
"""Serve the Treetop schema before making its policy fixture available."""

from __future__ import annotations

import argparse
import functools
import http.server
import pathlib
import threading


class FixtureHandler(http.server.SimpleHTTPRequestHandler):
    schema_served = threading.Event()

    def _wait_for_schema(self) -> bool:
        if self.path.split("?", 1)[0] != "/test-fixture.cedar":
            return True
        if self.schema_served.wait(timeout=30):
            return True
        self.send_error(503, "schema fixture was not fetched first")
        return False

    def do_HEAD(self) -> None:  # noqa: N802 - inherited HTTP handler API
        if self._wait_for_schema():
            super().do_HEAD()

    def do_GET(self) -> None:  # noqa: N802 - inherited HTTP handler API
        path = self.path.split("?", 1)[0]
        if not self._wait_for_schema():
            return
        super().do_GET()
        if path == "/schema.json":
            self.schema_served.set()


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--bind", default="127.0.0.1")
    parser.add_argument("--port", required=True, type=int)
    parser.add_argument("--directory", required=True, type=pathlib.Path)
    args = parser.parse_args()

    for fixture in ("schema.json", "test-fixture.cedar"):
        if not (args.directory / fixture).is_file():
            parser.error(f"missing fixture: {args.directory / fixture}")

    handler = functools.partial(FixtureHandler, directory=str(args.directory))
    server = http.server.ThreadingHTTPServer((args.bind, args.port), handler)
    server.serve_forever()


if __name__ == "__main__":
    main()
