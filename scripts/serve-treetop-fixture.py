#!/usr/bin/env python3
"""Serve the Treetop schema before making its policy fixture available."""

from __future__ import annotations

import argparse
import functools
import http.server
import pathlib
import threading


class FixtureHandler(http.server.SimpleHTTPRequestHandler):
    def __init__(
        self,
        *args: object,
        schema_permits: threading.Semaphore,
        schema_wait_timeout: float = 30,
        **kwargs: object,
    ) -> None:
        self.schema_permits = schema_permits
        self.schema_wait_timeout = schema_wait_timeout
        super().__init__(*args, **kwargs)

    def _consume_schema_fetch(self) -> bool:
        if self.schema_permits.acquire(timeout=self.schema_wait_timeout):
            return True
        self.send_error(503, "schema fixture was not fetched first")
        return False

    def do_HEAD(self) -> None:  # noqa: N802 - inherited HTTP handler API
        super().do_HEAD()

    def do_GET(self) -> None:  # noqa: N802 - inherited HTTP handler API
        path = self.path.split("?", 1)[0]
        if path == "/test-fixture.cedar" and not self._consume_schema_fetch():
            return
        super().do_GET()
        if path == "/schema.json":
            # Release the policy fetch only after the complete schema response
            # has been written, so strict validation cannot observe it early.
            self.schema_permits.release()


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--bind", default="127.0.0.1")
    parser.add_argument("--port", required=True, type=int)
    parser.add_argument("--directory", required=True, type=pathlib.Path)
    args = parser.parse_args()

    for fixture in ("schema.json", "test-fixture.cedar"):
        if not (args.directory / fixture).is_file():
            parser.error(f"missing fixture: {args.directory / fixture}")

    handler = functools.partial(
        FixtureHandler,
        directory=str(args.directory),
        schema_permits=threading.Semaphore(0),
    )
    server = http.server.ThreadingHTTPServer((args.bind, args.port), handler)
    server.serve_forever()


if __name__ == "__main__":
    main()
