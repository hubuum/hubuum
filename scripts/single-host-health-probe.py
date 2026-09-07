#!/usr/bin/env python3
"""Probe a read-only health endpoint across a bounded proxy-reload disconnect."""

import argparse
import math
import subprocess
import sys
import time
from typing import NamedTuple
from urllib.parse import urlsplit


class ProbeResult(NamedTuple):
    status: str
    attempts: int
    error: str = ""


def probe(url: str, timeout: float) -> ProbeResult:
    parsed = urlsplit(url)
    if (
        parsed.scheme not in {"http", "https"}
        or not parsed.hostname
        or parsed.username is not None
        or parsed.path not in {"/healthz", "/readyz"}
        or parsed.query
        or parsed.fragment
    ):
        raise ValueError("only read-only /healthz and /readyz URLs are supported")
    if not math.isfinite(timeout) or timeout <= 0:
        raise ValueError("timeout must be a positive finite number")

    deadline = time.monotonic() + timeout
    attempts = 0
    for _ in range(2):
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            return ProbeResult("000", attempts, "health probe deadline expired")
        attempts += 1
        try:
            result = subprocess.run(
                [
                    "curl",
                    "--disable",
                    "--silent",
                    "--show-error",
                    "--request",
                    "GET",
                    "--output",
                    "/dev/null",
                    "--write-out",
                    "%{http_code}",
                    "--connect-timeout",
                    "1",
                    "--max-time",
                    str(remaining),
                    url,
                ],
                capture_output=True,
                text=True,
                timeout=remaining,
                check=False,
            )
        except subprocess.TimeoutExpired:
            return ProbeResult("000", attempts, "health probe deadline expired")
        status = result.stdout.strip()
        if result.returncode == 0:
            return ProbeResult(
                status,
                attempts,
                "" if status == "200" else f"unexpected HTTP status {status}",
            )
        # Caddy reloads shut down the old Go HTTP server. A newly accepted
        # connection can close before its first response with healthy upstreams.
        # Replay this read-only GET once, never HTTP responses or other errors.
        if attempts == 1 and result.returncode in {52, 56} and status == "000":
            continue
        return ProbeResult(
            status, attempts, result.stderr.strip() or f"curl exit {result.returncode}"
        )
    raise AssertionError("both probe attempts must return a result")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("url")
    parser.add_argument("--timeout", type=float, default=7)
    args = parser.parse_args()
    try:
        result = probe(args.url, args.timeout)
    except ValueError as error:
        parser.error(str(error))
    print(result.status)
    if result.attempts == 2:
        print(
            "Retried one pre-response health connection disconnect within the original deadline",
            file=sys.stderr,
        )
    if result.error:
        print(result.error, file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
