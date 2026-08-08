#!/usr/bin/env python3
"""Check whether a package has a crates.io release baseline."""

from __future__ import annotations

import argparse
import sys
from collections.abc import Callable
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.parse import quote
from urllib.request import Request, urlopen


CRATES_IO_API = "https://crates.io/api/v1/crates"
USER_AGENT = "hubuum-rust-api-policy (https://github.com/hubuum/hubuum)"


class BaselineLookupError(RuntimeError):
    """Raised when registry availability cannot be determined safely."""


def registry_baseline_exists(
    package: str,
    *,
    opener: Callable[..., Any] = urlopen,
    api_root: str = CRATES_IO_API,
) -> bool:
    url = f"{api_root.rstrip('/')}/{quote(package, safe='')}"
    request = Request(
        url,
        headers={"Accept": "application/json", "User-Agent": USER_AGENT},
    )
    try:
        response = opener(request, timeout=30)
    except HTTPError as error:
        status = error.code
        error.close()
        if status == 404:
            return False
        raise BaselineLookupError(
            f"crates.io returned HTTP {status} for {package}"
        ) from error
    except URLError as error:
        raise BaselineLookupError(
            f"could not query crates.io for {package}: {error.reason}"
        ) from error

    with response:
        status = response.status
    if status == 200:
        return True
    raise BaselineLookupError(
        f"crates.io returned unexpected HTTP {status} for {package}"
    )


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("package")
    arguments = parser.parse_args()
    try:
        exists = registry_baseline_exists(arguments.package)
    except BaselineLookupError as error:
        print(f"Rust API baseline lookup error: {error}", file=sys.stderr)
        return 1
    print("present" if exists else "missing")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
