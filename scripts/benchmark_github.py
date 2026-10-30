"""Small GitHub CLI transport shared by benchmark reporting and maintenance."""

import sys

if sys.version_info < (3, 11):
    sys.exit(
        "Hubuum tooling requires Python 3.11 or newer; found "
        + sys.version.split()[0]
        + ". Install Python 3.11+ and ensure python3 on PATH selects it."
    )

import json
import subprocess


def api(path, *, method="GET", body=None, raw=False):
    command = ["gh", "api", "--method", method, path]
    data = None
    if body is not None:
        command += ["--input", "-"]
        data = json.dumps(body).encode()
    result = subprocess.run(command, input=data, capture_output=True, check=True, timeout=120)
    return result.stdout if raw else json.loads(result.stdout or b"null")


def pages(path, field=None):
    result = []
    page = 1
    while True:
        separator = "&" if "?" in path else "?"
        payload = api(f"{path}{separator}per_page=100&page={page}")
        items = payload[field] if field else payload
        result.extend(items)
        if len(items) < 100:
            return result
        page += 1
