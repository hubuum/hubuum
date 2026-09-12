#!/usr/bin/env python3
"""Install the pinned EDB server and development files without its Windows installer."""

from __future__ import annotations

import sys

if sys.version_info < (3, 11):
    sys.exit(
        "Hubuum tooling requires Python 3.11 or newer; found "
        + sys.version.split()[0]
        + ". Install Python 3.11+ and ensure python3 on PATH selects it."
    )

import hashlib
import os
from pathlib import Path, PurePosixPath
import shutil
import subprocess
import tempfile
import time
from urllib.request import urlopen
from zipfile import ZipFile


POLICY = Path(__file__).with_name("check-supply-chain-policy.py")
COMPONENTS = {"bin", "lib", "share", "include"}
REQUIRED_FILES = {
    "pgsql/bin/postgres.exe",
    "pgsql/bin/initdb.exe",
    "pgsql/bin/pg_ctl.exe",
    "pgsql/bin/pg_isready.exe",
    "pgsql/bin/createdb.exe",
    "pgsql/bin/dropdb.exe",
    "pgsql/bin/psql.exe",
    "pgsql/bin/libpq.dll",
    "pgsql/lib/libpq.lib",
    "pgsql/share/postgresql.conf.sample",
}


def tool_value(key: str) -> str:
    return subprocess.check_output(
        [sys.executable, str(POLICY), "--tool-value", key], text=True
    ).strip()


def report(message: str) -> None:
    print(message, flush=True)
    if summary := os.environ.get("GITHUB_STEP_SUMMARY"):
        with open(summary, "a", encoding="utf-8") as output:
            output.write(f"- {message}\n")


def extract_archive(archive: Path, destination: Path, expected_digest: str) -> Path:
    with archive.open("rb") as source:
        digest = hashlib.file_digest(source, "sha256").hexdigest()
    if digest != expected_digest:
        raise ValueError(f"PostgreSQL archive checksum mismatch: {digest}")

    with ZipFile(archive) as source:
        missing = REQUIRED_FILES - set(source.namelist())
        if missing:
            raise ValueError(
                f"PostgreSQL archive is missing required files: {sorted(missing)}"
            )
        members = []
        for entry in source.infolist():
            path = PurePosixPath(entry.filename)
            if (
                path.is_absolute()
                or ".." in path.parts
                or "\\" in entry.filename
                or ":" in entry.filename
            ):
                raise ValueError(f"Unsafe PostgreSQL archive path: {entry.filename}")
            if len(path.parts) >= 2 and path.parts[0] == "pgsql":
                # Keep server/tools, headers, libraries, and their licenses. The
                # pgAdmin application makes up most of the expanded archive.
                if path.parts[1] in COMPONENTS or (
                    len(path.parts) == 2 and not entry.is_dir()
                ):
                    members.append(entry)
        destination.mkdir(parents=True, exist_ok=False)
        source.extractall(destination, members=members)

    for name in REQUIRED_FILES:
        if not (destination / name).is_file():
            raise ValueError(f"PostgreSQL archive did not provide file {name}")
    return destination / "pgsql"


def main() -> None:
    version = tool_value("POSTGRES_WINDOWS_VERSION")
    digest = tool_value("POSTGRES_WINDOWS_SHA256")
    filename = f"postgresql-{version}-windows-x64-binaries.zip"
    url = f"https://get.enterprisedb.com/postgresql/{filename}"
    runner_temp = Path(os.environ["RUNNER_TEMP"])
    destination = runner_temp / f"postgresql-{version}"

    with tempfile.TemporaryDirectory(
        prefix="postgres-download-", dir=runner_temp
    ) as temporary:
        archive = Path(temporary) / filename
        start = time.perf_counter()
        with urlopen(url, timeout=60) as response, archive.open("wb") as output:
            shutil.copyfileobj(response, output)
        report(
            f"PostgreSQL {version} download: {time.perf_counter() - start:.2f}s "
            f"({archive.stat().st_size} bytes)"
        )
        start = time.perf_counter()
        root = extract_archive(archive, destination, digest)
        report(
            "PostgreSQL checksum verification and extraction: "
            f"{time.perf_counter() - start:.2f}s"
        )

    report(f"PostgreSQL archive: `{filename}`, `sha256:{digest}`")
    with open(os.environ["GITHUB_PATH"], "a", encoding="utf-8") as output:
        output.write(f"{root / 'bin'}\n")
    with open(os.environ["GITHUB_ENV"], "a", encoding="utf-8") as output:
        output.write(f"PG_BIN={root / 'bin'}\n")
        output.write(f"LIB={root / 'lib'};{os.environ.get('LIB', '')}\n")
        output.write(f"PG_EXPECTED_VERSION={version.split('-')[0]}\n")


if __name__ == "__main__":
    main()
