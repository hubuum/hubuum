"""Run Rust tests with an isolated, split-role destructive-restore database."""

import argparse
import os
from pathlib import Path
import secrets
import subprocess
from urllib.parse import urlsplit, urlunsplit
import uuid

from support import ROOT


def run_tests(command, admin, database_url):
    suffix = uuid.uuid4().hex[:16]
    database = f"hubuum_restore_{suffix}"
    owner, migrator, runtime = (f"hubuum_{role}_{suffix}" for role in ("owner", "migrator", "runtime"))
    password = secrets.token_hex(24)
    source = urlsplit(database_url)
    if source.scheme not in ("postgres", "postgresql") or not source.hostname:
        raise ValueError("Rust test provisioning requires a PostgreSQL test database URL")
    host = f"[{source.hostname}]" if ":" in source.hostname else source.hostname
    authority = host + (f":{source.port}" if source.port else "")

    def url(role):
        return urlunsplit((source.scheme, f"{role}:{password}@{authority}", f"/{database}", source.query, ""))

    psql = str(Path(os.environ["PG_BIN"]) / "psql.exe") if os.name == "nt" else "psql"

    def sql(statement):
        return subprocess.run([psql, database_url, "-X", "-v", "ON_ERROR_STOP=1"],
                              input=statement, text=True, check=True, stdout=subprocess.DEVNULL)

    try:
        # All interpolated identifiers and the password are generated hex/ASCII;
        # caller-supplied URL components never enter SQL text.
        sql(f"CREATE ROLE {owner} NOLOGIN;\n"
            f"CREATE ROLE {migrator} LOGIN PASSWORD '{password}';\n"
            f"CREATE ROLE {runtime} LOGIN PASSWORD '{password}';\n"
            f"GRANT {owner} TO {migrator};\n"
            f"CREATE DATABASE {database} OWNER {owner};\n")
        migration_env = {**os.environ,
                         "HUBUUM_DATABASE_URL": url(runtime),
                         "HUBUUM_MIGRATION_DATABASE_URL": url(migrator),
                         "HUBUUM_DATABASE_ROLE_MODE": "split",
                         "HUBUUM_DATABASE_OWNER_ROLE": owner,
                         "HUBUUM_DATABASE_MIGRATOR_ROLE": migrator,
                         "HUBUUM_DATABASE_RUNTIME_ROLE": runtime}
        subprocess.run([str(admin), "--migrate"], env=migration_env, cwd=ROOT, check=True)
        test_env = {**os.environ,
                    "HUBUUM_RESTORE_TEST_DATABASE_URL": url(runtime),
                    "HUBUUM_RESTORE_TEST_MIGRATION_DATABASE_URL": url(migrator)}
        return subprocess.run(command, env=test_env, cwd=ROOT, check=False).returncode
    finally:
        sql(f"DROP DATABASE IF EXISTS {database} WITH (FORCE);\n"
            f"DROP ROLE IF EXISTS {runtime};\n"
            f"DROP ROLE IF EXISTS {migrator};\n"
            f"DROP ROLE IF EXISTS {owner};\n")


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--admin-binary", type=Path,
                        default=ROOT / "target" / "release" / ("hubuum-admin.exe" if os.name == "nt" else "hubuum-admin"))
    parser.add_argument("--database-url", default=os.environ.get("HUBUUM_DATABASE_URL"))
    parser.add_argument("command", nargs=argparse.REMAINDER)
    args = parser.parse_args(argv)
    command = args.command[1:] if args.command[:1] == ["--"] else args.command
    if not args.database_url or not command:
        parser.error("provide a test database URL and a command after --")
    return run_tests(command, args.admin_binary, args.database_url)
