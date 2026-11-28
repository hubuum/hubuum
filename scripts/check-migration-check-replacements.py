#!/usr/bin/env python3
"""Prove explicitly marked literal enum CHECK replacements preserve old values."""

import sys

if sys.version_info < (3, 11):
    sys.exit(
        "Hubuum tooling requires Python 3.11 or newer; found "
        + sys.version.split()[0]
        + ". Install Python 3.11+ and ensure python3 on PATH selects it."
    )

import re
import hashlib
import json
import subprocess
from pathlib import Path


IDENTIFIER = r"[a-z_][a-z_0-9]*"
TABLE = rf"(?:public\.)?{IDENTIFIER}"
OFFLINE_TABLE = rf"(?:(?:public|hubuum_control)\.)?{IDENTIFIER}"
ALTER = re.compile(rf"^\s*ALTER\s+TABLE\s+({TABLE})\s+([^;]+);", re.I | re.M)
CREATE = re.compile(rf"^\s*CREATE\s+TABLE\s+({TABLE})\s*\(([^;]+)\)\s*;", re.I | re.M)
DROP = re.compile(rf"^\s*DROP\s+TABLE\s+(?:IF\s+EXISTS\s+)?({TABLE})(?:\s+(?:CASCADE|RESTRICT))?\s*;", re.I | re.M)
ADD = re.compile(rf"ADD\s+CONSTRAINT\s+({IDENTIFIER})\s+(.+)", re.I | re.S)
LITERAL_CHECK = (
    rf"CHECK\s*\(\s*({IDENTIFIER})\s+IN\s*\(\s*"
    r"('[a-z_0-9-]+'(?:\s*,\s*'[a-z_0-9-]+')*)\s*\)\s*\)"
)
CHECK = re.compile(LITERAL_CHECK + r"(?:\s+NOT\s+VALID)?\s*", re.I)
INLINE_CHECK = re.compile(
    rf"(?:^|,)\s*({IDENTIFIER})\s+(?:TEXT|VARCHAR)(?:\s+NOT\s+NULL)?\s+"
    rf"({LITERAL_CHECK})(?=\s*(?:,|$))", re.I,
)
REPLACEMENT = re.compile(
    rf"^\s*ALTER\s+TABLE\s+({TABLE})\s+DROP\s+CONSTRAINT\s+({IDENTIFIER});"
    r"[ \t]*-- hubuum-compat: widen-enum-check[ \t]*\n"
    r"\s*ALTER\s+TABLE\s+\1\s+ADD\s+CONSTRAINT\s+\2\s+([^;]+)\s+NOT\s+VALID;"
    r"\s*ALTER\s+TABLE\s+\1\s+VALIDATE\s+CONSTRAINT\s+\2\s*;",
    re.I | re.M,
)


def enum_check(expression):
    match = CHECK.fullmatch(expression)
    if match is None:
        return None
    return match[1].lower(), frozenset(re.findall(r"'([^']+)'", match[2]))


def approved_replacements(baseline_documents, candidate):
    """Fail closed for unknown expressions, columns, constraints, and SQL shapes."""
    constraints = {}
    for document in baseline_documents:
        document = re.sub(r"--[^\n]*|/\*.*?\*/", "", document, flags=re.S)
        operations = sorted(
            (match.start(), kind, match)
            for kind, pattern in (("create", CREATE), ("alter", ALTER), ("drop", DROP))
            for match in pattern.finditer(document)
        )
        for _, kind, match in operations:
            table = match[1].lower().removeprefix("public.")
            if kind != "alter":
                constraints = {key: value for key, value in constraints.items() if key[0] != table}
                if kind == "create":
                    for column in INLINE_CHECK.finditer(match[2]):
                        name = f"{table}_{column[1].lower()}_check"
                        check = enum_check(column[2])
                        # PostgreSQL may truncate or suffix generated names. Only
                        # accept an unambiguous, short default column CHECK name.
                        if (len(name) <= 63 and check[0] == column[1].lower()
                                and not re.search(rf"\bCONSTRAINT\s+{name}\b", match[2], re.I)):
                            constraints[(table, name)] = check
                continue
            operation = match[2]
            for name in re.findall(
                rf"\b(?:ADD|DROP|RENAME|ALTER)\s+CONSTRAINT\s+({IDENTIFIER})", operation, re.I
            ):
                constraints.pop((table, name.lower()), None)
            added = ADD.fullmatch(operation.strip())
            if added:
                constraints[(table, added[1].lower())] = enum_check(added[2])
            elif re.search(r"\b(?:RENAME\s+(?:COLUMN|TO)|DROP\s+COLUMN)\b", operation, re.I):
                # Structural renaming invalidates the original table/column identity.
                constraints = {key: value for key, value in constraints.items() if key[0] != table}
    approved = set()
    # Block comments must not supply executable replacement evidence. Keep the
    # required end-of-line review marker for the strict replacement grammar.
    candidate = re.sub(r"/\*.*?\*/", "", candidate, flags=re.S)
    for table, name, expression in REPLACEMENT.findall(candidate):
        key = (table.lower().removeprefix("public."), name.lower())
        old = constraints.get(key)
        new = enum_check(expression.strip())
        if old and new and old[0] == new[0] and old[1] < new[1]:
            approved.add(":".join(key).upper())
    return approved


def approved_offline_drops(reviews, candidate_path, candidate):
    """Permit declared constraint drops and function renames in an offline upgrade.

    These are explicitly NOT rolling-compatible migrations. The hash pins the
    review to its SQL; any later edit needs a fresh review in the same PR.
    """
    if reviews.get("schema_version") != 1:
        raise ValueError("Unsupported offline migration review schema")
    approved = set()
    for review in reviews.get("reviews", []):
        if review.get("migration") != candidate_path:
            continue
        if not review.get("reason") or not review.get("upgrade_action"):
            raise ValueError("Offline migration review requires rationale and operator action")
        if review.get("sha256") != hashlib.sha256(candidate.encode()).hexdigest():
            raise ValueError("Offline migration SQL changed; review its new content and hash")
        for table, names in review["drop_constraints"].items():
            for name in names:
                if not re.fullmatch(OFFLINE_TABLE, table) or not re.fullmatch(IDENTIFIER, name):
                    raise ValueError("Offline review contains an invalid constraint identifier")
                table_identity = table.removeprefix("public.")
                approved.add(f"OFFLINE:{table_identity}:{name}".upper())
        for source, destination in review.get("rename_functions", {}).items():
            if not re.fullmatch(IDENTIFIER, source) or not re.fullmatch(IDENTIFIER, destination):
                raise ValueError("Offline review contains an invalid function identifier")
            approved.add(f"OFFLINE_FUNCTION_RENAME:{source}:{destination}".upper())
    return approved


def main():
    repository, baseline, candidate = sys.argv[1:]

    def git(*arguments):
        return subprocess.check_output(["git", "-C", repository, *arguments], text=True)

    paths = git("ls-tree", "-r", "--name-only", baseline, "--", "migrations",
                "crates/hubuum-storage-postgres/migrations").splitlines()
    paths = sorted((path for path in paths if path.endswith("/up.sql")),
                   key=lambda path: (Path(path).parent.name, path))
    documents = [git("show", f"{baseline}:{path}") for path in paths]
    content = Path(candidate).read_text()
    approved = approved_replacements(documents, content)
    review_path = Path(repository) / ".github/migration-offline-reviews.json"
    if review_path.exists():
        approved |= approved_offline_drops(json.loads(review_path.read_text()), Path(candidate).relative_to(repository).as_posix(), content)
    for name in sorted(approved):
        print(name)


if __name__ == "__main__":
    main()
