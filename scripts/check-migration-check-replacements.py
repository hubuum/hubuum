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
import subprocess
from pathlib import Path


IDENTIFIER = r"[a-z_][a-z_0-9]*"
TABLE = rf"(?:public\.)?{IDENTIFIER}"
ALTER = re.compile(rf"^\s*ALTER\s+TABLE\s+({TABLE})\s+([^;]+);", re.I | re.M)
ADD = re.compile(rf"ADD\s+CONSTRAINT\s+({IDENTIFIER})\s+(.+)", re.I | re.S)
CHECK = re.compile(
    rf"CHECK\s*\(\s*({IDENTIFIER})\s+IN\s*\(\s*"
    r"('[a-z_0-9-]+'(?:\s*,\s*'[a-z_0-9-]+')*)\s*\)\s*\)"
    r"(?:\s+NOT\s+VALID)?\s*",
    re.I,
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
        for table, operation in ALTER.findall(document):
            table = table.lower().removeprefix("public.")
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


def main():
    repository, baseline, candidate = sys.argv[1:]

    def git(*arguments):
        return subprocess.check_output(["git", "-C", repository, *arguments], text=True)

    paths = git("ls-tree", "-r", "--name-only", baseline, "--", "migrations",
                "crates/hubuum-storage-postgres/migrations").splitlines()
    paths = sorted((path for path in paths if path.endswith("/up.sql")),
                   key=lambda path: (Path(path).parent.name, path))
    documents = [git("show", f"{baseline}:{path}") for path in paths]
    for name in sorted(approved_replacements(documents, Path(candidate).read_text())):
        print(name)


if __name__ == "__main__":
    main()
