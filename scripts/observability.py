#!/usr/bin/env python3
"""Generate, validate, or operate the shared monitoring package."""
import sys

if sys.version_info < (3, 11):
    sys.exit(
        "Hubuum tooling requires Python 3.11 or newer; found "
        + sys.version.split()[0]
        + ". Install Python 3.11+ and ensure python3 on PATH selects it."
    )

import argparse
from pathlib import Path

# Also support isolated Python (-I -S), without installing a package.
sys.path.insert(0, str(Path(__file__).resolve().parent))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("command", choices=("generate", "check", "record-job"))
    args = parser.parse_args(sys.argv[1:2])
    from monitoring import generate, validate, jobs
    {"generate": generate, "check": validate, "record-job": jobs}[args.command].main(sys.argv[2:])


if __name__ == "__main__":
    main()
