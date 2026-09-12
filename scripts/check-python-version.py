#!/usr/bin/env python3
"""Check the interpreter before starting repository tooling."""

import sys

if sys.version_info < (3, 11):
    sys.exit(
        "Hubuum tooling requires Python 3.11 or newer; found "
        + sys.version.split()[0]
        + ". Install Python 3.11+ and ensure python3 on PATH selects it."
    )


if __name__ == "__main__":
    print("Python " + sys.version.split()[0] + " meets the 3.11+ tooling requirement.")
