#!/usr/bin/env python3
"""Run discovered Python regressions or an explicit integration harness."""
import sys

if sys.version_info < (3, 11):
    sys.exit(
        "Hubuum tooling requires Python 3.11 or newer; found "
        + sys.version.split()[0]
        + ". Install Python 3.11+ and ensure python3 on PATH selects it."
    )

import argparse
import importlib
from pathlib import Path
import re
import subprocess
import unittest

# Keep -I -S support without installing a package or relying on the working directory.
sys.path.insert(0, str(Path(__file__).resolve().parent))
from support import SCRIPTS, SUITE
sys.path.insert(1, str(SCRIPTS))

INTEGRATION = {
    "monitoring": "integration.monitoring",
    "monitoring-fixture": "integration.monitoring_fixture",
    "event-transports": "integration.event_transports",
    "corpus": "integration.corpus",
    "atlas": "integration.atlas",
    "schema-budget": "integration.schema_budget",
    "treetop-server": "support.treetop_server",
}


def discover(selectors):
    loader = unittest.TestLoader()
    suite = unittest.TestSuite()
    for selector in selectors or [""]:
        if selector and not re.fullmatch(r"[a-zA-Z_]\w*(?:\.[a-zA-Z_]\w*)*", selector):
            raise ValueError(f"Invalid test selector: {selector}")
        directory = SUITE / "unit"
        if selector:
            directory = directory.joinpath(*selector.split("."))
        if directory.is_dir():
            suite.addTests(loader.discover(str(directory), top_level_dir=str(SUITE)))
        else:
            suite.addTests(loader.loadTestsFromName("unit." + selector))
    if loader.errors:
        raise ValueError("\n".join(loader.errors))
    if suite.countTestCases() == 0:
        raise ValueError("No Python tests discovered")
    return suite


def test_ids(suite):
    for test in suite:
        if isinstance(test, unittest.TestSuite):
            yield from test_ids(test)
        else:
            yield test.id()


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command")
    unit = commands.add_parser("unit", help="discover local tooling tests; no containers or live services")
    unit.add_argument("selectors", nargs="*", help="optional category, module, class or method (e.g. deployment.test_image_tags)")
    unit.add_argument("--list", action="store_true", help="list discovered test IDs without running them")
    integration = commands.add_parser("integration", help="explicit live-system tests and Rust fixture drivers")
    integration.add_argument("name", choices=INTEGRATION)
    integration.add_argument("arguments", nargs=argparse.REMAINDER, help="arguments forwarded to the selected harness")
    args = parser.parse_args(argv)
    if args.command == "integration":
        module = importlib.import_module(INTEGRATION[args.name])
        return module.main(args.arguments)
    suite = discover(getattr(args, "selectors", []))
    if getattr(args, "list", False):
        print("\n".join(test_ids(suite)))
        return 0
    return 0 if unittest.TextTestRunner(verbosity=2).run(suite).wasSuccessful() else 1


if __name__ == "__main__":
    try:
        sys.exit(main())
    except subprocess.TimeoutExpired:
        sys.exit("Integration command exceeded its deadline")
    except (ValueError, OSError, RuntimeError, KeyError) as error:
        sys.exit(str(error))
