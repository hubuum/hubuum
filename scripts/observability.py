#!/usr/bin/env python3
"""Generate, validate, test, or operate the shared monitoring package."""
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
    parser.add_argument("command", choices=("generate", "check", "test", "test-fixture", "test-live", "record-job"))
    args = parser.parse_args(sys.argv[1:2])
    remaining = sys.argv[2:]
    if args.command in ("generate", "check", "record-job"):
        from monitoring import generate, validate, jobs
        {"generate": generate, "check": validate, "record-job": jobs}[args.command].main(remaining)
    elif args.command == "test":
        import unittest
        from monitoring import test_unit, test_installer
        tests = argparse.ArgumentParser(description="Run monitoring unit and installer tests")
        tests.parse_args(remaining)
        suite = unittest.TestSuite(unittest.defaultTestLoader.loadTestsFromModule(module)
                                   for module in (test_unit, test_installer))
        if not unittest.TextTestRunner(verbosity=2).run(suite).wasSuccessful():
            raise SystemExit(1)
    elif args.command == "test-fixture":
        from monitoring.fixture import live
        tests = argparse.ArgumentParser(description="Test real monitoring services against metrics fixtures")
        tests.add_argument("--engine", choices=("docker", "podman"), default="docker")
        tests.add_argument("--without-resource-limits", action="store_true", help="local rootless testing only")
        options = tests.parse_args(remaining)
        live(options.engine, options.without_resource_limits)
    else:
        from monitoring.acceptance import main as acceptance
        acceptance(remaining)


if __name__ == "__main__":
    main()
