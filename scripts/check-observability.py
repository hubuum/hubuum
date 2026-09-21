#!/usr/bin/env python3
"""Validate operator assets against the committed metric contract and Prometheus."""
import sys

if sys.version_info < (3, 11):
    sys.exit(
        "Hubuum tooling requires Python 3.11 or newer; found "
        + sys.version.split()[0]
        + ". Install Python 3.11+ and ensure python3 on PATH selects it."
    )

import argparse
import json
from pathlib import Path
import re
import subprocess

ROOT = Path(__file__).resolve().parents[1]
PROMETHEUS = "docker.io/prom/prometheus:v3.5.0@sha256:8672a850efe2f9874702406c8318704edb363587f8c2ca88586b4c8fdb5cea24"


def validate(root):
    metrics = {m["name"]: m for m in json.loads((root / "docs/operational-contract.json").read_text())["metrics"]}
    assets = root / "observability"
    rules = json.loads((assets / "prometheus/alerts.json").read_text())["groups"][0]["rules"]
    dashboard = json.loads((assets / "dashboards/overview.json").read_text())
    expressions = [r["expr"] for r in rules]
    expressions += [t["expr"] for p in dashboard["panels"] for t in p["targets"]]
    expressions += [v["query"] for v in dashboard["templating"]["list"] if v["type"] == "query"]
    for expression in expressions:
        for name, selectors in re.findall(r'(hubuum_[a-z_]+)(?:\{([^}]*)\})?', expression):
            if name not in metrics:
                raise ValueError(f"Unknown metric: {name}")
            labels = {label["name"]: label["values"] for label in metrics[name]["labels"]}
            for label, operator, value in re.findall(r'(\w+)\s*(=~|!~|!=|=)\s*"([^"]*)"', selectors):
                if label in ("deployment", "instance", "job"):
                    continue
                if label not in labels:
                    raise ValueError(f"Unknown label: {name}.{label}")
                if operator == "=" and labels[label] and value not in labels[label]:
                    raise ValueError(f"Unknown value: {name}.{label}={value}")
    tests = json.loads((assets / "prometheus/tests.json").read_text())["tests"]
    tested = {check["alertname"] for test in tests for check in test.get("alert_rule_test", [])}
    for rule in rules:
        path = assets / "runbooks" / rule["annotations"]["runbook_url"].rsplit("/", 1)[1]
        if not path.is_file() or rule["alert"] not in tested:
            raise ValueError(f"Missing runbook or fixture: {rule['alert']}")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--promtool", action="store_true", help="also evaluate rule fixtures using pinned Prometheus in Docker")
    args = parser.parse_args()
    validate(ROOT)
    if args.promtool:
        for arguments in (("check", "rules", "alerts.json"), ("test", "rules", "tests.json")):
            subprocess.run(
                ["docker", "run", "--rm", "--network", "none", "--user", "0:0",
                 "--read-only", "--tmpfs", "/tmp:rw,nosuid,nodev,size=256m",
                 "--cap-drop", "ALL", "--entrypoint", "/bin/promtool",
                 "--volume", f"{ROOT / 'observability'}:/work:ro,z", "--workdir", "/work/prometheus",
                 PROMETHEUS, *arguments], check=True, timeout=120,
            )
    print("Operator assets match the metric contract")


if __name__ == "__main__":
    main()
