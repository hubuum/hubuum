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
import tempfile

ROOT = Path(__file__).resolve().parents[1]
PROMETHEUS = "docker.io/prom/prometheus:v3.13.1@sha256:3c42b892cf723fa54d2f262c37a0e1f80aa8c8ddb1da7b9b0df9455a35a7f893"


def query_expression(expression):
    if expression.startswith("label_values("):
        return expression[len("label_values("):].rsplit(",", 1)[0]
    return expression


def metric_names(expression):
    """Find vector names, ignoring strings, functions, durations and label lists.

    This is a reference check, not a PromQL parser. Promtool validates syntax.
    """
    tokens = re.findall(r'"(?:\\.|[^"\\])*"|[a-zA-Z_:][a-zA-Z0-9_:]*|[0-9]+(?:\.[0-9]+)?|[{}()\[\],]', expression)
    context = []
    label_lists = {"by", "without", "on", "ignoring", "group_left", "group_right"}
    reserved = label_lists | {"and", "or", "unless", "bool", "offset", "Inf", "NaN"}
    for index, token in enumerate(tokens):
        previous = tokens[index - 1] if index else ""
        following = tokens[index + 1] if index + 1 < len(tokens) else ""
        if token in ("{", "["):
            context.append("ignore")
        elif token == "(":
            context.append("ignore" if previous in label_lists else "expression")
        elif token in ("}", "]", ")"):
            if context:
                context.pop()
        elif "ignore" not in context and re.fullmatch(r"[a-zA-Z_:][a-zA-Z0-9_:]*", token):
            if token not in reserved and following != "(" and following not in ("by", "without"):
                yield token


def validate(root):
    metrics = {m["name"]: m for m in json.loads((root / "docs/operational-contract.json").read_text())["metrics"]}
    assets = root / "observability"
    alert_groups = json.loads((assets / "prometheus/alerts.json").read_text())["groups"]
    rules = [r for group in alert_groups for r in group["rules"]]
    recordings = json.loads((assets / "prometheus/recording-rules.json").read_text())["groups"]
    recorded = {r["record"] for group in recordings for r in group["rules"]}
    external = json.loads((assets / "external-metrics.json").read_text())
    metrics.update(external)
    expressions = [r["expr"] for r in rules]
    expressions += [r["expr"] for group in recordings for r in group["rules"]]
    uids = set()
    for path in sorted((assets / "dashboards").glob("*.json")):
        dashboard = json.loads(path.read_text())
        if dashboard["uid"] in uids or not dashboard["panels"]:
            raise ValueError(f"Duplicate UID or empty dashboard: {path}")
        uids.add(dashboard["uid"])
        expressions += [t["expr"] for p in dashboard["panels"] for t in p.get("targets", [])]
        expressions += [v["query"] for v in dashboard["templating"]["list"] if v["type"] == "query"]
    for expression in expressions:
        expression = query_expression(expression)
        selectors_by_name = {}
        for name, selectors in re.findall(r'([a-zA-Z_:][a-zA-Z0-9_:]*)\{([^}]*)\}', expression):
            selectors_by_name.setdefault(name, []).append(selectors)
        for name in metric_names(expression):
            if name in recorded:
                continue
            base = re.sub(r'_(bucket|sum|count)$', '', name)
            definition = metrics.get(name)
            if definition is None and metrics.get(base, {}).get("kind") == "histogram":
                definition = metrics[base]
            if definition is None:
                raise ValueError(f"Unknown metric: {name}")
            labels = {label["name"]: label["values"] for label in definition["labels"]}
            if name.endswith("_bucket") and definition.get("kind") == "histogram":
                labels["le"] = []
            for selectors in selectors_by_name.get(name, []):
                for label, operator, value in re.findall(r'(\w+)\s*(=~|!~|!=|=)\s*"([^"]*)"', selectors):
                    if label in ("deployment", "instance", "job"):
                        continue
                    if label not in labels:
                        raise ValueError(f"Unknown label: {name}.{label}")
                    if operator == "=" and labels[label] and value not in labels[label]:
                        raise ValueError(f"Unknown value: {name}.{label}={value}")
        # These snapshot gauges describe one database, not one replica.
        if re.search(r'sum[^()]*\(\s*(hubuum_tasks|hubuum_inventory_entities|hubuum_event_queue_items|hubuum_task_oldest_age_seconds|hubuum_event_oldest_age_seconds)\b', expression):
            raise ValueError("Database-wide gauges must be deduplicated before summing")
        if 'hubuum_http_' in expression and 'hubuum:api_' in next((r["record"] for g in recordings for r in g["rules"] if r["expr"] == expression), '') and 'route=~"/api/.*"' not in expression:
            raise ValueError("Application SLI must exclude probes and scrape traffic")
    tests = json.loads((assets / "prometheus/tests.json").read_text())["tests"]
    tests += json.loads((assets / "prometheus/extended-tests.json").read_text())["tests"]
    checks = [c for test in tests for c in test.get("alert_rule_test", [])]
    names = {r["alert"] for r in rules}
    for rule in rules:
        path = assets / "runbooks" / rule["annotations"]["runbook_url"].rsplit("/", 1)[1]
        outcomes = {bool(c["exp_alerts"]) for c in checks if c["alertname"] == rule["alert"]}
        if not path.is_file() or outcomes != {True, False}:
            raise ValueError(f"Missing runbook or firing/recovery fixture: {rule['alert']}")
    for path in (assets / "runbooks").glob("*.md"):
        for name in re.findall(r'`(Hubuum[A-Za-z]+)`', path.read_text()):
            if name not in names:
                raise ValueError(f"Unknown runbook alert: {name}")
    operator = json.loads((assets / "prometheus/operator-rule.json").read_text())
    if operator["spec"]["groups"] != recordings + alert_groups:
        raise ValueError("Prometheus Operator resource differs from direct-consumption rules")
    return expressions


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--promtool", action="store_true", help="also evaluate rule fixtures using pinned Prometheus in Docker")
    args = parser.parse_args()
    expressions = validate(ROOT)
    if args.promtool:
        subprocess.run([sys.executable, str(ROOT / "scripts/generate-observability.py"), "--check"], check=True)
        with tempfile.TemporaryDirectory() as directory:
            queries = []
            for index, expression in enumerate(expressions):
                expression = expression.replace("$deployment", ".*")
                expression = query_expression(expression)
                queries.append({"record": f"validation:query_{index}", "expr": expression})
            Path(directory, "queries.json").write_text(json.dumps({"groups": [{"name": "validation", "rules": queries}]}))
            for arguments in (("check", "rules", "alerts.json", "recording-rules.json", "/queries/queries.json"),
                              ("test", "rules", "tests.json", "extended-tests.json")):
                subprocess.run(
                    ["docker", "run", "--rm", "--network", "none", "--user", "0:0",
                     "--read-only", "--tmpfs", "/tmp:rw,nosuid,nodev,size=256m",
                     "--cap-drop", "ALL", "--entrypoint", "/bin/promtool",
                     "--volume", f"{ROOT / 'observability'}:/work:ro,z",
                     "--volume", f"{directory}:/queries:ro,z", "--workdir", "/work/prometheus",
                     PROMETHEUS, *arguments], check=True, timeout=120,
                )
    print("Operator assets match the metric contract")


if __name__ == "__main__":
    main()
