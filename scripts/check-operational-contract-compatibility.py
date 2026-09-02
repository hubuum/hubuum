#!/usr/bin/env python3
"""Classify compatibility changes between two Hubuum operational contracts."""

from __future__ import annotations

import argparse
import datetime as dt
import hashlib
import json
import re
import sys
from pathlib import Path
from typing import Any


SEMVER_CORE = r"(?:0|[1-9]\d*)\.(?:0|[1-9]\d*)\.(?:0|[1-9]\d*)"
SEMVER_IDENTIFIER = r"(?:0|[1-9]\d*|[0-9A-Za-z-]*[A-Za-z-][0-9A-Za-z-]*)"
SEMVER_RELEASE = re.compile(
    rf"^{SEMVER_CORE}(?:-{SEMVER_IDENTIFIER}(?:\.{SEMVER_IDENTIFIER})*)?"
    r"(?:\+[0-9A-Za-z-]+(?:\.[0-9A-Za-z-]+)*)?$"
)
STABLE_SEMVER_RELEASE = re.compile(rf"^{SEMVER_CORE}$")
SEMVER_TAG = re.compile(rf"^v{SEMVER_CORE}$")
EXCEPTION_ID = re.compile(r"^[a-z0-9]+(?:[.-][a-z0-9]+)*$")


def load_json(path: Path) -> Any:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as error:
        raise ValueError(f"Invalid JSON document {path}: {error}") from error


def digest(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def stable(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=True)


class Comparator:
    def __init__(self) -> None:
        self.changes: list[dict[str, Any]] = []

    def add(
        self,
        classification: str,
        kind: str,
        path: str,
        before: Any,
        after: Any,
        reason: str,
    ) -> None:
        identity = stable([kind, path, before, after])
        self.changes.append(
            {
                "classification": classification,
                "kind": kind,
                "path": path,
                "before": before,
                "after": after,
                "reason": reason,
                "fingerprint": hashlib.sha256(identity.encode()).hexdigest()[:12],
            }
        )

    def keyed_items(
        self,
        family: str,
        baseline: list[dict[str, Any]],
        candidate: list[dict[str, Any]],
        key: str,
        compare_item: Any,
        classify_added: Any = None,
    ) -> None:
        old = {item[key]: item for item in baseline}
        new = {item[key]: item for item in candidate}
        for name in sorted(new.keys() - old.keys()):
            classification = classify_added(new[name]) if classify_added else "additive"
            self.add(classification, f"{family}-added", f"{family}.{name}", None, new[name], f"added {family} {name}")
        for name in sorted(old.keys() - new.keys()):
            self.add("breaking", f"{family}-removed", f"{family}.{name}", old[name], None, f"removed {family} {name}")
        for name in sorted(old.keys() & new.keys()):
            compare_item(name, old[name], new[name])

    def compare_metrics(self, baseline: dict[str, Any], candidate: dict[str, Any]) -> None:
        def compare(name: str, old: dict[str, Any], new: dict[str, Any]) -> None:
            path = f"metrics.{name}"
            for field in ("kind", "unit", "histogram_buckets"):
                if old.get(field) != new.get(field):
                    self.add("breaking", f"metric-{field}-changed", f"{path}.{field}", old.get(field), new.get(field), f"metric {field} changed")
            old_labels = {item["name"]: item for item in old.get("labels", [])}
            new_labels = {item["name"]: item for item in new.get("labels", [])}
            if old_labels.keys() != new_labels.keys():
                self.add("breaking", "metric-labels-changed", f"{path}.labels", sorted(old_labels), sorted(new_labels), "metric label names changed")
            for label in sorted(old_labels.keys() & new_labels.keys()):
                old_values = set(old_labels[label].get("values", []))
                new_values = set(new_labels[label].get("values", []))
                for value in sorted(new_values - old_values):
                    self.add("behavioral", "metric-label-value-added", f"{path}.labels.{label}.values.{value}", None, value, "expanded a bounded metric label domain")
                for value in sorted(old_values - new_values):
                    self.add("breaking", "metric-label-value-removed", f"{path}.labels.{label}.values.{value}", value, None, "narrowed a bounded metric label domain")
                if old_labels[label].get("bounded_by") != new_labels[label].get("bounded_by"):
                    self.add("behavioral", "metric-label-bound-changed", f"{path}.labels.{label}.bounded_by", old_labels[label].get("bounded_by"), new_labels[label].get("bounded_by"), "metric label bounding policy changed")
            for field in ("scope", "description", "feature"):
                if old.get(field) != new.get(field):
                    self.add("behavioral", f"metric-{field}-changed", f"{path}.{field}", old.get(field), new.get(field), f"metric {field} changed")

        self.keyed_items("metric", baseline["metrics"], candidate["metrics"], "name", compare)

    def compare_configuration(self, baseline: dict[str, Any], candidate: dict[str, Any]) -> None:
        exposure_rank = {"public": 0, "sensitive_metadata": 1, "secret": 2}

        def compare(name: str, old: dict[str, Any], new: dict[str, Any]) -> None:
            path = f"configuration.{name}"
            if old.get("value_kind") != new.get("value_kind"):
                self.add("breaking", "configuration-type-changed", f"{path}.value_kind", old.get("value_kind"), new.get("value_kind"), "configuration value type changed")
            old_allowed = set(old.get("allowed_values", []))
            new_allowed = set(new.get("allowed_values", []))
            for value in sorted(new_allowed - old_allowed):
                self.add("additive", "configuration-value-added", f"{path}.allowed_values.{value}", None, value, "expanded a configuration enum")
            for value in sorted(old_allowed - new_allowed):
                self.add("breaking", "configuration-value-removed", f"{path}.allowed_values.{value}", value, None, "narrowed a configuration enum")
            if old.get("default_is_set") != new.get("default_is_set"):
                self.add("behavioral", "configuration-default-presence-changed", f"{path}.default_is_set", old.get("default_is_set"), new.get("default_is_set"), "configuration default presence changed")
            if old.get("default") != new.get("default"):
                self.add("behavioral", "configuration-default-changed", f"{path}.default", old.get("default"), new.get("default"), "configuration default changed")
            if old.get("dynamic_default") != new.get("dynamic_default"):
                self.add("behavioral", "configuration-dynamic-default-changed", f"{path}.dynamic_default", old.get("dynamic_default"), new.get("dynamic_default"), "configuration dynamic default changed")
            for field, direction in (("minimum", "lower"), ("maximum", "upper")):
                old_bound = old.get(field)
                new_bound = new.get(field)
                if old_bound == new_bound:
                    continue
                if old_bound is None:
                    classification = "breaking"
                elif new_bound is None:
                    classification = "additive"
                elif direction == "lower":
                    classification = "breaking" if new_bound > old_bound else "additive"
                else:
                    classification = "breaking" if new_bound < old_bound else "additive"
                self.add(classification, f"configuration-{field}-changed", f"{path}.{field}", old_bound, new_bound, f"configuration {field} changed")
            if old.get("exposure") != new.get("exposure"):
                classification = "breaking" if exposure_rank.get(new.get("exposure"), -1) < exposure_rank.get(old.get("exposure"), -1) else "behavioral"
                self.add(classification, "configuration-exposure-changed", f"{path}.exposure", old.get("exposure"), new.get("exposure"), "configuration secret classification changed")
            self.compare_set(
                f"{path}.runtime_roles",
                old.get("runtime_roles", []),
                new.get("runtime_roles", []),
                "configuration-runtime-role",
                added_classification="behavioral",
                removed_classification="breaking",
            )
            if old.get("appears_in_running_configuration") != new.get("appears_in_running_configuration"):
                classification = "breaking" if old.get("appears_in_running_configuration") else "additive"
                self.add(
                    classification,
                    "configuration-running-visibility-changed",
                    f"{path}.appears_in_running_configuration",
                    old.get("appears_in_running_configuration"),
                    new.get("appears_in_running_configuration"),
                    "configuration visibility in the running configuration changed",
                )
            if old.get("dynamic_prefix") != new.get("dynamic_prefix"):
                self.add(
                    "breaking",
                    "configuration-dynamic-prefix-changed",
                    f"{path}.dynamic_prefix",
                    old.get("dynamic_prefix"),
                    new.get("dynamic_prefix"),
                    "configuration dynamic-prefix behavior changed",
                )
            for field in ("owner", "source"):
                if old.get(field) != new.get(field):
                    self.add("behavioral", f"configuration-{field}-changed", f"{path}.{field}", old.get(field), new.get(field), f"configuration {field} changed")

        self.keyed_items("configuration", baseline["configuration"], candidate["configuration"], "name", compare)
        self.compare_set(
            "configuration.constraints",
            baseline.get("configuration_constraints", []),
            candidate.get("configuration_constraints", []),
            "configuration-constraint",
            added_classification="breaking",
            removed_classification="additive",
        )

    def compare_set(
        self,
        path: str,
        baseline: list[Any],
        candidate: list[Any],
        kind: str,
        added_classification: str = "additive",
        removed_classification: str = "breaking",
    ) -> None:
        old = {stable(value): value for value in baseline}
        new = {stable(value): value for value in candidate}
        for key in sorted(new.keys() - old.keys()):
            self.add(added_classification, f"{kind}-added", path, None, new[key], f"added {kind}")
        for key in sorted(old.keys() - new.keys()):
            self.add(removed_classification, f"{kind}-removed", path, old[key], None, f"removed {kind}")

    def compare_fields(self, path: str, baseline: list[dict[str, Any]], candidate: list[dict[str, Any]], kind: str) -> None:
        old_fields = {item["name"]: item for item in baseline}
        new_fields = {item["name"]: item for item in candidate}
        for name in sorted(new_fields.keys() - old_fields.keys()):
            classification = "additive" if new_fields[name].get("nullable") else "breaking"
            self.add(classification, f"{kind}-added", f"{path}.{name}", None, new_fields[name], f"added a {kind}")
        for name in sorted(old_fields.keys() - new_fields.keys()):
            self.add("breaking", f"{kind}-removed", f"{path}.{name}", old_fields[name], None, f"removed a {kind}")
        for name in sorted(old_fields.keys() & new_fields.keys()):
            if old_fields[name].get("nullable") != new_fields[name].get("nullable"):
                classification = "breaking" if not new_fields[name].get("nullable") else "additive"
                self.add(classification, f"{kind}-nullability-changed", f"{path}.{name}.nullable", old_fields[name].get("nullable"), new_fields[name].get("nullable"), f"{kind} nullability changed")

    def compare_events(self, baseline: dict[str, Any], candidate: dict[str, Any]) -> None:
        old = baseline["events"]
        new = candidate["events"]
        shape_before = len(self.changes)
        self.compare_fields("events.envelope_fields", old["envelope_fields"], new["envelope_fields"], "event-field")
        self.compare_fields("events.provenance_fields", old.get("provenance_fields", []), new.get("provenance_fields", []), "event-provenance-field")
        self.compare_fields("events.sink_payload_fields", old.get("sink_payload_fields", []), new.get("sink_payload_fields", []), "event-sink-field")
        self.compare_set("events.schema_version_semantics", old.get("schema_version_semantics", []), new.get("schema_version_semantics", []), "event-schema-semantics")
        shape_changed = len(self.changes) != shape_before
        self.compare_set("events.actors", old["actors"], new["actors"], "event-actor")
        old_entities = {item["name"]: item for item in old["entities"]}
        new_entities = {item["name"]: item for item in new["entities"]}
        for name in sorted(new_entities.keys() - old_entities.keys()):
            self.add("additive", "event-entity-added", f"events.entities.{name}", None, new_entities[name], "added an event entity")
        for name in sorted(old_entities.keys() - new_entities.keys()):
            self.add("breaking", "event-entity-removed", f"events.entities.{name}", old_entities[name], None, "removed an event entity")
        for name in sorted(old_entities.keys() & new_entities.keys()):
            self.compare_set(f"events.entities.{name}.actions", old_entities[name]["actions"], new_entities[name]["actions"], "event-action")
        self.compare_set(
            "events.redaction_rules",
            old["redaction_rules"],
            new["redaction_rules"],
            "event-redaction-rule",
            added_classification="breaking",
            removed_classification="breaking",
        )
        self.compare_set("events.audit_document_versions", old.get("audit_document_versions", []), new.get("audit_document_versions", []), "event-audit-document-version")
        if new["schema_version"] < old["schema_version"]:
            self.add("breaking", "event-schema-version-decreased", "events.schema_version", old["schema_version"], new["schema_version"], "event schema version decreased")
        elif shape_changed and new["schema_version"] == old["schema_version"]:
            self.add("breaking", "event-schema-version-not-increased", "events.schema_version", old["schema_version"], new["schema_version"], "event shape changed without increasing schema_version")
        elif old["schema_version"] != new["schema_version"]:
            self.add("behavioral", "event-schema-version-changed", "events.schema_version", old["schema_version"], new["schema_version"], "event schema version changed")

    def compare_documents(self, baseline: dict[str, Any], candidate: dict[str, Any]) -> None:
        old = baseline["documents"]
        new = candidate["documents"]
        for family in ("backup", "import"):
            shape_before = len(self.changes)
            for field in ("required_fields", "optional_fields", "sections"):
                self.compare_set(f"documents.{family}.{field}", old[family][field], new[family][field], f"{family}-{field}")
            shape_changed = len(self.changes) != shape_before
            if old[family].get("rejection_policy") != new[family].get("rejection_policy"):
                self.add(
                    "breaking",
                    f"{family}-rejection-policy-changed",
                    f"documents.{family}.rejection_policy",
                    old[family].get("rejection_policy"),
                    new[family].get("rejection_policy"),
                    f"{family} rejection policy changed",
                )
            if new[family]["version"] < old[family]["version"]:
                self.add("breaking", f"{family}-version-decreased", f"documents.{family}.version", old[family]["version"], new[family]["version"], f"{family} document version decreased")
            elif shape_changed and new[family]["version"] == old[family]["version"]:
                self.add("breaking", f"{family}-version-not-increased", f"documents.{family}.version", old[family]["version"], new[family]["version"], f"{family} shape changed without increasing its version")
            elif old[family]["version"] != new[family]["version"]:
                self.add("behavioral", f"{family}-version-changed", f"documents.{family}.version", old[family]["version"], new[family]["version"], f"{family} document version changed")
        for field in ("scope_kinds", "content_types", "missing_data_policies", "template_kinds"):
            self.compare_set(f"documents.export.{field}", old["export"][field], new["export"][field], f"export-{field}")

    def compare_cli(self, baseline: dict[str, Any], candidate: dict[str, Any]) -> None:
        def compare_command(name: str, old: dict[str, Any], new: dict[str, Any]) -> None:
            def compare_option(option: str, old_option: dict[str, Any], new_option: dict[str, Any]) -> None:
                path = f"cli.{name}.options.{option}"
                for field in ("long", "short", "value_kind", "value_count", "required", "conflicts_with", "requires"):
                    if old_option.get(field) != new_option.get(field):
                        self.add("breaking", f"cli-option-{field}-changed", f"{path}.{field}", old_option.get(field), new_option.get(field), f"CLI option {field} changed")
                old_environment = old_option.get("environment")
                new_environment = new_option.get("environment")
                if old_environment != new_environment:
                    classification = "additive" if old_environment is None else "breaking"
                    self.add(
                        classification,
                        "cli-option-environment-changed",
                        f"{path}.environment",
                        old_environment,
                        new_environment,
                        "CLI option environment binding changed",
                    )
                for field in ("default", "dynamic_default"):
                    if old_option.get(field) != new_option.get(field):
                        self.add("behavioral", f"cli-option-{field}-changed", f"{path}.{field}", old_option.get(field), new_option.get(field), f"CLI option {field} changed")
                self.compare_set(f"{path}.allowed_values", old_option.get("allowed_values", []), new_option.get("allowed_values", []), "cli-option-value")

            self.keyed_items(
                f"cli.{name}.option",
                old["options"],
                new["options"],
                "id",
                compare_option,
                classify_added=lambda option: "breaking" if option.get("required") else "additive",
            )
            self.compare_set(f"cli.{name}.stable_output_modes", old.get("stable_output_modes", []), new.get("stable_output_modes", []), "cli-stable-output-mode")
            if old.get("exit_codes") != new.get("exit_codes"):
                self.add("breaking", "cli-exit-codes-changed", f"cli.{name}.exit_codes", old.get("exit_codes"), new.get("exit_codes"), "CLI exit-code categories changed")

        self.keyed_items("cli-command", baseline["cli"], candidate["cli"], "command", compare_command)


def validate_contract(document: Any, label: str) -> None:
    if not isinstance(document, dict) or document.get("schema_version") != 1:
        raise ValueError(f"Invalid {label} operational contract: unsupported schema_version")
    release_pattern = SEMVER_RELEASE if label == "candidate" else STABLE_SEMVER_RELEASE
    if not isinstance(document.get("release"), str) or not release_pattern.fullmatch(document["release"]):
        raise ValueError(f"Invalid {label} operational contract: release must be semantic-versioned")
    for field in ("metrics", "configuration", "events", "documents", "cli"):
        if field not in document:
            raise ValueError(f"Invalid {label} operational contract: missing {field}")


def validate_exceptions(policy: Any) -> None:
    if not isinstance(policy, dict) or policy.get("schema_version") != 1 or not isinstance(policy.get("exceptions"), list):
        raise ValueError("Invalid operational compatibility exception file")
    identifiers: set[str] = set()
    for exception in policy["exceptions"]:
        required = ("id", "baseline", "expires", "reason", "migration", "changelog_entry", "fingerprints")
        if not isinstance(exception, dict) or any(field not in exception for field in required):
            raise ValueError("Invalid operational compatibility exception file")
        if not EXCEPTION_ID.fullmatch(exception["id"]) or exception["id"] in identifiers:
            raise ValueError("Invalid operational compatibility exception file")
        identifiers.add(exception["id"])
        if not SEMVER_TAG.fullmatch(exception["baseline"]):
            raise ValueError("Invalid operational compatibility exception file")
        try:
            dt.date.fromisoformat(exception["expires"])
        except (TypeError, ValueError) as error:
            raise ValueError("Invalid operational compatibility exception file") from error
        if any(not isinstance(exception[field], str) or not exception[field].strip() for field in ("reason", "migration", "changelog_entry")):
            raise ValueError("Invalid operational compatibility exception file")
        if not isinstance(exception["fingerprints"], list) or not exception["fingerprints"] or any(not isinstance(value, str) or not value for value in exception["fingerprints"]):
            raise ValueError("Invalid operational compatibility exception file")


def changed_release_notes(changelog: str, candidate_version: str) -> str:
    sections: list[str] = []
    for heading in ("Unreleased", candidate_version):
        match = re.search(rf"^## \[{re.escape(heading)}\].*$", changelog, re.MULTILINE)
        if not match:
            continue
        start = match.end()
        next_heading = re.search(r"^## \[", changelog[start:], re.MULTILINE)
        end = start + next_heading.start() if next_heading else len(changelog)
        sections.append(changelog[start:end])
    return "\n".join(sections)


def apply_exceptions(
    changes: list[dict[str, Any]],
    policy: dict[str, Any],
    baseline_tag: str,
    today: dt.date,
    release_notes: str,
) -> list[str]:
    errors: list[str] = []
    breaking = {change["fingerprint"]: change for change in changes if change["classification"] == "breaking"}
    for exception in policy["exceptions"]:
        if exception["baseline"] != baseline_tag:
            continue
        if dt.date.fromisoformat(exception["expires"]) < today:
            errors.append(f"exception {exception['id']} expired on {exception['expires']}")
            continue
        if exception["changelog_entry"] not in release_notes:
            errors.append(f"exception {exception['id']} has no matching changelog migration entry")
        matched = set(exception["fingerprints"]) & breaking.keys()
        missing = set(exception["fingerprints"]) - breaking.keys()
        if missing:
            errors.append(f"exception {exception['id']} references absent fingerprints: {', '.join(sorted(missing))}")
        for fingerprint in matched:
            breaking[fingerprint]["accepted_by"] = exception["id"]
    return errors


def summary_markdown(report: dict[str, Any]) -> str:
    lines = ["## Operational contract compatibility", ""]
    if report["status"] == "skipped":
        return "\n".join(lines + ["No stable operational-contract baseline is available.", "", f"Reason: {report['reason']}", ""])
    counts = report["counts"]
    lines.extend(
        [
            f"Status: **{report['status']}**",
            "",
            f"Additive: {counts['additive']}; behavioral: {counts['behavioral']}; breaking: {counts['breaking']}; accepted: {counts['accepted']}; unaccepted: {counts['unaccepted']}.",
            "",
        ]
    )
    for classification in ("breaking", "behavioral", "additive"):
        selected = [change for change in report["changes"] if change["classification"] == classification]
        if not selected:
            continue
        lines.extend([f"### {classification.title()}", ""])
        for change in selected:
            accepted = f" (accepted by `{change['accepted_by']}`)" if change.get("accepted_by") else ""
            lines.append(f"- `{change['fingerprint']}` `{change['path']}`: {change['reason']}{accepted}")
        lines.append("")
    if report["exception_errors"]:
        lines.extend(["### Exception policy errors", ""])
        lines.extend(f"- {error}" for error in report["exception_errors"])
        lines.append("")
    return "\n".join(lines)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("baseline", type=Path)
    parser.add_argument("candidate", type=Path)
    parser.add_argument("metadata", type=Path)
    parser.add_argument("exceptions", type=Path)
    parser.add_argument("changelog", type=Path)
    parser.add_argument("report_directory", type=Path)
    arguments = parser.parse_args()
    try:
        candidate = load_json(arguments.candidate)
        metadata = load_json(arguments.metadata)
        policy = load_json(arguments.exceptions)
        validate_contract(candidate, "candidate")
        validate_exceptions(policy)
        if metadata.get("status") not in ("available", "unavailable"):
            raise ValueError("Invalid operational-contract baseline metadata")
        arguments.report_directory.mkdir(parents=True, exist_ok=True)
        if metadata["status"] == "unavailable":
            report = {
                "status": "skipped",
                "reason": metadata.get("reason"),
                "baseline": metadata,
                "candidate": {"sha256": digest(arguments.candidate)},
                "counts": {"additive": 0, "behavioral": 0, "breaking": 0, "accepted": 0, "unaccepted": 0},
                "changes": [],
                "exception_errors": [],
            }
        else:
            baseline = load_json(arguments.baseline)
            validate_contract(baseline, "baseline")
            if digest(arguments.baseline) != metadata.get("sha256"):
                raise ValueError("Operational-contract baseline digest mismatch")
            baseline_tag = metadata.get("tag")
            if not isinstance(baseline_tag, str) or not SEMVER_TAG.fullmatch(baseline_tag) or baseline["release"] != baseline_tag[1:]:
                raise ValueError("Operational-contract baseline release does not match metadata tag")
            comparator = Comparator()
            comparator.compare_metrics(baseline, candidate)
            comparator.compare_configuration(baseline, candidate)
            comparator.compare_events(baseline, candidate)
            comparator.compare_documents(baseline, candidate)
            comparator.compare_cli(baseline, candidate)
            today = dt.date.fromisoformat(__import__("os").environ.get("HUBUUM_OPERATIONAL_CONTRACT_TODAY", dt.date.today().isoformat()))
            notes = changed_release_notes(arguments.changelog.read_text(encoding="utf-8"), candidate["release"])
            exception_errors = apply_exceptions(comparator.changes, policy, baseline_tag, today, notes)
            counts = {classification: sum(change["classification"] == classification for change in comparator.changes) for classification in ("additive", "behavioral", "breaking")}
            counts["accepted"] = sum(change.get("accepted_by") is not None for change in comparator.changes if change["classification"] == "breaking")
            counts["unaccepted"] = counts["breaking"] - counts["accepted"]
            status = "failed" if counts["unaccepted"] or exception_errors else "passed"
            report = {
                "status": status,
                "baseline": metadata,
                "candidate": {"release": candidate["release"], "sha256": digest(arguments.candidate)},
                "counts": counts,
                "changes": sorted(comparator.changes, key=lambda change: (change["classification"], change["path"], change["fingerprint"])),
                "exception_errors": exception_errors,
            }
        (arguments.report_directory / "compatibility.json").write_text(json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8")
        (arguments.report_directory / "summary.md").write_text(summary_markdown(report), encoding="utf-8")
        return 1 if report["status"] == "failed" else 0
    except (OSError, ValueError) as error:
        print(error, file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
