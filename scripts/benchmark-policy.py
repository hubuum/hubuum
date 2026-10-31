#!/usr/bin/env python3
"""Resolve Hubuum performance scopes, target tags, and measurement profiles."""

import sys

if sys.version_info < (3, 11):
    sys.exit(
        "Hubuum tooling requires Python 3.11 or newer; found "
        + sys.version.split()[0]
        + ". Install Python 3.11+ and ensure python3 on PATH selects it."
    )

import argparse
from fnmatch import fnmatchcase
import json
import os
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
PREFIX = "ci:bench:"


def load_policy(path=ROOT / ".github/benchmark-policy.json"):
    policy = json.loads(path.read_text())
    if policy["version"] != 1:
        raise ValueError("Unsupported benchmark policy version")
    if set(policy["scopes"]) & set(policy["tag_scopes"]):
        raise ValueError("Benchmark scopes and tags must use distinct names")
    return policy


def csv(value):
    return [item.strip() for item in value.split(",") if item.strip()]


def affected(paths, policy):
    scopes, tags, reasons, benchmark_paths = set(), set(), [], []
    for path in paths:
        matches = [rule for rule in policy["path_rules"]
                   if any(fnmatchcase(path, pattern) for pattern in rule["patterns"])]
        if matches:
            for rule in matches:
                scopes.update(rule.get("scopes", []))
                tags.update(rule.get("tags", []))
                if rule.get("benchmark_paths"):
                    benchmark_paths.append(path)
            reasons.append(f"affected input: {path}")
        elif not any(fnmatchcase(path, pattern) for pattern in policy["ignore_paths"]):
            scopes.update(policy["scopes"])
            reasons.append(f"conservative fallback for {path}")
    return scopes, tags, reasons, benchmark_paths


def resolve(paths, *, labels=(), draft=False, manual=None, policy=None):
    policy = load_policy() if policy is None else policy
    labels = set(labels)
    manual = manual or {}
    force_all = bool(labels & {"ci:full", "ci:benchmarks"}) or manual.get("scopes") == "all"
    scopes, tags, targets = set(), set(), set(csv(manual.get("targets", "")))
    profiles = set()
    for label in labels:
        if label.startswith(PREFIX):
            kind, separator, value = label[len(PREFIX):].partition(":")
            if not separator or kind not in {"scope", "tag", "profile"}:
                raise ValueError(f"Unknown benchmark selector: {label}")
            {"scope": scopes, "tag": tags, "profile": profiles}[kind].add(value)
    if manual.get("scopes") not in {None, "", "auto", "all"}:
        scopes.update(csv(manual["scopes"]))
    tags.update(csv(manual.get("tags", "")))
    if manual.get("profile"):
        profiles.add(manual["profile"])
    for selected, available, kind in ((scopes, policy["scopes"], "scope"),
                                      (tags, policy["tag_scopes"], "tag"),
                                      (profiles, policy["profiles"], "profile")):
        if selected - set(available):
            raise ValueError(f"Unknown benchmark {kind}: {', '.join(sorted(selected - set(available)))}")
    explicit = bool(scopes or tags or targets)
    requested = force_all or explicit or bool(profiles) or not draft
    reasons, benchmark_paths = [], []
    if force_all:
        scopes, tags, targets = set(policy["scopes"]), set(), set()
        reasons.append("all scopes explicitly requested")
    elif explicit:
        reasons.append("explicit scope/tag/target selection replaces automatic scope")
    elif requested:
        scopes, tags, reasons, benchmark_paths = affected(paths, policy)
    else:
        reasons.append("draft without a performance request")
    # Names and effort ordering belong exclusively to this repository.
    order = ["quick", "standard", "thorough"]
    profile = max(profiles or {policy["default_profile"]}, key=order.index)
    if force_all and profile == "quick":
        profile = "standard"
    resolved_scopes = set(scopes)
    for tag in tags:
        resolved_scopes.update(policy["tag_scopes"][tag])
    if targets or benchmark_paths:
        resolved_scopes.update(("micro", "postgres"))
    return {
        "version": 1, "requested": requested, "draft": draft,
        "force_all": force_all, "explicit": explicit,
        "scopes": sorted(resolved_scopes), "whole_scopes": sorted(scopes),
        "tags": sorted(tags), "targets": sorted(targets), "profile": profile,
        "benchmark_paths": benchmark_paths, "reasons": reasons,
        "run_benchmarks": bool(resolved_scopes & {"micro", "postgres"}),
        "run_runtime": "runtime" in resolved_scopes,
        "run_resources": "resources" in resolved_scopes,
    }


def summary(selection):
    return ("## Performance selection\n\n"
            f"Scopes: {', '.join(selection['scopes']) or 'none'}. "
            f"Tags: {', '.join(selection['tags']) or 'none'}. "
            f"Profile: {selection['profile']}.\n\n"
            + "\n".join(f"- {reason}" for reason in selection["reasons"]) + "\n")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--paths", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    event = json.loads(Path(os.environ["GITHUB_EVENT_PATH"]).read_text()) if os.environ.get("GITHUB_EVENT_PATH") else {}
    pr = event.get("pull_request", {})
    inputs = event.get("inputs", {})
    paths = [path for path in args.paths.read_text().split("\0") if path]
    selection = resolve(paths, labels=[item["name"] for item in pr.get("labels", [])],
                        draft=pr.get("draft", False), manual=inputs)
    args.output.write_text(json.dumps(selection, indent=2) + "\n")
    if os.environ.get("GITHUB_OUTPUT"):
        with Path(os.environ["GITHUB_OUTPUT"]).open("a") as output:
            for name in ("run_benchmarks", "run_runtime", "run_resources"):
                output.write(f"{name}={str(selection[name]).lower()}\n")
            output.write(f"selection={json.dumps(selection, separators=(',', ':'))}\n")
    if os.environ.get("GITHUB_STEP_SUMMARY"):
        with Path(os.environ["GITHUB_STEP_SUMMARY"]).open("a") as output:
            output.write(summary(selection))


if __name__ == "__main__":
    main()
