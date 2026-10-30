#!/usr/bin/env python3
"""Publish complete performance evidence after the source workflow terminates."""

import sys

if sys.version_info < (3, 11):
    sys.exit(
        "Hubuum tooling requires Python 3.11 or newer; found "
        + sys.version.split()[0]
        + ". Install Python 3.11+ and ensure python3 on PATH selects it."
    )

import argparse
from datetime import datetime
import hashlib
import html
import io
import json
import math
import os
from pathlib import Path, PurePosixPath
import subprocess
import zipfile

from benchmark_github import api, pages


MARKER = "<!-- hubuum-performance -->"
REPORT_MARKERS = (MARKER, "<!-- rust-pr-bench -->", "<!-- gungraun-bench -->", "<!-- criterion-bench -->")
SCOPES = {
    "micro": "Benchmarks (micro)", "postgres": "Benchmarks (postgres)",
    "runtime": "Runtime behavior validation", "resources": "Template and schema resource evidence",
}


def seconds(start, end):
    if not start or not end:
        return 0
    return max(0, (datetime.fromisoformat(end.replace("Z", "+00:00"))
                   - datetime.fromisoformat(start.replace("Z", "+00:00"))).total_seconds())


def escape(value):
    return html.escape(str(value)).replace("|", "&#124;").replace("\n", " ")


def number(value):
    return value if isinstance(value, (int, float)) and not isinstance(value, bool) and math.isfinite(value) else 0


def read_evidence(data):
    """Read bounded JSON evidence in memory; never extract or execute artifacts."""
    result = []
    if len(data) > 32 * 1024 * 1024:
        raise ValueError("Evidence archive exceeds 32 MiB")
    with zipfile.ZipFile(io.BytesIO(data)) as archive:
        if sum(item.file_size for item in archive.infolist()) > 64 * 1024 * 1024:
            raise ValueError("Expanded evidence exceeds 64 MiB")
        for item in archive.infolist():
            path = PurePosixPath(item.filename)
            if path.is_absolute() or ".." in path.parts:
                raise ValueError("Invalid evidence archive path")
            if path.suffix not in {".json", ".jsonl"} or item.is_dir():
                continue
            text = archive.read(item).decode("utf-8")
            for line in text.splitlines() if path.suffix == ".jsonl" else [text]:
                try:
                    value = json.loads(line)
                except ValueError:
                    continue
                if isinstance(value, dict):
                    result.append((path.name, value))
    return result


def deduplicate(records):
    unique = {}
    for record in records:
        identity = record.get("operation_id") or hashlib.sha256(json.dumps(record, sort_keys=True).encode()).hexdigest()
        unique[identity] = record
    return list(unique.values())


def scope_status(jobs):
    if not jobs:
        return "unavailable"
    conclusions = {job.get("conclusion") for job in jobs}
    if conclusions & {"failure", "timed_out", "action_required", "startup_failure"}:
        return "failed"
    if "cancelled" in conclusions:
        return "cancelled"
    if any(job.get("status") != "completed" for job in jobs):
        return "pending"
    if conclusions <= {"skipped"}:
        return "skipped"
    return "passed" if conclusions <= {"success", "skipped", "neutral"} else "unavailable"


def render(run, jobs, evidence, artifacts, problems=()):
    selection = next((value for name, value in evidence if name == "selection.json" and "scopes" in value), None)
    targets = next((value for name, value in evidence if name == "target-selection.json"), None)
    records = deduplicate([value for name, value in evidence if name == "performance.jsonl" and value.get("kind") != "jobs"])
    rows = [MARKER, f"## Performance results — {escape(run.get('conclusion', 'unavailable'))}", "",
            f"[Workflow run and evidence]({run['html_url']}) · attempt {int(run.get('run_attempt', 1))}", ""]
    if selection:
        rows += [f"Scopes: {escape(', '.join(selection['scopes']) or 'none')}. "
                 f"Tags: {escape(', '.join(selection.get('tags', [])) or 'none')}. "
                 f"Profile: **{escape(selection['profile'])}**.", ""]
        rows += [escape(reason) for reason in selection.get("reasons", [])]
    else:
        rows += ["Selection evidence unavailable; the selection job may have failed or been cancelled."]
    rows += ["", "| Scope | Status | Runner time |", "| --- | --- | ---: |"]
    selected = set(selection["scopes"]) if selection else set(SCOPES)
    discovered = {item["name"] for item in targets.get("include", [])} if targets else None
    for scope, prefix in SCOPES.items():
        group = [job for job in jobs if job.get("name", "").startswith(prefix)]
        status = scope_status(group) if scope in selected else "not selected"
        if discovered is not None and scope in {"micro", "postgres"} and scope not in discovered:
            status = "no selected targets"
        duration = sum(seconds(job.get("started_at"), job.get("completed_at")) for job in group)
        rows.append(f"| {scope} | {status} | {duration / 60:.2f} min |")
    completed = [job for job in jobs if job.get("started_at") and job.get("completed_at")]
    runner = sum(seconds(job["started_at"], job["completed_at"]) for job in completed)
    elapsed = seconds(min(job["started_at"] for job in completed), max(job["completed_at"] for job in completed)) if completed else 0
    latency = seconds(run.get("created_at"), max(job["completed_at"] for job in completed)) if completed else 0
    transfer = sum(seconds(step.get("started_at"), step.get("completed_at"))
                   for job in completed for step in job.get("steps", [])
                   if step["name"].startswith(("Upload ", "Download ", "Retain ")))
    rows += ["", f"All workflow jobs: **{runner / 60:.2f} runner minutes**; "
             f"first job start to last completion: **{elapsed / 60:.2f} minutes**; "
             f"run creation to last completion: **{latency / 60:.2f} minutes**.",
             f"Artifact transfer steps: {transfer:.1f}s, included in runner time. "
             "Elapsed spans include dependency waits and gaps. Queue time: unavailable; "
             "these timestamps cannot isolate per-job queueing. Publisher time is separate.", ""]
    if targets:
        rows += ["<details><summary>Selected benchmark targets</summary>", ""]
        for lane in targets["include"]:
            rows += [f"- {escape(target)}" for target in lane.get("selected_targets", [])]
        rows += ["", "</details>", ""]
    results = [value for name, value in evidence if name == "result.json"]
    if results:
        rows += ["| Benchmark | Backend | Profile and features | Change | Status |",
                 "| --- | --- | --- | ---: | --- |"]
        for result in results:
            failed = result.get("head_error") or result.get("base_error")
            missing = result.get("head_missing") or result.get("base_missing")
            status = "failed" if failed else "unavailable" if missing else "measured"
            delta = result.get("delta_pct")
            delta_text = f"{delta:+.2f}%" if isinstance(delta, (int, float)) and math.isfinite(delta) else "unavailable"
            rows.append(f"| {escape(result.get('benchmark_name', 'unknown'))} | "
                        f"{escape(result.get('backend', 'unknown'))} | {escape(result.get('feature_name', ''))} | {delta_text} | {status} |")
    if records:
        compilation = sum(number(record.get("compile_seconds")) for record in records)
        execution = sum(number(record.get("benchmark_command_seconds")) for record in records)
        setup = sum(number(record.get("setup_seconds")) + number(record.get("readiness_seconds"))
                    + number(record.get("teardown_seconds")) for record in records)
        rows += ["", f"Observed compilation: {compilation:.2f}s; benchmark commands: {execution:.2f}s; "
                 f"setup/readiness/teardown: {setup:.2f}s. Benchmark command time includes "
                 "compilation if Cargo fallback was required."]
        rows += ["", "<details><summary>Build and cache evidence</summary>", "",
                 "| Work | Restore | Save | Compile | Fresh / rebuilt | Executables reused |",
                 "| --- | --- | --- | ---: | ---: | ---: |"]
        for record in records:
            if record.get("kind") == "execution":
                continue
            rows.append(f"| {escape(record.get('label', record.get('kind', 'build')))} | "
                        f"{escape(record.get('restore', '—'))} | {escape(record.get('save', '—'))} | "
                        f"{number(record.get('compile_seconds')):.2f}s | "
                        f"{number(record.get('fresh'))} / {number(record.get('rebuilt'))} | "
                        f"{number(record.get('executables_reused'))} |")
        rows += ["", "Compilation and cache operations are components of runner time, not additional totals.", "", "</details>"]
    rows += ["", "Artifacts: " + ", ".join(escape(item["name"]) for item in artifacts),
             "Runtime and resource measurements remain in their named evidence artifacts. "
             "Opt-in scale runs have separate reports and totals."]
    if problems:
        rows += ["", "Unavailable evidence: " + "; ".join(escape(problem) for problem in problems)]
    return "\n".join(rows) + "\n"


def current_run(run, pr, runs):
    if pr["head"]["sha"] != run["head_sha"]:
        return False
    return not any(other["head_sha"] == run["head_sha"] and
                   (other["run_number"], other.get("run_attempt", 1)) > (run["run_number"], run.get("run_attempt", 1))
                   for other in runs)


def latest_participating_run(run, runs, jobs_for):
    """Ignored label events must neither replace reports nor fence useful work."""
    candidates = [candidate for candidate in runs if candidate["head_sha"] == run["head_sha"]]
    if not any(candidate["id"] == run["id"] for candidate in candidates):
        candidates.append(run)
    for candidate in sorted(candidates, key=lambda item: (item["run_number"], item.get("run_attempt", 1)), reverse=True):
        if candidate["status"] != "completed":
            return None
        if any(job.get("conclusion") != "skipped" for job in jobs_for(candidate)):
            return candidate
    return None


def report_comments(comments):
    matches = [comment for comment in comments if comment.get("user", {}).get("login") == "github-actions[bot]"
               and any(marker in comment.get("body", "") for marker in REPORT_MARKERS)]
    return sorted(matches, key=lambda comment: MARKER not in comment["body"])


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--run-id", type=int, required=True)
    parser.add_argument("--output", type=Path, default=Path("performance-report.md"))
    parser.add_argument("--publish", action="store_true")
    args = parser.parse_args()
    repo = os.environ["GITHUB_REPOSITORY"]
    base = f"repos/{repo}"
    run = api(f"{base}/actions/runs/{args.run_id}")
    if (run["status"] != "completed" or run["event"] not in {"pull_request", "workflow_dispatch"}
            or run.get("path", "").split("@")[0] != ".github/workflows/benchmarks.yml"):
        return
    job_cache = {}
    def jobs_for(candidate):
        if candidate["id"] not in job_cache:
            job_cache[candidate["id"]] = pages(f"{base}/actions/runs/{candidate['id']}/jobs?filter=latest", "jobs")
        return job_cache[candidate["id"]]
    if run["event"] == "pull_request":
        recent = pages(f"{base}/actions/workflows/{run['workflow_id']}/runs?event=pull_request&head_sha={run['head_sha']}", "workflow_runs")
        run = latest_participating_run(run, recent, jobs_for)
        if run is None:
            return
    jobs = jobs_for(run)
    artifacts = pages(f"{base}/actions/runs/{run['id']}/artifacts", "artifacts")
    evidence, problems = [], []
    # Rerun artifact names are not always attempt-qualified. Only consume artifacts
    # created since this attempt started, including selection and failed-job evidence.
    for artifact in sorted(artifacts, key=lambda item: (item["created_at"], item["id"])):
        is_selection = artifact["name"].startswith("performance-selection") or artifact["name"].startswith("performance-target-selection")
        if (artifact.get("expired") or (not is_selection and artifact["created_at"] < run.get("run_started_at", run["created_at"]))
                or not artifact["name"].startswith(("performance-", "benchmark-selection-", "bench-result-", "runtime-behavior", "template-schema-evidence"))):
            continue
        try:
            # Newest selection wins; a partial rerun may reuse its original plan.
            parsed = read_evidence(api(f"{base}/actions/artifacts/{artifact['id']}/zip", raw=True))
            evidence = parsed + evidence if is_selection else evidence + parsed
        except (ValueError, OSError, subprocess.CalledProcessError, zipfile.BadZipFile) as error:
            problems.append(f"{artifact['name']}: {type(error).__name__}")
    body = render(run, jobs, evidence, artifacts, problems)
    # Keep the comment bounded; the complete document is retained as an artifact.
    args.output.write_text(body)
    if not args.publish or run["event"] != "pull_request":
        return
    prs = run.get("pull_requests", []) or pages(f"{base}/commits/{run['head_sha']}/pulls")
    for candidate in prs:
        pr = api(f"{base}/pulls/{int(candidate['number'])}")
        # Recheck after collecting evidence; a newer run may have started meanwhile.
        recent = pages(f"{base}/actions/workflows/{run['workflow_id']}/runs?event=pull_request&head_sha={run['head_sha']}", "workflow_runs")
        latest = latest_participating_run(run, recent, jobs_for)
        if (pr["state"] != "open" or latest is None or latest["id"] != run["id"]
                or latest.get("run_attempt", 1) != run.get("run_attempt", 1)
                or not current_run(run, pr, [latest])):
            continue
        comments = pages(f"{base}/issues/{pr['number']}/comments")
        previous_reports = report_comments(comments)
        previous = next(iter(previous_reports), None)
        posted = body if len(body) <= 60000 else body[:59000] + f"\n\nFull report: {run['html_url']}\n"
        if previous:
            api(f"{base}/issues/comments/{previous['id']}", method="PATCH", body={"body": posted})
        else:
            api(f"{base}/issues/{pr['number']}/comments", method="POST", body={"body": posted})
        for duplicate in previous_reports[1:]:
            api(f"{base}/issues/comments/{duplicate['id']}", method="DELETE")


if __name__ == "__main__":
    main()
