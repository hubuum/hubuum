"""Provision real held/large/dependent tasks; Rust verifies their lifecycle."""

import copy
import json
import time
import uuid

from integration.corpus import Api
from integration.task_retention import run_native, write_json
from support.task_retention_recovery import _admin, _wait


def _private_json(path, value):
    write_json(path, value)
    path.chmod(0o600)


def _native_safety(arguments, deployment, root, origin, phase):
    run_native(deployment, [
        str(arguments.scale_binary), "task-retention-safety", "--phase", phase,
        "--spec-file", str(root / "safety-spec.json"), "--state-file", str(root / "safety-state.json"),
        "--remote-state", str(root / "safety-remote-state.json"), "--origin", origin,
        "--token-file", str(root / "tokens.json"), "--database-url", deployment.host_url,
        "--admin-binary", str(arguments.admin_binary), "--repository", str(deployment.repository),
        "--key-file", str(deployment.key), "--artifact-directory", str(root / f"safety-{phase}-processes"),
        "--output", str(root / f"safety-{phase}.json"), "--timeout-seconds", "300",
    ], timeout=330)


def _remote(arguments, deployment, root, origins, phase, suffix=None):
    report = "safety-remote-" + phase + ("-" + suffix if suffix else "")
    run_native(deployment, [
        str(arguments.scale_binary), "task-retention-probe", "--phase", phase,
        "--origin", origins[0], "--other-origin", origins[-1],
        "--token-file", str(root / "tokens.json"), "--request-file", str(root / "safety-remote-request.json"),
        "--state-file", str(root / "safety-remote-state.json"), "--output", str(root / (report + ".json")),
        "--database-url", deployment.host_url, "--timeout-seconds", "120",
    ], timeout=150)


def _large_tasks(arguments, deployment, root, origins):
    collections = [{"name": "${cohort}-${sequence}-" + str(index),
                    "description": "Synthetic bounded child cleanup"} for index in range(1201)]
    body = {"version": 2, "dry_run": True, "graph": {"collections": collections}}
    changed = copy.deepcopy(body)
    changed["graph"]["collections"][0]["description"] = "Changed request"
    recipe = root / "safety-large-workload.json"
    _private_json(recipe, {"version": 1, "count": 2, "concurrency": 1, "timeout_seconds": 120,
                          "duplicate_every": 1, "conflict_every": 1, "endpoint": "/api/v1/imports",
                          "body": body, "conflict_body": changed})
    result = root / "safety-large-admission.json"
    run_native(deployment, [
        str(arguments.scale_binary), "task-retention", "--origins", *origins,
        "--database-url", deployment.host_url, "--token-file", str(root / "tokens.json"),
        "--workload-spec", str(recipe), "--cohort", "safety-" + uuid.uuid4().hex,
        "--output", str(result),
    ], timeout=180)
    rows = sorted(json.loads(result.read_text(encoding="utf-8"))["admissions"], key=lambda row: row["sequence"])
    holds = []
    for row in rows:
        hold = str(uuid.uuid4())
        deployment.admin("source", "--task-retention", "hold", "--task-hold-id", hold,
                         "--task-hold-task", row["task_identity"], "--task-hold-reason", "operator")
        holds.append(hold)
    return rows, holds


def _dependency(api, suffix):
    admin_group = next(group["id"] for group in api.pages("/api/v1/iam/groups")
                       if group["groupname"] == "admin")
    collection = api.post("/api/v1/collections", {"name": "safety-" + suffix,
                          "description": "Retained task dependency fixture", "group_id": admin_group})
    target = api.post("/api/v1/classes", {"name": "safety-" + suffix,
                      "collection_id": collection["id"], "description": "Repair report dependency",
                      "json_schema": None, "validate_schema": False})
    api.post(f"/api/v1/classes/{target['id']}/", {"name": "synthetic-object", "description": "Missing required field", "data": {}})
    revision = api.post(f"/api/v1/classes/{target['id']}/schema/revisions", {
        "json_schema": {"type": "object", "required": ["missing_field"]}, "validate_schema": True,
    })
    work = api.post(f"/api/v1/classes/{target['id']}/schema/revisions/{revision['revision']}/impact")
    return collection["id"], target["id"], work["task_id"]


def run_safety_drill(arguments, deployment, origins, root, monitor):
    """Return current origins after the real pre-terminal worker crash/recovery."""
    started = time.monotonic()
    api = Api(origins[0], json.loads((root / "tokens.json").read_text(encoding="utf-8"))[0])
    deployment.admin("source", "--task-retention", "set-policy",
                     "--task-retention-deduplication-seconds", "7200", "--task-retention-details-seconds", "30")
    rows, holds = _large_tasks(arguments, deployment, root, origins)
    probe = uuid.uuid4().hex
    collection, target_class, dependent_task = _dependency(api, probe)
    _private_json(root / "safety-spec.json", {"large_task": rows[0]["task_id"], "held_task": rows[1]["task_id"],
                                           "dependent_task": dependent_task, "dependent_class": target_class})
    _native_safety(arguments, deployment, root, origins[0], "capture")
    target = api.post("/api/v1/remote-targets", {
        "collection_id": collection, "class_id": None, "name": "safety-effect-" + probe,
        "description": "Response held after one external effect", "method": "post",
        "url_template": deployment.effects.remote_url, "timeout_ms": 900000,
        "headers_template": {"Content-Type": "application/json"},
        "body_template": "{{ body_override | tojson }}",
        "allowed_subject_types": ["collection"],
    })
    body = {"subject": {"type": "collection", "collection_id": collection},
            "parameters": {}, "body_override": {"probe": probe}}
    changed = copy.deepcopy(body)
    changed["body_override"]["changed"] = True
    _private_json(root / "safety-remote-request.json", {
        "version": 1, "endpoint": f"/api/v1/remote-targets/{target['id']}/invoke",
        "key": "safety-" + probe, "body": body, "conflict_body": changed,
        "counter_url": deployment.effects.counter_url + "?probe=" + probe,
    })
    deployment.effects.hold_response(probe)
    try:
        dispatch_started = time.monotonic()
        _remote(arguments, deployment, root, origins, "dispatched")
        for index, phase in enumerate(("held", "released")):
            deployment.admin("source", "--task-retention", "release-hold", "--task-hold-id", holds[index])
            _native_safety(arguments, deployment, root, origins[0], phase)
            _remote(arguments, deployment, root, origins, "still-dispatched", phase)

        # Span ordinary lease renewals as well as the actual physical cleanup.
        _wait("one minute of the independently running remote task",
              lambda: time.monotonic() - dispatch_started >= 60, timeout=65)
        _remote(arguments, deployment, root, origins, "still-dispatched", "before-crash")
        dispatched_ms = (time.monotonic() - dispatch_started) * 1000

        killed = []
        for container in deployment.application_containers:
            running = deployment.docker("inspect", "--format", "{{.State.Running}}", container).stdout.strip()
            if running == "true":
                deployment.docker("kill", "--signal", "KILL", container)
                killed.append(container)
        if not killed:
            raise RuntimeError("Safety drill did not interrupt any live producer")
        _wait("all interrupted producer containers to exit", lambda: all(
            deployment.docker("inspect", "--format", "{{.State.Running}}", container).stdout.strip() == "false"
            for container in deployment.application_containers))
        deployment.effects.release_response(probe)
        recover_ms = _admin(arguments, deployment, "--recover-control", root / "safety-recover.log")
        reapply_ms = _admin(arguments, deployment, "--reapply-control", root / "safety-reapply.log")
        active, container = deployment.start_measured_api(4, role="all")
        monitor.add(container)
        authenticated = deployment.login(active, "source", "admin")
        _private_json(root / "tokens.json", [authenticated.token])
        origins = [authenticated.base]
        _remote(arguments, deployment, root, origins, "recovered-dispatched")
        _native_safety(arguments, deployment, root, origins[0], "recovered")
        _remote(arguments, deployment, root, origins, "purged")
        deployment.admin("source", "--task-retention", "set-policy",
                         "--task-retention-deduplication-seconds", "120", "--task-retention-details-seconds", "30")
        _private_json(root / "safety.json", {
            "elapsed_ms": (time.monotonic() - started) * 1000, "producer_processes_killed": len(killed),
            "recover_ms": recover_ms, "reapply_ms": reapply_ms,
            "remote_running_before_crash_ms": dispatched_ms,
            "declared_children_per_import": 1201, "native_assertions_completed": True,
            "retained_obligation": "schema repair report and its source task remain available",
        })
        return origins
    finally:
        deployment.effects.release_response(probe)
