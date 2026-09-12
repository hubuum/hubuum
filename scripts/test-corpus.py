#!/usr/bin/env python3
"""Generate, inspect and rehearse the committed functional corpus (stdlib only)."""

import sys

if sys.version_info < (3, 11):
    sys.exit(
        "Hubuum tooling requires Python 3.11 or newer; found "
        + sys.version.split()[0]
        + ". Install Python 3.11+ and ensure python3 on PATH selects it."
    )

import argparse
from collections import Counter
import copy
import hashlib
import json
import os
from pathlib import Path
import re
import secrets
import shutil
import subprocess
import tempfile
import time
import urllib.error
import urllib.parse
import urllib.request
import uuid


ROOT = Path(__file__).resolve().parent.parent
CORPORA = ROOT / "test-corpora"
BACKUP = "comprehensive.json"
MANIFEST = "comprehensive.manifest.json"
MAX_BYTES = 25 * 1024 * 1024
POSTGRES_IMAGE = "postgres:18.4@sha256:22c89fe0d0f507606260237fd55e51f6137f58b2d5bcf6152242b96d9fe8f9a4"
READ_PERMISSIONS = [
    "ReadCollection", "ReadClass", "ReadObject", "ReadClassRelation",
    "ReadObjectRelation", "ReadAudit",
]


def require(condition, message):
    if not condition:
        raise ValueError(message)


def read_json(path):
    return json.loads(path.read_text(encoding="utf-8"))


def write_json(path, value):
    path.write_text(json.dumps(value, ensure_ascii=False, indent=2) + "\n", encoding="utf-8", newline="\n")


def digest(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def load_recipe(path):
    recipe = read_json(path)
    require(recipe["name"] == "comprehensive" and recipe["revision"] == 2,
            "Unsupported corpus recipe identity")
    specs = recipe["classes"]
    require(len(specs) == 12 and len({s["name"] for s in specs}) == 12,
            "Recipe must contain twelve distinct classes")
    for policy, counts in (
        ("absent", [0, 100, 300, 600]),
        ("advisory", [100, 200, 300, 400]),
        ("enforced", [100, 200, 300, 400]),
    ):
        require([s["objects"] for s in specs if s["policy"] == policy] == counts,
                f"Unexpected {policy} object distribution")
    require(all(s["collection"] in {"operations", "branch", "restricted"} for s in specs),
            "Unknown recipe collection")
    computed = recipe["computed"]
    require(computed["classes"] == ["untyped-notes", "advisory-servers", "enforced-servers"],
            "Computed examples must cover each schema policy")
    require(len(computed["shared"]) == 15
            and len({d["key"] for d in computed["shared"]}) == 15,
            "Expected fifteen distinct shared computed definitions")
    require({d["operation"]["type"] for d in computed["shared"]} == {
        "first_non_null", "sum", "average", "min", "max", "all_present",
        "any_present", "count_present", "all_present_and_equal",
    }, "Computed definitions must cover all nine operations")
    require({d["result_type"] for d in computed["shared"]} == {
        "string", "number", "integer", "boolean", "object", "array",
    }, "Computed definitions must cover all six result types")
    return recipe


def object_data(spec, number):
    """Stable payloads; even advisory examples deliberately violate the schema."""
    data = {
        "hostname": f"{spec['name']}-{number:04d}.example.invalid",
        "status": ["active", "maintenance", "retired"][number % 3],
        "site": {"city": ["Oslo", "Tromsø", "Bergen"][number % 3], "rack": number % 20 + 1},
        "tags": ["synthetic", spec["name"]],
        "capacity": number * 8,
        "resources": {"cpu_cores": 2 ** (number % 5 + 1), "memory_gib": 8 * (number % 8 + 1),
                      "disks_gib": [128, 256 + 128 * (number % 4)]},
        "costs": {"compute": 20 + (number % 9) * 5.25, "storage": (number % 4) * 2.5},
        "telemetry": {
            "cpu_pct": ([] if number % 6 == 0 else [None, None] if number % 6 == 1
                        else [number % 11, number % 11 + 3, number % 11 + 6]),
            "temperature_c": [-5 + number % 10, number % 10 + 0.5, number % 10 + 3],
        },
        "contacts": {"team": "Operations team"},
        "configuration": {
            "desired": {"replicas": number % 3 + 1, "features": ["http", "metrics"], "enabled": True},
            "observed": {"enabled": number % 4 != 0,
                         "features": ["metrics", "http"] if number % 4 == 1 else ["http", "metrics"],
                         "replicas": number % 3 + 1},
        },
        "interfaces": [
            {"name": "eth0", "address": f"192.0.2.{number % 254 + 1}", "speed_mbps": 1000, "up": True},
            {"name": "eth1", "address": None if number % 4 == 0 else f"198.51.100.{number % 254 + 1}",
             "speed_mbps": 10000, "up": number % 4 != 0},
        ],
        "labels": {"cost/center": f"CC-{number % 7:02d}", "rack~slot": f"R{number % 20 + 1}"},
        "checks": {"zero": 0, "disabled": False, "text": "", "items": [], "details": {}},
    }
    if number % 3 != 2:
        data["costs"]["network"] = 0 if number % 3 == 0 else None
    if number % 5:
        data["contacts"]["primary"] = (
            None if number % 5 == 1 else "" if number % 5 == 3 else f"person-{number}@example.invalid"
        )
    if number % 5 in (1, 3, 4):
        data["contacts"]["on_call"] = None if number % 5 == 1 else f"pager-{number}@example.invalid"
    if number % 3:
        data["owner"] = None if number % 3 == 1 else "Inventory team"
    if spec["policy"] == "advisory" and number % 2 == 0:
        data["hostname"] = number
        data["status"] = "unclassified"
        data["site"]["rack"] = "unknown"
        data["costs"]["network"] = "unmetered"
    if spec["policy"] == "absent":
        return [data, {}, ["loose", number], None][number % 4]
    return data


def object_name(spec, number):
    suffix = "-nonconforming" if spec["policy"] == "advisory" and number % 2 == 0 else ""
    return f"{spec['name']}-{number:04d}{suffix}"


class Api:
    def __init__(self, base, token=""):
        self.base = base
        self.token = token
        self.opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))

    def request(self, method, path, body=None, expected=(200,), headers=None):
        request_headers = {"Content-Type": "application/json"}
        if self.token:
            request_headers["Authorization"] = f"Bearer {self.token}"
        request_headers.update(headers or {})
        request = urllib.request.Request(
            self.base + path, method=method, headers=request_headers,
            data=None if body is None else json.dumps(body).encode(),
        )
        try:
            response = self.opener.open(request, timeout=30)
        except urllib.error.HTTPError as error:
            response = error
        with response:
            raw = response.read()
            require(response.status in expected,
                    f"{method} {path}: expected {expected}, received {response.status}: "
                    + raw.decode(errors="replace")[:400])
            return (json.loads(raw) if raw else None), response.headers

    def get(self, path):
        return self.request("GET", path)[0]

    def post(self, path, body=None, expected=(200, 201, 202)):
        return self.request("POST", path, body, expected)[0]

    def change(self, method, path, body=None, expected=(200, 204)):
        _, headers = self.request("GET", path)
        return self.request(method, path, body, expected, {"If-Match": headers["ETag"]})[0]

    def pages(self, path, limit=100):
        result = []
        cursor = None
        seen = set()
        while True:
            query = {"limit": limit}
            if cursor:
                query["cursor"] = cursor
            page, headers = self.request("GET", path + "?" + urllib.parse.urlencode(query))
            require(isinstance(page, list), f"Expected a page at {path}")
            result.extend(page)
            cursor = headers.get("X-Next-Cursor")
            if not cursor:
                return result
            require(cursor not in seen and len(seen) < 100, f"Pagination did not terminate: {path}")
            seen.add(cursor)


def wait_for(description, operation, timeout=180):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if operation():
            return
        time.sleep(0.5)
    raise TimeoutError(f"Timed out waiting for {description}")


class Deployment:
    """Own a new Docker network and database; never accept an existing database URL."""

    def __init__(self, image, postgres_image):
        self.image = image
        self.postgres_image = postgres_image
        self.name = "hubuum-corpus-" + uuid.uuid4().hex[:12]
        self.containers = []
        self.password = secrets.token_hex(16)
        self.token_key = secrets.token_hex(32)
        self.api_container = None

    def docker(self, *args, check=True, timeout=300):
        result = subprocess.run(
            ["docker", *map(str, args)], capture_output=True, text=True, timeout=timeout,
        )
        if check and result.returncode:
            diagnostic = result.stderr[-2000:].replace(self.password, "[redacted]")
            diagnostic = diagnostic.replace(self.token_key, "[redacted]")
            raise RuntimeError(f"Docker {args[0]} failed ({result.returncode}): {diagnostic}")
        return result

    def __enter__(self):
        # Fail before allocating resources if the selected application image is absent.
        self.docker("image", "inspect", self.image)
        try:
            self.docker("network", "create", self.name)
            name = self.name + "-db"
            self.containers.append(name)
            self.docker(
                "run", "--detach", "--name", name, "--network", self.name,
                "--network-alias", "postgres", "--env", "POSTGRES_USER=corpus",
                "--env", f"POSTGRES_PASSWORD={self.password}", "--env", "POSTGRES_DB=source",
                self.postgres_image,
            )
            wait_for("PostgreSQL", lambda: self.docker(
                "exec", name, "pg_isready", "-h", "127.0.0.1", "-U", "corpus",
                "-d", "source", check=False,
            ).returncode == 0)
            return self
        except BaseException:
            self.__exit__(*sys.exc_info())
            raise

    def __exit__(self, *_):
        for container in reversed(self.containers):
            self.docker("rm", "--force", "--volumes", container, check=False)
        self.docker("network", "rm", self.name, check=False)

    def url(self, database):
        return f"postgres://corpus:{self.password}@postgres/{database}?sslmode=disable"

    def environment(self, database):
        values = {
            "HUBUUM_DATABASE_URL": self.url(database),
            "HUBUUM_DATABASE_ROLE_MODE": "single",
            "HUBUUM_BIND_IP": "0.0.0.0",
            "HUBUUM_CLIENT_ALLOWLIST": "*",
            "HUBUUM_LOG_LEVEL": "warn",
            "HUBUUM_TOKEN_HASH_KEY": self.token_key,
            "HUBUUM_EVENT_RETENTION_PURGE_ENABLED": "false",
        }
        return [item for key, value in values.items() for item in ("--env", f"{key}={value}")]

    def admin(self, database, *args, mount=None):
        mounts = [] if mount is None else [
            "--mount", f"type=bind,source={mount.resolve()},target=/corpus,readonly",
        ]
        name = self.name + "-admin-" + uuid.uuid4().hex[:6]
        self.containers.append(name)
        return self.docker(
            "run", "--rm", "--name", name, "--network", self.name, *self.environment(database), *mounts,
            "--entrypoint", "hubuum-admin", self.image, *args,
        ).stdout

    def create_database(self, database):
        self.docker("exec", self.name + "-db", "createdb", "-U", "corpus", database)

    def start_api(self, database):
        require(self.api_container is None, "Stop the previous application before starting another")
        name = self.name + "-" + database
        self.containers.append(name)
        self.api_container = name
        self.docker(
            "run", "--detach", "--name", name, "--network", self.name,
            "--publish", "127.0.0.1::8080", *self.environment(database), self.image,
        )
        address = self.docker("port", name, "8080/tcp").stdout.strip()
        api = Api("http://" + address)

        def ready():
            try:
                api.get("/readyz")
                return True
            except (OSError, ValueError):
                return False

        wait_for("application readiness", ready)
        return api

    def stop_api(self):
        if self.api_container:
            self.docker("stop", self.api_container)
            self.api_container = None

    def login(self, api, database, username):
        output = self.admin(database, "--reset-password", username)
        match = re.search(r"reset to: (\S+)", output)
        require(match is not None, "Administrator command did not return a reset password")
        login = Api(api.base)
        login.token = login.post("/api/v0/auth/login", {
            "name": username, "password": match.group(1),
        })["token"]
        return login

    def backup(self, database, destination):
        # Copy the admin-owned file out of its container; no writable host mounts.
        name = self.name + "-backup-" + uuid.uuid4().hex[:6]
        self.containers.append(name)
        self.docker(
            "create", "--name", name, "--network", self.name, *self.environment(database),
            "--entrypoint", "hubuum-admin", self.image, "--backup", "/tmp/backup.json",
        )
        self.docker("start", "--attach", name)
        status = self.docker("inspect", "--format", "{{.State.ExitCode}}", name).stdout.strip()
        require(status == "0", "Backup command failed")
        self.docker("cp", name + ":/tmp/backup.json", destination)
        # Whitespace-only formatting leaves the production backup content intact.
        write_json(destination, read_json(destination))
        destination.chmod(0o644)

    def restore_verified(self, artifact, database):
        self.create_database(database)
        with tempfile.TemporaryDirectory(prefix="hubuum-corpus-verify-") as temporary:
            mount = Path(temporary)
            mount.chmod(0o755)
            shutil.copyfile(artifact, mount / BACKUP)
            (mount / BACKUP).chmod(0o644)
            offline = json.loads(self.admin(
                "source", "--verify-backup", "/corpus/" + BACKUP, "--json", mount=mount,
            ))
            require(offline["result"] == "passed", "Offline verification failed")
            report = json.loads(self.admin(
                "source", "--verify-backup", "/corpus/" + BACKUP,
                "--restore-test-database-url", self.url(database),
                "--keep-restore-test-database", "--json", mount=mount,
            ))
            require(report["result"] == "passed", "Isolated restore verification failed")
            return report


def seed(deployment, recipe):
    deployment.admin("source", "--migrate")
    api = deployment.start_api("source")
    api = deployment.login(api, "source", "admin")
    groups = {g["groupname"]: g["id"] for g in api.pages("/api/v1/iam/groups")}
    for role in ("editors", "readers", "outsiders"):
        name = "corpus-" + role
        groups[name] = api.post("/api/v1/iam/groups", {
            "groupname": name, "description": "Comprehensive corpus " + role,
        })["id"]
    for username, group in (
        ("corpus-admin", "admin"), ("corpus-editor", "corpus-editors"),
        ("corpus-reader", "corpus-readers"), ("corpus-outsider", "corpus-outsiders"),
    ):
        user = api.post("/api/v1/iam/users", {
            "name": username, "password": secrets.token_urlsafe(24),
            "proper_name": username.replace("-", " ").title(), "email": username + "@example.invalid",
        })
        api.post(f"/api/v1/iam/groups/{groups[group]}/members/{user['id']}")
    collections = {}
    for name, parent in (("inventory", None), ("operations", "inventory"),
                         ("branch", "operations"), ("restricted", "inventory")):
        body = {"name": "corpus-" + name, "description": "Synthetic " + name,
                "group_id": groups["admin"]}
        if parent:
            body["parent_collection_id"] = collections[parent]
        collections[name] = api.post("/api/v1/collections", body)["id"]
    for role, permissions in (
        ("readers", READ_PERMISSIONS),
        ("editors", READ_PERMISSIONS + ["CreateObject", "UpdateObject", "DeleteObject"]),
    ):
        api.post(
            f"/api/v1/collections/{collections['operations']}/permissions/group/{groups['corpus-' + role]}",
            permissions,
        )
    classes = {}
    objects = {}
    for spec in recipe["classes"]:
        print(f"Seeding {spec['name']}: {spec['objects']} objects", flush=True)
        class_id = api.post("/api/v1/classes", {
            "name": spec["name"], "description": "Comprehensive corpus: " + spec["policy"],
            "collection_id": collections[spec["collection"]],
            "json_schema": None if spec["policy"] == "absent" else recipe["schema"],
            "validate_schema": spec["policy"] == "enforced",
        })["id"]
        classes[spec["name"]] = class_id
        objects[spec["name"]] = []
        for number in range(1, spec["objects"] + 1):
            created = api.post(f"/api/v1/classes/{class_id}/", {
                "name": object_name(spec, number), "description": "Synthetic inventory example",
                "data": object_data(spec, number),
            })
            objects[spec["name"]].append(created["id"])
    relations = []
    for left, right in (
        ("enforced-servers", "enforced-switches"),
        ("enforced-servers", "untyped-locations"),
        ("enforced-servers", "enforced-applications"),
    ):
        relation = api.post("/api/v1/relations/classes", {
            "from_hubuum_class_id": classes[left], "to_hubuum_class_id": classes[right],
            "forward_template_alias": right.replace("-", "_"),
            "reverse_template_alias": left.replace("-", "_"),
        })
        for from_id, to_id in zip(objects[left], objects[right]):
            relations.append(api.post("/api/v1/relations/objects", {
                "from_hubuum_object_id": from_id, "to_hubuum_object_id": to_id,
                "class_relation_id": relation["id"],
            })["id"])
    server = classes["enforced-servers"]
    updated = objects["enforced-servers"][0]
    for status in ("retired", "active"):
        path = f"/api/v1/classes/{server}/{updated}"
        data = api.get(path)["data"]
        data["status"] = status
        api.change("PATCH", path, {"data": data})
    deleted = api.post(f"/api/v1/classes/{server}/", {
        "name": "retired-server-deleted", "description": "Retained deletion example",
        "data": object_data(recipe["classes"][8], 9999),
    })["id"]
    api.change("DELETE", f"/api/v1/classes/{server}/{deleted}")

    schema = copy.deepcopy(recipe["schema"])
    schema["properties"]["capacity"]["maximum"] = 100000
    revision = api.post(f"/api/v1/classes/{server}/schema/revisions", {
        "json_schema": schema, "validate_schema": True,
    })["revision"]
    api.post(f"/api/v1/classes/{server}/schema/revisions/{revision}/activate", {
        "expected_active_revision": 1, "policy": "allow_pending",
    })
    staged = copy.deepcopy(schema)
    staged["required"].append("owner")
    api.post(f"/api/v1/classes/{server}/schema/revisions", {
        "json_schema": staged, "validate_schema": True,
    })
    for name in recipe["computed"]["classes"]:
        for definition in recipe["computed"]["shared"]:
            api.post(f"/api/v1/classes/{classes[name]}/computed-fields", definition)
    personal = recipe["computed"]["personal"]
    reader = deployment.login(api, "source", personal["owner"])
    reader.post("/api/v1/iam/me/computed-fields", {
        "class_id": classes[personal["class"]], **personal["definition"],
    })
    wait_for("schema evidence", lambda: api.get(f"/api/v1/classes/{server}/schema")["counts"] == {
        "valid": 100, "invalid": 0, "pending": 0, "not_required": 0,
    })
    object_ids = {
        spec["name"]: {object_name(spec, n): object_id
                       for n, object_id in enumerate(objects[spec["name"]], 1)}
        for spec in recipe["classes"]
    }
    verify_computed_examples(api, classes, object_ids, recipe)
    deployment.stop_api()
    return {"classes": classes, "collections": collections,
            "updated_object": updated, "deleted_object": deleted,
            "visible_relation": relations[0], "restricted_relation": relations[200]}


def wait_computed(api, class_id, object_id, keys):
    path = f"/api/v1/classes/{class_id}/{object_id}?include=computed"

    def ready():
        obj = api.get(path)
        shared = obj.get("computed", {}).get("shared", {})
        return (shared.get("materialization_stale") is False
                and set(shared.get("values", {})) == keys)
    wait_for("computed-field rebuilding", ready)
    return api.get(path)["computed"]["shared"]


def verify_computed_examples(api, classes, object_ids, recipe):
    specs = {spec["name"]: spec for spec in recipe["classes"]}
    keys = {definition["key"] for definition in recipe["computed"]["shared"]}
    for example in recipe["computed"]["examples"]:
        name = example["class"]
        object_key = object_name(specs[name], example["number"])
        shared = wait_computed(api, classes[name], object_ids[name][object_key], keys)
        for key, expected in example["values"].items():
            actual = shared["values"][key]
            require(actual == expected and (isinstance(actual, bool) == isinstance(expected, bool)),
                    f"Computed value drift: {object_key}.{key}: expected {expected!r}, got {actual!r}")
        require({key: error["code"] for key, error in shared["errors"].items()} == example["errors"],
                f"Computed errors drift: {object_key}: {shared['errors']}")


def inspect_corpus(directory, recipe):
    artifact = directory / BACKUP
    manifest = read_json(directory / MANIFEST)
    require(manifest["name"] == recipe["name"] and manifest["description"] == recipe["description"],
            "Corpus identity metadata mismatch")
    require(artifact.stat().st_size <= MAX_BYTES, "Corpus exceeds the 25 MiB limit")
    require(manifest["sha256"] == digest(artifact), "Corpus checksum mismatch")
    require(manifest["byte_size"] == artifact.stat().st_size, "Corpus size metadata mismatch")
    require(manifest["recipe_revision"] == recipe["revision"], "Corpus recipe revision mismatch")
    document = read_json(artifact)
    require(document["history"] is not None, "Corpus must retain history")
    require(manifest["backup_version"] == document["backup_version"]
            and manifest["source_version"] == document["source_version"], "Backup metadata mismatch")
    state = document["state"]["sections"]
    require(len(state["objects"]) == 3000 and len(state["classes"]) == 12,
            "Corpus must contain exactly 3,000 live objects and twelve classes")
    counts = Counter(row["class_id"] for row in state["objects"])
    classes = {row["name"]: row for row in state["classes"]}
    for spec in recipe["classes"]:
        cls = classes[spec["name"]]
        require(counts[cls["id"]] == spec["objects"], f"Object count drift: {spec['name']}")
        require((cls["json_schema"] is not None) == (spec["policy"] != "absent")
                and cls["validate_schema"] == (spec["policy"] == "enforced"),
                f"Schema policy drift: {spec['name']}")
        expected_schema = None if spec["policy"] == "absent" else copy.deepcopy(recipe["schema"])
        if spec["name"] == "enforced-servers":
            expected_schema["properties"]["capacity"]["maximum"] = 100000
        require(cls["json_schema"] == expected_schema, f"Schema document drift: {spec['name']}")
        require(manifest["anchors"]["classes"][spec["name"]] == cls["id"], "Class anchor drift")
        require(cls["collection_id"] == manifest["anchors"]["collections"][spec["collection"]],
                f"Class collection drift: {spec['name']}")
        expected_names = {object_name(spec, n) for n in range(1, spec["objects"] + 1)}
        by_name = {row["name"]: row for row in state["objects"] if row["class_id"] == cls["id"]}
        require(set(by_name) == expected_names, f"Object name drift: {spec['name']}")
        for number in range(1, spec["objects"] + 1):
            expected_data = object_data(spec, number)
            if spec["name"] == "enforced-servers" and number == 1:
                expected_data["status"] = "active"
            require(by_name[object_name(spec, number)]["data"] == expected_data,
                    f"Object payload drift: {object_name(spec, number)}")
    actual_counts = {name: len(rows) for name, rows in state.items()}
    require(manifest["state_counts"] == actual_counts, "State section counts drifted")
    anchors = manifest["anchors"]
    for name, collection_id in anchors["collections"].items():
        require(any(row["id"] == collection_id and row["name"] == "corpus-" + name
                    for row in state["collections"]), "Collection anchor drift")
    live_ids = {row["id"] for row in state["objects"]}
    require(anchors["updated_object"] in live_ids and anchors["deleted_object"] not in live_ids,
            "History object anchor drift")
    relation_ids = {row["id"] for row in state["object_relations"]}
    require(anchors["visible_relation"] in relation_ids and anchors["restricted_relation"] in relation_ids,
            "Relation anchor drift")
    require({"corpus-admin", "corpus-editor", "corpus-reader", "corpus-outsider"}
            <= {row["name"] for row in state["principals"]}, "Named test accounts are missing")
    personal = recipe["computed"]["personal"]
    owner_id = next(row["id"] for row in state["principals"] if row["name"] == personal["owner"])
    expected_definitions = [
        {**definition, "class_id": classes[name]["id"], "visibility": "shared",
         "owner_principal_id": None, "semantics_version": 1}
        for name in recipe["computed"]["classes"] for definition in recipe["computed"]["shared"]
    ] + [{**personal["definition"], "class_id": classes[personal["class"]]["id"],
          "visibility": "personal", "owner_principal_id": owner_id, "semantics_version": 1}]
    actual_definitions = state["computed_field_definitions"]
    require(len(actual_definitions) == len(expected_definitions)
            and all(sum(all(row.get(key) == value for key, value in expected.items())
                        for row in actual_definitions) == 1 for expected in expected_definitions),
            "Computed definition or ownership drift")
    require(all(not row.get("enabled") for section in ("event_sinks", "event_subscriptions", "remote_targets")
                for row in state[section]), "Corpus contains enabled external integrations")
    return document, manifest


def verify_scenarios(deployment, api, manifest, recipe):
    admin = deployment.login(api, "recovery", "corpus-admin")
    anchors = manifest["anchors"]
    classes = anchors["classes"]
    object_ids = {}
    for spec in recipe["classes"]:
        rows = admin.pages(f"/api/v1/classes/{classes[spec['name']]}/", limit=100)
        require(len(rows) == spec["objects"] and len({r["id"] for r in rows}) == len(rows),
                f"Restored pagination mismatch: {spec['name']}")
        object_ids[spec["name"]] = {row["name"]: row["id"] for row in rows}

    server = classes["enforced-servers"]
    obj = anchors["updated_object"]
    object_path = f"/api/v1/classes/{server}/{obj}"
    wait_for("restored schema evidence", lambda: admin.get(f"/api/v1/classes/{server}/schema")["counts"] == {
        "valid": 100, "invalid": 0, "pending": 0, "not_required": 0,
    })
    verify_computed_examples(admin, classes, object_ids, recipe)
    filtered = admin.get(f"/api/v1/classes/{server}/?include=computed&limit=10"
                         "&computed.shared.monthly_cost__gte=40&sort=computed.shared.monthly_cost")
    costs = [row["computed"]["shared"]["values"]["monthly_cost"] for row in filtered]
    require(len(costs) == 10 and all(cost >= 40 for cost in costs) and costs == sorted(costs),
            "Computed numeric filtering or sorting failed after restore")
    history = admin.pages(object_path + "/history")
    require(len(history) >= 3, "Updated object history is missing")
    deleted_path = f"/api/v1/classes/{server}/{anchors['deleted_object']}"
    admin.request("GET", deleted_path, expected=(404,))
    require(len(admin.pages(deleted_path + "/history")) >= 2, "Deleted object history is missing")
    revisions = admin.get(f"/api/v1/classes/{server}/schema/revisions")
    require({r["status"] for r in revisions} >= {"retired", "active", "staged"},
            "Retained schema lifecycle examples are missing")
    admin.get(f"/api/v1/relations/objects/{anchors['visible_relation']}")
    admin.get(f"/api/v1/relations/objects/{anchors['restricted_relation']}")

    reader = deployment.login(api, "recovery", "corpus-reader")
    editor = deployment.login(api, "recovery", "corpus-editor")
    outsider = deployment.login(api, "recovery", "corpus-outsider")
    personal_key = recipe["computed"]["personal"]["definition"]["key"]
    for actor in (admin, reader, editor):
        enriched = actor.get(object_path + "?include=computed")
        expected = {personal_key: enriched["data"]["site"]} if actor is reader else {}
        require(enriched["computed"]["personal"] == {"values": expected, "errors": {}},
                "Personal definition was lost or exposed to another account after restore")
    for actor in (reader, editor):
        actor.get(object_path)
        actor.get(f"/api/v1/classes/{classes['enforced-services']}/")
        require(actor.get(f"/api/v1/classes/{classes['enforced-applications']}/") == [],
                "Restricted object list leaked rows")
        actor.request("GET", f"/api/v1/classes/{classes['enforced-applications']}", expected=(403, 404))
        actor.request("GET", f"/api/v1/relations/objects/{anchors['restricted_relation']}", expected=(403, 404))
        visible = actor.pages("/api/v1/classes")
        require({c["name"] for c in visible} == {
            s["name"] for s in recipe["classes"] if s["collection"] != "restricted"
        }, "Permission-filtered class list leaked or omitted classes")
    outsider.request("GET", object_path, expected=(403, 404))
    before = admin.get(object_path)
    reader.change("PATCH", object_path, {"description": "must be rejected"}, expected=(403,))
    require(admin.get(object_path) == before, "Reader mutation changed the object")
    editor.change("PATCH", object_path, {"description": "Edited during restore verification"})
    require(admin.get(object_path)["description"] == "Edited during restore verification",
            "Editor mutation was not applied")

    invalid = {"hostname": 42, "status": "unknown"}
    before = admin.get(object_path)
    admin.change("PATCH", object_path, {"data": invalid}, expected=(406,))
    require(admin.get(object_path) == before, "Rejected enforced write changed the object")
    for name in ("advisory-servers", "untyped-notes"):
        class_id = classes[name]
        row = admin.get(f"/api/v1/classes/{class_id}/?limit=1")[0]
        path = f"/api/v1/classes/{class_id}/{row['id']}"
        admin.change("PATCH", path, {"data": invalid})
        require(admin.get(path)["data"] == invalid, f"Unenforced write rejected: {name}")
    class_path = f"/api/v1/classes/{classes['untyped-empty']}"
    before = admin.get(class_path)
    admin.change("PATCH", class_path, {"validate_schema": True}, expected=(400,))
    require(admin.get(class_path) == before, "Schema-free validation request changed the class")


def verify(directory, recipe, image, postgres_image):
    _, manifest = inspect_corpus(directory, recipe)
    print("Restoring and checking the supplied corpus", flush=True)
    with Deployment(image, postgres_image) as deployment:
        deployment.restore_verified(directory / BACKUP, "recovery")
        api = deployment.start_api("recovery")
        verify_scenarios(deployment, api, manifest, recipe)
        deployment.stop_api()
        with tempfile.TemporaryDirectory(prefix="hubuum-corpus-roundtrip-") as temporary:
            subsequent = Path(temporary) / BACKUP
            deployment.backup("recovery", subsequent)
            deployment.restore_verified(subsequent, "second_recovery")
    print("Corpus restore, application scenarios and subsequent backup/restore passed", flush=True)


def generate(destination, recipe_path, image, postgres_image):
    recipe = load_recipe(recipe_path)
    with tempfile.TemporaryDirectory(prefix="hubuum-corpus-generate-") as temporary:
        directory = Path(temporary)
        with Deployment(image, postgres_image) as deployment:
            anchors = seed(deployment, recipe)
            deployment.backup("source", directory / BACKUP)
        document = read_json(directory / BACKUP)
        manifest = {
            "name": recipe["name"], "description": recipe["description"],
            "recipe_revision": recipe["revision"], "recipe_sha256": digest(recipe_path),
            "backup_version": document["backup_version"], "source_version": document["source_version"],
            "sha256": digest(directory / BACKUP), "byte_size": (directory / BACKUP).stat().st_size,
            "state_counts": {name: len(rows) for name, rows in document["state"]["sections"].items()},
            "anchors": anchors,
        }
        write_json(directory / MANIFEST, manifest)
        print(f"Generated {manifest['byte_size']:,} bytes; starting restore rehearsal", flush=True)
        verify(directory, recipe, image, postgres_image)
        # Publish only after the generated bytes pass the full restore rehearsal.
        destination.mkdir(parents=True, exist_ok=True)
        for name in (BACKUP, MANIFEST):
            pending = destination / ("." + name + "." + uuid.uuid4().hex + ".tmp")
            try:
                shutil.copyfile(directory / name, pending)
                # These synthetic public artifacts must be readable by container users.
                pending.chmod(0o644)
                pending.replace(destination / name)
            finally:
                pending.unlink(missing_ok=True)
        print(f"Wrote verified corpus: {destination / BACKUP} ({manifest['byte_size']:,} bytes)")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("command", choices=("check", "verify", "generate"))
    parser.add_argument("--directory", type=Path, default=CORPORA,
                        help="Corpus input directory, or generation destination")
    parser.add_argument("--recipe", type=Path, default=CORPORA / "recipe.json")
    parser.add_argument("--image", default=os.environ.get("HUBUUM_TEST_IMAGE", "hubuum-server:verify"))
    parser.add_argument("--postgres-image", default=POSTGRES_IMAGE)
    args = parser.parse_args()
    recipe = load_recipe(args.recipe)
    if args.command == "generate":
        generate(args.directory, args.recipe, args.image, args.postgres_image)
    else:
        manifest = read_json(args.directory / MANIFEST)
        require(manifest["recipe_sha256"] == digest(args.recipe), "Recipe changed; regenerate the corpus")
        if args.command == "verify":
            verify(args.directory, recipe, args.image, args.postgres_image)
        else:
            inspect_corpus(args.directory, recipe)
            print("Corpus metadata, counts, schema policies and anchors passed")


if __name__ == "__main__":
    try:
        main()
    except (ValueError, OSError, RuntimeError, subprocess.TimeoutExpired) as error:
        sys.exit(str(error))
