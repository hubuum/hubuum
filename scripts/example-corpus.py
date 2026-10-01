#!/usr/bin/env python3
"""Maintain the Atlas documentation corpus using the existing isolated harness."""

import sys

if sys.version_info < (3, 11):
    sys.exit(
        "Hubuum tooling requires Python 3.11 or newer; found "
        + sys.version.split()[0]
        + ". Install Python 3.11+ and ensure python3 on PATH selects it."
    )

import argparse
import copy
import importlib.util
import json
from pathlib import Path
import re
import secrets
import shutil
import tempfile
from urllib.parse import quote


ROOT = Path(__file__).resolve().parent.parent
DIRECTORY = ROOT / "docs" / "assets" / "atlas"
IMPORT = "atlas.import.json"
BACKUP = "atlas.backup.json"
MANIFEST = "atlas.manifest.json"
SPEC = importlib.util.spec_from_file_location("corpus", ROOT / "scripts" / "test-corpus.py")
CORPUS = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(CORPUS)
require = CORPUS.require
read_json = CORPUS.read_json
write_json = CORPUS.write_json
digest = CORPUS.digest
DATA_BLOCK = re.compile(
    r"^<!-- atlas-data: (.+?) -->\n\x60{3}json\n(.*?)\n\x60{3}", re.DOTALL | re.MULTILINE,
)


def load_import(directory):
    document = read_json(directory / IMPORT)
    require(document["version"] == 2 and document["dry_run"] is False,
            "Atlas must be an executable import v2 document")
    require(document["mode"] == {
        "atomicity": "strict", "collision_policy": "abort", "permission_policy": "abort",
    }, "Atlas import must abort atomically on collisions")
    graph = document["graph"]
    require(set(graph) == {
        "groups", "collections", "classes", "objects", "computed_fields",
        "class_relations", "object_relations", "collection_permissions",
    }, "Atlas must not contain credentials, memberships or external integrations")
    refs = [row["ref"] for rows in graph.values() for row in rows]
    require(len(refs) == len(set(refs)), "Atlas refs must be unique")
    for rows in graph.values():
        for row in rows:
            for key, value in row.items():
                if key.endswith("_ref"):
                    require(value in refs, f"Unresolved Atlas ref: {value}")
    require({row["name"] for row in graph["classes"]}
            == {"Service", "Server", "Location", "Context"},
            "Atlas documentation expects four named classes")
    require(len(graph["objects"]) == 10 and len(graph["object_relations"]) == 10,
            "Atlas documentation expects ten objects and ten relations")
    return document


def check_documentation(document, docs=ROOT / "docs"):
    """Compare explicitly marked examples with the canonical object data."""
    objects = {row["ref"]: row["data"] for row in document["graph"]["objects"]}
    count = 0
    for path in docs.rglob("*.md"):
        markdown = path.read_text(encoding="utf-8")
        blocks = DATA_BLOCK.findall(markdown)
        require(len(re.findall(r"^<!-- atlas-data:", markdown, re.MULTILINE)) == len(blocks),
                f"Malformed Atlas example marker in {path}")
        for reference, body in blocks:
            require(reference in objects, f"Unknown Atlas object in {path}: {reference}")
            require(json.loads(body) == objects[reference],
                    f"Atlas example drift in {path}: {reference}")
            count += 1
    require(count > 0, "Documentation must demonstrate canonical Atlas object data")


def class_relation_signatures(rows, names, from_key, to_key):
    # The API and backup canonicalize by database ID, not by submitted direction.
    return {
        tuple(sorted((
            (names[row[from_key]], row.get("forward_template_alias"), row.get("from_max_relations")),
            (names[row[to_key]], row.get("reverse_template_alias"), row.get("to_max_relations")),
        )))
        for row in rows
    }


def check_backup(directory, document):
    manifest = read_json(directory / MANIFEST)
    require(manifest["name"] == "Atlas documentation corpus" and manifest["revision"] == 1,
            "Unsupported Atlas manifest")
    require(manifest["counts"] == {name: len(rows) for name, rows in document["graph"].items()},
            "Atlas manifest counts drift")
    for filename in (IMPORT, BACKUP):
        require(manifest["files"][filename] == {
            "sha256": digest(directory / filename),
            "bytes": (directory / filename).stat().st_size,
        }, f"Atlas checksum or byte size mismatch: {filename}")
    backup = read_json(directory / BACKUP)
    require(backup["backup_version"] == manifest["backup_version"]
            and backup["source_version"] == manifest["source_version"],
            "Atlas backup version metadata drift")
    require(backup["history"] is not None, "Atlas backup must retain history")
    sections = backup["state"]["sections"]
    graph = document["graph"]
    classes = {row["id"]: row["name"] for row in sections["classes"]}
    objects = {row["id"]: (classes[row["class_id"]], row["name"])
               for row in sections["objects"]}
    expected_classes = {row["name"]: (row.get("json_schema"), row["validate_schema"])
                        for row in graph["classes"]}
    require({row["name"]: (row["json_schema"], row["validate_schema"])
             for row in sections["classes"]} == expected_classes,
            "Atlas backup class schema drift")
    expected_class_relations = class_relation_signatures(
        graph["class_relations"], {row["ref"]: row["name"] for row in graph["classes"]},
        "from_class_ref", "to_class_ref",
    )
    require(len(sections["class_relations"]) == len(expected_class_relations)
            and class_relation_signatures(sections["class_relations"], classes,
                                          "from_class_id", "to_class_id")
            == expected_class_relations, "Atlas backup class relation drift")
    expected_objects = {(row["class_ref"].removeprefix("class:"), row["name"]):
                        (row["description"], row["data"]) for row in graph["objects"]}
    require({objects[row["id"]]: (row["description"], row["data"])
             for row in sections["objects"]} == expected_objects,
            "Atlas backup object data drift")
    ref_objects = {row["ref"]: (row["class_ref"].removeprefix("class:"), row["name"])
                   for row in graph["objects"]}
    expected_edges = {frozenset((ref_objects[row["from_object_ref"]],
                                ref_objects[row["to_object_ref"]]))
                      for row in graph["object_relations"]}
    require(len(sections["object_relations"]) == len(expected_edges)
            and {frozenset((objects[row["from_object_id"]], objects[row["to_object_id"]]))
                 for row in sections["object_relations"]} == expected_edges,
            "Atlas backup relation drift")
    require(not any(sections[name] for name in (
        "remote_targets", "event_sinks", "event_subscriptions", "service_accounts",
    )), "Atlas backup must not enable external integrations")
    require(len(sections["users"]) == 1,
            "Atlas backup must contain only the generated default administrator")


def object_path(row):
    return ("/api/v1/classes/by-name/" + quote(row["class_ref"].removeprefix("class:"), safe="")
            + "/objects/by-name/" + quote(row["name"], safe=""))


def submit_import(api, document, expected="succeeded"):
    task = api.post("/api/v1/imports", document, expected=(202,))

    def completed():
        status = api.get(f"/api/v1/imports/{task['id']}")["status"]
        if status in {"succeeded", "failed", "cancelled", "partially_succeeded"}:
            require(status == expected,
                    f"Atlas import ended with {status}: "
                    + str(api.get(f"/api/v1/imports/{task['id']}/results"))[:1500])
            return True
        return False

    CORPUS.wait_for("Atlas import completion", completed)


def verify_reads(api, document):
    graph = document["graph"]
    classes = api.pages("/api/v1/classes")
    require({row["name"] for row in classes} == {row["name"] for row in graph["classes"]},
            "Atlas class inventory mismatch")
    expected_class_relations = class_relation_signatures(
        graph["class_relations"], {row["ref"]: row["name"] for row in graph["classes"]},
        "from_class_ref", "to_class_ref",
    )
    class_relations = api.pages("/api/v1/relations/classes")
    require(len(class_relations) == len(expected_class_relations)
            and class_relation_signatures(
                class_relations, {row["id"]: row["name"] for row in classes},
                "from_hubuum_class_id", "to_hubuum_class_id",
            ) == expected_class_relations, "Atlas class relation aliases or limits mismatch")
    actual_objects = {}
    for specification in graph["classes"]:
        name = specification["name"]
        path = "/api/v1/classes/by-name/" + name
        actual = api.get(path)
        require(actual["json_schema"] == specification.get("json_schema")
                and actual["validate_schema"] == specification["validate_schema"],
                f"Schema policy mismatch: {name}")
        # A deliberately small page checks cursor handling in this small corpus.
        rows = api.pages(path + "/objects", limit=1)
        require({row["name"] for row in rows} == {
            row["name"] for row in graph["objects"] if row["class_ref"] == specification["ref"]
        }, f"Atlas object inventory mismatch: {name}")
        actual_objects.update({row["id"]: (name, row["name"]) for row in rows})
    for row in graph["objects"]:
        actual = api.get(object_path(row))
        require(actual["data"] == row["data"] and actual["description"] == row["description"],
                f"Object mismatch: {row['ref']}")
    expected_objects = {row["ref"]: (row["class_ref"].removeprefix("class:"), row["name"])
                        for row in graph["objects"]}
    expected_edges = {frozenset((expected_objects[row["from_object_ref"]],
                                expected_objects[row["to_object_ref"]]))
                      for row in graph["object_relations"]}
    relations = api.pages("/api/v1/relations/objects")
    require(len(relations) == len(expected_edges)
            and {frozenset((actual_objects[row["from_hubuum_object_id"]],
                            actual_objects[row["to_hubuum_object_id"]]))
                 for row in relations} == expected_edges, "Atlas object relations mismatch")
    rows = api.get("/api/v1/classes/by-name/Server/objects?name__startswith=web-&sort=name")
    require([row["name"] for row in rows] == ["web-01", "web-02"],
            "Documented server filter mismatch")
    path = "/api/v1/classes/by-name/Server/objects/by-name/web-01?include=computed"
    CORPUS.wait_for("Atlas computed field", lambda: api.get(path).get("computed", {}).get(
        "shared", {}).get("values", {}).get("monthly_cost") == 50)
    require(api.get(path)["computed"]["shared"]["errors"] == {},
            "Atlas computed field errors")


def verify_writes_and_access(deployment, api, database, document):
    # All mutations run in a harness-owned database, never a caller's deployment.
    server = next(row for row in document["graph"]["objects"] if row["name"] == "web-01")
    path = object_path(server)
    before = api.get(path)
    invalid = copy.deepcopy(server["data"])
    invalid["hostname"] = 42
    api.change("PATCH", path, {"data": invalid}, expected=(406,))
    require(api.get(path) == before, "Rejected schema write changed the server")
    notes = next(row for row in document["graph"]["objects"] if row["name"] == "Research notes")
    notes_path = object_path(notes)
    api.change("PATCH", notes_path, {"data": ["schema-free", {"new_shape": True}]})
    require(api.get(notes_path)["data"] == ["schema-free", {"new_shape": True}],
            "Schema-free class rejected a new data shape")
    api.change("PATCH", notes_path, {"data": notes["data"]})

    groups = {row["groupname"]: row["id"] for row in api.pages("/api/v1/iam/groups")}
    for role in ("readers", "operators"):
        username = "atlas-test-" + role
        user = api.create_user({"name": username, "password": secrets.token_urlsafe(24)})
        api.post(f"/api/v1/iam/groups/{groups['atlas-' + role]}/members/{user['id']}")
        actor = deployment.login(api, database, username)
        actor.get(path)
        if role == "readers":
            actor.get(notes_path)
            actor.change("PATCH", path, {"description": "forbidden"}, expected=(403,))
            require(api.get(path) == before, "Reader changed the server")
        else:
            actor.request("GET", notes_path, expected=(403, 404))
            actor.change("PATCH", path, {"description": "Operator verification"})
            require(api.get(path)["description"] == "Operator verification",
                    "Operator could not update the operations collection")
            api.change("PATCH", path, {"description": server["description"]})


def verify(directory, image, postgres_image):
    document = load_import(directory)
    check_backup(directory, document)
    with CORPUS.Deployment(image, postgres_image) as deployment:
        deployment.admin("source", "--migrate")
        api = deployment.login(deployment.start_api("source"), "source", "admin")
        dry_run = copy.deepcopy(document)
        dry_run["dry_run"] = True
        submit_import(api, dry_run)
        require(api.pages("/api/v1/classes") == [], "Dry run changed the database")
        submit_import(api, document)
        verify_reads(api, document)
        submit_import(api, document, expected="failed")
        verify_reads(api, document)
        deployment.stop_api()
        deployment.restore_verified(directory / BACKUP, "recovery")
        api = deployment.login(deployment.start_api("recovery"), "recovery", "admin")
        verify_reads(api, document)
        verify_writes_and_access(deployment, api, "recovery", document)
        deployment.stop_api()
        with tempfile.TemporaryDirectory(prefix="hubuum-atlas-roundtrip-") as temporary:
            subsequent = Path(temporary) / BACKUP
            deployment.backup("recovery", subsequent)
            deployment.restore_verified(subsequent, "second_recovery")
            api = deployment.login(deployment.start_api("second_recovery"),
                                   "second_recovery", "admin")
            verify_reads(api, document)
            deployment.stop_api()
    print("Atlas import, collision safety, permissions, schemas and two restores passed")


def generate(destination, source, image, postgres_image):
    document = load_import(source)
    with tempfile.TemporaryDirectory(prefix="hubuum-atlas-generate-") as temporary:
        directory = Path(temporary)
        shutil.copyfile(source / IMPORT, directory / IMPORT)
        with CORPUS.Deployment(image, postgres_image) as deployment:
            deployment.admin("source", "--migrate")
            api = deployment.login(deployment.start_api("source"), "source", "admin")
            submit_import(api, document)
            verify_reads(api, document)
            deployment.stop_api()
            deployment.backup("source", directory / BACKUP)
        backup = read_json(directory / BACKUP)
        write_json(directory / MANIFEST, {
            "name": "Atlas documentation corpus",
            "revision": 1,
            "source_version": backup["source_version"],
            "backup_version": backup["backup_version"],
            "files": {name: {"sha256": digest(directory / name),
                             "bytes": (directory / name).stat().st_size}
                      for name in (IMPORT, BACKUP)},
            "counts": {name: len(rows) for name, rows in document["graph"].items()},
        })
        # Publish artifacts only after the actual restore and application checks pass.
        verify(directory, image, postgres_image)
        destination.mkdir(parents=True, exist_ok=True)
        for name in (IMPORT, BACKUP, MANIFEST):
            shutil.copyfile(directory / name, destination / name)
    print(f"Wrote verified Atlas corpus to {destination}")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("command", choices=("check", "generate", "verify"))
    parser.add_argument("--directory", type=Path, default=DIRECTORY)
    parser.add_argument("--source", type=Path, default=DIRECTORY,
                        help="Canonical import directory for generation")
    parser.add_argument("--image", default="hubuum-server:verify")
    parser.add_argument("--postgres-image", default=CORPUS.POSTGRES_IMAGE)
    args = parser.parse_args()
    if args.command == "generate":
        generate(args.directory, args.source, args.image, args.postgres_image)
    elif args.command == "verify":
        verify(args.directory, args.image, args.postgres_image)
    else:
        document = load_import(args.directory)
        check_backup(args.directory, document)
        check_documentation(document)
        print("Atlas artifacts and documentation examples match")


if __name__ == "__main__":
    try:
        main()
    except (ValueError, OSError, TimeoutError, KeyError) as error:
        sys.exit(str(error))
