"""Check that a packaged worker can finish a queued export after logout and API restart."""

import argparse
from datetime import datetime, timedelta
import secrets

from integration.corpus import Api, Deployment, POSTGRES_IMAGE, require, wait_for


class ReconnectDeployment(Deployment):
    role = "api"

    def environment(self, database):
        return [*super().environment(database),
                "--env", f"HUBUUM_RUNTIME_ROLE={self.role}",
                "--env", "HUBUUM_EXPORT_OUTPUT_RETENTION_HOURS=48"]

    def run_worker(self):
        self.role = "worker"
        name = self.name + "-worker"
        self.containers.append(name)
        self.docker("run", "--detach", "--name", name, "--network", self.name,
                    *self.environment("source"), self.image)
        self.role = "api"
        return name

    def task_succeeded(self, task_id):
        # Only observe the disposable database, while no API process is running.
        require(type(task_id) is int and task_id > 0, "Invalid task reference")
        status = self.docker(
            "exec", self.name + "-db", "psql", "-XAt", "-U", "corpus", "-d", "source",
            "-c", f"SELECT status FROM tasks WHERE id = {task_id}",
        ).stdout.strip()
        require(status in {"queued", "running", "validating", "succeeded"},
                f"Warm export entered unexpected status {status!r}")
        return status == "succeeded"


def sign_in(origin, name, password):
    reader = Api(origin)
    credentials = reader.post("/api/v1/auth/login", {
        "client_id": "reconnect-test", "name": name, "password": password,
    }, expected=(200,))
    reader.token = credentials["access_token"]
    return reader, credentials["session_id"]


def verify(deployment):
    deployment.admin("source", "--migrate")
    api = deployment.start_api("source")
    admin = deployment.login(api, "source", "admin")
    password = secrets.token_urlsafe(24)
    name = "reconnect-owner"
    user = admin.create_user({"name": name, "password": password,
                              "email": f"{name}@example.invalid", "proper_name": name})
    other_name = "reconnect-outsider"
    other_password = secrets.token_urlsafe(24)
    admin.create_user({"name": other_name, "password": other_password,
                       "email": f"{other_name}@example.invalid", "proper_name": other_name})
    group = admin.post("/api/v1/iam/groups", {"groupname": "export-owners",
                                               "description": "Reconnect fixture"})
    admin.post(f"/api/v1/iam/groups/{group['id']}/members/{user['id']}")
    collection = admin.post("/api/v1/collections", {
        "name": "reconnect-collection", "group_id": group["id"], "description": "Retained marker",
    })
    admin.post("/api/v1/auth/logout", {}, expected=(200,))

    owner, first_session = sign_in(api.base, name, password)
    task = owner.post("/api/v1/exports", {"scope": {"kind": "collections"}}, expected=(202,))
    require(task["status"] == "queued", "Export must still be queued before logout")
    owner.post("/api/v1/auth/logout", {}, expected=(200,))
    owner.request("GET", task["links"]["task"], expected=(401,))

    # Both runtime processes serving the next request are created after submission.
    previous = deployment.api_container
    deployment.stop_api()
    deployment.docker("rm", previous)
    worker = deployment.run_worker()
    wait_for("background export completion without a login", lambda: deployment.task_succeeded(task["id"]))
    deployment.docker("stop", worker)
    restarted = deployment.start_api("source")
    reader, new_session = sign_in(restarted.base, name, password)
    require(new_session != first_session, "Expected an independent login")
    discovered = reader.get("/api/v1/tasks?kind=export&status=succeeded&limit=10")
    require(any(item["id"] == task["id"] for item in discovered), "Completed export is missing")
    details = reader.get(task["links"]["task"])
    require(details["id"] == task["id"] and details["status"] == "succeeded", "Wrong durable task")
    finished = datetime.fromisoformat(details["finished_at"])
    readable_until = datetime.fromisoformat(details["details"]["export"]["output_expires_at"])
    require(readable_until - finished > timedelta(hours=24), "Output lacks its next-day window")
    result = reader.get(details["links"]["export_output"])
    require(any(item["id"] == collection["id"] and item["name"] == collection["name"]
                for item in result["items"]), "Export lost its collection across sessions")

    outsider, _ = sign_in(restarted.base, other_name, other_password)
    visible = outsider.get("/api/v1/tasks?kind=export&status=succeeded&limit=10")
    require(all(item["id"] != task["id"] for item in visible), "Another owner discovered the task")
    outsider.request("GET", details["links"]["export_output"], expected=(403, 404))



def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--image", default="hubuum-server:verify")
    parser.add_argument("--postgres-image", default=POSTGRES_IMAGE)
    args = parser.parse_args(argv)
    with ReconnectDeployment(args.image, args.postgres_image) as deployment:
        verify(deployment)
    print("A fresh login discovered and read the export completed while the API was stopped.")
