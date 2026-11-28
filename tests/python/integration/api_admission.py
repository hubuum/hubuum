"""Check shared expensive-request occupancy across two packaged API replicas."""

import argparse
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
from http.client import HTTPConnection
from pathlib import Path
import secrets
import subprocess
import tempfile
from urllib.parse import urlsplit

from integration.corpus import Api, Deployment, POSTGRES_IMAGE, require, wait_for


CAPACITY = "/api/v1/admin/api-request-capacity"


class AdmissionDeployment(Deployment):
    def environment(self, database):
        return [*super().environment(database), "--env", "HUBUUM_RUNTIME_ROLE=api"]

    def psql(self):
        return ["exec", "-i", self.name + "-db", "psql", "-XqAt",
                "-v", "ON_ERROR_STOP=1", "-U", "corpus", "-d", "source"]

    def scalar(self, sql):
        return self.docker(*self.psql(), "-c", sql, timeout=10).stdout.strip()

    @contextmanager
    def hold_history(self):
        # Pause a regular history read on a table in this disposable database.
        with subprocess.Popen(["docker", *self.psql()], stdin=subprocess.PIPE,
                              stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True) as session:
            try:
                session.stdin.write(
                    "SET application_name = 'admission_fixture';\n"
                    "SET idle_in_transaction_session_timeout = '30s';\n"
                    "BEGIN;\nLOCK TABLE public.collections_history IN ACCESS EXCLUSIVE MODE;\n"
                )
                session.stdin.flush()
                wait_for("fixture transaction", lambda: self.scalar(
                    "SELECT EXISTS (SELECT FROM pg_locks l JOIN pg_stat_activity a USING (pid) "
                    "WHERE l.relation = 'public.collections_history'::regclass AND l.granted "
                    "AND l.mode = 'AccessExclusiveLock' AND a.application_name = 'admission_fixture')"
                ) == "t", timeout=10)
                yield
            finally:
                try:
                    _, error = session.communicate("ROLLBACK;\n", timeout=10)
                except subprocess.TimeoutExpired:
                    session.kill()
                    session.communicate()
                    raise
                require(session.returncode == 0, f"Closing fixture transaction failed: {error.strip()}")

    def first_request_is_waiting(self):
        return self.scalar(
            "SELECT (SELECT count(*) FROM hubuum_control.api_request_owners) = 1 AND EXISTS "
            "(SELECT FROM pg_locks w JOIN pg_stat_activity holder "
            "ON holder.pid = ANY(pg_blocking_pids(w.pid)) WHERE NOT w.granted "
            "AND w.relation = 'public.collections_history'::regclass "
            "AND holder.application_name = 'admission_fixture')"
        ) == "t"

    def wait_for_settlement(self):
        wait_for("request settlement", lambda: self.scalar(
            "SELECT count(*) FROM hubuum_control.api_request_owners"
        ) == "0", timeout=10)


def verify(deployment, backup_directory):
    deployment.admin("source", "--migrate")
    first = deployment.start_api("source")
    second = deployment.start_replica("source", "replica-two")
    admin = deployment.login(first, "source", "admin")
    same_admin = Api(second.base, admin.token)
    username, password = "capacity-reader", secrets.token_urlsafe(24)
    user = admin.create_user({"name": username, "password": password,
                              "email": f"{username}@example.invalid", "proper_name": username})
    group = admin.post("/api/v1/iam/groups", {"groupname": "capacity-readers", "description": ""})
    admin.post(f"/api/v1/iam/groups/{group['id']}/members/{user['id']}")
    collection = admin.post("/api/v1/collections", {
        "name": "capacity-collection", "group_id": group["id"], "description": "",
    })
    login = second.post("/api/v1/auth/login", {"client_id": "capacity-test",
                        "name": username, "password": password}, expected=(200,))
    reader = Api(second.base, login["access_token"])
    history = f"/api/v1/collections/{collection['id']}/history?limit=1"
    expected = reader.get(history)
    require(len(expected) == 1 and expected[0]["id"] == collection["id"], "Missing history fixture")

    deployment.backup("source", backup_directory / "before-policy.json")
    current = admin.get(CAPACITY)
    updated, _ = admin.request("PUT", CAPACITY, {
        "expected_revision": current["revision"],
        "policy": {**current["policy"], "deployment": 1, "principal": 1, "duration": 30000},
    })
    require(same_admin.get(CAPACITY) == updated, "Replicas disagree on the committed policy")

    with ThreadPoolExecutor(max_workers=1) as executor:
        with deployment.hold_history():
            admitted = executor.submit(admin.get, history)
            wait_for("admitted read at the fixture barrier", deployment.first_request_is_waiting, timeout=10)
            for caller, reason in ((same_admin, "api_capacity_principal"), (reader, "api_capacity_deployment")):
                body, _ = caller.request("GET", history, expected=(429,))
                require(body["reason"] == reason and body["retryable"] is True, f"Expected {reason}")
            second.get("/readyz")
            require(reader.get(f"/api/v1/collections/{collection['id']}")["id"] == collection["id"],
                    "An ordinary collection read could not complete while the lane was occupied")
        require(admitted.result(timeout=10) == expected, "The admitted history read lost its result")
    deployment.wait_for_settlement()
    require(reader.get(history) == expected, "The second replica could not use the released slot")
    verify_disconnect(deployment, admin, reader, history, expected)
    verify_crash_recovery(deployment, admin, reader, history, expected)
    verify_logical_restore(deployment, backup_directory, updated, history, expected)


def verify_disconnect(deployment, admin, reader, history, expected):
    deployment.wait_for_settlement()
    client = HTTPConnection(urlsplit(admin.base).netloc, timeout=10)
    with deployment.hold_history():
        try:
            client.request("GET", history, headers={"Authorization": f"Bearer {admin.token}"})
            wait_for("client read at the fixture barrier", deployment.first_request_is_waiting, timeout=10)
        finally:
            client.close()
    deployment.wait_for_settlement()
    require(reader.get(history) == expected, "A disconnected request prevented a later read")


def verify_crash_recovery(deployment, admin, reader, history, expected):
    charge = "SELECT admitted_attempts FROM hubuum_control.api_request_policy"
    deployment.wait_for_settlement()
    before = int(deployment.scalar(charge))
    with ThreadPoolExecutor(max_workers=1) as executor:
        with deployment.hold_history():
            pending = executor.submit(admin.get, history)
            wait_for("first replica to own the read", deployment.first_request_is_waiting, timeout=10)
            deployment.docker("kill", deployment.api_container)
        try:
            pending.result(timeout=10)
        except ConnectionError:
            pass
        else:
            raise ValueError("The interrupted read unexpectedly returned a result")

    denial, _ = reader.request("GET", history, expected=(429,))
    require(denial["reason"] == "api_capacity_deployment", "A crashed owner lost its reservation")
    deployment.docker("stop", deployment.name + "-replica-two")
    # Follow the operator recovery procedure only after both HTTP producers stop.
    require(deployment.scalar("SELECT count(*) FROM hubuum_control.api_request_owners") == "1",
            "Expected exactly one abandoned owner before recovery")
    deployment.scalar("SELECT public.hubuum_api_request_settle(owner) FROM hubuum_control.api_request_owners")
    require(int(deployment.scalar(charge)) == before + 1, "Recovery reset or duplicated the attempt charge")
    restarted = deployment.start_replica("source", "recovered")
    require(Api(restarted.base, reader.token).get(history) == expected, "Restarted replica lost the history read")


def verify_logical_restore(deployment, backup_directory, policy, history, expected):
    deployment.wait_for_settlement()
    deployment.docker("stop", deployment.name + "-recovered")

    def accounting():
        row = deployment.scalar(
            "SELECT admitted_attempts, charged_milliseconds FROM hubuum_control.api_request_policy"
        )
        return tuple(map(int, row.split("|")))

    before = accounting()
    image = "SELECT incarnation FROM hubuum_control.application_control WHERE id = 1"
    previous_image = deployment.scalar(image)
    deployment.admin("source", "--restore", "/corpus/before-policy.json",
                     "--restore-confirmation", "REPLACE ALL HUBUUM DATA", mount=backup_directory)
    require(deployment.scalar(image) != previous_image, "Restore did not replace the application incarnation")
    require(accounting() == before, "Logical restore changed physical request charges")

    restored = deployment.start_replica("source", "after-restore")
    admin = deployment.login(restored, "source", "admin")
    require(admin.get(CAPACITY) == policy, "Restoring an older backup rolled back the shared policy")
    require(admin.get(history) == expected, "The restored collection history differs from the original")
    deployment.wait_for_settlement()
    require(accounting() == (before[0] + 1, before[1] + policy["policy"]["duration"]),
            "The first read after restore did not charge the retained policy exactly once")


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--image", default="hubuum-server:verify")
    parser.add_argument("--postgres-image", default=POSTGRES_IMAGE)
    args = parser.parse_args(argv)
    with tempfile.TemporaryDirectory(prefix="hubuum-admission-") as temporary:
        backup_directory = Path(temporary) / "backup"
        backup_directory.mkdir()
        backup_directory.chmod(0o755)
        with AdmissionDeployment(args.image, args.postgres_image) as deployment:
            verify(deployment, backup_directory)
    print("Two replicas enforced shared capacity and recovered the abandoned reservation after a replica crash. "
          "Logical restore preserved the newer policy and physical charges.")
