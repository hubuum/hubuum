"""Physical recovery and interrupted reapplication in an owned disposable fixture.

Only the fixture's anonymous PostgreSQL volume is replaced. The independent
checkpoint repository and real remote-effect counter remain untouched.
"""

import hashlib
import ipaddress
import json
import os
from pathlib import Path, PurePosixPath
import re
import signal
import subprocess
import tarfile
import time
import uuid


def _wait(description, predicate, timeout=90):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        value = predicate()
        if value:
            return value
        time.sleep(0.1)
    raise TimeoutError(f"Timed out waiting for {description}")


def _write(path, value):
    with path.open("x", encoding="utf-8") as stream:
        os.chmod(path, 0o600)
        json.dump(value, stream, indent=2)
        stream.write("\n")
        stream.flush()
        os.fsync(stream.fileno())


def _owned_path(deployment, path):
    root = deployment.root.resolve(strict=True)
    if root.stat().st_mode & 0o077 or root.stat().st_uid != os.getuid():
        raise ValueError("Physical recovery requires an owner-only fixture directory")
    path = Path(path)
    if path.is_symlink() or path.parent.resolve(strict=True) != root:
        raise ValueError("Recovery artifact must be directly inside the owned fixture directory")
    return root / path.name


def _container(deployment):
    name = deployment.name + "-db"
    if (not re.fullmatch(r"hubuum-corpus-[0-9a-f]{12}", deployment.name)
            or name not in deployment.containers):
        raise ValueError("Physical recovery requires its disposable managed database")
    value = json.loads(deployment.docker("inspect", name).stdout)
    if len(value) != 1:
        raise ValueError("Database container identity is ambiguous")
    return name, value[0]


def _refresh_database_address(deployment):
    # A stopped rootless container can receive a different address on restart.
    # Refresh both consumer endpoints before new runtimes inherit --add-host.
    name, info = _container(deployment)
    network = info.get("NetworkSettings", {}).get("Networks", {}).get(deployment.name)
    if not network:
        raise ValueError("Restored database is outside its managed fixture network")
    address = ipaddress.ip_address(network.get("IPAddress", ""))
    if address.is_unspecified or address.is_multicast:
        raise ValueError("Restored database has no usable network address")
    if deployment.direct_container_network:
        published = deployment.host_endpoint(name, 5432)
    else:
        published = deployment.docker("port", name, "5432/tcp").stdout.strip()
        match = re.fullmatch(r"127\.0\.0\.1:([0-9]{1,5})", published)
        if not match or not 1 <= int(match[1]) <= 65535:
            raise ValueError("Restored database has no single loopback port binding")
    deployment.database_ip = str(address)
    deployment.host_url = f"postgres://corpus:{deployment.password}@{published}/source?sslmode=disable"


def _layout(info):
    candidates = [value.removeprefix("PGDATA=") for value in info["Config"]["Env"]
                  if value.startswith("PGDATA=")]
    if len(candidates) != 1:
        raise ValueError("Managed PostgreSQL has no single PGDATA")
    pgdata = candidates[0]
    path = PurePosixPath(pgdata)
    if (not re.fullmatch(r"/var/lib/postgresql/[A-Za-z0-9_/-]+", pgdata)
            or str(path) != pgdata or ".." in path.parts):
        raise ValueError("PGDATA is outside the managed PostgreSQL tree")
    mounts = [mount for mount in info["Mounts"]
              if PurePosixPath(mount["Destination"]) in path.parents]
    if not mounts:
        raise ValueError("PGDATA must be beneath an anonymous managed volume")
    mount = max(mounts, key=lambda value: len(value["Destination"]))
    if (mount["Type"] != "volume" or not mount.get("RW")
            or not re.fullmatch(r"[0-9a-f]{64}", mount.get("Name", ""))):
        raise ValueError("Refusing to replace a bind mount or a named external volume")
    return {"pgdata": pgdata, "volume": mount["Name"], "destination": mount["Destination"],
            "image": info["Image"]}


def _volume_security(info):
    label = info.get("ProcessLabel", "")
    if not label:
        return []
    fields = label.split(":", 3)
    if len(fields) != 4 or not re.fullmatch(r"s0(?::c[0-9]+(?:,c[0-9]+)*)?", fields[3]):
        raise ValueError("Managed PostgreSQL has an unsupported SELinux process level")
    # Keep SELinux enabled and share only this stopped fixture's volume category.
    return ["--security-opt", "label=level:" + fields[3]]


def _sql(deployment, query):
    name, _ = _container(deployment)
    return deployment.docker("exec", name, "psql", "-X", "-qAt", "-v", "ON_ERROR_STOP=1",
                             "-U", "corpus", "-d", "source", "-c", query).stdout.strip()


def _digest(path):
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def _validate_archive(path):
    # This fixture has no tablespaces or WAL symlinks. Refuse links and special
    # files instead of allowing a restore archive to escape the owned directory.
    if not 0 < path.stat().st_size <= 64 * 1024**3:
        raise ValueError("Physical backup has an invalid bounded size")
    required = {"PG_VERSION", "global/pg_control", "backup_label", "backup_manifest"}
    count = 0
    with tarfile.open(path, "r|") as archive:
        for member in archive:
            count += 1
            name = PurePosixPath(member.name)
            if (count > 100000 or name.is_absolute() or ".." in name.parts
                    or not (member.isfile() or member.isdir())):
                raise ValueError("Physical backup contains an unsafe archive entry")
            required.discard(str(name))
    if required:
        raise ValueError("Archive is not a complete PostgreSQL physical base backup")


def capture_primary(deployment, path):
    """Capture pg_basebackup with streamed WAL while the remote task is held."""
    path = _owned_path(deployment, path)
    if path.exists() or path.with_suffix(".json").exists():
        raise ValueError("Physical recovery artifacts already exist")
    name, info = _container(deployment)
    layout = _layout(info)
    if not info["State"]["Running"]:
        raise ValueError("Physical capture requires the live managed primary")
    pgdata = layout["pgdata"]
    ownership = deployment.docker("exec", name, "stat", "-c", "%u:%g", pgdata).stdout.strip()
    if not re.fullmatch(r"[0-9]+:[0-9]+", ownership):
        raise ValueError("Invalid PostgreSQL directory ownership")
    deployment.docker("exec", name, "sh", "-eu", "-c",
                      'test ! -L "$1" && test ! -L "$1/pg_wal" && test "$(cat "$1/PG_VERSION")" = 18',
                      "verify-pgdata", pgdata)
    if _sql(deployment, "SELECT count(*) FROM pg_tablespace WHERE spcname NOT IN ('pg_default','pg_global')") != "0":
        raise ValueError("Physical fixture must not contain external tablespaces")
    system = _sql(deployment, "SELECT system_identifier FROM pg_control_system()")
    suffix = uuid.uuid4().hex
    temporary = "/tmp/retention-base-" + suffix
    archive = temporary + ".tar"
    started = time.monotonic()
    try:
        deployment.docker("exec", name, "pg_basebackup", "-U", "corpus", "-D", temporary,
                          "-Fp", "-Xstream", "--checkpoint=fast", "--no-password", timeout=600)
        deployment.docker("exec", name, "pg_verifybackup", temporary, timeout=600)
        deployment.docker("exec", name, "tar", "-C", temporary, "-cpf", archive, ".", timeout=600)
        deployment.docker("cp", name + ":" + archive, path, timeout=600)
        path.chmod(0o600)
        _validate_archive(path)
        _write(path.with_suffix(".json"), {
            "format": "postgresql-plain-basebackup-streamed-wal", "postgres_major": 18,
            "system_identifier": system, "layout": layout, "ownership": ownership,
            "sha256": _digest(path), "bytes": path.stat().st_size,
            "capture_ms": (time.monotonic() - started) * 1000,
        })
    finally:
        deployment.docker("exec", name, "rm", "-rf", "--", temporary, archive, check=False)


def _admin_command(arguments, deployment, operation):
    return deployment.host_command([
        str(arguments.admin_binary), operation, str(deployment.repository),
        "--control-checkpoint-key", str(deployment.key),
    ])


def _admin_environment(deployment, application_name=None):
    environment = deployment.host_environment()
    url = deployment.host_url
    if application_name is not None:
        if not re.fullmatch(r"retention-reapply-[0-9a-f]{32}", application_name):
            raise ValueError("Invalid owned administrator application name")
        url += ("&" if "?" in url else "?") + "application_name=" + application_name
        environment["PGAPPNAME"] = application_name
    # Privileged commands prefer the migration URL, including in single-role
    # mode. Never let an inherited operator URL escape the disposable fixture.
    environment["HUBUUM_DATABASE_URL"] = url
    environment["HUBUUM_MIGRATION_DATABASE_URL"] = url
    return environment


def _admin(arguments, deployment, operation, log):
    started = time.monotonic()
    with log.open("xb") as stream:
        os.chmod(log, 0o600)
        result = subprocess.run(_admin_command(arguments, deployment, operation),
                                env=_admin_environment(deployment), stdout=stream,
                                stderr=subprocess.STDOUT, timeout=300, check=False)
    if result.returncode:
        raise RuntimeError(f"Physical recovery administrator failed ({result.returncode}); inspect private drill log")
    return (time.monotonic() - started) * 1000


def _interrupted_reapplication(arguments, deployment, root):
    state = json.loads((root / "probe-state.json").read_text(encoding="utf-8"))
    task = state["task_id"]
    if type(task) is not int or not 0 < task <= 2147483647:
        raise ValueError("Invalid seeded task ID")
    identity = str(uuid.UUID(state["task_identity"]))
    exists = ("SELECT count(*) FROM public.remote_call_results r JOIN public.tasks t ON t.id=r.task_id "
              f"WHERE t.id={task} AND t.logical_identity #>> '{{subject,identity,identity}}'='{identity}'")
    if _sql(deployment, exists) != "1":
        raise ValueError("Physical backup did not restore the exact seeded remote result")
    blocker = "retention-block-" + uuid.uuid4().hex
    administrator = "retention-reapply-" + uuid.uuid4().hex
    name, _ = _container(deployment)
    child = None
    started = time.monotonic()
    try:
        deployment.docker("exec", "--detach", "--env", "PGAPPNAME=" + blocker, name,
                          "psql", "-X", "-qAt", "-v", "ON_ERROR_STOP=1", "-U", "corpus", "-d", "source",
                          "-c", f"BEGIN; SELECT task_id FROM public.remote_call_results WHERE task_id={task} FOR UPDATE; SELECT pg_sleep(600); ROLLBACK;")
        _wait("owned cleanup row blocker", lambda: _sql(deployment,
              f"SELECT count(*) FROM pg_stat_activity WHERE application_name='{blocker}' AND wait_event='PgSleep'") == "1")
        environment = _admin_environment(deployment, administrator)
        with (root / "physical-interrupted-reapply.log").open("xb") as log:
            os.chmod(log.name, 0o600)
            child = subprocess.Popen(_admin_command(arguments, deployment, "--reapply-control"),
                                     env=environment, stdout=log, stderr=subprocess.STDOUT,
                                     start_new_session=True)

            def waiting():
                if child.poll() is not None:
                    raise RuntimeError("Reapplication exited before the controlled cleanup interruption")
                return _sql(deployment,
                            f"SELECT count(*) FROM pg_stat_activity a WHERE a.application_name='{administrator}' "
                            "AND a.wait_event_type='Lock' AND a.query LIKE '%hubuum_task_purge_execute%' "
                            "AND EXISTS (SELECT 1 FROM pg_stat_activity b "
                            f"WHERE b.application_name='{blocker}' AND b.pid=ANY(pg_blocking_pids(a.pid)))") == "1"

            _wait("actual reapplication cleanup SQL blocked on the owned row", waiting)
            os.killpg(child.pid, signal.SIGKILL)
            child.wait(timeout=30)
        _sql(deployment, f"SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE application_name='{blocker}'")
        _wait("interrupted administrator SQL settlement", lambda: _sql(deployment,
              f"SELECT count(*) FROM pg_stat_activity WHERE application_name='{administrator}'") == "0")
        remaining = _sql(deployment, exists)
        if remaining not in {"0", "1"}:
            raise RuntimeError("Interrupted cleanup produced an invalid physical result count")
        if _sql(deployment, "SELECT public.hubuum_deletion_exposure_allowed()") != "f":
            raise RuntimeError("Interrupted reapplication released application quarantine")
        return {"interrupted_stage": "protected task reapplication waiting for child-row lock",
                # A dead client can lose acknowledgement of an already dispatched
                # operation. Record the physical outcome, then prove retry safety.
                "exit_signal": "SIGKILL", "result_rows_after_interruption": int(remaining),
                "quarantine_retained": True,
                "elapsed_ms": (time.monotonic() - started) * 1000}
    finally:
        if child is not None and child.poll() is None:
            os.killpg(child.pid, signal.SIGKILL)
            child.wait(timeout=30)
        _sql(deployment, f"SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE application_name='{blocker}'")


def _restore_files(deployment, path, metadata):
    path = _owned_path(deployment, path)
    name, info = _container(deployment)
    layout = _layout(info)
    if info["State"]["Running"] or metadata["layout"] != layout:
        raise ValueError("Physical restore requires the same stopped managed PostgreSQL volume")
    ownership = metadata["ownership"]
    if not re.fullmatch(r"[0-9]+:[0-9]+", ownership):
        raise ValueError("Invalid captured PostgreSQL ownership")
    _validate_archive(path)
    if metadata["sha256"] != _digest(path):
        raise ValueError("Physical backup changed after capture")
    helper = deployment.name + "-physical-restore-" + uuid.uuid4().hex[:6]
    deployment.containers.append(helper)
    # Parameters have been validated independently of this constant shell code.
    # The helper sees only the stopped database volume and a read-only archive.
    script = '''test -d "$1" && test ! -L "$1"
find "$1" -mindepth 1 -maxdepth 1 -exec rm -rf -- {} +
tar -xpf /retention-primary.tar --no-same-owner -C "$1"
chown -R "$2" "$1"
chmod 0700 "$1"
test "$(cat "$1/PG_VERSION")" = 18
test -s "$1/global/pg_control"
'''
    deployment.docker("run", "--rm", "--name", helper, "--network", "none",
                      *_volume_security(info),
                      "--volumes-from", name + ":rw",
                      "--volume", f"{path}:/retention-primary.tar:ro,z",
                      "--entrypoint", "sh", layout["image"], "-eu", "-c", script,
                      "restore-owned-pgdata", layout["pgdata"], ownership, timeout=600)


def rewind_primary(arguments, deployment, root, monitor, probe_callback):
    """Replace only the stopped owned PGDATA, then exercise production recovery."""
    path = _owned_path(deployment, root / "before-purge.primary.tar")
    metadata = json.loads(path.with_suffix(".json").read_text(encoding="utf-8"))
    name, info = _container(deployment)
    for container in deployment.application_containers:
        deployment.docker("stop", "--time", "45", container)
        if deployment.docker("inspect", "--format", "{{.State.Running}}", container).stdout.strip() != "false":
            raise RuntimeError("An application runtime remains alive before physical rewind")
    _wait("all managed client sessions to settle", lambda: _sql(deployment,
          "SELECT count(*) FROM pg_stat_activity WHERE backend_type='client backend' AND pid<>pg_backend_pid()") == "0")
    head = (deployment.repository / "HEAD").read_bytes()
    started = time.monotonic()
    deployment.docker("stop", "--time", "45", name)
    _restore_files(deployment, path, metadata)
    deployment.docker("start", name)
    _wait("physically restored PostgreSQL", lambda: deployment.docker(
        "exec", name, "pg_isready", "-h", "127.0.0.1", "-U", "corpus", "-d", "source", check=False).returncode == 0)
    _refresh_database_address(deployment)
    monitor.add(name)
    if _sql(deployment, "SELECT system_identifier FROM pg_control_system()") != metadata["system_identifier"]:
        raise RuntimeError("Physical restore changed the PostgreSQL system identifier")
    if _container(deployment)[1]["Mounts"] != info["Mounts"]:
        raise RuntimeError("Physical restore replaced the managed volume identity")
    if (deployment.repository / "HEAD").read_bytes() != head:
        raise RuntimeError("Independent checkpoint repository changed during primary rewind")
    recover_ms = _admin(arguments, deployment, "--recover-control", root / "physical-recover.log")
    interruption = _interrupted_reapplication(arguments, deployment, root)
    reapply_ms = _admin(arguments, deployment, "--reapply-control", root / "physical-reapply.log")
    if _sql(deployment, "SELECT public.hubuum_deletion_exposure_allowed()") != "t":
        raise RuntimeError("Successful reapplication did not release application quarantine")
    api, container = deployment.start_measured_api(3, role="all")
    monitor.add(container)
    authenticated = deployment.login(api, "source", "admin")
    tokens = root / "tokens.json"
    tokens.write_text(json.dumps([authenticated.token]) + "\n", encoding="utf-8")
    tokens.chmod(0o600)
    probe_callback([authenticated.base])
    _write(root / "physical-recovery.json", {
        "backup_format": metadata["format"], "backup_sha256": metadata["sha256"],
        "system_identifier_preserved": True, "independent_head_preserved_during_rewind": True,
        "interruption": interruption, "recover_ms": recover_ms, "reapply_ms": reapply_ms,
        "elapsed_ms": (time.monotonic() - started) * 1000,
        "native_probe_verified": True,
    })
