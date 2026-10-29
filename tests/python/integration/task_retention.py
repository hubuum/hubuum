"""Provision disposable, resource-limited task-retention measurements (stdlib only).

The Rust frontend owns admission, lifecycle, and external-effect assertions.
Raw constrained evidence must be evaluated before claiming the P6 target.
"""
import argparse
import copy
import hashlib
import ipaddress
import json
import os
from pathlib import Path
import secrets
import subprocess
import threading
import time
import uuid

from integration.corpus import Api, Deployment, POSTGRES_IMAGE, wait_for
from support import ROOT
from support.task_effects import EffectFixture


def write_json(path, value):
    path.write_text(json.dumps(value, indent=2) + "\n", encoding="utf-8")


def run_native(deployment, command, *, timeout, capture_output=False):
    try:
        return subprocess.run(deployment.host_command(command), check=True, timeout=timeout,
                              capture_output=capture_output, text=capture_output,
                              env=deployment.host_environment())
    except subprocess.CalledProcessError as error:
        raise RuntimeError(f"Measured {Path(command[0]).name} failed (exit {error.returncode}); inspect retained artifacts") from None
    except subprocess.TimeoutExpired:
        raise RuntimeError(f"Measured {Path(command[0]).name} exceeded its declared timeout") from None


def proc_memory(pid):
    """Linux process values; threads are never counted as separate processes."""
    return parse_proc_memory(Path(f"/proc/{pid}/status").read_text(encoding="ascii"))


def parse_proc_memory(status):
    values = {}
    for line in status.splitlines():
        name, _, rest = line.partition(":")
        if name in {"VmRSS", "VmHWM"}:
            fields = rest.split()
            if len(fields) != 2 or fields[1] != "kB":
                raise ValueError("Unexpected kernel memory units")
            size = int(fields[0])
            if size < 0:
                raise ValueError("Negative kernel memory value")
            values[name] = size * 1024
    return values


def cpu_set(value):
    result = set()
    for item in value.split(","):
        first, separator, last = item.partition("-")
        lower = int(first)
        upper = int(last) if separator else lower
        if lower < 0 or upper < lower or upper - lower > 65536:
            raise ValueError("Invalid CPU mask")
        result.update(range(lower, upper + 1))
    return result


def affinity_plan(profile):
    available = iter(sorted(os.sched_getaffinity(0)))
    plan = {}

    def take(name, count):
        try:
            cpus = []
            for _ in range(count):
                cpus.append(next(available))
            plan[name] = ",".join(map(str, cpus))
        except StopIteration:
            raise ValueError("Not enough distinct allowed CPUs for the declared measurement profile") from None

    take("database", 4 if profile == "shared" else 2)
    take("api-0", 2)
    if profile == "shared":
        take("api-1", 2)
    if profile != "local":
        take("worker-0", 2)
    if profile == "shared":
        take("worker-1", 2)
    take("administrator", 2)
    plan["all-0"] = plan["api-0"]
    plan["all-2"] = plan["api-0"]
    plan["all-3"] = plan["api-0"]
    plan["all-4"] = plan["api-0"]
    return plan


def process_tree_threads(pid):
    pending, seen = [pid], set()
    while pending:
        current = pending.pop()
        if current in seen:
            continue
        seen.add(current)
        try:
            for thread in Path(f"/proc/{current}/task").iterdir():
                yield thread
                try:
                    pending.extend(int(value) for value in (thread / "children").read_text(encoding="ascii").split())
                except FileNotFoundError:
                    pass
        except FileNotFoundError:
            pass


def verify_thread_affinity(pid, expected):
    allowed = cpu_set(expected)
    checked = 0
    for thread in process_tree_threads(pid):
        try:
            status = (thread / "status").read_text(encoding="ascii")
        except FileNotFoundError:
            continue
        mask = next((line.partition(":")[2].strip() for line in status.splitlines()
                     if line.startswith("Cpus_allowed_list:")), None)
        if mask is None:
            raise ValueError("Measured thread has no kernel CPU mask")
        if not cpu_set(mask).issubset(allowed):
            raise ValueError("A measured thread escaped its declared CPU affinity")
        checked += 1
    return checked


class Monitor:
    def __init__(self, deployment, repository):
        self.deployment = deployment
        self.repository = repository
        self.processes = {}
        self.peaks = {}
        self.repository_peak_bytes = 0
        self.repository_peak_files = 0
        self.samples = 0
        self.errors = []
        self.enforcement = {}
        self.stop = threading.Event()
        self.thread = threading.Thread(target=self.run, daemon=True)

    def add(self, container):
        pid = int(self.deployment.docker("inspect", "--format", "{{.State.Pid}}", container).stdout)
        if pid <= 0:
            raise ValueError("Measured container has no live process")
        group = next(line.split(":", 2)[2] for line in Path(f"/proc/{pid}/cgroup").read_text().splitlines()
                     if line.startswith("0::"))
        cgroup = Path("/sys/fs/cgroup") / group.lstrip("/")
        limit = (cgroup / "memory.max").read_text().strip()
        expected = self.deployment.memory_limit(container)
        if limit == "max" or not 0 < int(limit) <= expected:
            raise ValueError("Container memory ceiling is not actually enforced")
        mask = self.deployment.container_affinities.get(container)
        if mask and verify_thread_affinity(pid, mask) == 0:
            raise ValueError("Measured process vanished before CPU verification")
        cpu_max = (cgroup / "cpu.max").read_text().strip() if (cgroup / "cpu.max").exists() else None
        if not mask:
            if cpu_max is None:
                raise ValueError("Container CPU quota is not actually enforced")
            quota, period = cpu_max.split()
            if quota == "max" or int(period) <= 0 or int(quota) / int(period) > self.deployment.cpu_limit(container):
                raise ValueError("Container CPU quota exceeds the declared budget")
        self.enforcement[container] = {"memory_max_bytes": int(limit), "cpu_affinity": mask,
                                       "cpu_max": cpu_max, "cgroup": str(cgroup), "memory_peak_bytes": 0}
        # Publish only after its complete enforcement record is available to
        # the already-running sampler thread.
        self.processes[container] = pid

    def run(self):
        while not self.stop.wait(0.020):
            for name, pid in tuple(self.processes.items()):
                try:
                    memory = proc_memory(pid)
                    previous = self.peaks.setdefault(name, {"sampled_rss_bytes": 0, "kernel_hwm_bytes": 0})
                    previous["sampled_rss_bytes"] = max(previous["sampled_rss_bytes"], memory.get("VmRSS", 0))
                    previous["kernel_hwm_bytes"] = max(previous["kernel_hwm_bytes"], memory.get("VmHWM", 0))
                    enforcement = self.enforcement[name]
                    if enforcement["cpu_affinity"]:
                        verify_thread_affinity(pid, enforcement["cpu_affinity"])
                    peak = Path(enforcement["cgroup"]) / "memory.peak"
                    enforcement["memory_peak_bytes"] = max(enforcement["memory_peak_bytes"], int(peak.read_text().strip()))
                except (FileNotFoundError, ProcessLookupError):
                    pass
                except (OSError, ValueError) as error:
                    if len(self.errors) < 8:
                        self.errors.append({"process": name, "error": type(error).__name__})
            try:
                sizes = [entry.stat().st_size for entry in self.repository.iterdir() if entry.is_file()]
            except FileNotFoundError:
                continue
            except OSError as error:
                if len(self.errors) < 8:
                    self.errors.append({"repository": True, "error": type(error).__name__})
                continue
            self.repository_peak_bytes = max(self.repository_peak_bytes, sum(sizes))
            self.repository_peak_files = max(self.repository_peak_files, len(sizes))
            self.samples += 1

    def __enter__(self):
        self.thread.start()
        return self

    def __exit__(self, *_):
        self.stop.set()
        self.thread.join()


class RetentionDeployment(Deployment):
    def __init__(self, image, root, profile="diagnostic", recovery_probes=False, affinity=False,
                 safety_probes=False):
        super().__init__(image, POSTGRES_IMAGE)
        self.root = root
        self.repository = root / "repository"
        self.repository.mkdir(mode=0o700)
        self.key = root / "control.key"
        self.key.write_bytes(secrets.token_bytes(32))
        self.key.chmod(0o600)
        self.role = "api"
        self.profile = profile
        self.application_containers = []
        self.recovery_probes = recovery_probes
        self.safety_probes = safety_probes
        self.effects = None
        self.affinity = affinity
        self.cpu_plan = affinity_plan(profile) if affinity else {}
        self.container_affinities = {}
        self.engine = json.loads(self.docker("info", "--format", "{{json .}}").stdout)
        host = self.engine.get("host", self.engine.get("Host", {}))
        self.rootless = host.get("security", {}).get("rootless", False)
        controllers = host.get("cgroupControllers")
        if controllers is not None and "memory" not in controllers:
            raise ValueError("Container memory controller is unavailable")
        if not affinity and controllers is not None and "cpu" not in controllers:
            raise ValueError("CPU quota controller unavailable; use the verified affinity image and --cpu-affinity")
        if not affinity and self.engine.get("CpuCfsQuota") is False:
            raise ValueError("CPU quota controller unavailable; use --cpu-affinity")

    def __enter__(self):
        self.docker("image", "inspect", self.image)
        try:
            self.docker("network", "create", self.name)
            name = self.name + "-db"
            self.containers.append(name)
            cpu_args = (["--cpus", "4" if self.profile == "shared" else "2"] if not self.affinity else [])
            database_command = (["--entrypoint", "taskset", self.postgres_image, "-c", self.cpu_plan["database"],
                                 "/usr/local/bin/docker-entrypoint.sh", "postgres"] if self.affinity else [self.postgres_image])
            if self.affinity:
                self.container_affinities[name] = self.cpu_plan["database"]
            self.docker("run", "--detach", "--name", name, "--network", self.name,
                        "--network-alias", "postgres", "--publish", "127.0.0.1::5432",
                        *cpu_args,
                        "--memory", "4g" if self.profile == "shared" else "2g",
                        "--memory-swap", "4g" if self.profile == "shared" else "2g",
                        "--env", "POSTGRES_USER=corpus", "--env", f"POSTGRES_PASSWORD={self.password}",
                        "--env", "POSTGRES_DB=source", *database_command,
                        "-c", "log_min_messages=fatal", "-c", "log_min_error_statement=panic")
            wait_for("PostgreSQL", lambda: self.docker("exec", name, "pg_isready", "-h", "127.0.0.1", "-U", "corpus", "-d", "source", check=False).returncode == 0)
            networks = json.loads(self.docker("inspect", "--format", "{{json .NetworkSettings.Networks}}", name).stdout)
            address = ipaddress.ip_address(networks[self.name]["IPAddress"])
            if address.is_unspecified or address.is_multicast:
                raise ValueError("Managed PostgreSQL container has no usable network address")
            self.database_ip = str(address)
            address = self.docker("port", name, "5432/tcp").stdout.strip()
            self.host_url = f"postgres://corpus:{self.password}@{address}/source?sslmode=disable"
            if self.recovery_probes or self.safety_probes:
                self.effects = EffectFixture(self.root / "effects").__enter__()
            return self
        except BaseException:
            self.__exit__(None, None, None)
            raise

    def __exit__(self, *error):
        try:
            super().__exit__(*error)
        finally:
            if self.effects is not None:
                self.effects.__exit__(*error)

    def environment(self, database):
        values = [*super().environment(database),
                  "--add-host", f"postgres:{self.database_ip}",
                  "--user", f"{os.getuid()}:{os.getgid()}",
                  "--memory", "512m", "--memory-swap", "512m",
                  "--volume", f"{self.repository.resolve()}:/control:rw,z",
                  "--volume", f"{self.key.resolve()}:/control.key:ro,z"]
        if not self.affinity:
            values += ["--cpus", "2"]
        if self.rootless:
            values += ["--userns", "keep-id"]
        config = {
            "HUBUUM_CONTROL_RECOVERY_DIRECTORY": "/control",
            "HUBUUM_CONTROL_RECOVERY_KEY": "/control.key",
            "HUBUUM_CONTROL_CHECKPOINT_MEASURE": "true",
            "HUBUUM_RUNTIME_ROLE": self.role,
            "HUBUUM_TASK_WORKERS": "2" if self.profile == "shared" else "1",
            "HUBUUM_DB_POOL_SIZE": "8",
            "HUBUUM_METRICS_ENABLED": "true",
            "HUBUUM_LOGIN_RATE_LIMIT_ENABLED": "false",
            "HUBUUM_SESSION_ACCESS_SECONDS": "86400",
            "HUBUUM_EVENT_FANOUT_WORKERS": "0",
            "HUBUUM_EVENT_DELIVERY_WORKERS": "0",
            "HUBUUM_EXPORT_OUTPUT_RETENTION_HOURS": "1",
            "HUBUUM_BACKUP_OUTPUT_RETENTION_HOURS": "1",
        }
        if self.effects is not None:
            values += ["--add-host", "retention-effect:host-gateway",
                       "--volume", f"{self.effects.certificate.resolve()}:/effect-ca.pem:ro,z"]
            config.update({"SSL_CERT_FILE": "/effect-ca.pem",
                           "HUBUUM_REMOTE_CALL_ALLOW_PRIVATE_TARGETS": "true"})
        if self.safety_probes:
            config.update({"HUBUUM_REMOTE_CALL_TIMEOUT_MS": "900000",
                           "HUBUUM_TASK_REMOTE_CALL_EXECUTION_TIMEOUT_SECONDS": "900"})
        return values + [part for key, value in config.items() for part in ("--env", f"{key}={value}")]

    def memory_limit(self, container):
        if container.endswith("-db"):
            return (4 if self.profile == "shared" else 2) * 1024**3
        return 512 * 1024**2

    def cpu_limit(self, container):
        return 4 if self.profile == "shared" and container.endswith("-db") else 2

    def admin(self, database, *args, mount=None):
        if not self.affinity:
            return super().admin(database, *args, mount=mount)
        mounts = [] if mount is None else ["--mount", f"type=bind,source={mount.resolve()},target=/corpus,readonly"]
        name = self.name + "-admin-" + uuid.uuid4().hex[:6]
        self.containers.append(name)
        return self.docker("run", "--rm", "--name", name, "--network", self.name,
                           *self.environment(database), *mounts, "--entrypoint", "taskset", self.image,
                           "-c", self.cpu_plan["administrator"], "hubuum-admin", *args).stdout

    def backup(self, database, destination):
        if not self.affinity:
            return super().backup(database, destination)
        name = self.name + "-backup-" + uuid.uuid4().hex[:6]
        self.containers.append(name)
        self.docker("create", "--name", name, "--network", self.name, *self.environment(database),
                    "--entrypoint", "taskset", self.image, "-c", self.cpu_plan["administrator"],
                    "hubuum-admin", "--backup", "/tmp/backup.json")
        self.docker("start", "--attach", name)
        if self.docker("inspect", "--format", "{{.State.ExitCode}}", name).stdout.strip() != "0":
            raise RuntimeError("Production backup failed")
        self.docker("cp", name + ":/tmp/backup.json", destination)
        destination.chmod(0o600)

    def host_command(self, command):
        if self.affinity:
            return ["taskset", "-c", self.cpu_plan["administrator"], *command]
        return command

    def application_command(self, container, role):
        if not self.affinity:
            return [self.image]
        mask = self.cpu_plan[role]
        self.container_affinities[container] = mask
        return ["--entrypoint", "taskset", self.image, "-c", mask, "/entrypoint.sh"]

    def start_worker(self, index=0):
        self.role = "worker"
        name = self.name + f"-worker-{index}"
        self.containers.append(name)
        self.application_containers.append(name)
        self.docker("run", "--detach", "--name", name, "--network", self.name,
                    *self.environment("source"), *self.application_command(name, f"worker-{index}"))
        self.role = "api"
        return name

    def start_measured_api(self, index=0, role="api"):
        self.role = role
        name = self.name + f"-{role}-{index}"
        self.containers.append(name)
        self.application_containers.append(name)
        self.docker("run", "--detach", "--name", name, "--network", self.name,
                    "--publish", "127.0.0.1::8080", *self.environment("source"),
                    *self.application_command(name, f"{role}-{index}"))
        address = self.docker("port", name, "8080/tcp").stdout.strip()
        api = Api("http://" + address)

        def ready():
            try:
                api.get("/readyz")
                return True
            except (OSError, ValueError):
                return False

        wait_for("application readiness", ready)
        self.role = "api"
        return api, name

    def host_environment(self):
        values = {**os.environ, "HUBUUM_DATABASE_URL": self.host_url,
                "HUBUUM_MIGRATION_DATABASE_URL": self.host_url,
                "HUBUUM_DATABASE_ROLE_MODE": "single", "HUBUUM_TOKEN_HASH_KEY": self.token_key,
                "HUBUUM_CONTROL_RECOVERY_DIRECTORY": str(self.repository.resolve()),
                "HUBUUM_CONTROL_RECOVERY_KEY": str(self.key.resolve())}
        if self.affinity:
            values["HUBUUM_MEASUREMENT_CPU_AFFINITY"] = self.cpu_plan["administrator"]
        return values


def workload_principals(api, count):
    tokens = [api.token]
    if count == 1:
        return tokens
    admin_group = next(group["id"] for group in api.pages("/api/v1/iam/groups")
                       if group["groupname"] == "admin")
    for index in range(1, count):
        password = secrets.token_urlsafe(24)
        username = f"retention-principal-{index}"
        user = api.create_user({"name": username, "password": password,
                               "proper_name": username, "email": username + "@example.invalid"})
        api.post(f"/api/v1/iam/groups/{admin_group}/members/{user['id']}")
        session = Api(api.base).post("/api/v1/auth/login", {
            "client_id": "hubuum-retention-measurement", "name": username, "password": password,
        })
        tokens.append(session["access_token"])
    return tokens


def remote_probe(arguments, deployment, origins, root, phase, suffix=None):
    report = f"probe-{phase}" + (f"-{suffix}" if suffix else "") + ".json"
    command = [str(arguments.scale_binary), "task-retention-probe", "--phase", phase,
               "--origin", origins[0], "--other-origin", origins[-1],
               "--token-file", str(root / "tokens.json"), "--request-file", str(root / "probe-request.json"),
               "--state-file", str(root / "probe-state.json"), "--output", str(root / report),
               "--database-url", deployment.host_url, "--timeout-seconds", "120"]
    run_native(deployment, command, timeout=180)


def prepare_recovery_probe(arguments, deployment, origins, root, api):
    deployment.admin("source", "--task-retention", "set-policy",
                     "--task-retention-deduplication-seconds", "7200", "--task-retention-details-seconds", "30")
    admin_group = next(group["id"] for group in api.pages("/api/v1/iam/groups")
                       if group["groupname"] == "admin")
    collection = api.post("/api/v1/collections", {"name": "retention-effects", "description": "Synthetic counter",
                                               "group_id": admin_group})
    target = api.post("/api/v1/remote-targets", {
        "collection_id": collection["id"], "class_id": None, "name": "retention-probe",
        "description": "Synthetic counted effect", "method": "post", "url_template": deployment.effects.remote_url,
        "allowed_subject_types": ["collection"],
    })
    probe = uuid.uuid4().hex
    body = {"subject": {"type": "collection", "collection_id": collection["id"]},
            "parameters": {}, "body_override": {"probe": probe}}
    changed = copy.deepcopy(body)
    changed["body_override"]["changed"] = True
    request = root / "probe-request.json"
    write_json(request, {"version": 1, "endpoint": f"/api/v1/remote-targets/{target['id']}/invoke",
                         "key": f"probe-{probe}", "body": body, "conflict_body": changed,
                         "counter_url": deployment.effects.counter_url + "?probe=" + probe})
    request.chmod(0o600)
    remote_probe(arguments, deployment, origins, root, "seed")
    state = json.loads((root / "probe-state.json").read_text(encoding="utf-8"))
    hold = str(uuid.uuid4())
    deployment.admin("source", "--task-retention", "hold", "--task-hold-id", hold,
                     "--task-hold-task", state["task_identity"], "--task-hold-reason", "operator")
    deployment.backup("source", root / "before-purge.json")
    if arguments.physical_recovery_probes:
        from support.task_retention_recovery import capture_primary
        capture_primary(deployment, root / "before-purge.primary.tar")
    deployment.admin("source", "--task-retention", "release-hold", "--task-hold-id", hold)
    deployment.admin("source", "--task-retention", "set-policy",
                     "--task-retention-deduplication-seconds", "120", "--task-retention-details-seconds", "30")


def prepare_output_probe(arguments, deployment, origins, root):
    deployment.admin("source", "--task-retention", "set-policy", "--task-retention-deduplication-seconds", "7200")
    recipe = root / "output-workload.json"
    write_json(recipe, {"version": 1, "count": 1, "concurrency": 1, "timeout_seconds": 120,
                        "duplicate_every": 1, "conflict_every": 1, "endpoint": "/api/v1/exports",
                        "body": {"scope": {"kind": "collections"}},
                        "conflict_body": {"scope": {"kind": "classes"}}})
    command = [str(arguments.scale_binary), "task-retention", "--origins", *origins,
               "--database-url", deployment.host_url, "--token-file", str(root / "tokens.json"),
               "--workload-spec", str(recipe), "--cohort", "output-" + uuid.uuid4().hex,
               "--output", str(root / "output-admission.json")]
    run_native(deployment, command, timeout=180)
    deployment.admin("source", "--task-retention", "set-policy",
                     "--task-retention-deduplication-seconds", "120", "--task-retention-details-seconds", "30")


def observe_output_probe(arguments, deployment, root):
    command = [str(arguments.scale_binary), "task-retention-observe", "--output-only",
               "--database-url", deployment.host_url, "--admission-report", str(root / "output-admission.json"),
               "--admin-binary", str(arguments.admin_binary), "--repository", str(deployment.repository),
               "--key-file", str(deployment.key), "--artifact-directory", str(root / "output-processes"),
               "--output", str(root / "output-expiry.json"), "--timeout-seconds", "4200"]
    run_native(deployment, command, timeout=4380)


def replay_recovery_probe(arguments, deployment, origins, root, monitor):
    remote_probe(arguments, deployment, origins, root, "purged")
    for container in deployment.application_containers:
        deployment.docker("stop", "--time", "45", container, check=False)
    started = time.monotonic()
    result = run_native(deployment, [
        str(arguments.admin_binary), "--restore", str(root / "before-purge.json"),
        "--restore-confirmation", "REPLACE ALL HUBUUM DATA",
    ], timeout=300, capture_output=True)
    (root / "logical-restore.log").write_text(result.stdout + result.stderr, encoding="utf-8")
    restored, container = deployment.start_measured_api(2, role="all")
    monitor.add(container)
    authenticated = deployment.login(restored, "source", "admin")
    tokens = root / "tokens.json"
    write_json(tokens, [authenticated.token])
    tokens.chmod(0o600)
    remote_probe(arguments, deployment, [authenticated.base], root, "restored")
    write_json(root / "logical-recovery.json", {"elapsed_ms": (time.monotonic() - started) * 1000,
                                              "external_counter_preserved": True})


def measured_run(arguments, root, run_index):
    root.mkdir(mode=0o700)
    workload = json.loads(arguments.workload.read_text(encoding="utf-8"))
    if arguments.count is not None:
        workload["count"] = arguments.count
    recipe = root / "workload.json"
    write_json(recipe, workload)
    with RetentionDeployment(arguments.image, root, arguments.profile, arguments.recovery_probes,
                             arguments.cpu_affinity, arguments.safety_probes) as deployment:
        deployment.admin("source", "--migrate")
        deployment.admin("source", "--control-checkpoint", "/control", "--control-checkpoint-key", "/control.key", "--initialize-control-checkpoint")
        deployment.admin("source", "--task-retention", "set-policy", "--task-retention-deduplication-seconds", "120", "--task-retention-details-seconds", "30")
        preview = json.loads(deployment.admin("source", "--task-retention", "preview"))
        enabled = json.loads(deployment.admin("source", "--task-retention", "enable", "--task-retention-confirmation", preview["preview"]["confirmation"]))
        write_json(root / "activation.json", enabled)
        first_api, first_container = deployment.start_measured_api()
        api = deployment.login(first_api, "source", "admin")
        origins = [api.base]
        api_containers = [first_container]
        if arguments.profile == "shared":
            extra, container = deployment.start_measured_api(1)
            origins.append(extra.base)
            api_containers.append(container)
        current = api.get("/api/v1/admin/task-capacity")["policy"]
        policy = copy.deepcopy(current["policy"])
        for name in ("deployment", "principal", "collection"):
            policy[name]["outstanding"] = 20000
        for kind in policy["kinds"]:
            kind["capacity"]["outstanding"] = 20000
            kind["principal_outstanding"] = 20000
        capacity = api.request("PUT", "/api/v1/admin/task-capacity", {"expected_revision": current["revision"], "policy": policy})[0]
        write_json(root / "capacity-policy.json", capacity)
        tokens = root / "tokens.json"
        write_json(tokens, workload_principals(api, 1 if arguments.profile == "diagnostic" else 32))
        tokens.chmod(0o600)
        metrics = root / "metrics"
        metrics.mkdir()
        monitor = Monitor(deployment, deployment.repository)
        completed = False
        try:
            with monitor:
                monitor.add(deployment.name + "-db")
                for container in api_containers:
                    monitor.add(container)
                command = [str(arguments.scale_binary), "task-retention", "--origins", *origins,
                           "--database-url", deployment.host_url, "--token-file", str(tokens),
                           "--workload-spec", str(recipe), "--cohort", f"retention-{run_index}-{uuid.uuid4().hex[:12]}",
                           "--output", str(root / "admission.json"), "--metric-directory", str(metrics)]
                run_native(deployment, command, timeout=arguments.timeout)
                if not arguments.admission_only:
                    if arguments.profile == "local":
                        deployment.docker("stop", "--time", "45", first_container)
                        active, container = deployment.start_measured_api(role="all")
                        origins = [active.base]
                        monitor.add(container)
                    else:
                        for index in range(2 if arguments.profile == "shared" else 1):
                            monitor.add(deployment.start_worker(index))
                    if arguments.output_probe:
                        prepare_output_probe(arguments, deployment, origins, root)
                    if arguments.recovery_probes:
                        prepare_recovery_probe(arguments, deployment, origins, root, Api(origins[0], api.token))
                    observe = [str(arguments.scale_binary), "task-retention-observe",
                                    "--database-url", deployment.host_url, "--admission-report", str(root / "admission.json"),
                                    "--admin-binary", str(arguments.admin_binary), "--repository", str(deployment.repository),
                                    "--key-file", str(deployment.key), "--artifact-directory", str(root / "cleanup-processes"),
                                    "--output", str(root / "cleanup.json"), "--timeout-seconds", str(arguments.timeout)]
                    if arguments.steady_seconds:
                        observe += ["--steady-seconds", str(arguments.steady_seconds),
                                    "--refill-origins", *origins, "--refill-token-file", str(tokens),
                                    "--refill-workload", str(recipe)]
                    run_native(deployment, observe, timeout=arguments.timeout + 180)
                    if arguments.output_probe:
                        observe_output_probe(arguments, deployment, root)
                    if arguments.safety_probes:
                        from support.task_retention_safety import run_safety_drill
                        origins = run_safety_drill(arguments, deployment, origins, root, monitor)
                    if arguments.recovery_probes:
                        replay_recovery_probe(arguments, deployment, origins, root, monitor)
                    if arguments.physical_recovery_probes:
                        from support.task_retention_recovery import rewind_primary
                        rewind_primary(arguments, deployment, root, monitor,
                                       lambda origins: remote_probe(arguments, deployment, origins, root,
                                                                    "restored", suffix="whole-primary"))
            completed = True
        finally:
            write_json(root / "resources.json", {
                "profile": ("task-retention-diagnostic-v1" if arguments.profile == "diagnostic"
                            else f"lifecycle-{arguments.profile}-v1"), "sample_interval_ms": 20,
                "processes": monitor.peaks, "samples": monitor.samples,
                "monitor_errors": monitor.errors, "measurement_valid": not monitor.errors and monitor.samples > 0,
                "enforced_resources": monitor.enforcement, "cpu_affinity_plan": deployment.cpu_plan,
                "container_engine": deployment.engine,
                "workload_completed": completed,
                "repository_peak_sampled_bytes": monitor.repository_peak_bytes,
                "repository_peak_sampled_files": monitor.repository_peak_files,
                "application_limits": {"cpu": 2, "memory_bytes": 536870912, "pool": 8},
                "database_limits": {"cpu": 4 if arguments.profile == "shared" else 2,
                                    "memory_bytes": 4294967296 if arguments.profile == "shared" else 2147483648},
                "postgres_image": POSTGRES_IMAGE, "application_image": arguments.image,
                "workload_sha256": hashlib.sha256(recipe.read_bytes()).hexdigest(),
                "warmup": {"readiness_probes": True, "authentication": True, "task_executions": 0},
                "steady_seconds": arguments.steady_seconds,
                "steady_state": arguments.steady_seconds > 0, "p6_acceptance": False,
                "selected_probes": {"real_output_expiry": arguments.output_probe,
                                    "logical_recovery": arguments.recovery_probes,
                                    "physical_recovery": arguments.physical_recovery_probes,
                                    "retention_safety": arguments.safety_probes},
                "remote_execution_timeout_seconds": 900 if arguments.safety_probes else None,
                "limitations": ["recovery/output/hold drills are separate", "raw occupancy and eligible denominators require evaluation"],
            })
            tokens.unlink(missing_ok=True)
            for index, container in enumerate(deployment.application_containers):
                deployment.docker("stop", "--time", "45", container, check=False)
                result = deployment.docker("logs", container, check=False)
                (root / f"application-{index}.log").write_text(
                    result.stdout + result.stderr, encoding="utf-8")
        if monitor.errors:
            raise RuntimeError("Resource measurement failed; inspect resources.json")


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--image", required=True)
    parser.add_argument("--scale-binary", type=Path, required=True)
    parser.add_argument("--admin-binary", type=Path, required=True)
    parser.add_argument("--output-directory", type=Path, required=True)
    parser.add_argument("--workload", type=Path, default=ROOT / "scale-benchmarks/lifecycle/task-retention-v1.json")
    parser.add_argument("--runs", type=int, default=1)
    parser.add_argument("--count", type=int)
    parser.add_argument("--timeout", type=int, default=1200)
    parser.add_argument("--admission-only", action="store_true")
    parser.add_argument("--profile", choices=("diagnostic", "local", "shared"), default="diagnostic")
    parser.add_argument("--steady-seconds", type=int, default=0)
    parser.add_argument("--recovery-probes", action="store_true")
    parser.add_argument("--cpu-affinity", action="store_true",
                        help="Use verified fixed CPU masks; image must include taskset")
    parser.add_argument("--output-probe", action="store_true",
                        help="Also measure one real export output expiry; requires at least one hour")
    parser.add_argument("--physical-recovery-probes", action="store_true",
                        help="Rewind the complete disposable primary and interrupt protected reapplication")
    parser.add_argument("--safety-probes", action="store_true",
                        help="Exercise held/dependent details, large child cleanup, and an interrupted external effect")
    arguments = parser.parse_args(argv)
    if arguments.physical_recovery_probes:
        arguments.recovery_probes = True
    if (not 1 <= arguments.runs <= 5
            or arguments.count is not None and not 1 <= arguments.count <= 10000
            or not 1 <= arguments.timeout <= 86400
            or not 0 <= arguments.steady_seconds < arguments.timeout):
        parser.error("runs must be 1..5, count 1..10000 and timeout 1..86400 seconds")
    if arguments.admission_only and (arguments.output_probe or arguments.recovery_probes
                                    or arguments.safety_probes):
        parser.error("Output, recovery and safety probes require workers")
    arguments.scale_binary = arguments.scale_binary.resolve(strict=True)
    arguments.admin_binary = arguments.admin_binary.resolve(strict=True)
    arguments.output_directory.mkdir(mode=0o700)
    for run_index in range(arguments.runs):
        measured_run(arguments, arguments.output_directory / f"run-{run_index + 1}", run_index + 1)
