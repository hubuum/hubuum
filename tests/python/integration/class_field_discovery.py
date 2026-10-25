"""Compare live field discovery with the existing multi-request workflow.

The fixture must already exist. This driver performs reads only. JSON decode
measurements use Python's standard-library reference client, not the Rust CLI.
Optional CLI diagnostics are retained separately without attributing HTTP time
or request overhead to pure deserialization.
"""
import argparse
import http.client
import json
import os
from pathlib import Path
import statistics
import subprocess
import tempfile
import time
from urllib.parse import quote, urlencode, urlsplit

MAX_BODY = 128 * 1024 * 1024


def percentile(values, percentage):
    ordered = sorted(values)
    return ordered[min(len(ordered) - 1, (len(ordered) * percentage + 99) // 100 - 1)]


def process_ticks(pid):
    fields = Path(f"/proc/{pid}/stat").read_text().rpartition(")")[2].split()
    return int(fields[11]) + int(fields[12])


class Client:
    def __init__(self, url, token):
        parsed = urlsplit(url)
        if parsed.scheme not in {"http", "https"} or not parsed.hostname or parsed.path not in {"", "/"}:
            raise ValueError("URL must be an HTTP(S) origin without a path")
        connection = http.client.HTTPSConnection if parsed.scheme == "https" else http.client.HTTPConnection
        self.connection = connection(parsed.hostname, parsed.port, timeout=60)
        self.token = token
        self.reset()

    def reset(self):
        self.requests = 0
        self.response_bytes = 0
        self.decode_ns = 0

    def get(self, path):
        self.connection.request("GET", path, headers={"Authorization": f"Bearer {self.token}", "Accept-Encoding": "identity"})
        response = self.connection.getresponse()
        body = response.read(MAX_BODY + 1)
        if len(body) > MAX_BODY:
            raise RuntimeError("Benchmark response exceeds 128 MiB")
        if response.status != 200:
            raise RuntimeError(f"Benchmark request returned HTTP {response.status}")
        self.requests += 1
        self.response_bytes += len(body)
        started = time.process_time_ns()
        decoded = json.loads(body)
        self.decode_ns += time.process_time_ns() - started
        return decoded, response.getheader("X-Next-Cursor")

    def legacy(self, name, limit):
        selected, _ = self.get(f"/api/v1/classes/by-name/{quote(name, safe='')}")
        class_id = selected["id"]
        shared, _ = self.get(f"/api/v1/classes/{class_id}/computed-fields")
        personal = []
        cursor = None
        for _ in range(32):
            query = {"class_id": class_id, "include_total": "false"}
            if cursor:
                query["cursor"] = cursor
            page, cursor = self.get("/api/v1/iam/me/computed-fields?" + urlencode(query))
            personal.extend(page)
            if not cursor:
                break
        else:
            raise RuntimeError("Personal definition pagination exceeded the fixture bound")
        query = {"limit": limit, "sort": "id", "include_total": "false"}
        if any(field["enabled"] for field in shared["definitions"] + personal):
            query["include"] = "computed"
        return self.get(f"/api/v1/classes/{class_id}/?" + urlencode(query))[0]

    def inventory(self, name, limit):
        return self.get(f"/api/v1/classes/by-name/{quote(name, safe='')}/fields?limit={limit}")[0]


def measure(client, operation, name, limit, iterations, server_pid):
    operation(name, limit)  # Warm connection, prepared statements and materialization.
    samples = []
    cpu_ticks = 0
    for _ in range(iterations):
        client.reset()
        before = process_ticks(server_pid) if server_pid else 0
        started = time.perf_counter_ns()
        result = operation(name, limit)
        elapsed = time.perf_counter_ns() - started
        cpu_ticks += process_ticks(server_pid) - before if server_pid else 0
        samples.append({"wall_ms": elapsed / 1e6, "reference_decode_ms": client.decode_ns / 1e6,
                        "requests": client.requests, "response_bytes": client.response_bytes})
    return {
        "iterations": iterations,
        "p50_ms": percentile([sample["wall_ms"] for sample in samples], 50),
        "p95_ms": percentile([sample["wall_ms"] for sample in samples], 95),
        "requests_per_iteration": sorted({sample["requests"] for sample in samples}),
        "response_bytes": sorted({sample["response_bytes"] for sample in samples}),
        "reference_python_decode_mean_ms": statistics.mean(sample["reference_decode_ms"] for sample in samples),
        "server_cpu_mean_ms": cpu_ticks * 1000 / os.sysconf("SC_CLK_TCK") / iterations if server_pid else None,
        "server_cpu_tick_ms": 1000 / os.sysconf("SC_CLK_TCK") if server_pid else None,
        "sampled_objects": result["sampled_objects"] if isinstance(result, dict) else len(result),
    }


def cli_diagnostics(binary, url, token_file, name, limit):
    parsed = urlsplit(url)
    command = [str(binary), "--hostname", parsed.hostname, "--port", str(parsed.port or (443 if parsed.scheme == "https" else 80)),
               "--protocol", parsed.scheme, "--token-file", str(token_file), "--debug",
               "class", "fields", "--name", name, "--limit", str(limit), "--json"]
    with tempfile.TemporaryDirectory(prefix="hubuum-fields-cli-") as directory:
        config = Path(directory) / "config.toml"
        config.write_text("")
        command[1:1] = ["--config", str(config)]
        result = subprocess.run(command, check=True, capture_output=True, text=True, timeout=120)
    return json.loads(result.stdout)["debug"]


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--url", required=True)
    parser.add_argument("--token-file", required=True, type=Path)
    parser.add_argument("--class-name", required=True)
    parser.add_argument("--revision", required=True, help="Exact server commit or image digest")
    parser.add_argument("--limit", type=int, default=100)
    parser.add_argument("--iterations", type=int, default=100)
    parser.add_argument("--server-pid", type=int)
    parser.add_argument("--cli", type=Path)
    parser.add_argument("--cli-revision")
    parser.add_argument("--output", required=True, type=Path)
    args = parser.parse_args(argv)
    if not 1 <= args.limit <= 250 or not 1 <= args.iterations <= 1000:
        raise ValueError("limit must be 1–250 and iterations must be 1–1000")
    if bool(args.cli) != bool(args.cli_revision):
        raise ValueError("--cli and --cli-revision must be supplied together")
    client = Client(args.url, args.token_file.read_text().strip())
    try:
        result = {"server_revision": args.revision, "sample_limit": args.limit,
                  "notes": "Wall time includes local reference-client JSON decoding and network waits. Server CPU excludes PostgreSQL. Reference Python decode is not Rust CLI deserialization."}
        for name, operation in [("legacy", client.legacy), ("inventory", client.inventory)]:
            result[name] = measure(client, operation, args.class_name, args.limit, args.iterations, args.server_pid)
        if args.cli:
            result["legacy_cli"] = {"revision": args.cli_revision, "diagnostics": cli_diagnostics(args.cli, args.url, args.token_file, args.class_name, args.limit)}
        args.output.write_text(json.dumps(result, indent=2) + "\n")
    finally:
        client.connection.close()
    return 0
