"""Run immutable native authentication consumers against one accepted server.

Only fixture/source selection changes: owning SDK, CLI and frontend consumers
retain their original code and contract assertions. No third-party imports.
"""
import argparse
from contextlib import contextmanager
from dataclasses import dataclass
import hashlib
import importlib.util
import json
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile

ROOT = Path(__file__).resolve().parents[3]
PINS = ROOT / "docs/authentication/ecosystem-source.json"


def sha256(path):
    with path.open("rb") as stream:
        return hashlib.file_digest(stream, "sha256").hexdigest()


def git(root, *arguments):
    return subprocess.check_output(["git", "-C", str(root), *arguments], stderr=subprocess.PIPE, text=True).strip()


@dataclass(frozen=True)
class AcceptedCheckout:
    root: Path
    source: str
    tree: str

    @classmethod
    def resolve(cls, path, pin):
        path = path.resolve()
        if git(path, "rev-parse", "HEAD") != pin["source"] or git(path, "rev-parse", "HEAD^{tree}") != pin["tree"]:
            raise RuntimeError("Checkout differs from its accepted source/tree")
        if git(path, "status", "--porcelain"):
            raise RuntimeError("Native and server checkouts must be clean")
        return cls(path, pin["source"], pin["tree"])

    def verified_file(self, relative, expected):
        path = self.root / relative
        if path.is_symlink() or not path.resolve().is_relative_to(self.root) or sha256(path) != expected:
            raise RuntimeError("Pinned consumer input differs")
        return path


def qualified_image(image, server):
    value = json.loads(subprocess.check_output(["docker", "image", "inspect", image], stderr=subprocess.PIPE, text=True))[0]
    pin = next((pin for pin in server["qualified_server_images"] if value["Id"] in (pin.get("docker_image_id"), pin["manifest"], pin["config"])), None)
    if pin is None:
        raise RuntimeError("Server image is not a qualified immutable candidate")
    return value["Id"], pin



@contextmanager
def fixture_imports(server, driver):
    """Import fixtures from the verified server even when run.py cached fixture packages."""
    previous_path = sys.path[:]
    previous = {name: value for name, value in sys.modules.items()
                if name.split(".", 1)[0] in ("integration", "support")}
    for name in previous:
        del sys.modules[name]
    sys.path[:0] = [str(driver.parent), str(server / "tests/python")]
    try:
        yield
    finally:
        for name in list(sys.modules):
            if name.split(".", 1)[0] in ("integration", "support"):
                del sys.modules[name]
        sys.modules.update(previous)
        sys.path[:] = previous_path


def compatible_contract(original, server):
    for field in ("source_openapi_sha256", "source_fixture_sha256"):
        if original[field] != server[field]:
            raise RuntimeError("Native consumer targets a different wire contract")


def result_count(report):
    if report.get("passed") is not True:
        raise RuntimeError("Owning consumer did not report acceptance")
    if "modes" in report:
        return sum(len(checks) for checks in report["modes"].values())
    return len(report["checks"])


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--client", choices=("rust", "python", "cli", "frontend"), required=True)
    parser.add_argument("--profile", choices=("local", "ldap", "id_token", "userinfo", "oauth", "treetop"), required=True)
    parser.add_argument("--client-source", type=Path, required=True)
    parser.add_argument("--server-source", type=Path, required=True)
    parser.add_argument("--image", required=True)
    parser.add_argument("--consumer", type=Path, required=True, help="Rust/CLI executable, Python consumer script, or Node.js runtime")
    parser.add_argument("--evidence", type=Path, required=True)
    args = parser.parse_args(argv)
    pins = json.loads(PINS.read_text())
    pin = pins["clients"][args.client]
    if args.profile not in pin["profiles"]:
        parser.error("This owning consumer does not qualify the requested profile")
    client = AcceptedCheckout.resolve(args.client_source, pin)
    server = AcceptedCheckout.resolve(args.server_source, {"source": pins["server"]["accepted_server_source"], "tree": pins["server_tree"]})
    for relative, expected in pins["server"]["source_live_fixture_sha256"].items():
        server.verified_file(relative, expected)
    image, image_pin = qualified_image(args.image, pins["server"])
    consumer = args.consumer.resolve()
    if sha256(consumer) != pin["consumer_sha256"]:
        raise RuntimeError("Native consumer differs from the explicit qualified build")
    driver_key = "treetop" if args.profile == "treetop" else "default"
    driver_pin = pin["drivers"][driver_key]
    driver = client.verified_file(driver_pin["path"], driver_pin["sha256"])
    with fixture_imports(server.root, driver):
        spec = importlib.util.spec_from_file_location("ecosystem_" + args.client, driver)
        module = importlib.util.module_from_spec(spec)
        sys.modules[spec.name] = module
        spec.loader.exec_module(module)
        original = module.SOURCE
        compatible_contract(original, pins["server"])
        # This is an explicit new fixture target, never a rewrite of accepted client
        # sources or a bypass of their source/image/contract checks.
        module.SOURCE = pins["server"]
        args.evidence.mkdir(parents=True, exist_ok=True)
        record = {"client": args.client, "profile": args.profile, "client_source": client.source, "client_tree": client.tree,
                  "server_source": server.source, "server_tree": server.tree, "server_image": image, "server_image_provenance": image_pin,
                  "owning_driver": driver_pin, "original_fixture_server_source": original["accepted_server_source"],
                  "fixture_selection": "explicit accepted-server override; owning consumer code and wire assertions unchanged",
                  "consumer_sha256": sha256(consumer), "coordinator_sha256": sha256(Path(__file__)), "manifest_sha256": sha256(PINS),
                  "coordinator_source": git(ROOT, "rev-parse", "HEAD"), "coordinator_source_dirty": bool(git(ROOT, "status", "--porcelain")),
                  "runtime_python": sys.version.split()[0], "passed": False}
        previous = sys.argv
        try:
            with tempfile.TemporaryDirectory(prefix="hubuum-ecosystem-consumer-") as temporary:
                if args.client in ("rust", "cli"):
                    frozen = Path(temporary) / "consumer"
                    shutil.copyfile(consumer, frozen)
                    frozen.chmod(0o700)
                    if sha256(frozen) != pin["consumer_sha256"]:
                        raise RuntimeError("Frozen consumer digest differs")
                    consumer = frozen
                forwarded = [str(driver), "--server-source", str(server.root), "--image", image, "--evidence", str(args.evidence)]
                if args.client == "frontend":
                    forwarded += ["--node", str(consumer)]
                    if args.profile != "treetop":
                        forwarded += ["--profile", args.profile, "--admin-ui"]
                    report_name = "authorization-qualification.json" if args.profile == "treetop" else "bff-qualification.json"
                else:
                    forwarded += ["--profile", args.profile, "--binary" if args.client == "cli" else "--consumer", str(consumer)]
                    report_name = "cli-qualification.json" if args.client == "cli" else "sdk-qualification.json"
                sys.argv = forwarded
                result = module.main()
                if result not in (None, 0):
                    raise RuntimeError("Owning consumer returned failure")
                report_path = args.evidence / report_name
                record["checks"] = result_count(json.loads(report_path.read_text()))
                record["owning_report_sha256"] = sha256(report_path)
                if git(client.root, "status", "--porcelain") or git(server.root, "status", "--porcelain"):
                    raise RuntimeError("Source changed during native qualification")
                record["passed"] = True
        except Exception as error:
            # Exception text can contain callback queries or credential-bearing URLs.
            record["failure_type"] = type(error).__name__
            raise RuntimeError("Native ecosystem consumer failed; inspect sanitized evidence") from None
        finally:
            sys.argv = previous
            (args.evidence / "ecosystem-qualification.json").write_text(json.dumps(record, indent=2) + "\n")
        return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as error:
        print("Native ecosystem qualification failed (" + type(error).__name__ + ").", file=sys.stderr)
        raise SystemExit(1) from None
