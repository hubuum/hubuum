"""Pinned LDAP and SMTP fixtures used by the production transport contract."""

import sys

if sys.version_info < (3, 11):
    sys.exit(
        "Hubuum tooling requires Python 3.11 or newer; found "
        + sys.version.split()[0]
        + ". Install Python 3.11+ and ensure python3 on PATH selects it."
    )

import base64
import json
import subprocess
import urllib.request

LDAP_IMAGE = (
    "ghcr.io/rroemhild/docker-test-openldap@sha256:"
    "3470e15c60119a1c0392cc162cdce71edfb42b55affdc69da574012f956317cd"
)
SMTP_IMAGE = (
    "docker.io/axllent/mailpit:v1.29.2@sha256:"
    "e09b9e78336e0e8c2174790ea5ce7587de986884c5640a6ae1deb1ef23c70f5f"
)


def start(root, tls, untrusted, password, suffix, network, containers, command, ports, ready):
    """Return test environment; register containers before starting for cleanup."""
    for image in (LDAP_IMAGE, SMTP_IMAGE):
        print(f"Pinned fixture: {image}", flush=True)
        command("docker", "pull", image, timeout=300)
    env = {}
    for variant, certs in (("LDAP", tls), ("LDAP_UNTRUSTED", untrusted)):
        container = f"hubuum-transport-contract-{variant.lower()}-{suffix}"
        plain, secure = ports(2)
        config = root / f"{variant}.conf"
        config.write_text(
            "include /etc/ldap/schema/core.schema\n"
            "include /etc/ldap/schema/cosine.schema\n"
            "include /etc/ldap/schema/inetorgperson.schema\n"
            "pidfile /tmp/slapd.pid\nargsfile /tmp/slapd.args\n"
            "modulepath /usr/lib/ldap\nmoduleload back_mdb\n"
            "TLSCertificateFile /fixtures/server.pem\n"
            "TLSCertificateKeyFile /fixtures/server.key\n"
            "database mdb\nmaxsize 10485760\ndirectory /tmp\n"
            "suffix dc=example,dc=test\nrootdn cn=admin,dc=example,dc=test\n"
            f"rootpw {password}\naccess to * by * read\n",
            encoding="utf-8",
        )
        containers.append(container)
        command(
            "docker", "run", "--detach", "--name", container, "--network", network,
            "--publish", f"127.0.0.1:{plain}:10389", "--publish", f"127.0.0.1:{secure}:10636",
            "--volume", f"{certs}:/fixtures:ro,z",
            "--volume", f"{config}:/contract.conf:ro,Z",
            "--entrypoint", "/usr/sbin/slapd", LDAP_IMAGE,
            "-d", "0", "-f", "/contract.conf", "-h", "ldap://0.0.0.0:10389 ldaps://0.0.0.0:10636",
        )
        def probe():
            command("docker", "exec", container, "ldapwhoami", "-x", "-H", "ldap://127.0.0.1:10389",
                    "-D", "cn=admin,dc=example,dc=test", "-w", password, timeout=5)
        ready(probe, variant)
        ldif = (
            "dn: dc=example,dc=test\nobjectClass: domain\ndc: example\n\n"
            "dn: uid=human,dc=example,dc=test\nobjectClass: inetOrgPerson\n"
            "uid: human\ncn: Contract Human\nsn: Human\nemployeeNumber: stable-1\n"
            "employeeType: readers\nmail: human@example.test\n"
            f"userPassword:: {base64.b64encode(password.encode()).decode()}\n"
        )
        result = subprocess.run(
            ["docker", "exec", "-i", container, "ldapadd", "-x", "-H", "ldap://127.0.0.1:10389",
             "-D", "cn=admin,dc=example,dc=test", "-w", password],
            input=ldif, capture_output=True, text=True, timeout=10, check=False,
        )
        if result.returncode:
            raise RuntimeError("LDAP fixture initialization failed")
        env[f"HUBUUM_CONTRACT_{variant}_URI"] = f"ldaps://127.0.0.1:{secure}"
        env[f"HUBUUM_CONTRACT_{variant}_STARTTLS_URI"] = f"ldap://127.0.0.1:{plain}"
        env[f"HUBUUM_CONTRACT_{variant}_CONTAINER"] = container

    # Separate servers make failure scenarios deterministic without shared chaos state.
    variants = (("SMTP", tls, ""), ("SMTP_TEMPORARY", tls, "Recipient:451:100"),
                ("SMTP_PERMANENT", tls, "Recipient:550:100"), ("SMTP_UNTRUSTED", untrusted, ""))
    for variant, certs, chaos in variants:
        container = f"hubuum-transport-contract-{variant.lower()}-{suffix}"
        smtp, api = ports(2)
        containers.append(container)
        command(
            "docker", "run", "--detach", "--name", container, "--network", network,
            "--publish", f"127.0.0.1:{smtp}:1025", "--publish", f"127.0.0.1:{api}:8025",
            "--volume", f"{certs}:/fixtures:ro,z",
            "--env", "MP_SMTP_TLS_CERT=/fixtures/server.pem",
            "--env", "MP_SMTP_TLS_KEY=/fixtures/server.key",
            "--env", "MP_SMTP_REQUIRE_TLS=true",
            "--env", f"MP_SMTP_AUTH=contract:{password}",
            "--env", f"MP_CHAOS_TRIGGERS={chaos}", SMTP_IMAGE,
        )
        management = f"http://127.0.0.1:{api}"
        def probe():
            with urllib.request.urlopen(management + "/api/v1/messages", timeout=2) as response:
                json.load(response)
        ready(probe, variant)
        env[f"HUBUUM_CONTRACT_{variant}_URI"] = f"smtps://contract:{{secret}}@127.0.0.1:{smtp}"
        env[f"HUBUUM_CONTRACT_{variant}_MANAGEMENT"] = management
        env[f"HUBUUM_CONTRACT_{variant}_CONTAINER"] = container
    return env
