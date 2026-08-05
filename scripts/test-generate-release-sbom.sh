#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
temp_root="$(mktemp -d "${TMPDIR:-/tmp}/hubuum-sbom-test.XXXXXX")"
trap 'rm -rf "$temp_root"' EXIT

printf 'release archive contents' > "$temp_root/archive.tar.gz"
cat > "$temp_root/metadata.json" <<'JSON'
{
  "packages": [
    {
      "id": "path+file:///hubuum#hubuum@1.2.3",
      "name": "hubuum",
      "version": "1.2.3",
      "source": null,
      "license": "MIT",
      "checksum": null,
      "repository": "https://github.com/hubuum/hubuum",
      "homepage": null,
      "documentation": null
    },
    {
      "id": "registry+https://github.com/rust-lang/crates.io-index#serde@1.0.0",
      "name": "serde",
      "version": "1.0.0",
      "source": "registry+https://github.com/rust-lang/crates.io-index",
      "license": "MIT OR Apache-2.0",
      "checksum": "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
      "repository": null,
      "homepage": null,
      "documentation": null
    }
  ],
  "resolve": {
    "nodes": [
      {
        "id": "path+file:///hubuum#hubuum@1.2.3",
        "deps": [
          {
            "pkg": "registry+https://github.com/rust-lang/crates.io-index#serde@1.0.0"
          }
        ]
      },
      {
        "id": "registry+https://github.com/rust-lang/crates.io-index#serde@1.0.0",
        "deps": []
      }
    ]
  },
  "workspace_members": ["path+file:///hubuum#hubuum@1.2.3"]
}
JSON

cat > "$temp_root/base.cdx.json" <<'JSON'
{
  "bomFormat": "CycloneDX",
  "specVersion": "1.6",
  "version": 1,
  "metadata": {"component": {"bom-ref": "base-image"}},
  "components": [
    {"type": "library", "bom-ref": "pkg:apk/alpine/ca-certificates@1", "name": "ca-certificates", "version": "1"},
    {"type": "library", "bom-ref": "pkg:generic/base-one@1", "name": "base-one", "version": "1"},
    {"type": "library", "bom-ref": "pkg:generic/base-two@1", "name": "base-two", "version": "1"},
    {"type": "library", "name": "unreferenced", "version": "1"}
  ],
  "dependencies": [
    {"ref": "base-image", "dependsOn": ["pkg:apk/alpine/ca-certificates@1"]},
    {"ref": "pkg:apk/alpine/ca-certificates@1", "dependsOn": ["pkg:generic/base-one@1"]},
    {"ref": "pkg:apk/alpine/ca-certificates@1", "dependsOn": ["pkg:generic/base-two@1"]}
  ]
}
JSON

cd "$repo_root"
python3 scripts/generate-release-sbom.py \
  --artifact "$temp_root/archive.tar.gz" \
  --base-sbom "$temp_root/base.cdx.json" \
  --metadata-json "$temp_root/metadata.json" \
  --output "$temp_root/release.cdx.json" \
  --source-revision 0123456789012345678901234567890123456789 \
  --source-tag v1.2.3 \
  --target linux-amd64

python3 - "$temp_root/release.cdx.json" <<'PY'
import hashlib
import json
import pathlib
import sys

document = json.loads(pathlib.Path(sys.argv[1]).read_text(encoding="utf-8"))
assert document["bomFormat"] == "CycloneDX"
assert document["specVersion"] == "1.6"
components = {component["name"] for component in document["components"]}
assert {"hubuum", "serde", "ca-certificates", "unreferenced"} <= components
root = document["metadata"]["component"]
expected = hashlib.sha256(b"release archive contents").hexdigest()
assert root["hashes"] == [{"alg": "SHA-256", "content": expected}]
assert any(dependency["ref"] == root["bom-ref"] for dependency in document["dependencies"])
ca_dependency = next(
    dependency
    for dependency in document["dependencies"]
    if dependency["ref"] == "pkg:apk/alpine/ca-certificates@1"
)
assert ca_dependency["dependsOn"] == [
    "pkg:generic/base-one@1",
    "pkg:generic/base-two@1",
]
PY

if python3 scripts/generate-release-sbom.py \
  --metadata-json "$temp_root/metadata.json" \
  --output "$temp_root/invalid-digest.cdx.json" \
  --subject-name example.invalid/hubuum \
  --subject-digest "sha256:$(printf 'A%.0s' {1..64})" \
  --subject-type container \
  --source-revision 0123456789012345678901234567890123456789 \
  --source-tag v1.2.3 \
  --target linux-amd64 2>/dev/null; then
  echo "uppercase subject digest was unexpectedly accepted" >&2
  exit 1
fi

if python3 scripts/generate-release-sbom.py \
  --artifact "$temp_root/archive.tar.gz" \
  --metadata-json "$temp_root/metadata.json" \
  --output "$temp_root/invalid-revision.cdx.json" \
  --source-revision main \
  --source-tag v1.2.3 \
  --target linux-amd64 2>/dev/null; then
  echo "abbreviated source revision was unexpectedly accepted" >&2
  exit 1
fi

echo "Release SBOM generator tests passed."
