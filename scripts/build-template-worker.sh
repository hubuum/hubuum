#!/usr/bin/env bash
set -euo pipefail

# Print Cargo's executable path, including when the target directory is relocated.
cargo build "$@" --locked -p hubuum-templates --bin hubuum-template-worker \
    --message-format=json | python3 -c '
import json, sys
workers = [entry["executable"] for line in sys.stdin
           if (entry := json.loads(line)).get("reason") == "compiler-artifact"
           and entry["target"]["name"] == "hubuum-template-worker"
           and entry.get("executable")]
if len(workers) != 1:
    sys.exit("Cargo did not report exactly one template worker executable")
print(workers[0])
'
