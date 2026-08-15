#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
# shellcheck source=.github/treetop-conformance.env
source "$repo_root/.github/treetop-conformance.env"

report_dir="${HUBUUM_TREETOP_REPORT_DIR:-$repo_root/target/treetop-conformance}"
fixture_dir="$repo_root/docs/treetop"
runtime_dir="$(mktemp -d)"
container_name="hubuum-treetop-conformance-${GITHUB_RUN_ID:-local}-$$"
fixture_pid=""
tls_proxy_pid=""
container_started=false

mkdir -p "$report_dir"

capture_service_logs() {
  if [[ "$container_started" == "true" ]]; then
    docker logs "$container_name" > "$report_dir/treetop.log" 2>&1 || true
    docker inspect "$container_name" > "$report_dir/treetop-container.json" 2>&1 || true
  fi
}

# ShellCheck cannot see that this function is invoked by the EXIT trap.
# shellcheck disable=SC2317
cleanup() {
  set +e
  capture_service_logs
  if [[ -n "$tls_proxy_pid" ]]; then
    kill "$tls_proxy_pid" 2>/dev/null || true
  fi
  if [[ -n "$fixture_pid" ]]; then
    kill "$fixture_pid" 2>/dev/null || true
  fi
  if [[ "$container_started" == "true" ]]; then
    docker rm --force "$container_name" >/dev/null 2>&1 || true
  fi
  rm -rf "$runtime_dir"
}
trap cleanup EXIT

for command_name in cargo curl docker jq openssl python3 socat; do
  if ! command -v "$command_name" >/dev/null; then
    echo "Required command is unavailable: $command_name" >&2
    exit 1
  fi
done

python3 "$repo_root/scripts/test-serve-treetop-fixture.py"

case "$HUBUUM_TREETOP_TEST_IMAGE" in
  *@sha256:????????????????????????????????????????????????????????????????) ;;
  *)
    echo "Treetop image must be pinned by a full SHA-256 digest" >&2
    exit 1
    ;;
esac
if [[ ! "$HUBUUM_TREETOP_TEST_REVISION" =~ ^[0-9a-f]{40}$ ]]; then
  echo "Treetop source revision must be a full hexadecimal commit SHA" >&2
  exit 1
fi
if [[ ! "$HUBUUM_TREETOP_EXPECTED_TESTS" =~ ^[1-9][0-9]*$ ]]; then
  echo "Expected Treetop test count must be a positive integer" >&2
  exit 1
fi

jq --null-input \
  --arg image "$HUBUUM_TREETOP_TEST_IMAGE" \
  --arg revision "$HUBUUM_TREETOP_TEST_REVISION" \
  '{schema_version: 1, image: $image, source_revision: $revision}' \
  > "$report_dir/fixture.json"

python3 "$repo_root/scripts/serve-treetop-fixture.py" \
  --port "$HUBUUM_TREETOP_FIXTURE_PORT" \
  --bind 127.0.0.1 \
  --directory "$fixture_dir" \
  > "$report_dir/fixture-http.log" 2>&1 &
fixture_pid=$!

docker run --detach \
  --name "$container_name" \
  --label hubuum.test=treetop-conformance \
  --network host \
  --env TREETOP_LISTEN=127.0.0.1 \
  --env "TREETOP_PORT=$HUBUUM_TREETOP_TEST_PORT" \
  --env TREETOP_WORKERS=2 \
  --env TREETOP_RAYON_THREADS=2 \
  --env TREETOP_CLIENT_ALLOWLIST=127.0.0.1,::1 \
  --env TREETOP_TRUST_IP_HEADERS=false \
  --env TREETOP_ALLOW_UPLOAD=false \
  --env "TREETOP_POLICY_URL=http://127.0.0.1:$HUBUUM_TREETOP_FIXTURE_PORT/test-fixture.cedar" \
  --env TREETOP_POLICY_UPDATE_FREQUENCY=1 \
  --env "TREETOP_SCHEMA_URL=http://127.0.0.1:$HUBUUM_TREETOP_FIXTURE_PORT/schema.json" \
  --env TREETOP_SCHEMA_UPDATE_FREQUENCY=1 \
  --env TREETOP_SCHEMA_VALIDATION_MODE=strict \
  --env RUST_LOG=warn \
  "$HUBUUM_TREETOP_TEST_IMAGE" \
  > "$report_dir/container-id.txt"
container_started=true

service_url="http://127.0.0.1:$HUBUUM_TREETOP_TEST_PORT"
service_ready=false
for attempt in $(seq 1 120); do
  if curl --fail --silent --show-error "$service_url/api/v1/status" \
      > "$report_dir/status.json" 2> "$report_dir/status-error.log"; then
    if jq --exit-status \
      '.policy_configuration.policies.entries > 0 and
       .policy_configuration.schema.entries > 0 and
       .request_context.schema_backed == true' \
      "$report_dir/status.json" >/dev/null; then
      service_ready=true
      break
    fi
  fi
  if [[ "$attempt" == 120 ]]; then
    break
  fi
  sleep 0.5
done
if [[ "$service_ready" != "true" ]]; then
  echo "Pinned Treetop fixture did not load its schema and policies within 60 seconds" >&2
  capture_service_logs
  exit 1
fi

ca_key="$runtime_dir/ca.key"
ca_cert="$runtime_dir/ca.pem"
server_key="$runtime_dir/server.key"
server_request="$runtime_dir/server.csr"
server_cert="$runtime_dir/server.pem"
server_extensions="$runtime_dir/server.ext"

openssl req -x509 -newkey rsa:2048 -nodes \
  -keyout "$ca_key" \
  -out "$ca_cert" \
  -days 1 \
  -sha256 \
  -subj '/CN=Hubuum Treetop Conformance CA' \
  > "$report_dir/openssl.log" 2>&1
openssl req -newkey rsa:2048 -nodes \
  -keyout "$server_key" \
  -out "$server_request" \
  -subj '/CN=localhost' \
  >> "$report_dir/openssl.log" 2>&1
printf '%s\n' \
  'subjectAltName=DNS:localhost,IP:127.0.0.1' \
  'extendedKeyUsage=serverAuth' \
  > "$server_extensions"
openssl x509 -req \
  -in "$server_request" \
  -CA "$ca_cert" \
  -CAkey "$ca_key" \
  -CAcreateserial \
  -out "$server_cert" \
  -days 1 \
  -sha256 \
  -extfile "$server_extensions" \
  >> "$report_dir/openssl.log" 2>&1

socat \
  "OPENSSL-LISTEN:$HUBUUM_TREETOP_TLS_PORT,reuseaddr,fork,cert=$server_cert,key=$server_key,cafile=$ca_cert,verify=0" \
  "TCP:127.0.0.1:$HUBUUM_TREETOP_TEST_PORT" \
  > "$report_dir/tls-proxy.log" 2>&1 &
tls_proxy_pid=$!

tls_url="https://localhost:$HUBUUM_TREETOP_TLS_PORT"
tls_ready=false
for attempt in $(seq 1 30); do
  if curl --fail --silent --show-error \
      --cacert "$ca_cert" \
      "$tls_url/api/v1/health" >/dev/null; then
    tls_ready=true
    break
  fi
  if [[ "$attempt" == 30 ]]; then
    break
  fi
  sleep 0.25
done
if [[ "$tls_ready" != "true" ]]; then
  echo "Private-CA TLS proxy did not become ready" >&2
  exit 1
fi

export HUBUUM_TREETOP_TEST_URL="$service_url"
export HUBUUM_TREETOP_TLS_TEST_URL="$tls_url"
export HUBUUM_TREETOP_TEST_CA_CERT="$ca_cert"
export HUBUUM_TREETOP_TEST_CONTAINER_NAME="$container_name"
export HUBUUM_TREETOP_TEST_IMAGE
export HUBUUM_TREETOP_TEST_REVISION

cargo_test_args=(
  test
  --features 'permissions-treetop,integration-test-support'
  --locked
  --release
  'tests::permissions::live_treetop_parity::'
)

cargo "${cargo_test_args[@]}" -- --list \
  > "$report_dir/test-list.txt"
test_count="$(
  grep --count --extended-regexp \
    '^tests::permissions::live_treetop_parity::.*: test$' \
    "$report_dir/test-list.txt" || true
)"
if [[ "$test_count" -ne "$HUBUUM_TREETOP_EXPECTED_TESTS" ]]; then
  echo "Expected exactly $HUBUUM_TREETOP_EXPECTED_TESTS live Treetop tests, found $test_count" >&2
  exit 1
fi

set +e
cargo "${cargo_test_args[@]}" -- \
  --ignored \
  --test-threads=1 \
  --nocapture \
  2>&1 | tee "$report_dir/hubuum-tests.log"
test_status=${PIPESTATUS[0]}
set -e

capture_service_logs
if grep --recursive --fixed-strings --quiet \
  'treetop-conformance-secret-canary' "$report_dir"; then
  echo "Secret canary appeared in conformance diagnostics" >&2
  exit 1
fi

if [[ -n "${GITHUB_STEP_SUMMARY:-}" ]]; then
  {
    echo "### Treetop authorization conformance"
    echo
    echo "- Result: $([[ "$test_status" -eq 0 ]] && echo passed || echo failed)"
    echo "- Tests: $test_count"
    echo "- Image: \`$HUBUUM_TREETOP_TEST_IMAGE\`"
    echo "- Source revision: \`$HUBUUM_TREETOP_TEST_REVISION\`"
  } >> "$GITHUB_STEP_SUMMARY"
fi

exit "$test_status"
