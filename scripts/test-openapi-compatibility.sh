#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
checker="$repo_root/scripts/check-openapi-compatibility.sh"
oasdiff_bin="${OASDIFF_BIN:-oasdiff}"
temporary_dir="$(mktemp -d)"
trap 'rm -rf "$temporary_dir"' EXIT

base="$temporary_dir/base.json"
candidate="$temporary_dir/candidate.json"
metadata="$temporary_dir/metadata.json"
exceptions="$temporary_dir/exceptions.json"
changelog="$temporary_dir/CHANGELOG.md"

sha256_file() {
  if command -v sha256sum >/dev/null 2>&1; then
    sha256sum "$1" | awk '{print $1}'
  else
    shasum -a 256 "$1" | awk '{print $1}'
  fi
}

write_base() {
  jq --null-input '{
    openapi: "3.1.0",
    info: {title: "contract fixture", version: "1.0.0"},
    paths: {
      "/items": {
        post: {
          parameters: [{
            name: "filter",
            in: "query",
            required: false,
            style: "form",
            explode: true,
            schema: {type: "array", maxItems: 10, items: {type: "string"}}
          }],
          requestBody: {
            required: true,
            content: {
              "application/json": {
                schema: {
                  type: "object",
                  required: ["kind"],
                  properties: {
                    kind: {type: "string", enum: ["one", "two"]},
                    note: {type: ["string", "null"]}
                  }
                }
              }
            }
          },
          responses: {
            "200": {
              description: "ok",
              content: {
                "application/json": {
                  schema: {
                    type: "object",
                    discriminator: {propertyName: "kind"},
                    required: ["id"],
                    properties: {id: {type: "integer", format: "int64"}}
                  }
                }
              }
            }
          }
        }
      }
    }
  }' > "$base"
}

write_metadata() {
  local baseline_sha256
  baseline_sha256="$(sha256_file "$base")"
  jq --null-input \
    --arg source "fixture:$base" \
    --arg baseline_sha256 "$baseline_sha256" \
    '{
      status: "available",
      tag: "v1.0.0",
      source: $source,
      sha256: $baseline_sha256,
      reason: null,
      candidate_ref: "test",
      candidate_sha: "test"
    }' > "$metadata"
}

write_empty_exceptions() {
  jq --null-input '{schema_version: 1, exceptions: []}' > "$exceptions"
}

write_changelog() {
  local extra="${1:-}"
  {
    echo "# Changelog"
    echo
    echo "## [Unreleased]"
    echo
    echo "$extra"
    echo
    echo "## [1.0.0]"
  } > "$changelog"
}

run_checker() {
  local report_dir="$1"
  OASDIFF_BIN="$oasdiff_bin" \
    HUBUUM_OPENAPI_TODAY="${HUBUUM_OPENAPI_TODAY:-2026-08-04}" \
    "$checker" \
    "$base" \
    "$candidate" \
    "$metadata" \
    "$exceptions" \
    "$changelog" \
    "$report_dir"
}

assert_rejected_with() {
  local name="$1"
  local expected="$2"
  local report_dir="$temporary_dir/$name"
  local stderr_path="$report_dir.stderr"
  if run_checker "$report_dir" >/dev/null 2>"$stderr_path"; then
    echo "Expected invalid fixture '$name' to be rejected" >&2
    exit 1
  fi
  if ! grep --fixed-strings --quiet "$expected" "$stderr_path"; then
    echo "Expected invalid fixture '$name' to report '$expected', got:" >&2
    cat "$stderr_path" >&2
    exit 1
  fi
}

assert_policy_fails_with() {
  local name="$1"
  local expected="$2"
  local report_dir="$temporary_dir/$name"
  if run_checker "$report_dir" >/dev/null 2>&1; then
    echo "Expected policy fixture '$name' to fail" >&2
    exit 1
  fi
  jq --exit-status \
    --arg expected "$expected" \
    '.exception_errors | any(contains($expected))' \
    "$report_dir/compatibility.json" >/dev/null
}

assert_passes() {
  local name="$1"
  local report_dir="$temporary_dir/$name"
  if ! run_checker "$report_dir"; then
    echo "Expected compatible fixture '$name' to pass" >&2
    exit 1
  fi
  jq --exit-status '.status == "pass"' "$report_dir/compatibility.json" >/dev/null
}

assert_fails_with() {
  local name="$1"
  local finding_id="$2"
  local report_dir="$temporary_dir/$name"
  if run_checker "$report_dir" >/dev/null 2>&1; then
    echo "Expected incompatible fixture '$name' to fail" >&2
    exit 1
  fi
  jq --exit-status \
    --arg finding_id "$finding_id" \
    '.breaking | any(.id == $finding_id and .accepted == false)' \
    "$report_dir/compatibility.json" >/dev/null
}

write_base
write_metadata
write_empty_exceptions
write_changelog

# The test override records an exact baseline tag and digest without network access.
resolver_report="$temporary_dir/resolver"
HUBUUM_OPENAPI_BASELINE_FILE="$base" \
  HUBUUM_OPENAPI_BASELINE_TAG="v1.0.0" \
  "$repo_root/scripts/resolve-openapi-baseline.sh" "$resolver_report"
jq --exit-status \
  '.status == "available" and .tag == "v1.0.0" and (.sha256 | length == 64)' \
  "$resolver_report/baseline-metadata.json" >/dev/null

# A baseline document cannot be labeled as a different release.
jq '.info.version = "0.9.0"' "$base" > "$temporary_dir/wrong-version-baseline.json"
resolver_error="$temporary_dir/resolver-version.stderr"
if HUBUUM_OPENAPI_BASELINE_FILE="$temporary_dir/wrong-version-baseline.json" \
  HUBUUM_OPENAPI_BASELINE_TAG="v1.0.0" \
  "$repo_root/scripts/resolve-openapi-baseline.sh" \
    "$temporary_dir/resolver-wrong-version" \
    >/dev/null 2>"$resolver_error"; then
  echo "Expected the resolver to reject a mismatched baseline version" >&2
  exit 1
fi
grep --fixed-strings --quiet \
  "OpenAPI baseline document does not match tag v1.0.0" \
  "$resolver_error"

# The compared baseline must match the digest recorded by the resolver.
cp "$base" "$candidate"
jq '.sha256 = "0000000000000000000000000000000000000000000000000000000000000000"' \
  "$metadata" > "$temporary_dir/wrong-digest.json"
cp "$temporary_dir/wrong-digest.json" "$metadata"
assert_rejected_with "baseline-digest" "OpenAPI baseline digest mismatch"
write_metadata

# The checker independently binds the baseline document version to its tag.
cp "$temporary_dir/wrong-version-baseline.json" "$base"
cp "$base" "$candidate"
write_metadata
assert_rejected_with "baseline-version" "OpenAPI baseline document does not match metadata tag v1.0.0"
write_base
write_metadata

# Candidate metadata needed for changelog scoping is required even without oasdiff.
jq 'del(.info.version)' "$base" > "$candidate"
assert_rejected_with "candidate-version" "Invalid candidate OpenAPI document"

# Repositories without a stable release record an explicit skipped result.
cp "$base" "$candidate"
jq '.status = "unavailable"
  | .tag = null
  | .sha256 = null
  | .reason = "No stable release fixture."' "$metadata" > "$temporary_dir/unavailable.json"
cp "$temporary_dir/unavailable.json" "$metadata"
no_baseline_report="$temporary_dir/no-baseline"
run_checker "$no_baseline_report"
jq --exit-status '.status == "skipped"' "$no_baseline_report/compatibility.json" >/dev/null

# A missing baseline does not bypass validation of the checked-in policy.
jq --null-input '{
  schema_version: 1,
  exceptions: [{
    id: "invalid-date",
    baseline: "v1.0.0",
    expires: "2026-02-29",
    reason: "fixture",
    migration: "fixture",
    changelog_entry: "fixture",
    fingerprints: ["fixture"]
  }]
}' > "$exceptions"
assert_rejected_with "invalid-expiry" "Invalid OpenAPI compatibility exception file"

jq '.exceptions[0].expires = "2099-01-01" | .exceptions[0].reason = "   "' \
  "$exceptions" > "$temporary_dir/blank-reason.json"
cp "$temporary_dir/blank-reason.json" "$exceptions"
assert_rejected_with "blank-reason" "Invalid OpenAPI compatibility exception file"
write_empty_exceptions
write_metadata

HUBUUM_OPENAPI_TODAY="2026-02-29" \
  assert_rejected_with "invalid-policy-date" "Invalid OpenAPI policy date"

# Additive operations and optional response fields are compatible.
jq '.paths["/health"] = {get: {responses: {"200": {description: "ok"}}}}
  | .paths["/items"].post.responses["200"].content["application/json"].schema.properties.label = {type: "string"}' \
  "$base" > "$candidate"
assert_passes "additive"

# Representative incompatible changes must be classified as breaking.
jq 'del(.paths["/items"].post)' "$base" > "$candidate"
assert_fails_with "removed-operation" "api-removed-without-deprecation"

jq '.paths["/items"].post.requestBody.content["application/json"].schema.required += ["new_required"]
  | .paths["/items"].post.requestBody.content["application/json"].schema.properties.new_required = {type: "string"}' \
  "$base" > "$candidate"
assert_fails_with "required-request-field" "new-required-request-property"

jq '.paths["/items"].post.requestBody.content["application/json"].schema.properties.kind.enum = ["one"]' \
  "$base" > "$candidate"
assert_fails_with "narrowed-request-enum" "request-property-enum-value-removed"

jq '.paths["/items"].post.security = [{bearer_auth: []}]
  | .components.securitySchemes.bearer_auth = {type: "http", scheme: "bearer"}' \
  "$base" > "$candidate"
assert_fails_with "security-required" "api-security-added"

jq '.paths["/items"].post.requestBody.content["application/json"].schema.properties.note.type = "string"' \
  "$base" > "$candidate"
assert_fails_with "request-nullability" "request-property-became-not-nullable"

jq '.paths["/items"].post.parameters[0].schema.maxItems = 5' "$base" > "$candidate"
assert_fails_with "array-bound" "request-parameter-max-items-decreased"

jq '.paths["/items"].post.parameters[0].style = "spaceDelimited"' "$base" > "$candidate"
assert_fails_with "serialization" "request-parameter-serialization-changed"

jq '.paths["/items"].post.parameters[0].in = "header"
  | .paths["/items"].post.parameters[0].style = "simple"' "$base" > "$candidate"
assert_fails_with "parameter-location" "request-parameter-location-changed"

jq '.paths["/items"].post.responses["200"].content["application/json"].schema.discriminator.propertyName = "type"' \
  "$base" > "$candidate"
assert_fails_with "discriminator" "response-body-discriminator-property-name-changed"

jq '.paths["/items"].post.responses["200"].content["application/json"].schema.properties.id.format = "uuid"' \
  "$base" > "$candidate"
assert_fails_with "response-format" "response-property-type-changed"

# A narrow, current-baseline exception with changelog migration guidance passes.
jq 'del(.paths["/items"].post)' "$base" > "$candidate"
fingerprint="$($oasdiff_bin changelog \
  "$base" \
  "$candidate" \
  --allow-external-refs=false \
  --severity-levels "$repo_root/.github/oasdiff-severity-levels.txt" \
  --format json \
  | jq --raw-output '.[] | select(.id == "api-removed-without-deprecation") | .fingerprint')"
migration="Clients must use POST /replacement before upgrading."
changelog_entry="Breaking API: removed POST /items."
write_changelog "$changelog_entry $migration"
jq --null-input \
  --arg fingerprint "$fingerprint" \
  --arg migration "$migration" \
  --arg changelog_entry "$changelog_entry" \
  '{
    schema_version: 1,
    exceptions: [{
      id: "v1.0.0-accepted-fixture-break",
      baseline: "v1.0.0",
      expires: "2099-01-01",
      reason: "The fixture endpoint was replaced.",
      migration: $migration,
      changelog_entry: $changelog_entry,
      fingerprints: [$fingerprint]
  }]
}' > "$exceptions"
cp "$exceptions" "$temporary_dir/accepted-policy.json"
assert_passes "accepted-break"
jq --exit-status \
  '.counts.accepted == 1 and .breaking[0].exception_id == "v1.0.0-accepted-fixture-break"' \
  "$temporary_dir/accepted-break/compatibility.json" >/dev/null

# Exception IDs remain unique so reports and review references are unambiguous.
jq '.exceptions += [.exceptions[0]]' "$exceptions" > "$temporary_dir/duplicate-id.json"
cp "$temporary_dir/duplicate-id.json" "$exceptions"
assert_policy_fails_with "duplicate-id" "duplicate exception id"
cp "$temporary_dir/accepted-policy.json" "$exceptions"

# Candidate versions are matched as literal changelog headings, not regular expressions.
jq '.info.version = "1.0.1"' "$candidate" > "$temporary_dir/release-candidate.json"
cp "$temporary_dir/release-candidate.json" "$candidate"
{
  echo "# Changelog"
  echo
  echo "## [Unreleased]"
  echo
  echo "## [1x0x1] - 2026-08-04"
  echo
  echo "$changelog_entry $migration"
  echo
  echo "## [1.0.0]"
} > "$changelog"
assert_fails_with "literal-release-heading" "api-removed-without-deprecation"

# Release preparation may move the approved text into the candidate version section.
{
  echo "# Changelog"
  echo
  echo "## [Unreleased]"
  echo
  echo "## [1.0.1] - 2026-08-04"
  echo
  echo "$changelog_entry $migration"
  echo
  echo "## [1.0.0]"
} > "$changelog"
assert_passes "release-changelog-section"
jq --exit-status \
  '.policy.changelog_sections == ["Unreleased", "1.0.1"]' \
  "$temporary_dir/release-changelog-section/compatibility.json" >/dev/null

# Baseline mismatch and expiry automatically stop an exception from applying.
jq '.exceptions[0].baseline = "v0.9.0"' "$exceptions" > "$temporary_dir/mismatch.json"
cp "$temporary_dir/mismatch.json" "$exceptions"
assert_fails_with "baseline-mismatch" "api-removed-without-deprecation"

jq '.exceptions[0].baseline = "v1.0.0" | .exceptions[0].expires = "2000-01-01"' \
  "$exceptions" > "$temporary_dir/expired.json"
cp "$temporary_dir/expired.json" "$exceptions"
assert_fails_with "expired-exception" "api-removed-without-deprecation"

echo "OpenAPI compatibility tests passed."
