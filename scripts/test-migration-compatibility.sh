#!/usr/bin/env bash
set -euo pipefail

repository_root="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)"
test_root="$(mktemp -d)"
trap 'rm -rf "$test_root"' EXIT

git -C "$test_root" init --quiet
git -C "$test_root" config user.email test@example.com
git -C "$test_root" config user.name test
git -C "$test_root" config commit.gpgsign false
legacy_migrations="$test_root/migrations"
adapter_migrations="$test_root/crates/hubuum-storage-postgres/migrations"
mkdir -p "$test_root/scripts" "$legacy_migrations/0001_safe"
cp "$repository_root/scripts/check-migration-compatibility.sh" "$test_root/scripts/"
printf '%s\n' 'SELECT 1;' 'SELECT 2;' 'SELECT 3;' 'SELECT 4;' \
  > "$legacy_migrations/0001_safe/up.sql"
git -C "$test_root" add .
git -C "$test_root" commit --quiet -m baseline
git -C "$test_root" tag v1.0.0

git -C "$test_root" switch --quiet -c modified-relocation
mkdir -p "$adapter_migrations"
git -C "$test_root" mv migrations/0001_safe \
  crates/hubuum-storage-postgres/migrations/0001_safe
printf '%s\n' 'ALTER TABLE widgets DROP COLUMN name;' \
  >> "$adapter_migrations/0001_safe/up.sql"
git -C "$test_root" add .
git -C "$test_root" commit --quiet -m modified-relocation
if bash "$test_root/scripts/check-migration-compatibility.sh" v1.0.0 >/dev/null 2>&1; then
  echo "modified relocated migration unexpectedly passed compatibility review" >&2
  exit 1
fi

git -C "$test_root" switch --quiet -c candidate v1.0.0
mkdir -p "$adapter_migrations"
git -C "$test_root" mv migrations/0001_safe \
  crates/hubuum-storage-postgres/migrations/0001_safe
mkdir -p "$adapter_migrations/0002_candidate"
printf '%s\n' \
  'ALTER TABLE widgets ADD COLUMN revision BIGINT NOT NULL DEFAULT 1;' \
  'ALTER TABLE widgets ADD CONSTRAINT widgets_revision_positive CHECK (revision > 0) NOT VALID;' \
  'ALTER TABLE widgets VALIDATE CONSTRAINT widgets_revision_positive;' \
  'CREATE INDEX CONCURRENTLY widgets_revision_idx ON widgets (revision);' \
  > "$adapter_migrations/0002_candidate/up.sql"
git -C "$test_root" add .
git -C "$test_root" commit --quiet -m safe
review_output="$(bash "$test_root/scripts/check-migration-compatibility.sh" v1.0.0)"
if [[ "$review_output" != *"passed for 1 migration file(s)"* ]]; then
  echo "unchanged relocated migration was unexpectedly reviewed" >&2
  exit 1
fi

git -C "$test_root" tag v1.1.0
GITHUB_REF_TYPE=tag GITHUB_REF_NAME=v1.1.0 \
  bash "$test_root/scripts/check-migration-compatibility.sh" >/dev/null

printf '%s\n' 'ALTER TABLE widgets DROP COLUMN name;' > "$adapter_migrations/0002_candidate/up.sql"
git -C "$test_root" add .
git -C "$test_root" commit --quiet -m unsafe
if bash "$test_root/scripts/check-migration-compatibility.sh" v1.0.0 >/dev/null 2>&1; then
  echo "unsafe migration unexpectedly passed compatibility review" >&2
  exit 1
fi

printf '%s\n' 'ALTER TABLE widgets ADD CONSTRAINT widgets_name_required CHECK (length(name) > 0);' \
  > "$adapter_migrations/0002_candidate/up.sql"
git -C "$test_root" add .
git -C "$test_root" commit --quiet -m unsafe-constraint
if bash "$test_root/scripts/check-migration-compatibility.sh" v1.0.0 >/dev/null 2>&1; then
  echo "blocking constraint unexpectedly passed compatibility review" >&2
  exit 1
fi

printf '%s\n' \
  'ALTER TABLE widgets' \
  '    ADD CONSTRAINT widgets_name_required' \
  '    CHECK (length(name) > 0);' \
  > "$adapter_migrations/0002_candidate/up.sql"
git -C "$test_root" add .
git -C "$test_root" commit --quiet -m unsafe-multiline-constraint
if bash "$test_root/scripts/check-migration-compatibility.sh" v1.0.0 >/dev/null 2>&1; then
  echo "multiline blocking constraint unexpectedly passed compatibility review" >&2
  exit 1
fi

echo "Migration compatibility checker tests passed."
