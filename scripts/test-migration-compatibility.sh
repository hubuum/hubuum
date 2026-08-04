#!/usr/bin/env bash
set -euo pipefail

repository_root="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)"
test_root="$(mktemp -d)"
trap 'rm -rf "$test_root"' EXIT

git -C "$test_root" init --quiet
git -C "$test_root" config user.email test@example.com
git -C "$test_root" config user.name test
git -C "$test_root" config commit.gpgsign false
mkdir -p "$test_root/scripts" "$test_root/migrations/0001_safe"
cp "$repository_root/scripts/check-migration-compatibility.sh" "$test_root/scripts/"
printf '%s\n' 'SELECT 1;' > "$test_root/migrations/0001_safe/up.sql"
git -C "$test_root" add .
git -C "$test_root" commit --quiet -m baseline
git -C "$test_root" tag v1.0.0

mkdir -p "$test_root/migrations/0002_candidate"
printf '%s\n' \
  'ALTER TABLE widgets ADD COLUMN revision BIGINT NOT NULL DEFAULT 1;' \
  'ALTER TABLE widgets ADD CONSTRAINT widgets_revision_positive CHECK (revision > 0) NOT VALID;' \
  'ALTER TABLE widgets VALIDATE CONSTRAINT widgets_revision_positive;' \
  'CREATE INDEX CONCURRENTLY widgets_revision_idx ON widgets (revision);' \
  > "$test_root/migrations/0002_candidate/up.sql"
git -C "$test_root" add .
git -C "$test_root" commit --quiet -m safe
bash "$test_root/scripts/check-migration-compatibility.sh" v1.0.0 >/dev/null

printf '%s\n' 'ALTER TABLE widgets DROP COLUMN name;' > "$test_root/migrations/0002_candidate/up.sql"
git -C "$test_root" add .
git -C "$test_root" commit --quiet -m unsafe
if bash "$test_root/scripts/check-migration-compatibility.sh" v1.0.0 >/dev/null 2>&1; then
  echo "unsafe migration unexpectedly passed compatibility review" >&2
  exit 1
fi

printf '%s\n' 'ALTER TABLE widgets ADD CONSTRAINT widgets_name_required CHECK (length(name) > 0);' \
  > "$test_root/migrations/0002_candidate/up.sql"
git -C "$test_root" add .
git -C "$test_root" commit --quiet -m unsafe-constraint
if bash "$test_root/scripts/check-migration-compatibility.sh" v1.0.0 >/dev/null 2>&1; then
  echo "blocking constraint unexpectedly passed compatibility review" >&2
  exit 1
fi

echo "Migration compatibility checker tests passed."
