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
cp "$repository_root/scripts/check-migration-check-replacements.py" "$test_root/scripts/"
python3 "$repository_root/scripts/test-migration-check-replacements.py"
printf '%s\n' 'SELECT 1;' 'SELECT 2;' 'SELECT 3;' 'SELECT 4;' \
  "ALTER TABLE tasks ADD CONSTRAINT tasks_kind_check CHECK (kind IN ('import', 'export'));" \
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

for scenario in bounded unmarked missing-lock missing-statement zero-timeout late-limits nontransactional; do
  candidate="$adapter_migrations/0002_candidate"
  rm -f "$candidate/metadata.toml"
  {
    if [[ "$scenario" != missing-lock && "$scenario" != late-limits ]]; then
      printf '%s\n' "SET LOCAL lock_timeout = '5s';"
    fi
    if [[ "$scenario" != missing-statement && "$scenario" != late-limits ]]; then
      printf '%s\n' "SET LOCAL statement_timeout = '60s';"
    fi
    if [[ "$scenario" == zero-timeout ]]; then
      printf '%s\n' "SET LOCAL statement_timeout = '0s';"
    fi
    if [[ "$scenario" == unmarked ]]; then
      printf '%s\n' 'CREATE INDEX widgets_revision_idx ON widgets (revision);'
    else
      printf '%s\n' 'CREATE INDEX widgets_revision_idx ON widgets (revision); -- hubuum-compat: bounded-transactional-index'
    fi
    if [[ "$scenario" == late-limits ]]; then
      printf '%s\n' "SET LOCAL lock_timeout = '5s';" "SET LOCAL statement_timeout = '60s';"
    fi
  } > "$candidate/up.sql"
  if [[ "$scenario" == nontransactional ]]; then
    printf '%s\n' 'run_in_transaction = false' > "$candidate/metadata.toml"
  fi
  git -C "$test_root" add .
  git -C "$test_root" commit --quiet -m "index-$scenario"
  if bash "$test_root/scripts/check-migration-compatibility.sh" v1.0.0 >/dev/null 2>&1; then
    if [[ "$scenario" != bounded ]]; then
      echo "$scenario index unexpectedly passed compatibility review" >&2
      exit 1
    fi
  elif [[ "$scenario" == bounded ]]; then
    echo "bounded transactional index unexpectedly failed compatibility review" >&2
    exit 1
  fi
done

for scenario in widened narrowing missing-lock missing-statement nontransactional; do
  candidate="$adapter_migrations/0002_candidate"
  rm -f "$candidate/metadata.toml"
  {
    if [[ "$scenario" != missing-lock ]]; then
      printf '%s\n' "SET LOCAL lock_timeout = '5s';"
    fi
    if [[ "$scenario" != missing-statement ]]; then
      printf '%s\n' "SET LOCAL statement_timeout = '60s';"
    fi
    printf '%s\n' 'ALTER TABLE tasks DROP CONSTRAINT tasks_kind_check; -- hubuum-compat: widen-enum-check'
    if [[ "$scenario" == narrowing ]]; then
      printf '%s\n' "ALTER TABLE tasks ADD CONSTRAINT tasks_kind_check CHECK (kind IN ('import')) NOT VALID;"
    else
      printf '%s\n' "ALTER TABLE tasks ADD CONSTRAINT tasks_kind_check CHECK (kind IN ('import', 'export', 'schema_validation')) NOT VALID;"
    fi
    printf '%s\n' 'ALTER TABLE tasks VALIDATE CONSTRAINT tasks_kind_check;'
  } > "$candidate/up.sql"
  if [[ "$scenario" == nontransactional ]]; then
    printf '%s\n' 'run_in_transaction = false' > "$candidate/metadata.toml"
  fi
  git -C "$test_root" add .
  git -C "$test_root" commit --quiet -m "enum-$scenario"
  if bash "$test_root/scripts/check-migration-compatibility.sh" v1.0.0 >/dev/null 2>&1; then
    if [[ "$scenario" != widened ]]; then
      echo "$scenario enum replacement unexpectedly passed compatibility review" >&2
      exit 1
    fi
  elif [[ "$scenario" == widened ]]; then
    echo "reviewed enum widening unexpectedly failed compatibility review" >&2
    exit 1
  fi
done

echo "Migration compatibility checker tests passed."
