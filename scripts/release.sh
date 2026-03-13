#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$repo_root"

usage() {
  cat <<'EOF'
Usage:
  ./scripts/release.sh prepare <version>
  ./scripts/release.sh tag

Commands:
  prepare <version>  Create release/v<version> from main, bump the package version,
                     roll Unreleased notes into the release section, regenerate
                     docs/openapi.json, and run release checks.
  tag                Verify main is clean and create annotated tag v<current-version>.
EOF
}

die() {
  echo "$*" >&2
  exit 1
}

ensure_clean_worktree() {
  if ! git diff --quiet --ignore-submodules HEAD --; then
    die "Working tree has uncommitted changes. Commit or stash them before running the release helper."
  fi

  if [[ -n "$(git ls-files --others --exclude-standard)" ]]; then
    die "Working tree has untracked files. Commit, move, or clean them before running the release helper."
  fi
}

current_branch() {
  git rev-parse --abbrev-ref HEAD
}

require_branch() {
  local expected="$1"
  local actual
  actual="$(current_branch)"
  if [[ "$actual" != "$expected" ]]; then
    die "Expected to be on branch $expected, but found $actual."
  fi
}

extract_version() {
  awk '
    $0 == "[package]" { in_package = 1; next }
    /^\[/ { in_package = 0 }
    in_package && $1 == "version" {
      if (match($0, /"([^"]+)"/)) {
        print substr($0, RSTART + 1, RLENGTH - 2)
        exit
      }
    }
  ' Cargo.toml
}

validate_version() {
  local version="$1"
  if [[ ! "$version" =~ ^[0-9]+\.[0-9]+\.[0-9]+([.-][0-9A-Za-z.-]+)?$ ]]; then
    die "Version must look like semantic versioning, for example 0.0.2 or 1.0.0-rc.1"
  fi
}

update_cargo_version() {
  local version="$1"
  local tmp
  tmp="$(mktemp)"

  awk -v version="$version" '
    $0 == "[package]" { in_package = 1 }
    /^\[/ && $0 != "[package]" { in_package = 0 }
    in_package && $1 == "version" {
      sub(/"[^"]+"/, "\"" version "\"")
    }
    { print }
  ' Cargo.toml > "$tmp"

  mv "$tmp" Cargo.toml
}

roll_unreleased_into_release() {
  local version="$1"
  local today="$2"

  if grep -Eq "^## \\[$version\\]" CHANGELOG.md; then
    return 0
  fi

  TARGET_VERSION="$version" TODAY="$today" perl -0pi -e '
    my $version = $ENV{TARGET_VERSION};
    my $today = $ENV{TODAY};
    my $updated = s/^## \[Unreleased\]\n(.*?)(?=^## \[|\z)/"## [Unreleased]\n\n## [$version] - $today\n" . $1/sme;
    die "CHANGELOG.md is missing an [Unreleased] section\n" unless $updated;
  ' CHANGELOG.md
}

prepare_release() {
  local version="$1"
  local branch="release/v$version"
  local tag="v$version"
  local current_version
  local today

  validate_version "$version"
  ensure_clean_worktree
  require_branch "main"

  if git show-ref --verify --quiet "refs/heads/$branch"; then
    die "Branch $branch already exists."
  fi

  if git show-ref --verify --quiet "refs/tags/$tag"; then
    die "Tag $tag already exists."
  fi

  current_version="$(extract_version)"
  if [[ "$current_version" == "$version" ]]; then
    die "Cargo.toml is already at version $version."
  fi

  git checkout -b "$branch"

  today="$(date +%F)"
  update_cargo_version "$version"
  roll_unreleased_into_release "$version" "$today"

  cargo run --bin hubuum-openapi --locked > docs/openapi.json
  cargo fmt --all

  ./scripts/check-version-bump.sh
  ./scripts/check-release-readiness.sh "$tag"

  cat <<EOF
Release branch created: $branch

Next steps:
1. Review and edit CHANGELOG.md if the release notes need cleanup.
2. Commit the release branch changes.
3. Open and merge the release branch.
4. After merge, check out main and run ./scripts/release.sh tag
EOF
}

tag_release() {
  local version
  local tag

  ensure_clean_worktree
  require_branch "main"

  version="$(extract_version)"
  tag="v$version"

  if git show-ref --verify --quiet "refs/tags/$tag"; then
    die "Tag $tag already exists."
  fi

  ./scripts/check-release-readiness.sh "$tag"
  git tag -a "$tag" -m "Release $tag"

  cat <<EOF
Created annotated tag $tag

Next steps:
1. Push main and the tag: git push origin main "$tag"
2. Wait for the GitHub release workflow to publish binaries and containers.
EOF
}

main() {
  if [[ $# -lt 1 ]]; then
    usage
    exit 1
  fi

  case "$1" in
    prepare)
      [[ $# -eq 2 ]] || die "prepare requires exactly one version argument."
      prepare_release "$2"
      ;;
    tag)
      [[ $# -eq 1 ]] || die "tag does not take any arguments."
      tag_release
      ;;
    -h|--help|help)
      usage
      ;;
    *)
      usage
      exit 1
      ;;
  esac
}

main "$@"
