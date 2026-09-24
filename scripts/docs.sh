#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$repo_root"

command="${1:-build}"
version="${2:-main}"
case "$command" in
  build | serve) ;;
  *) echo "Usage: bash scripts/docs.sh [build|serve] [main|vX.Y.Z]" >&2; exit 2 ;;
esac
if [[ $# -gt 2 || ! "$version" =~ ^(main|v[0-9]+\.[0-9]+\.[0-9]+)$ ]]; then
  echo "Usage: bash scripts/docs.sh [build|serve] [main|vX.Y.Z]" >&2
  exit 2
fi

python3 scripts/check-python-version.py
python3 scripts/check-docs.py
mkdir -p target
source_root="$repo_root"
if [[ "$version" == main ]]; then
  source_sha="$(git rev-parse HEAD)"
else
  if [[ "$command" == serve ]]; then
    echo 'Use build for a release snapshot, then serve target/docs-site/ with a static server.' >&2
    exit 2
  fi
  source_sha="$(git rev-parse --verify "refs/tags/$version^{commit}")"
  source_root="$(mktemp -d "$repo_root/target/docs-release.XXXXXX")"
  trap 'rm -rf "$source_root"' EXIT
  git archive "$source_sha" | tar -x -C "$source_root"
fi
python3 scripts/docs-versions.py prepare --source "$source_root" \
  --destination target/docs-source --version "$version" --source-sha "$source_sha"
python3 scripts/check-docs.py --root target/docs-source
# shellcheck source=../.github/docs-tools.env
source .github/docs-tools.env
mkdir -p target/docs-site target/docs-cache
mount_root="$repo_root/target/docs-source"
if [[ "$command" == serve ]]; then
  mount_root="$repo_root"
fi

options=(
  --rm
  --user "$(id -u):$(id -g)"
  --mount "type=bind,src=$mount_root/docs,dst=/docs/docs,readonly"
  --mount "type=bind,src=$mount_root/docs-site,dst=/docs/docs-site,readonly"
  --mount "type=bind,src=$repo_root/target/docs-source/zensical.toml,dst=/docs/zensical.toml,readonly"
  --mount "type=bind,src=$repo_root/target/docs-site,dst=/docs/site"
  --mount "type=bind,src=$repo_root/target/docs-cache,dst=/docs/.cache"
)

if [[ "$command" == serve ]]; then
  # Bind the host port to loopback; listen on all interfaces only inside Docker.
  # Mount working sources for live editing; the prepared config sets the edition.
  exec docker run "${options[@]}" \
    --publish 127.0.0.1:8000:8000 \
    "$ZENSICAL_IMAGE" serve --dev-addr 0.0.0.0:8000
fi

# Zensical clears the output on every build. Its --clean option also removes
# the cache directory itself, which is a bind mount here; retain that cache.
docker run "${options[@]}" "$ZENSICAL_IMAGE" build --strict
python3 scripts/check-docs.py --root target/docs-source --site-dir target/docs-site
cp target/docs-source/build.json target/docs-site/build.json
