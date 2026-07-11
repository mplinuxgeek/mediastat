#!/usr/bin/env bash
# Bump ha-addon/config.yaml version, commit, tag, and push.
# Usage: ./release.sh [major|minor|patch]  (default: patch)
set -euo pipefail

cd "$(git rev-parse --show-toplevel)"

BUMP="${1:-patch}"
CONFIG="ha-addon/config.yaml"

if [[ -n "$(git status --porcelain)" ]]; then
  echo "Working tree not clean. Commit or stash changes first." >&2
  exit 1
fi

CURRENT="$(grep -m1 '^version:' "$CONFIG" | sed -E 's/version: *"([^"]+)"/\1/')"
IFS='.' read -r MAJOR MINOR PATCH <<< "$CURRENT"

case "$BUMP" in
  major) MAJOR=$((MAJOR + 1)); MINOR=0; PATCH=0 ;;
  minor) MINOR=$((MINOR + 1)); PATCH=0 ;;
  patch) PATCH=$((PATCH + 1)) ;;
  *) echo "Unknown bump type: $BUMP (use major|minor|patch)" >&2; exit 1 ;;
esac

NEW="${MAJOR}.${MINOR}.${PATCH}"
TAG="v${NEW}"

if git rev-parse "$TAG" >/dev/null 2>&1; then
  echo "Tag $TAG already exists." >&2
  exit 1
fi

sed -i "s/^version: .*/version: \"${NEW}\"/" "$CONFIG"
git add "$CONFIG"
git commit -m "chore: bump add-on version to ${NEW}"
git push origin main
git tag "$TAG"
git push origin "$TAG"

echo "Released ${TAG}"
