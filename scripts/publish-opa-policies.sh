#!/usr/bin/env bash
# Bundle the contents of ./policies into bundle.tar.gz.

set -euo pipefail

OPA_BUNDLE_KEY="${OPA_BUNDLE_KEY:-bundle.tar.gz}"
POLICIES_DIR="${POLICIES_DIR:-$(git rev-parse --show-toplevel)/policies}"
export AWS_REGION="${AWS_REGION:-europe-1}"

if [[ ! -d "$POLICIES_DIR" ]]; then
  echo "policies dir not found: $POLICIES_DIR" >&2
  exit 1
fi

bundle="$POLICIES_DIR/$OPA_BUNDLE_KEY"

echo "==> Bundling $POLICIES_DIR -> $bundle"
tar --exclude="$OPA_BUNDLE_KEY" -czf "$bundle" -C "$POLICIES_DIR" .
