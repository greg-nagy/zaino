#!/usr/bin/env bash
set -euo pipefail

ENV_FILE=".env.testing-artifacts"
DOCKERFILE="Dockerfile.ci"
IMAGE_TAG="zaino-ci:local"

# Load the env file
set -a
source "$ENV_FILE"
set +a

echo "🚧 Building $IMAGE_TAG with:"
echo "  RUST_VERSION=$RUST_VERSION"
echo "  GO_VERSION=$GO_VERSION"
echo "  LIGHTWALLETD_VERSION=$LIGHTWALLETD_VERSION"
echo "  ZCASH_VERSION=$ZCASH_VERSION"
echo "  ZEBRA_VERSION=$ZEBRA_VERSION"

docker build -f "$DOCKERFILE" -t "$IMAGE_TAG" . \
  --build-arg RUST_VERSION="$RUST_VERSION" \
  --build-arg GO_VERSION="$GO_VERSION" \
  --build-arg LIGHTWALLETD_VERSION="$LIGHTWALLETD_VERSION" \
  --build-arg ZCASH_VERSION="$ZCASH_VERSION" \
  --build-arg ZEBRA_VERSION="$ZEBRA_VERSION"

