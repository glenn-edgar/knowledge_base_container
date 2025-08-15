#!/bin/bash
set -e

# Change these
REPO="nanodatacenter"
IMAGE="mosquitto-ram-ws"


# Ensure QEMU/binfmt is installed
docker run --privileged --rm tonistiigi/binfmt --install all

# Create and use builder (if not exists)
docker buildx create --name multiarch --use || docker buildx use multiarch

# Build + push multi-arch image
docker buildx build \
  --platform linux/amd64,linux/arm64 \
  -t ${REPO}/${IMAGE}:latest \
  --push --load .


