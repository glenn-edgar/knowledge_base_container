#!/bin/bash
set -e

if [ -z "$1" ]; then
  echo "Usage: $0 <name-suffix>"
  exit 1
fi

SUFFIX="$1"
CONTAINER_NAME="mosquitto-ram-ws_${SUFFIX}"

docker run -d \
  --name "${CONTAINER_NAME}" \
  -p 1883:1883 \
  -p 9001:9001 \
  nanodatacenter/mosquitto-ram-ws:latest

