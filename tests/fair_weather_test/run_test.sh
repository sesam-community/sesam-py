#!/bin/bash

set -e

function fail {
  echo $1 >&2
  exit 1
}

function retry {
  local n=1
  local max=3
  local delay=15
  while true; do
    "$@" && break || {
      if [[ $n -lt $max ]]; then
        ((n++))
        echo "Command failed. Attempt $n/$max:"
        sleep $delay;
      else
        fail "The command has failed after $n attempts."
      fi
    }
  done
}

sleep 15

echo "Running fairweather tests.."

# Start with a clean slate to avoid "singlenode" issues from any other tests
retry $SESAM_CLIENT -node $NODE_URL -jwt $PUBLIC_CI_TOKEN -skip-tls-verification -vv reset

sleep 5

retry $SESAM_CLIENT -node $NODE_URL -jwt $PUBLIC_CI_TOKEN -skip-tls-verification -vv -print-scheduler-log -whitelist-file whitelist.txt -run-unit-tests tests test
retry $SESAM_CLIENT -node $NODE_URL -jwt $PUBLIC_CI_TOKEN -skip-tls-verification -vv update
retry $SESAM_CLIENT -node $NODE_URL -jwt $PUBLIC_CI_TOKEN -skip-tls-verification -vv status
retry $SESAM_CLIENT -node $NODE_URL -jwt $PUBLIC_CI_TOKEN -skip-tls-verification -dump -vv download -sesamconfig-file .mysesamconfig.json

# Clean up completely after a run
retry $SESAM_CLIENT -node $NODE_URL -jwt $PUBLIC_CI_TOKEN -skip-tls-verification -vv reset

if [ ! -f "sesam-config.zip" ]; then
    echo "Test of download failed!"
    exit 1
fi

echo "Fairweather tests passed!"
