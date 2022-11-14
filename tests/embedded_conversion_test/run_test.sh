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

echo "Running conversion test.."

rm -rf test_after
mkdir test_after
cp -r before/* test_after/

pushd test_after

retry $SESAM_CLIENT -node $NODE_URL -jwt $PUBLIC_CI_TOKEN -skip-tls-verification -vv wipe

# First, convert
retry $SESAM_CLIENT -node $NODE_URL -jwt $PUBLIC_CI_TOKEN -vv convert

# Then, test if it works to upload (along with testdata), run, and verify
retry $SESAM_CLIENT -node $NODE_URL -jwt $PUBLIC_CI_TOKEN -skip-tls-verification -vv -use-internal-scheduler -print-scheduler-log test

# Clean up completely after a run
retry $SESAM_CLIENT -node $NODE_URL -jwt $PUBLIC_CI_TOKEN -skip-tls-verification -vv reset

popd

if ! diff -r expected_after/ test_after/
then
    echo "Conversion test failed. Found diff to expected output."
    rm -rf test_after
    exit 1
else
    echo "Conversion test passed!"
    rm -rf test_after
fi


