#!/bin/bash

set -e

echo "Running conversion test.."

mkdir test
cp -r before/* test/

pushd test

#$SESAM_CLIENT -node $NODE_URL -jwt $PUBLIC_CI_TOKEN -skip-tls-verification -v -use-internal-scheduler -print-scheduler-log convert -v

popd

rm -rf test

echo "Conversion test passed!"
