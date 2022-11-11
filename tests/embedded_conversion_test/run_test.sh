#!/bin/bash

set -e

echo "Running conversion test.."

rm -rf test_after
mkdir test_after
cp -r before/* test_after/

pushd test_after

$SESAM_CLIENT -node $NODE_URL -jwt $PUBLIC_CI_TOKEN -skip-tls-verification -vv wipe

# First, convert
$SESAM_CLIENT -node $NODE_URL -jwt $PUBLIC_CI_TOKEN -vv convert

# Then, test if it works to upload (along with testdata), run, and verify
$SESAM_CLIENT -node $NODE_URL -jwt $PUBLIC_CI_TOKEN -skip-tls-verification -vv -use-internal-scheduler -print-scheduler-log test

# Clean up completely after a run
$SESAM_CLIENT -node $NODE_URL -jwt $PUBLIC_CI_TOKEN -skip-tls-verification -vv reset

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


