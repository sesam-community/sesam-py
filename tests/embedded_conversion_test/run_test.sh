#!/bin/bash

set -e

echo "Running conversion test.."

rm -rf test_after
mkdir test_after
cp -r before/* test_after/

pushd test_after

$SESAM_CLIENT -node $NODE_URL -jwt $PUBLIC_CI_TOKEN -v convert

popd

if ! diff -r expected_after/ test_after/
then
    echo "Conversion test failed. Found diff to expected output."
else
    echo "Conversion test passed!"
fi

rm -rf test_after

