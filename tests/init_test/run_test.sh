#!/bin/bash

set -e

echo "Running init test.."

rm -rf test_after
mkdir test_after

pushd test_after

$SESAM_CLIENT init
$SESAM_CLIENT init

popd

if ! diff -r before/ test_after/
then
    echo "Init test failed. Found diff to expected output."
else
    echo "Init test passed!"
fi

rm -rf test_after