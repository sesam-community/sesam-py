#!/usr/bin/env bash

set -e

echo "Testing..."

export NODE_URL=https://datahub-cd9f97d6.sesam.cloud/api
export PUBLIC_CI_TOKEN=$SESAM_TOKEN

pushd tests

for test_dir in *; do
    if [ -d "$test_dir" -a ! -L "$test_dir" ]; then
        cd "$test_dir"
        echo
        echo "Running tests in $test_dir.."
        /bin/bash run_test.sh
        cd ..
    fi
done

popd
