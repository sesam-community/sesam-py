#!/usr/bin/env bash

set -e

echo "Testing..."

if [ "$TRAVIS_OS_NAME" == "linux"   ] || [ "$TRAVIS_OS_NAME" == "osx"   ] ; then
    export SESAM_CLIENT=$PWD/dist/sesam
fi

if [ "$TRAVIS_OS_NAME" == "windows"   ] ; then
    export SESAM_CLIENT=$PWD/dist/sesam.exe
fi

if [ -z "$TRAVIS_OS_NAME" ] ; then
    export SESAM_CLIENT=~/bin/sesam-py
fi

$SESAM_CLIENT -h

# Only run the tests on linux for now
if [ "$TRAVIS_OS_NAME" == "linux"   ] ; then
    export NODE_URL=https://datahub-cd9f97d6.sesam.cloud/api
    export PUBLIC_CI_TOKEN=$SESAM_TOKEN

    pushd tests

    for test_dir in *; do
        if [ -d "$test_dir" -a ! -L "$test_dir" ]; then
            cd $test_dir
            echo "Running tests in $test_dir.."
            /bin/bash run_test.sh
            cd ..
        fi
    done

    popd
fi
