#!/usr/bin/env bash

set -e

echo "Testing..."

export TRAVIS_OS_NAME=linux

if [ "$TRAVIS_OS_NAME" == "linux"   ] || [ "$TRAVIS_OS_NAME" == "osx"   ] ; then
    export SESAM_CLIENT='python /home/brano/Sesam/sesam-py/sesam.py'
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
    export NODE_URL=http://172.20.0.26:9042/api
    export PUBLIC_CI_TOKEN=eyJ0eXAiOiJKV1QiLCJhbGciOiJSUzI1NiJ9.eyJpYXQiOjE2MDM3MTA3MjcuNjc2ODM3NywiZXhwIjoxNjM1MjQyOTg2LCJ1c2VyX2lkIjoicG9ydGFsX2FkbWluQHNlc2FtLmlvIiwidXNlcl9wcm9maWxlIjp7ImVtYWlsIjoicG9ydGFsX2FkbWluQHNlc2FtLmlvIiwibmFtZSI6IlBvcnRhbCBBZG1pbnNvbiIsInBpY3R1cmUiOiIifSwidXNlcl9wcmluY2lwYWwiOiJncm91cDpFdmVyeW9uZSIsInByaW5jaXBhbHMiOnsiZGIyOWNjZTItOGUwYy00YjViLTgzYjgtNDM1ZWM0YTk4NzYxIjpbImdyb3VwOkFkbWluIl19LCJhcGlfdG9rZW5faWQiOiI3ZTI4ODk2OS05NzIzLTQ5MmMtOTA5OC0xMjlhODdkZThmMDAifQ.fRZ3CwuWCBa-j9_2g0fh8NHhiLrGy0jEsoZcdUATRWj6OELqTtkSWHWXYvL0GpNla_w_4iRr8By73H6n1I72LdxajJyP4GF3c4IEpIUqfdMfS4ky8xFi2oy5z0fEMpcJErYoBI2eKgt4Rz_Xr8RVJ1eYe-TNk_bDwQJI3wBy3bCj6ZrSSSukL_QKDcZmUeRuvR-9ODf297aw5EdkRfAgv6I6obBtMFtAAhS3NOl9me9yk6Te712qK4EWwwA8VClKUlcZru-BgyIa8N6PihXwFVAKmH10UI5LGcrkBsSv4to3CgZmbN_AnoJSoC064UWmS-_spPyX-ZQLzwFe388L3A

    pushd tests

    for test_dir in *; do
        if [ -d "$test_dir" -a ! -L "$test_dir" ]; then
            cd $test_dir
            echo
            echo "Running tests in $test_dir.."
            /bin/bash run_test.sh
            cd ..
        fi
    done

    popd
fi
