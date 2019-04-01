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
    export PUBLIC_CI_TOKEN=eyJ0eXAiOiJKV1QiLCJhbGciOiJSUzI1NiJ9.eyJpYXQiOjE1NTQxMDc3NzguNjQwNDM3OCwiZXhwIjoxODkzNDkwNDY5LCJ1c2VyX2lkIjoiZjQxZmM5MzQtZjE1Yy00ZDc1LTgyNjEtYzYyYzJjOGUyODE3IiwidXNlcl9wcm9maWxlIjp7ImVtYWlsIjoidG9tLmJlY2hAc2VzYW0uaW8iLCJuYW1lIjoiVG9tIEJlY2giLCJwaWN0dXJlIjoiaHR0cHM6Ly9zLmdyYXZhdGFyLmNvbS9hdmF0YXIvMGQzNWYwMzQ0ZWI4Mzc4ZjA4NmNkZjU0NGU1Yjc5OWU_cz00ODAmcj1wZyZkPWh0dHBzJTNBJTJGJTJGY2RuLmF1dGgwLmNvbSUyRmF2YXRhcnMlMkZ0Yi5wbmcifSwidXNlcl9wcmluY2lwYWwiOiJncm91cDpFdmVyeW9uZSIsInByaW5jaXBhbHMiOnsiY2Q5Zjk3ZDYtZmNiNi00YjVlLTliMmYtMWFiNmY5NzFhMDhhIjpbImdyb3VwOkFkbWluIl19LCJhcGlfdG9rZW5faWQiOiIwNGI2NjY0NS1lYmZiLTQyMzMtYmIyOS0wODFkN2I0MzYwZTIifQ.oJa_50cvTkWC-YEZ8jlUwg7-xsnrNCQeRqxHUUG5c905G2FzdFIujfPGKoiK22iTQn1uWtr7_vQGpmxqkoRu6AftH9yeg9ssA3Pxh_Y00miHieZBuppKT1oUqDh8RM20PHksUsbqnXGsD4BQDwpi0RBO0M_3NCXs5_QhXbQxqZoqgfthD0oT2MiCSBVu3ofkPceOnF__0UQrSrqrCwC9J_i7cUSbaT5M79dSXKRJPjkxktXeec6iFjQw22W9t-sPxjXhfzLh_xzQMoneFbb4Q8XIwj3iIVImgw0Pg9Vl1ugYxDurQdQgHMAMT5DO2rTGUOFnOFudZslcQL_tNLEGxg

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
