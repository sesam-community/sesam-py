#!/bin/bash

set -e

echo "Running fairweather tests.."

$SESAM_CLIENT -node $NODE_URL -jwt $PUBLIC_CI_TOKEN -skip-tls-verification -v -print-scheduler-log test
$SESAM_CLIENT -node $NODE_URL -jwt $PUBLIC_CI_TOKEN -skip-tls-verification -v update
$SESAM_CLIENT -node $NODE_URL -jwt $PUBLIC_CI_TOKEN -skip-tls-verification -v status
$SESAM_CLIENT -node $NODE_URL -jwt $PUBLIC_CI_TOKEN -skip-tls-verification -dump -v download -sesamconfig-file .mysesamconfig.json

if [ ! -f "sesam-config.zip" ]; then
    echo "Test of download failed!"
    exit 1
fi

echo "Fairweather tests passed!"
