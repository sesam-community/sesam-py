#!/usr/bin/env bash
set -x
TAG=${SESAM_TAG:-1.18.3}

wget -O sesam.tar.gz https://github.com/sesam-community/sesam-py/releases/download/$TAG/sesam-linux-$TAG.tar.gz
tar -xf sesam.tar.gz
./sesam -version
