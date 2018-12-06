#!/usr/bin/env bash

if [ -n "$TRAVIS_TAG" ] ; then
    echo "Packaging..."
    export SESAM_ARTIFACT_NAME=${TRAVIS_BUILD_DIR}/sesam-${TRAVIS_OS_NAME}-${TRAVIS_TAG}-${TRAVIS_BUILD_NUMBER}.tar.gz
    tar -zcf ${SESAM_ARTIFACT_NAME} dist/sesam
fi
