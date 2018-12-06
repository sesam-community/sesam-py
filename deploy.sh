#!/usr/bin/env bash

if [ -n "$TRAVIS_TAG" ] ; then
    echo "Packaging..."
    export SESAM_ARTIFACT_NAME=${TRAVIS_BUILD_DIR}/sesam-${TRAVIS_OS_NAME}-${TRAVIS_TAG}-${TRAVIS_BUILD_NUMBER}.tar.gz
    pushd dist
    tar -zcf ${SESAM_ARTIFACT_NAME} sesam
    popd
    echo "Created artifact:"
    ls -al ${SESAM_ARTIFACT_NAME}
fi
