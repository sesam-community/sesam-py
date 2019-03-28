#!/usr/bin/env bash

if [ -n "$TRAVIS_TAG" ] ; then
    echo "Packaging..."
    pushd dist

    if [ "$TRAVIS_OS_NAME" == "linux"   ] || [ "$TRAVIS_OS_NAME" == "osx"   ] ; then
        export SESAM_ARTIFACT_NAME=${TRAVIS_BUILD_DIR}/sesam-${TRAVIS_OS_NAME}-${TRAVIS_TAG}.tar.gz
        tar -zcf ${SESAM_ARTIFACT_NAME} sesam
    fi

    if [ "$TRAVIS_OS_NAME" == "windows"   ] ; then
        export SESAM_ARTIFACT_NAME=${TRAVIS_BUILD_DIR}/sesam-${TRAVIS_OS_NAME}-${TRAVIS_TAG}.zip
        7z a ${SESAM_ARTIFACT_NAME} sesam.exe
    fi

    popd
    echo "Created artifact:"
    ls -al ${SESAM_ARTIFACT_NAME}
fi
