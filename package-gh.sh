#!/usr/bin/env bash

echo "Packaging..."

pushd dist

if [ "$RUNNER_OS" == "Linux" ] || [ "$RUNNER_OS" == "macOS" ] ; then
    export SESAM_ARTIFACT_NAME=sesam-${RUNNER_OS}-${TAG}.tar.gz
    tar -zcf ${SESAM_ARTIFACT_NAME} sesam
fi

if [ "$RUNNER_OS" == "Windows" ] ; then
    export SESAM_ARTIFACT_NAME=sesam-${RUNNER_OS}-${TAG}.zip
    7z a ${SESAM_ARTIFACT_NAME} sesam.exe
fi

popd

echo "Created artifact:"
ls -al dist/${SESAM_ARTIFACT_NAME}
