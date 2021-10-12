#!/usr/bin/env bash

echo "Packaging..."

pushd dist

if [ "$OS" == "ubuntu-18.04" ] ; then
    export SESAM_ARTIFACT_NAME=sesam-linux-${TAG}.tar.gz
    tar -zcf ${SESAM_ARTIFACT_NAME} sesam
fi

if [ "$OS" == "macos-latest" ] ; then
    export SESAM_ARTIFACT_NAME=sesam-macos-${TAG}.tar.gz
    tar -zcf ${SESAM_ARTIFACT_NAME} sesam
fi

if [ "$OS" == "windows-latest" ] ; then
    export SESAM_ARTIFACT_NAME=sesam-windows-${TAG}.zip
    7z a ${SESAM_ARTIFACT_NAME} sesam.exe
fi

popd

echo "Created artifact:"
ls -al dist/${SESAM_ARTIFACT_NAME}
