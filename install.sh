#!/usr/bin/env bash
echo "Installing"

python --version

if [ "$TRAVIS_OS_NAME" == "osx"   ] ; then
    echo "Building on OSX.."
fi

if [ "$TRAVIS_OS_NAME" == "linux"   ] ; then
    echo "Building on Linux.."
fi
