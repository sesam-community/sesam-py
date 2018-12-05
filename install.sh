#!/usr/bin/env bash
echo "Installing"

if [ "$TRAVIS_OS_NAME" == "osx"   ] ; then
    echo "Building on OSX.."

    brew install python3
    # make brews python the system default
    brew link --overwrite python3

    python --version
fi

if [ "$TRAVIS_OS_NAME" == "linux"   ] ; then
    echo "Building on Linux.."

    python --version
fi
