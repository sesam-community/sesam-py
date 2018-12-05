#!/usr/bin/env bash
echo "Installing"

if [ "$TRAVIS_OS_NAME" == "osx"   ] ; then
    echo "Building on OSX.."
    brew upgrade python
    # make brews python the system default
    python3 --version
fi

if [ "$TRAVIS_OS_NAME" == "linux"   ] ; then
    echo "Building on Linux.."

    python3 --version
fi
