#!/usr/bin/env bash
set -x

echo "Installing.."

if [ "$TRAVIS_OS_NAME" == "osx"   ] ; then
    echo "Building on OSX.."
    brew upgrade python
    # make brews python the system default
    python3 --version

    pip3 install pyinstaller

    pip3 install -U -r requirements.txt

    pyinstaller --onefile sesam.py

    /Users/travis/build/tombech/sesam-py/dist/sesam -h
fi

if [ "$TRAVIS_OS_NAME" == "linux"   ] ; then
    echo "Building on Linux.."

    python3 --version

    pip3 install pyinstaller

    pip3 install -U -r requirements.txt

    pyinstaller --onefile sesam.py

    /home/travis/build/tombech/sesam-py/dist/sesam -h
fi

if [ "$TRAVIS_OS_NAME" == "windows"   ] ; then
    echo "Building on Windows.."

    export PATH=$PY38PATH:$PATH
    choco install python

    python3 --version

    python3 -m pip install -r pyinstaller

    python3 -m pip install -U -r requirements.txt

    pyinstaller --onefile sesam.py

    /c/Users/travis/build/tombech/sesam-py/dist/sesam.exe -h
fi
