#!/usr/bin/env bash
set -x

echo "Installing.."

if [ "$TRAVIS_OS_NAME" == "osx"   ] ; then
    echo "Building on OSX.."
    brew upgrade python
    # make brews python the system default
    python3 --version

    pip3 install pyinstaller

    pip3 install --user -U -r requirements.txt 

    pyinstaller --onefile sesam.py

    /Users/travis/build/sesam-community/sesam-py/dist/sesam -h
fi

if [ "$TRAVIS_OS_NAME" == "linux"   ] ; then
    echo "Building on Linux.."

    python3 --version

    pip3 install pyinstaller

    pip3 install -U -r requirements.txt

    pyinstaller --onefile sesam.py

    /home/travis/build/sesam-community/sesam-py/dist/sesam -h
fi

if [ "$TRAVIS_OS_NAME" == "windows"   ] ; then
    echo "Building on Windows.."

    export PATH=$PY37PATH:$PATH
    choco install python --version 3.7.4

    python --version

    python -m pip install pyinstaller

    python -m pip install -U -r requirements.txt

    pyinstaller --onefile sesam.py

    /c/Users/travis/build/sesam-community/sesam-py/dist/sesam.exe -h
fi
