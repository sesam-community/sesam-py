#!/usr/bin/env bash
set -x

echo "Installing.."

pip install pyinstaller

pip install --user -U -r requirements.txt

pyinstaller --onefile sesam.py

if [ "$RUNNER_OS" == "Linux" ] || [ "$RUNNER_OS" == "macOS" ] ; then
    export SESAM_CLIENT=$PWD/dist/sesam
fi

if [ "$RUNNER_OS" == "Windows" ] ; then
    export SESAM_CLIENT=$PWD/dist/sesam.exe
fi

$SESAM_CLIENT -h
