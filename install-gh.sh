#!/usr/bin/env bash
set -x

echo "Installing.."

pip3 install pyinstaller

pip3 install --user -U -r requirements.txt

pyinstaller --onefile sesam.py

"$PWD"/dist/sesam -h
