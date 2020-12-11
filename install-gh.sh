#!/usr/bin/env bash
set -x

echo "Installing.."

pip install pyinstaller

pip install --user -U -r requirements.txt

pyinstaller --onefile sesam.py

"$PWD"/dist/sesam -h
