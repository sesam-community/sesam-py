#!/usr/bin/env bash
set -x -e

echo "Installing required packages.."

pip install pyinstaller

pip install --user -U -r requirements.txt

OS="`uname`"
if [ "$OS" = "Linux" ] || [ "$OS" = "Darwin" ]; then
  echo "Installing sesam-py on Linux/Mac"
  pyinstaller --onefile --add-data "connector_cli:connector_cli" sesam.py
else
  echo "Installing sesam-py on Windows"
  pyinstaller --onefile --add-data "connector_cli;connector_cli" sesam.py
fi