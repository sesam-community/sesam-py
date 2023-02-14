#!/usr/bin/env bash
set -x

echo "Installing.."

pip install pyinstaller

pip install --user -U -r requirements.txt

pyinstaller --onefile --add-data "connector_cli:connector_cli" sesam.py