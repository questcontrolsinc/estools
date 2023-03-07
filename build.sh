#!/usr/bin/env bash

set -eu

SECONDS=0

echo
echo "Building (pyinstaller)..."
echo

# reset
rm -rf ./dist
pyinstaller --onefile --clean --name estools-pyi --distpath ./dist ./src/main.py

echo
echo "Running cleanup..."
if [[ -f ./dist/estools-pyi ]]; then
  mv ./dist/estools-pyi ./dist/estools
fi
rm -rf ./build ./estools-pyi.spec
echo

echo
echo "Build done in ${SECONDS}s."
echo