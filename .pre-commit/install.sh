#!/bin/bash
precommitDefaultFile='./.pre-commit/pre-commit.default'
precommitDir='.git/hooks/pre-commit.d'
precommitFile='.git/hooks/pre-commit'

echo "Preparing pre-commit files:"
rm -rf $precommitDir $precommitFile
mkdir $precommitDir && echo "Done."

echo "Installing pre-commit:"
chmod +x $precommitDefaultFile
cp $precommitDefaultFile $precommitFile
[ -f $precommitFile ] && echo "Done." || echo "Something went wrong."
