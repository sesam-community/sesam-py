#!/bin/bash

if [ "$(id -u)" -ne 0 ]; then
        echo "Please run as root"
        exit
fi

if [[ "$OSTYPE" == "linux-gnu"* ]]; then
        os=linux
elif [[ "$OSTYPE" == "darwin"* ]]; then
        os=osx
fi

while getopts o: flag
do
    case "${flag}" in
        o) output_file=${OPTARG};;
        *) output_file="sesam-latest";;
    esac
done


echo "[+] Downloading latest executable"
tag_url=$(curl -Ls -o /dev/null -w %{url_effective} https://github.com/sesam-community/sesam-py/releases/latest)
tag=$(basename $tag_url)
filename=sesam-$os-$tag.tar.gz

wget -q https://github.com/sesam-community/sesam-py/releases/download/$tag/$filename
tar -zxf $filename


echo "[-] Cleanup and move executable to /usr/local/bin"
rm $filename
mv sesam /usr/local/bin/$output_file
echo "[!] Done! '$output_file' command is now usable."
