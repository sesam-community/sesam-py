#!/bin/bash

case "$OSTYPE" in
	"linux-gnu") os="linux";;
	darwin*) os="osx";;
	*)
		echo "OS isn't supported by this script"
		exit;;
esac

usage() {
	echo "By default the output filename is 'sesam-latest'"
	echo "Usage: $0 [OPTIONS]"
	echo "Options:"
	echo " -h		Show this help message"
	echo " -o filename	Specify the output filename"
}

while getopts 'o:h' flag
do
    case "${flag}" in
        o) output_file="$OPTARG";;
	h)
		usage
		exit;;
	?)
		echo "Non-existant flag: $1"
		usage
		exit;;
    esac
done

if [ -z "$output_file" ]; then
	output_file="sesam-latest"
fi

echo "[+] Downloading latest executable"
tag_url=$(curl -Ls -o /dev/null -w %{url_effective} https://github.com/sesam-community/sesam-py/releases/latest)
tag=$(basename $tag_url)
filename=sesam-$os-$tag.tar.gz

wget -q https://github.com/sesam-community/sesam-py/releases/download/$tag/$filename
tar -zxf $filename


echo "[-] Cleanup"
rm $filename
if [ "$(id -u)" -ne 0 ]; then
	mv sesam $output_file
else
	mv sesam /usr/local/bin/$output_file
fi

echo "[!] Done! '$output_file' command is now usable."
