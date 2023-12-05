#!/bin/bash

case "$OSTYPE" in
	"linux-gnu") os="linux";;
	"darwin") os="osx";;
	*)
		echo "OS isn't supported by this script"
		exit;;
esac

tag=$SESAM_TAG

if [ -z "$tag" ]; then
	echo "No tag found in environment variable SESAM_TAG"
    exit;
fi

filename=sesam-$os-$tag.tar.gz

wget -q https://github.com/sesam-community/sesam-py/releases/download/$tag/$filename
tar -zxf $filename


echo "[-] Cleanup"
rm $filename