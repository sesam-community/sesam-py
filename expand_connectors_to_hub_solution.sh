#!/bin/bash

# This bash script is used to expand one or more connectors into a hub solution.
# Usage: bash expand_connectors_to_hub_solution.sh <connector_directory1> <connector_directory2> ... <target_directory>
if [ "$#" -lt 2 ]; then
    echo "Usage: bash $0 <connector_directory1> <connector_directory2> ... <target_directory>"
    echo "<connector_directory>: The directories that you want to expand"
    echo "<target_directory>: The directory where you want to merge the expanded configs into."
    exit 1
fi

target_dir="${!#}"
echo "Target directory: $target_dir"

for ((i=1; i<$#; i++)); do
    connector_dir=${!i}
    echo "Expanding connector #$i: $connector_dir"
    placeholder=$(echo "$connector_dir" | cut -d '-' -f 1)
    cd $connector_dir || exit
    python3 ../sesam.py expand --system-placeholder $placeholder
    echo "Copying expanded config into $target_dir - transform pipes are ignored."
    rsync -av --exclude='*-transform.*' .expanded/pipes/ ../$target_dir/pipes
    cp -r .expanded/systems ../$target_dir
    cd ..
done