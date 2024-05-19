#!/usr/bin/env bash

cd "$(dirname "$0")"
cd ..

root=$(pwd)

if [ ! -d "$root/datasets/stock" ] || [ ! -d "$root/datasets/rovio" ]; then
    echo "Downloading datasets..."
    wget -q -O "$root/datasets.tar.gz" "https://www.dropbox.com/scl/fi/y4qkcvci7yqcypg41tu85/datasets.tar.gz?rlkey=6o2d4byhx95d860pojddka4iq&dl=0"
    tar -zvxf datasets.tar.gz
    rm datasets.tar.gz
    echo "Datasets downloaded."
fi
