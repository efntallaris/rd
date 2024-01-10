#!/bin/bash

sudo apt-get update
sudo apt-get install -y libtool autoconf automake libibverbs-dev librdmacm-dev libibumad-dev libpci-dev
sudo apt-get install libibverbs-dev
sudo apt-get install librdmacm-dev
sudo apt-get install libibumad-dev
sudo apt-get install libpci-dev
sudo apt install maven

sudo apt-get install pkg-config
sudo apt-get install libuv1-dev
sudo apt install build-essential
sudo apt install libatomic-ops-dev

# Navigate to the 'deps' directory
cd ../deps

# Check if the script exists and is executable
if [ -x "update-jemalloc.sh" ]; then
    # Execute the script
    sudo ./update-jemalloc.sh
else
    echo "update-jemalloc.sh not found or is not executable"
fi

# Return to the original directory
cd -

