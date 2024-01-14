#!/bin/bash
#
sudo apt-get update
sudo apt-get install -y libtool autoconf automake libibverbs-dev librdmacm-dev libibumad-dev libpci-dev
sudo apt-get install -y libibverbs-dev
sudo apt-get install -y librdmacm-dev
sudo apt-get install -y libibumad-dev
sudo apt-get install -y libpci-dev
sudo apt install -y maven

sudo apt-get install -y pkg-config
sudo apt-get install -y libuv1-dev
sudo apt install -y build-essential
sudo apt install -y libatomic-ops-dev
sudo apt-get install -y sysstat
sudo apt-get install -y ifstat
sudo apt-get install -y libtool autoconf automake 
#libibverbs-dev librdmacm-dev libibumad-dev libpci-dev
sudo apt-get install -y build-essential cmake gcc libudev-dev libnl-3-dev libnl-route-3-dev ninja-build pkg-config valgrind python3-dev cython3 python3-docutils pandoc


# Clone the rdma-core repository
git clone https://github.com/linux-rdma/rdma-core.git
cd rdma-core

# Create a build directory and navigate into it
mkdir build
cd build

# Configure the build
cmake ..

# Compile the source code
make

# Install the library (requires root permission)
sudo make install

# Update the library cache
sudo ldconfig

echo "Installation of librdmacm is complete."


cd ..
cd ..

chmod +x loadibmodules.sh
sudo ./loadibmodules.sh

# Navigate to the 'deps' directory
cd ../deps

# Execute the script
chmod +x update-jemalloc.sh
/bin/sh update-jemalloc.sh

cd jemalloc
/bin/sh autogen.sh
/bin/sh configure
cd ..
sudo make jemalloc
sudo make hiredis
sudo make hdr_histogram
sudo make linenoise

# Return to the original directory
cd -

cd ..
cd ..
pwd
chmod +x src/mkreleasehdr.sh
sudo make


