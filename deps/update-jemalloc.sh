#!/bin/bash
#VER=$1
VER="5.1.0"

#URL="http://www.canonware.com/download/jemalloc/jemalloc-${VER}.tar.bz2"
URL="https://launchpad.net/ubuntu/+archive/primary/+sourcefiles/jemalloc/5.1.0-3/jemalloc_5.1.0.orig.tar.bz2"
echo "Downloading $URL"
curl -L $URL > /tmp/jemalloc.tar.bz2
tar xvjf /tmp/jemalloc.tar.bz2
rm -rf jemalloc
mv jemalloc-${VER} jemalloc
echo "Use git status, add all files and commit changes."
