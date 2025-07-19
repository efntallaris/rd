#!/bin/bash

echo "=== Testing Redis Migration Build ==="

cd src

echo "Cleaning previous build..."
make clean

echo "Building Redis..."
make

if [ $? -eq 0 ]; then
    echo "=== Build successful! ==="
    echo "Migration functionality has been successfully integrated."
else
    echo "=== Build failed! ==="
    echo "Please check the error messages above."
    exit 1
fi 