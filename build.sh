#!/bin/bash

# Build script for Redis with migration functionality

echo "=== Building Redis with Migration Support ==="

# Change to the src directory
cd src

# Clean previous build
echo "Cleaning previous build..."
make clean

# Build Redis
echo "Building Redis..."
make

# Check if build was successful
if [ $? -eq 0 ]; then
    echo "=== Build successful! ==="
    echo "Redis server binary: ./redis-server"
    echo ""
    echo "To test the migration functionality:"
    echo "1. Start Redis: ./redis-server"
    echo "2. In another terminal, run: python3 ../test_migration.py"
else
    echo "=== Build failed! ==="
    exit 1
fi 