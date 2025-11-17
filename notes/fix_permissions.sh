#!/bin/bash

# Script: fix_permissions.sh
# Description: Fixes ownership and permissions for a directory and all its contents
# Usage: ./fix_permissions.sh <directory_path>

# Check if directory path is provided
if [ $# -eq 0 ]; then
    echo "Error: No directory path provided"
    echo "Usage: $0 <directory_path>"
    exit 1
fi

# Step 1: Change ownership to current user
sudo chown -R $(whoami):$(whoami) "$1"

# Step 2: Set permissions to 755 (rwxr-xr-x)
sudo chmod -R 755 "$1"

