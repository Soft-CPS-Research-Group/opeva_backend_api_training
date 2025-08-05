#!/bin/bash

# Set your variables
HEAD_IP="<head-ip>"
NFS_SERVER="<server-ip>"
MOUNT_POINT="/opt/opeva_shared_data"

# Create and activate virtual environment
if [ ! -d "rayenv" ]; then
    python3 -m venv rayenv
fi
source rayenv/bin/activate

# Install dependencies if not installed
pip install --quiet "ray[default]" docker

# Mount NFS only if not mounted
if ! mountpoint -q "$MOUNT_POINT"; then
    echo "Mounting shared storage..."
    sudo mount -t nfs ${NFS_SERVER}:/opt/opeva_shared_data $MOUNT_POINT
else
    echo "Shared storage already mounted."
fi

# Start Ray worker if not already running
if ! pgrep -f "ray::"; then
    echo "Joining Ray cluster at ${HEAD_IP}..."
    ray start --address=${HEAD_IP}:6379 --block
else
    echo "Ray is already running."
fi
