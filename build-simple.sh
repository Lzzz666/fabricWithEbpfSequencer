#!/bin/bash

# Simple build script for Fabric eBPF Sequencer
# Fixes the original typo and adds basic error handling

set -e  # Exit on any error

echo "🔨 Building Fabric eBPF Sequencer..."

# Fix typo: dockef -> docker
echo "📦 Building Docker images..."
make docker

# Install Fabric binaries
echo "⬇️  Installing Fabric binaries..."
cd scripts
./install-fabric.sh b
cd ..

# Build peer binary
echo "🏗️  Building peer binary..."
make peer

# Copy peer binary with directory creation
echo "📁 Copying peer binary..."
mkdir -p scripts/bin
cp build/bin/peer scripts/bin/peer

echo "✅ Build completed successfully!"
echo "📍 Peer binary available at: scripts/bin/peer" 