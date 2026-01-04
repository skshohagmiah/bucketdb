#!/bin/bash

# BucketDB Cluster Startup Script
# This script starts a 3-node distributed BucketDB cluster locally.

echo "🚀 Starting BucketDB Cluster..."

# Create base data directory
mkdir -p data

# Cleanup previous data if any (optional)
# rm -rf data/*

# Start Node 1 (Bootstrap)
echo "🔗 Starting Node 1 (Bootstrap) on port 9080..."
go run cmd/bucketdb/main.go \
    -id node-1 \
    -http :8080 \
    -api :9080 \
    -storage ./data/node1/chunks \
    -metadata ./data/node1/metadata > node1.log 2>&1 &
NODE1_PID=$!

sleep 2 # Wait for bootstrap

# Start Node 2 (Joining Node 1)
echo "🔗 Starting Node 2 on port 9081..."
go run cmd/bucketdb/main.go \
    -id node-2 \
    -http :8081 \
    -api :9081 \
    -join localhost:8080 \
    -storage ./data/node2/chunks \
    -metadata ./data/node2/metadata > node2.log 2>&1 &
NODE2_PID=$!

# Start Node 3 (Joining Node 1)
echo "🔗 Starting Node 3 on port 9082..."
go run cmd/bucketdb/main.go \
    -id node-3 \
    -http :8082 \
    -api :9082 \
    -join localhost:8080 \
    -storage ./data/node3/chunks \
    -metadata ./data/node3/metadata > node3.log 2>&1 &
NODE3_PID=$!

echo "✅ Cluster is starting up!"
echo "--------------------------------------------------"
echo "Node 1 API: http://localhost:9080"
echo "Node 2 API: http://localhost:9081"
echo "Node 3 API: http://localhost:9082"
echo "--------------------------------------------------"
echo "Logs are being written to node1.log, node2.log, node3.log"
echo ""
echo "Instructions:"
echo "1. Upload an object: curl -X POST -d 'Hello Cluster' http://localhost:9080/objects/my-bucket/test.txt"
echo "2. Download from any node: curl http://localhost:9082/objects/my-bucket/test.txt"
echo "3. Check cluster status: curl http://localhost:8080/cluster"
echo ""
echo "Press Ctrl+C to stop all nodes."

# Handle shutdown
trap "kill $NODE1_PID $NODE2_PID $NODE3_PID; exit" INT TERM
wait
