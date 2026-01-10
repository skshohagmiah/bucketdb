#!/bin/bash

# BucketDB Cluster Startup Script
# This script starts a 3-node distributed BucketDB cluster locally.

echo "🚀 Starting BucketDB Cluster..."

# Ensure we are in the project root
cd "$(dirname "$0")/.." || exit

# Create base data directory
mkdir -p data
mkdir -p logs

# Cleanup previous data if any (optional)
# rm -rf data/*

# Start Node 1 (Bootstrap)
echo "🔗 Starting Node 1 (Bootstrap) on port 9080..."

TLS_FLAGS=""
PROTOCOL="http"
if [ "$BUCKETDB_TLS" == "true" ]; then
    echo "🔐 TLS Enabled"
    TLS_FLAGS="-tls -tls-cert ./scripts/certs/node-1.crt -tls-key ./scripts/certs/node-1.key -tls-ca ./scripts/certs/ca.crt -tls-insecure"
    PROTOCOL="https"
fi

go run cmd/bucketdb/main.go \
    -id node-1 \
    -http :8080 \
    -api :9080 \
    -bootstrap \
    -storage ./data/node1/chunks \
    -metadata ./data/node1/metadata \
    $TLS_FLAGS > logs/node1.log 2>&1 &
NODE1_PID=$!

sleep 2 # Wait for bootstrap

# Start Node 2 (Joining Node 1)
echo "🔗 Starting Node 2 on port 9081..."
if [ "$BUCKETDB_TLS" == "true" ]; then
    TLS_FLAGS="-tls -tls-cert ./scripts/certs/node-2.crt -tls-key ./scripts/certs/node-2.key -tls-ca ./scripts/certs/ca.crt -tls-insecure"
fi
go run cmd/bucketdb/main.go \
    -id node-2 \
    -http :8081 \
    -api :9081 \
    -join localhost:8080 \
    -storage ./data/node2/chunks \
    -metadata ./data/node2/metadata \
    $TLS_FLAGS > logs/node2.log 2>&1 &
NODE2_PID=$!

# Start Node 3 (Joining Node 1)
echo "🔗 Starting Node 3 on port 9082..."
if [ "$BUCKETDB_TLS" == "true" ]; then
    TLS_FLAGS="-tls -tls-cert ./scripts/certs/node-3.crt -tls-key ./scripts/certs/node-3.key -tls-ca ./scripts/certs/ca.crt -tls-insecure"
fi
go run cmd/bucketdb/main.go \
    -id node-3 \
    -http :8082 \
    -api :9082 \
    -join localhost:8080 \
    -storage ./data/node3/chunks \
    -metadata ./data/node3/metadata \
    $TLS_FLAGS > logs/node3.log 2>&1 &
NODE3_PID=$!

echo "✅ Cluster is starting up!"
echo "--------------------------------------------------"
echo "Node 1 API: $PROTOCOL://localhost:9080"
echo "Node 2 API: $PROTOCOL://localhost:9081"
echo "Node 3 API: $PROTOCOL://localhost:9082"
echo "--------------------------------------------------"
echo "Logs are being written to logs/node1.log, logs/node2.log, logs/node3.log"
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
