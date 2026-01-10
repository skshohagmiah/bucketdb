#!/bin/bash

# End-to-End Feature Test for BucketDB Cluster

echo "🧪 Starting End-to-End Feature Tests..."

# Ensure we are in the project root
cd "$(dirname "$0")/.." || exit

echo "🧹 Cleaning up previous processes..."
fuser -k 9080/tcp 9081/tcp 9082/tcp 8080/tcp 8081/tcp 8082/tcp > /dev/null 2>&1
sleep 2 # Wait for sockets to close
rm -rf data/ # Ensure clean state

# 1. Start Cluster
echo "🚀 Launching Cluster..."
scripts/run_cluster.sh > logs/cluster_output.log 2>&1 &
CLUSTER_PID=$!

# Function to cleanup on exit
cleanup() {
    echo ""
    echo "🛑 Stopping Cluster (PID $CLUSTER_PID)..."
    # Kill the run_cluster.sh script and its children (the go run processes)
    pkill -P $CLUSTER_PID
    kill $CLUSTER_PID
    # Also kill by name to be safe
    pkill -f "go run cmd/bucketdb/main.go"
    echo "✅ Cleanup complete."
}
trap cleanup EXIT

PROTOCOL="http"
if [ "$BUCKETDB_TLS" == "true" ]; then
    PROTOCOL="https"
    CURL_OPTS="-k"
fi

# 2. Wait for Readiness
echo "⏳ Waiting for cluster to be ready..."
RETRIES=30
while [ $RETRIES -gt 0 ]; do
    if curl -s $CURL_OPTS $PROTOCOL://localhost:9080/cluster > /dev/null; then
        echo "✅ Cluster is online!"
        break
    fi
    sleep 1
    RETRIES=$((RETRIES-1))
    echo -n "."
done

if [ $RETRIES -eq 0 ]; then
    echo "❌ Cluster failed to start."
    exit 1
fi

sleep 5 # Give Raft a moment to elect leader

# 3. Test S3 Compatibility Layer
echo ""
echo "🔹 Testing S3 Compatibility Layer"

echo -n "   1. Create Bucket (PUT /s3-test)... "
RESPONSE=$(curl -s $CURL_OPTS -w "\n%{http_code}" -X PUT $PROTOCOL://localhost:9080/s3-test)
HTTP_CODE=$(echo "$RESPONSE" | tail -n1)
BODY=$(echo "$RESPONSE" | head -n -1)

if [ "$HTTP_CODE" == "200" ]; then 
    echo "✅ OK"
elif [ "$HTTP_CODE" == "409" ]; then
    echo "✅ OK (Already Existed)"
else 
    echo "❌ Failed ($HTTP_CODE)"
    echo "Response Body: $BODY"
    exit 1
fi

echo -n "   2. Upload Object (PUT /s3-test/hello.txt via Node 1)... "
echo "Hello S3 World" > s3_upload.txt
HTTP_CODE=$(curl -s $CURL_OPTS -o /dev/null -w "%{http_code}" -X PUT -T s3_upload.txt $PROTOCOL://localhost:9080/s3-test/hello.txt)
if [ "$HTTP_CODE" == "200" ]; then echo "✅ OK"; else echo "❌ Failed ($HTTP_CODE)"; exit 1; fi

echo -n "   3. Download Object (GET /s3-test/hello.txt via Node 3)... "
RETRIES=5
while [ $RETRIES -gt 0 ]; do
    CONTENT=$(curl -s $CURL_OPTS $PROTOCOL://localhost:9082/s3-test/hello.txt)
    if [ "$CONTENT" == "Hello S3 World" ]; then 
        echo "✅ OK"
        break
    fi
    sleep 1
    RETRIES=$((RETRIES-1))
    echo -n "."
done

if [ $RETRIES -eq 0 ]; then
    echo "❌ Mismatch: '$CONTENT'"
    exit 1
fi

echo -n "   4. List Objects (GET /s3-test)... "
RESPONSE=$(curl -s $CURL_OPTS $PROTOCOL://localhost:9080/s3-test)
if [[ "$RESPONSE" == *"ListBucketResult"* && "$RESPONSE" == *"hello.txt"* ]]; then echo "✅ OK"; else echo "❌ Failed: $RESPONSE"; exit 1; fi


# 4. Test Standard API
echo ""
echo "🔹 Testing Standard API"

echo -n "   1. Cluster Status (GET /cluster via Node 3)... "
STATUS=$(curl -s $CURL_OPTS $PROTOCOL://localhost:9082/cluster)
if [[ "$STATUS" == *"\"status\":\"active\""* ]]; then echo "✅ OK"; else echo "❌ Failed: $STATUS"; exit 1; fi

echo -n "   2. JSON List Objects (GET /objects?bucket=s3-test)... "
JSON=$(curl -s $CURL_OPTS "$PROTOCOL://localhost:9080/objects?bucket=s3-test")
if [[ "$JSON" == *"hello.txt"* ]]; then echo "✅ OK"; else echo "❌ Failed: $JSON"; exit 1; fi


# 5. Dashboard Accessibility
echo ""
echo "🔹 Testing Dashboard Entry"
echo -n "   1. Dashboard HTML (GET /)... "
HTML=$(curl -s $CURL_OPTS -H "Accept: text/html" $PROTOCOL://localhost:9080/)
if [[ "$HTML" == *"<!DOCTYPE html>"* ]]; then echo "✅ OK"; else echo "❌ Failed"; exit 1; fi

# 6. Test Observability
echo ""
echo "🔹 Testing Observability"
echo -n "   1. Prometheus Metrics (GET /metrics)... "
METRICS=$(curl -s $CURL_OPTS $PROTOCOL://localhost:9080/metrics)
if [[ "$METRICS" == *"bucketdb_http_requests_total"* ]]; then echo "✅ OK"; else echo "❌ Failed"; exit 1; fi


echo ""
echo "🎉 All Tests Passed Successfully!"
