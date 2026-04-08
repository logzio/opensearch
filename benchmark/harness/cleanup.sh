#!/bin/bash
# Cleanup after benchmark: pause ingestion, delete indices, scale down wrapper.
#
# Usage: ./cleanup.sh [os-host] [target-kafka-brokers]

set -e

OS_HOST=${1:-localhost:9200}
TARGET_KAFKA=${2:-localhost:9092}

echo "=== Pausing and deleting benchmark indices ==="

for MODE in assign subscribe share; do
    INDEX="ingestion-kafka-benchmark-$MODE"
    if curl -s -o /dev/null -w "%{http_code}" "$OS_HOST/$INDEX" | grep -q "200"; then
        echo "Pausing $INDEX..."
        curl -s -X POST "$OS_HOST/_ingestion/$INDEX/_pause" | jq .
        sleep 2
        echo "Deleting $INDEX..."
        curl -s -X DELETE "$OS_HOST/$INDEX" | jq .
    else
        echo "$INDEX does not exist, skipping"
    fi
done

echo ""
echo "=== Deleting index template ==="
curl -s -X DELETE "$OS_HOST/_index_template/ingestion-kafka-benchmark" | jq . 2>/dev/null || echo "Template not found"

echo ""
echo "=== Scaling down wrapper ==="
kubectl scale deployment kafka-message-wrapper -n opensearch-benchmark --replicas=0 2>/dev/null || echo "Wrapper deployment not found"

echo ""
echo "=== Deleting target topic ==="
kafka-topics.sh --bootstrap-server "$TARGET_KAFKA" --delete --topic ingestion-kafka-formatted-logs 2>/dev/null || echo "Topic not found or already deleted"

echo ""
echo "=== Cleanup complete ==="
