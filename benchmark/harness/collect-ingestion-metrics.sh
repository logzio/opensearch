#!/bin/bash
# Collect ingestion-kafka specific metrics via the Ingestion State API.
# Complements Prometheus ES exporter and klag-exporter.
#
# Usage: ./collect-ingestion-metrics.sh <index-name> [interval-sec] [os-host]
# Example: ./collect-ingestion-metrics.sh ingestion-kafka-benchmark-assign 10 localhost:9200

set -e

INDEX_NAME=${1:?Usage: $0 <index-name> [interval] [host]}
INTERVAL=${2:-10}
OS_HOST=${3:-localhost:9200}
OUTFILE="/tmp/ingestion-metrics-$(echo "$INDEX_NAME" | sed 's/.*-//').tsv"

echo "Collecting ingestion metrics for $INDEX_NAME every ${INTERVAL}s"
echo "Output: $OUTFILE"
echo "Press Ctrl+C to stop"
echo ""

echo -e "timestamp\tshard\tstate\tlag\tpolled\tdropped\tfailed" > "$OUTFILE"

while true; do
    TS=$(date -u +%Y-%m-%dT%H:%M:%SZ)
    curl -s "$OS_HOST/_ingestion/$INDEX_NAME/_state" | \
        jq -r --arg ts "$TS" \
        '.shards[]? | [$ts, .shard_id, .poller_state, .pointer_based_lag,
         .stats.consumer_stats.total_polled_count,
         .stats.consumer_stats.total_poller_message_dropped_count,
         .stats.message_processor_stats.total_failed_count] | @tsv' \
        >> "$OUTFILE" 2>/dev/null || echo -e "${TS}\tERROR\t-\t-\t-\t-\t-" >> "$OUTFILE"
    sleep "$INTERVAL"
done
