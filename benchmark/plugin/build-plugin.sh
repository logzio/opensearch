#!/bin/bash
set -e

# Build the ingestion-kafka plugin ZIP from the logzio/OpenSearch fork.
# Usage: ./build-plugin.sh [branch]
#   branch: defaults to feature/ingestion-kafka-upgrade-4.2-3.5.0

BRANCH=${1:-feature/ingestion-kafka-upgrade-4.2-3.5.0}
REPO_ROOT="$(cd "$(dirname "$0")/../.." && pwd)"

export JAVA_HOME=${JAVA_HOME:-/opt/homebrew/opt/openjdk@21/libexec/openjdk.jdk/Contents/Home}
export PATH="$JAVA_HOME/bin:$PATH"

echo "=== Building ingestion-kafka plugin ==="
echo "Branch: $BRANCH"
echo "JAVA_HOME: $JAVA_HOME"
echo ""

cd "$REPO_ROOT"
git checkout "$BRANCH"

./gradlew :plugins:ingestion-kafka:assemble

ZIP=$(ls -1 plugins/ingestion-kafka/build/distributions/ingestion-kafka-*.zip 2>/dev/null | head -1)
if [ -z "$ZIP" ]; then
    echo "ERROR: Plugin ZIP not found"
    exit 1
fi

echo ""
echo "=== Plugin built successfully ==="
echo "ZIP: $ZIP"
echo "Size: $(du -h "$ZIP" | cut -f1)"

# Verify version
echo ""
echo "=== Version check ==="
unzip -p "$ZIP" ingestion-kafka/plugin-descriptor.properties | grep -E "^(opensearch\.version|version|classname)="
