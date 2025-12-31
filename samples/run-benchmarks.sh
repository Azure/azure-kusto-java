#!/bin/bash
# Quick Start Script for Running Kusto Ingestion V2 Benchmarks
#
# Usage:
#   1. Edit the environment variables below with your cluster details
#   2. Make executable: chmod +x run-benchmarks.sh
#   3. Run: ./run-benchmarks.sh

echo "=================================="
echo "Kusto Ingestion V2 Benchmarks"
echo "=================================="
echo ""
echo "Cluster: $KUSTO_CLUSTER_URL"
echo "Database: $TEST_DATABASE"
echo "Table: $KUSTO_TABLE"
echo ""

# Check if JAR exists
if [ ! -f "target/samples.jar" ]; then
    echo "❌ Samples JAR not found!"
    echo "Building..."
    mvn clean package -DskipTests

    if [ $? -ne 0 ]; then
        echo "❌ Build failed!"
        exit 1
    fi
    echo "✅ Build successful!"
fi

# Automatically run all benchmarks and save to JSON with timestamp
TIMESTAMP=$(date +%Y%m%d_%H%M%S)
RESULT_FILE="benchmark_results_${TIMESTAMP}.json"

echo "Results will be saved to: $RESULT_FILE"
echo ""
echo "Starting benchmarks..."
echo "=================================="
echo ""

# Run all benchmarks with JSON output
java -jar target/samples.jar JavaBenchmarkTest -rf json -rff "$RESULT_FILE"

echo ""
echo "=================================="
echo "Benchmark complete!"
echo ""
echo "✅ Results saved to: $RESULT_FILE"
echo ""
echo "View results:"
echo "  cat $RESULT_FILE"
echo "  jq . $RESULT_FILE  # if you have jq installed"
echo ""
echo "Quick summary:"
if command -v jq &> /dev/null; then
    echo ""
    jq -r '.[] | "  \(.benchmark): \(.primaryMetric.score) ± \(.primaryMetric.scoreError) \(.primaryMetric.scoreUnit)"' "$RESULT_FILE" 2>/dev/null || echo "  (Run 'cat $RESULT_FILE' to see full results)"
else
    echo "  (Install jq to see formatted summary: sudo apt install jq)"
fi
echo ""
echo "For custom options, run directly:"
echo "  java -jar target/samples.jar -h"

