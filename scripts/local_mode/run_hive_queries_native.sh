#!/bin/bash

# ============================================================
#   run_hive_queries_native.sh
#   Native Apache Hive Execution Script (Downscaled Data Optimized)
# ============================================================

export JAVA_HOME=/usr/lib/jvm/java-17-openjdk
export HADOOP_HOME=/opt/hadoop
export HIVE_HOME=/opt/hive
export PATH=$PATH:$HIVE_HOME/bin:$HADOOP_HOME/bin

# JVM flags for Java 17 reflection
export JAVA_TOOL_OPTIONS="--add-opens=java.base/java.nio=ALL-UNNAMED --add-opens=java.base/java.io=ALL-UNNAMED --add-opens=java.base/java.lang=ALL-UNNAMED --add-opens=java.base/java.lang.reflect=ALL-UNNAMED --add-opens=java.base/java.util=ALL-UNNAMED --add-opens=java.base/java.util.concurrent=ALL-UNNAMED --add-opens=java.base/java.util.concurrent.atomic=ALL-UNNAMED --add-opens=java.base/java.math=ALL-UNNAMED --add-opens=java.base/sun.nio.ch=ALL-UNNAMED"

OUT_DIR="/Users/s4n/Documents/clg/sem7/bda_capstone2/scripts/local_mode/output/hive_results"
mkdir -p "$OUT_DIR"

HIVE_URL="jdbc:hive2://127.0.0.1:10000/taxi_analytics"

echo ""
echo "============================================================"
echo "      NATIVE APACHE HIVE ANALYTICS SUITE (NO DOCKER)       "
echo "============================================================"
echo ""

# Run all 12 queries in a single Beeline session
beeline -u "$HIVE_URL" --showHeader=true --outputformat=table -f /Users/s4n/Documents/clg/sem7/bda_capstone2/scripts/local_mode/hive/all_queries.sql 2>&1 | tee "$OUT_DIR/all_hive_results.log"

echo ""
echo "============================================================"
echo "       ALL 12 NATIVE HIVE QUERIES EXECUTED SUCCESSFULLY!   "
echo "       Full report log saved to: $OUT_DIR/all_hive_results.log"
echo "============================================================"
