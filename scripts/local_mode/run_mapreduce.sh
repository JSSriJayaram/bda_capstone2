#!/bin/bash

# ============================================================
#   NYC Taxi - Run Both MapReduce Jobs (Zone & Hourly)
# ============================================================

set -e

echo ""
echo "============================================================"
echo "   NYC Taxi MapReduce Suite (Hadoop Local Mode)"
echo "============================================================"
echo ""

# ---- Environment Setup ----
export JAVA_HOME=/usr/lib/jvm/java-17-openjdk
export HADOOP_HOME=/opt/hadoop
export HADOOP_CONF_DIR=/Users/s4n/Documents/clg/sem7/bda_capstone2/scripts/local_mode/local-conf
export PATH=$PATH:$HADOOP_HOME/bin

INPUT=../../data/csv/yellow_tripdata_cleaned.csv
[ ! -f "$INPUT" ] && INPUT=/Users/s4n/Documents/clg/sem7/bda_capstone2/scripts/local_mode/taxi_30mb.csv

OUTPUT_DIR=/Users/s4n/Documents/clg/sem7/bda_capstone2/scripts/local_mode/output
BUILD_DIR=/Users/s4n/Documents/clg/sem7/bda_capstone2/scripts/local_mode/build_classes
JAR=/Users/s4n/Documents/clg/sem7/bda_capstone2/scripts/local_mode/all-taxi-jobs.jar

if [ ! -f "$INPUT" ]; then
    echo "[ERROR] Input file not found."
    exit 1
fi

echo "[INFO] Compiling Java classes..."
mkdir -p "$BUILD_DIR" "$OUTPUT_DIR"
javac -cp "$HADOOP_HOME/share/hadoop/common/*:$HADOOP_HOME/share/hadoop/mapreduce/*:$HADOOP_HOME/share/hadoop/common/lib/*" -d "$BUILD_DIR" /Users/s4n/Documents/clg/sem7/bda_capstone2/scripts/local_mode/*.java
jar -cf "$JAR" -C "$BUILD_DIR" .

echo ""
echo "------------------------------------------------------------"
echo " 1. Running Taxi Zone Performance MapReduce Job"
echo "------------------------------------------------------------"
rm -rf "$OUTPUT_DIR/zone_performance"
hadoop jar "$JAR" TaxiDriver "$INPUT" "$OUTPUT_DIR/zone_performance"
echo "[SUCCESS] Zone Performance output saved to: $OUTPUT_DIR/zone_performance/part-r-00000"

echo ""
echo "------------------------------------------------------------"
echo " 2. Running Hourly Peak Demand MapReduce Job"
echo "------------------------------------------------------------"
rm -rf "$OUTPUT_DIR/hourly_performance"
hadoop jar "$JAR" HourlyTaxiDriver "$INPUT" "$OUTPUT_DIR/hourly_performance"
echo "[SUCCESS] Hourly Performance output saved to: $OUTPUT_DIR/hourly_performance/part-r-00000"

echo ""
echo "============================================================"
echo "   Both MapReduce Jobs Completed Successfully!"
echo "============================================================"
echo "Outputs:"
echo " - Zone Performance   : $OUTPUT_DIR/zone_performance/part-r-00000"
echo " - Hourly Performance : $OUTPUT_DIR/hourly_performance/part-r-00000"
echo ""
