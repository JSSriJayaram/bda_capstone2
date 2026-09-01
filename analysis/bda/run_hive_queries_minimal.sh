#!/bin/bash

# ============================================================
#   run_hive_queries_minimal.sh
#   Native Hive Runner (Full Java 17 Kryo Open Flags)
# ============================================================

export JAVA_HOME=/usr/lib/jvm/java-17-openjdk
export HADOOP_HOME=/opt/hadoop
export HIVE_HOME=/opt/hive
export HIVE_CONF_DIR=/home/real/bda/hive/conf
export PATH=$PATH:$HIVE_HOME/bin:$HADOOP_HOME/bin

# Set full JVM reflection opens for Java 17 + Kryo serialization in Hive 4
export JAVA_TOOL_OPTIONS="--add-opens=java.base/java.nio=ALL-UNNAMED --add-opens=java.base/java.io=ALL-UNNAMED --add-opens=java.base/java.lang=ALL-UNNAMED --add-opens=java.base/java.lang.reflect=ALL-UNNAMED --add-opens=java.base/java.util=ALL-UNNAMED --add-opens=java.base/java.util.concurrent=ALL-UNNAMED --add-opens=java.base/java.util.concurrent.atomic=ALL-UNNAMED --add-opens=java.base/java.math=ALL-UNNAMED --add-opens=java.base/sun.nio.ch=ALL-UNNAMED"

pkill -9 -f hiveserver2 2>/dev/null || true
rm -f /home/real/bda/hive/conf/hiveserver2.pid 2>/dev/null || true

echo "[INFO] Starting HiveServer2..."
hiveserver2 > /home/real/bda/hive/hs2_java17.log 2>&1 &
HS2_PID=$!

echo "[INFO] HiveServer2 PID: $HS2_PID. Waiting for port 10000..."

READY=0
for i in {1..35}; do
  if ss -tln | grep -q 10000; then
    echo "[SUCCESS] HiveServer2 is ready on port 10000!"
    READY=1
    break
  fi
  sleep 1
done

if [ $READY -eq 0 ]; then
  echo "[ERROR] HiveServer2 failed to start."
  exit 1
fi

echo ""
echo "============================================================"
echo "          RUNNING HIVE QUERIES VIA BEELINE CLI             "
echo "============================================================"

OUT="/home/real/bda/output/hive_results"
mkdir -p "$OUT"

# 1. Setup DB & Table
echo "[SETUP] Executing taxi_analytics.sql..."
beeline -u "jdbc:hive2://127.0.0.1:10000" -f /home/real/bda/hive/taxi_analytics.sql

echo ""
echo "[QUERY] Q1 — Total Number of Taxi Trips"
beeline -u "jdbc:hive2://127.0.0.1:10000/taxi_analytics" -e "SELECT COUNT(*) AS total_trips FROM taxi_trips;"

echo ""
echo "[QUERY] Q2 — Total Revenue Generated"
beeline -u "jdbc:hive2://127.0.0.1:10000/taxi_analytics" -e "SELECT ROUND(SUM(total_amount), 2) AS total_revenue FROM taxi_trips;"

echo ""
echo "[QUERY] Q3 — Platform-wide Averages"
beeline -u "jdbc:hive2://127.0.0.1:10000/taxi_analytics" -e "SELECT ROUND(AVG(fare_amount), 2) AS avg_fare, ROUND(AVG(trip_distance), 2) AS avg_distance_miles, ROUND(AVG(trip_duration_min), 2) AS avg_duration_min, ROUND(AVG(avg_speed_mph), 2) AS avg_speed_mph FROM taxi_trips;"

echo ""
echo "[QUERY] Q4 — Top 20 Busiest Zones"
beeline -u "jdbc:hive2://127.0.0.1:10000/taxi_analytics" -e "SELECT PULocationID, COUNT(*) AS trip_count FROM taxi_trips GROUP BY PULocationID ORDER BY trip_count DESC LIMIT 20;"

echo ""
echo "[QUERY] Q5 — Top 20 Revenue Zones"
beeline -u "jdbc:hive2://127.0.0.1:10000/taxi_analytics" -e "SELECT PULocationID, COUNT(*) AS trip_count, ROUND(SUM(total_amount), 2) AS total_revenue, ROUND(AVG(total_amount), 2) AS avg_revenue_per_trip FROM taxi_trips GROUP BY PULocationID ORDER BY total_revenue DESC LIMIT 20;"

echo ""
echo "[QUERY] Q6 — Top 10 Highest-Value Trips"
beeline -u "jdbc:hive2://127.0.0.1:10000/taxi_analytics" -e "SELECT PULocationID, DOLocationID, tpep_pickup_datetime, ROUND(trip_distance, 2) AS distance_mi, ROUND(trip_duration_min, 2) AS duration_min, ROUND(fare_amount, 2) AS fare, ROUND(tip_amount, 2) AS tip, ROUND(total_amount, 2) AS total FROM taxi_trips ORDER BY total_amount DESC LIMIT 10;"

echo ""
echo "[QUERY] Q7 — Distance Statistics"
beeline -u "jdbc:hive2://127.0.0.1:10000/taxi_analytics" -e "SELECT MAX(trip_distance) AS max_dist, MIN(trip_distance) AS min_dist, ROUND(AVG(trip_distance), 2) AS avg_dist, COUNT(*) AS total_trips FROM taxi_trips WHERE trip_distance > 0;"

echo ""
echo "[QUERY] Q8 — Trip Demand by Pickup Hour"
beeline -u "jdbc:hive2://127.0.0.1:10000/taxi_analytics" -e "SELECT pickup_hour, COUNT(*) AS trip_count, ROUND(AVG(trip_duration_min), 2) AS avg_duration_min, ROUND(AVG(total_amount), 2) AS avg_total_amount FROM taxi_trips GROUP BY pickup_hour ORDER BY pickup_hour;"

echo ""
echo "[QUERY] Q9 — Weekday vs Weekend Analysis"
beeline -u "jdbc:hive2://127.0.0.1:10000/taxi_analytics" -e "SELECT CASE WHEN is_weekend = 1 THEN 'Weekend' ELSE 'Weekday' END AS day_type, COUNT(*) AS trip_count, ROUND(AVG(total_amount), 2) AS avg_total_amount, ROUND(AVG(trip_distance), 2) AS avg_distance_miles, ROUND(AVG(tip_amount), 2) AS avg_tip FROM taxi_trips GROUP BY is_weekend ORDER BY is_weekend;"

echo ""
echo "[QUERY] Q10 — Payment Type Analysis"
beeline -u "jdbc:hive2://127.0.0.1:10000/taxi_analytics" -e "SELECT CASE WHEN payment_type = 1 THEN 'Credit Card' WHEN payment_type = 2 THEN 'Cash' WHEN payment_type = 3 THEN 'No Charge' WHEN payment_type = 4 THEN 'Dispute' ELSE 'Unknown' END AS payment_method, COUNT(*) AS trip_count, ROUND(SUM(total_amount), 2) AS total_revenue, ROUND(AVG(tip_amount), 2) AS avg_tip, ROUND(AVG(total_amount), 2) AS avg_fare FROM taxi_trips GROUP BY payment_type ORDER BY trip_count DESC;"

echo ""
echo "[QUERY] Q11 — High-Volume Zones (> 500 Trips)"
beeline -u "jdbc:hive2://127.0.0.1:10000/taxi_analytics" -e "SELECT PULocationID, COUNT(*) AS trip_count, ROUND(AVG(total_amount), 2) AS avg_total_amount, ROUND(AVG(tip_amount), 2) AS avg_tip, ROUND(SUM(total_amount), 2) AS total_revenue FROM taxi_trips GROUP BY PULocationID HAVING COUNT(*) > 500 ORDER BY trip_count DESC;"

echo ""
echo "[QUERY] Q12 — Monthly Performance Breakdown"
beeline -u "jdbc:hive2://127.0.0.1:10000/taxi_analytics" -e "SELECT pickup_year, pickup_month, COUNT(*) AS trip_count, ROUND(SUM(total_amount), 2) AS total_revenue, ROUND(AVG(trip_distance), 2) AS avg_distance_miles, ROUND(AVG(trip_duration_min), 2) AS avg_duration_min, ROUND(AVG(fare_amount), 2) AS avg_fare FROM taxi_trips GROUP BY pickup_year, pickup_month ORDER BY pickup_year, pickup_month;"

echo ""
echo "============================================================"
echo "          ALL HIVE QUERIES COMPLETED SUCCESSFULLY!          "
echo "============================================================"
