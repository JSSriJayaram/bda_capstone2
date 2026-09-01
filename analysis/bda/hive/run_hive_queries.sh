#!/bin/bash

# ============================================================
#   run_hive_queries.sh
#   Runs all 12 Hive analytics queries on NYC Taxi dataset
#   No Docker required - uses local Hive with embedded Derby
# ============================================================

set -e

export JAVA_HOME=/usr/lib/jvm/java-17-openjdk
export HADOOP_HOME=/opt/hadoop
export HIVE_HOME=/opt/hive
export HIVE_CONF_DIR=/home/real/bda/hive/conf
export PATH=$PATH:$HIVE_HOME/bin:$HADOOP_HOME/bin
export HADOOP_CONF_DIR=/home/real/bda/local-conf
export HADOOP_CLIENT_OPTS='-Xmx4g'

# Derby tmp dir in /home to avoid root disk full
export DERBY_HOME=/opt/hive/lib
mkdir -p /home/real/bda/hive/scratch /home/real/bda/hive/logs /home/real/bda/hive/output

HIVE_CMD="hive --hiveconf hive.execution.engine=mr --hiveconf hive.conf.dir=/home/real/bda/hive/conf"
OUT=/home/real/bda/hive/output

echo ""
echo "============================================================"
echo "   NYC Taxi - Hive Analytics Suite (Local Mode, No Docker)"
echo "============================================================"
echo ""

# ---- Q0: Setup Database & Table ----
echo "------------------------------------------------------------"
echo " Q0: Setting up Database & Loading Data..."
echo "------------------------------------------------------------"
$HIVE_CMD -f /home/real/bda/hive/taxi_analytics.sql 2>/dev/null
echo "[OK] Database and table created."
echo ""

run_query() {
    local label="$1"
    local sql="$2"
    local outfile="$3"
    echo "------------------------------------------------------------"
    echo " $label"
    echo "------------------------------------------------------------"
    $HIVE_CMD -e "USE taxi_analytics; $sql" 2>/dev/null | tee "$OUT/$outfile"
    echo "[Saved to: $OUT/$outfile]"
    echo ""
}

# ---- Q1: Total Trips ----
run_query "Q1: Total Number of Taxi Trips" \
    "SELECT COUNT(*) AS total_trips FROM taxi_trips;" \
    "q1_total_trips.txt"

# ---- Q2: Total Revenue ----
run_query "Q2: Total Revenue Generated" \
    "SELECT ROUND(SUM(total_amount), 2) AS total_revenue FROM taxi_trips;" \
    "q2_total_revenue.txt"

# ---- Q3: Platform-wide Averages ----
run_query "Q3: Platform-wide Averages" \
    "SELECT ROUND(AVG(fare_amount), 2) AS avg_fare, ROUND(AVG(trip_distance), 2) AS avg_distance_miles, ROUND(AVG(trip_duration_min), 2) AS avg_duration_min, ROUND(AVG(avg_speed_mph), 2) AS avg_speed_mph FROM taxi_trips;" \
    "q3_platform_averages.txt"

# ---- Q4: Top 20 Busiest Zones ----
run_query "Q4: Top 20 Busiest Pickup Zones" \
    "SELECT PULocationID, COUNT(*) AS trip_count FROM taxi_trips GROUP BY PULocationID ORDER BY trip_count DESC LIMIT 20;" \
    "q4_top20_busiest_zones.txt"

# ---- Q5: Top 20 Revenue Zones ----
run_query "Q5: Top 20 Revenue Zones" \
    "SELECT PULocationID, COUNT(*) AS trip_count, ROUND(SUM(total_amount), 2) AS total_revenue, ROUND(AVG(total_amount), 2) AS avg_revenue_per_trip FROM taxi_trips GROUP BY PULocationID ORDER BY total_revenue DESC LIMIT 20;" \
    "q5_top20_revenue_zones.txt"

# ---- Q6: Top 10 Highest-Value Trips ----
run_query "Q6: Top 10 Highest-Value Trips" \
    "SELECT PULocationID, DOLocationID, tpep_pickup_datetime, ROUND(trip_distance, 2) AS distance_mi, ROUND(trip_duration_min, 2) AS duration_min, ROUND(fare_amount, 2) AS fare, ROUND(tip_amount, 2) AS tip, ROUND(total_amount, 2) AS total FROM taxi_trips ORDER BY total_amount DESC LIMIT 10;" \
    "q6_top10_high_value_trips.txt"

# ---- Q7: Distance Statistics ----
run_query "Q7: Distance Statistics" \
    "SELECT MAX(trip_distance) AS max_dist, MIN(trip_distance) AS min_dist, ROUND(AVG(trip_distance), 2) AS avg_dist, COUNT(*) AS total_trips FROM taxi_trips WHERE trip_distance > 0;" \
    "q7_distance_stats.txt"

# ---- Q8: Trip Demand by Pickup Hour ----
run_query "Q8: Trip Demand by Pickup Hour" \
    "SELECT pickup_hour, COUNT(*) AS trip_count, ROUND(AVG(trip_duration_min), 2) AS avg_duration_min, ROUND(AVG(total_amount), 2) AS avg_total_amount FROM taxi_trips GROUP BY pickup_hour ORDER BY pickup_hour;" \
    "q8_demand_by_hour.txt"

# ---- Q9: Weekday vs Weekend ----
run_query "Q9: Weekday vs Weekend Analysis" \
    "SELECT CASE WHEN is_weekend = 1 THEN 'Weekend' ELSE 'Weekday' END AS day_type, COUNT(*) AS trip_count, ROUND(AVG(total_amount), 2) AS avg_total_amount, ROUND(AVG(trip_distance), 2) AS avg_distance_miles, ROUND(AVG(tip_amount), 2) AS avg_tip FROM taxi_trips GROUP BY is_weekend ORDER BY is_weekend;" \
    "q9_weekday_vs_weekend.txt"

# ---- Q10: Payment Type Analysis ----
run_query "Q10: Payment Type Analysis" \
    "SELECT CASE WHEN payment_type = 1 THEN 'Credit Card' WHEN payment_type = 2 THEN 'Cash' WHEN payment_type = 3 THEN 'No Charge' WHEN payment_type = 4 THEN 'Dispute' ELSE 'Unknown' END AS payment_method, COUNT(*) AS trip_count, ROUND(SUM(total_amount), 2) AS total_revenue, ROUND(AVG(tip_amount), 2) AS avg_tip, ROUND(AVG(total_amount), 2) AS avg_fare FROM taxi_trips GROUP BY payment_type ORDER BY trip_count DESC;" \
    "q10_payment_types.txt"

# ---- Q11: High-Volume Zones (> 500 trips in sample) ----
run_query "Q11: High-Volume Zones (>500 trips)" \
    "SELECT PULocationID, COUNT(*) AS trip_count, ROUND(AVG(total_amount), 2) AS avg_total_amount, ROUND(AVG(tip_amount), 2) AS avg_tip, ROUND(SUM(total_amount), 2) AS total_revenue FROM taxi_trips GROUP BY PULocationID HAVING COUNT(*) > 500 ORDER BY trip_count DESC;" \
    "q11_high_volume_zones.txt"

# ---- Q12: Monthly Performance Breakdown ----
run_query "Q12: Monthly Performance Breakdown" \
    "SELECT pickup_year, pickup_month, COUNT(*) AS trip_count, ROUND(SUM(total_amount), 2) AS total_revenue, ROUND(AVG(trip_distance), 2) AS avg_distance_miles, ROUND(AVG(trip_duration_min), 2) AS avg_duration_min, ROUND(AVG(fare_amount), 2) AS avg_fare FROM taxi_trips GROUP BY pickup_year, pickup_month ORDER BY pickup_year, pickup_month;" \
    "q12_monthly_breakdown.txt"

echo "============================================================"
echo "   ALL QUERIES COMPLETE!"
echo "   Outputs saved in: $OUT/"
echo "============================================================"
ls -la "$OUT/"
