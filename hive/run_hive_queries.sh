#!/usr/bin/env bash
# =============================================================
# run_hive_queries.sh
# NYC Yellow Taxi — Apache Hive Analytics Pipeline
# Runs all 12 HiveQL analytical queries against taxi_analytics.taxi_trips
# 
# IMPORTANT: HADOOP_CLIENT_OPTS='-Xmx4g' is required.
# Hive local-MR mode runs map/reduce tasks inside the CLI JVM.
# Without 4 GB heap, ORDER BY on 7.6M rows causes OOM.
# =============================================================
set -e

CONTAINER="taxi-hive-server"
RESULTS_DIR="results/hive"
mkdir -p "$RESULTS_DIR"

# Key fix: increase Hive CLI JVM heap to 4 GB
HIVE_ENV="HADOOP_CLIENT_OPTS='-Xmx4g' HADOOP_HEAPSIZE=4096"
HIVE_OPTS="--hiveconf hive.execution.engine=mr"

run_query() {
    local qname="$1"
    local qsql="$2"
    echo ""
    echo "========================================"
    echo "Running $qname ..."
    echo "========================================"
    docker exec "$CONTAINER" bash -c "
        export HADOOP_CLIENT_OPTS='-Xmx4g'
        export HADOOP_HEAPSIZE=4096
        hive $HIVE_OPTS --hiveconf hive.cli.print.header=true -e \"USE taxi_analytics; $qsql\"
    " 2>&1 | grep -v SLF4J | grep -v 'Class path' | grep -v 'Found binding' | grep -v 'See http' | grep -v 'Actual binding' \
      | tee -a "$RESULTS_DIR/all_queries_output.txt"
    echo ""
}

echo "===== NYC YELLOW TAXI HIVE ANALYTICS =====" | tee "$RESULTS_DIR/all_queries_output.txt"
echo "Started: $(date)" | tee -a "$RESULTS_DIR/all_queries_output.txt"

# --------------------------------------------------
# STEP 1: Setup — Create database and tables
# --------------------------------------------------
echo ""
echo "=== STEP 1: Database & Table Setup ==="
docker exec "$CONTAINER" bash -c "
    export HADOOP_CLIENT_OPTS='-Xmx4g'
    hive $HIVE_OPTS -f /tmp/taxi_analytics.sql
" 2>&1 | grep -v SLF4J | grep -v 'Class path' | grep -v 'Found binding' | grep -v 'See http' | grep -v 'Actual binding'

# --------------------------------------------------
# STEP 2: Run all 12 queries
# --------------------------------------------------

run_query "Q1: Total Taxi Trips" \
  "SELECT COUNT(*) AS total_trips FROM taxi_trips;"

run_query "Q2: Total Revenue" \
  "SELECT ROUND(SUM(total_amount),2) AS total_revenue FROM taxi_trips;"

run_query "Q3: Platform-wide Averages" \
  "SELECT ROUND(AVG(fare_amount),2) AS avg_fare,
          ROUND(AVG(trip_distance),2) AS avg_distance_miles,
          ROUND(AVG(trip_duration_min),2) AS avg_duration_min,
          ROUND(AVG(avg_speed_mph),2) AS avg_speed_mph
   FROM taxi_trips;"

run_query "Q4: Trip Volume by Pickup Zone (Top 20)" \
  "SELECT PULocationID, COUNT(*) AS trip_count
   FROM taxi_trips GROUP BY PULocationID
   ORDER BY trip_count DESC LIMIT 20;"

run_query "Q5: Revenue by Pickup Zone (Top 20)" \
  "SELECT PULocationID, COUNT(*) AS trip_count,
          ROUND(SUM(total_amount),2) AS total_revenue,
          ROUND(AVG(total_amount),2) AS avg_revenue_per_trip
   FROM taxi_trips GROUP BY PULocationID
   ORDER BY total_revenue DESC LIMIT 20;"

run_query "Q6: Top 10 Highest-Value Trips" \
  "SELECT PULocationID, DOLocationID, tpep_pickup_datetime,
          ROUND(trip_distance,2) AS trip_distance_miles,
          ROUND(trip_duration_min,2) AS trip_duration_min,
          ROUND(fare_amount,2) AS fare_amount,
          ROUND(tip_amount,2) AS tip_amount,
          ROUND(total_amount,2) AS total_amount
   FROM taxi_trips ORDER BY total_amount DESC LIMIT 10;"

run_query "Q7: Trip Distance Statistics" \
  "SELECT MAX(trip_distance) AS max_distance_miles,
          MIN(trip_distance) AS min_distance_miles,
          ROUND(AVG(trip_distance),2) AS avg_distance_miles,
          COUNT(*) AS total_trips
   FROM taxi_trips WHERE trip_distance > 0;"

run_query "Q8: Trip Demand by Pickup Hour" \
  "SELECT pickup_hour, COUNT(*) AS trip_count,
          ROUND(AVG(trip_duration_min),2) AS avg_duration_min,
          ROUND(AVG(total_amount),2) AS avg_total_amount
   FROM taxi_trips GROUP BY pickup_hour ORDER BY pickup_hour;"

run_query "Q9: Weekday vs Weekend Analysis" \
  "SELECT CASE WHEN is_weekend=1 THEN 'Weekend' ELSE 'Weekday' END AS day_type,
          COUNT(*) AS trip_count,
          ROUND(AVG(total_amount),2) AS avg_total_amount,
          ROUND(AVG(trip_distance),2) AS avg_distance_miles,
          ROUND(AVG(tip_amount),2) AS avg_tip
   FROM taxi_trips GROUP BY is_weekend ORDER BY is_weekend;"

run_query "Q10: Payment Type Analysis" \
  "SELECT CASE WHEN payment_type=1 THEN 'Credit Card'
               WHEN payment_type=2 THEN 'Cash'
               WHEN payment_type=3 THEN 'No Charge'
               WHEN payment_type=4 THEN 'Dispute'
               ELSE 'Unknown' END AS payment_method,
          COUNT(*) AS trip_count,
          ROUND(SUM(total_amount),2) AS total_revenue,
          ROUND(AVG(tip_amount),2) AS avg_tip,
          ROUND(AVG(total_amount),2) AS avg_fare
   FROM taxi_trips GROUP BY payment_type ORDER BY trip_count DESC;"

run_query "Q11: High-Volume Pickup Zones (>10000 trips)" \
  "SELECT PULocationID, COUNT(*) AS trip_count,
          ROUND(AVG(total_amount),2) AS avg_total_amount,
          ROUND(AVG(tip_amount),2) AS avg_tip,
          ROUND(SUM(total_amount),2) AS total_revenue
   FROM taxi_trips GROUP BY PULocationID
   HAVING COUNT(*) > 10000 ORDER BY trip_count DESC;"

run_query "Q12: Monthly Performance" \
  "SELECT pickup_year, pickup_month, COUNT(*) AS trip_count,
          ROUND(SUM(total_amount),2) AS total_revenue,
          ROUND(AVG(trip_distance),2) AS avg_distance_miles,
          ROUND(AVG(trip_duration_min),2) AS avg_duration_min,
          ROUND(AVG(fare_amount),2) AS avg_fare
   FROM taxi_trips GROUP BY pickup_year, pickup_month
   ORDER BY pickup_year, pickup_month;"

echo ""
echo "===== ALL 12 HIVE QUERIES COMPLETE =====" | tee -a "$RESULTS_DIR/all_queries_output.txt"
echo "Finished: $(date)" | tee -a "$RESULTS_DIR/all_queries_output.txt"
echo "Results saved to: $RESULTS_DIR/all_queries_output.txt"
