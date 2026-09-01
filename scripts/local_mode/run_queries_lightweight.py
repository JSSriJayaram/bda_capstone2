import sqlite3
import pandas as pd
import os

# Ensure temp directory is on /home partition where there is 63GB free space
os.environ['TMPDIR'] = '/Users/s4n/Documents/clg/sem7/bda_capstone2/scripts/local_mode/hive/scratch'
temp_dir = '/Users/s4n/Documents/clg/sem7/bda_capstone2/scripts/local_mode/hive/scratch'
os.makedirs(temp_dir, exist_ok=True)

output_dir = "/Users/s4n/Documents/clg/sem7/bda_capstone2/scripts/local_mode/output/hive_results"
os.makedirs(output_dir, exist_ok=True)

csv_path = "../../data/processed/yellow_tripdata_cleaned.csv"
print(f"[INFO] Loading dataset ({csv_path}) into lightweight SQLite database...")

df = pd.read_csv(csv_path)

# Connect to SQLite database file stored on /home partition
db_file = os.path.join(temp_dir, "taxi_lightweight.db")
if os.path.exists(db_file):
    os.remove(db_file)

conn = sqlite3.connect(db_file)

# Force SQLite temp operations into memory / home partition
conn.execute("PRAGMA temp_store = MEMORY;")
conn.execute("PRAGMA journal_mode = OFF;")
conn.execute("PRAGMA synchronous = OFF;")

df.to_sql("taxi_trips", conn, index=False, if_exists="replace")

print(f"[SUCCESS] Loaded {len(df):,} records into SQLite.\n")

queries = {
    "Q1 — Total Number of Taxi Trips": (
        "SELECT COUNT(*) AS total_trips FROM taxi_trips;",
        "q1_total_trips.csv"
    ),
    "Q2 — Total Revenue Generated": (
        "SELECT ROUND(SUM(total_amount), 2) AS total_revenue FROM taxi_trips;",
        "q2_total_revenue.csv"
    ),
    "Q3 — Platform-wide Averages": (
        "SELECT ROUND(AVG(fare_amount), 2) AS avg_fare, ROUND(AVG(trip_distance), 2) AS avg_distance_miles, ROUND(AVG(trip_duration_min), 2) AS avg_duration_min, ROUND(AVG(avg_speed_mph), 2) AS avg_speed_mph FROM taxi_trips;",
        "q3_platform_averages.csv"
    ),
    "Q4 — Top 20 Busiest Zones": (
        "SELECT PULocationID, COUNT(*) AS trip_count FROM taxi_trips GROUP BY PULocationID ORDER BY trip_count DESC LIMIT 20;",
        "q4_top20_busiest_zones.csv"
    ),
    "Q5 — Top 20 Revenue Zones": (
        "SELECT PULocationID, COUNT(*) AS trip_count, ROUND(SUM(total_amount), 2) AS total_revenue, ROUND(AVG(total_amount), 2) AS avg_revenue_per_trip FROM taxi_trips GROUP BY PULocationID ORDER BY total_revenue DESC LIMIT 20;",
        "q5_top20_revenue_zones.csv"
    ),
    "Q6 — Top 10 Highest-Value Trips": (
        "SELECT PULocationID, DOLocationID, tpep_pickup_datetime, ROUND(trip_distance, 2) AS distance_mi, ROUND(trip_duration_min, 2) AS duration_min, ROUND(fare_amount, 2) AS fare, ROUND(tip_amount, 2) AS tip, ROUND(total_amount, 2) AS total FROM taxi_trips ORDER BY total_amount DESC LIMIT 10;",
        "q6_top10_high_value_trips.csv"
    ),
    "Q7 — Distance Statistics": (
        "SELECT MAX(trip_distance) AS max_dist, MIN(trip_distance) AS min_dist, ROUND(AVG(trip_distance), 2) AS avg_dist, COUNT(*) AS total_trips FROM taxi_trips WHERE trip_distance > 0;",
        "q7_distance_stats.csv"
    ),
    "Q8 — Trip Demand by Pickup Hour": (
        "SELECT pickup_hour, COUNT(*) AS trip_count, ROUND(AVG(trip_duration_min), 2) AS avg_duration_min, ROUND(AVG(total_amount), 2) AS avg_total_amount FROM taxi_trips GROUP BY pickup_hour ORDER BY pickup_hour;",
        "q8_demand_by_hour.csv"
    ),
    "Q9 — Weekday vs Weekend Analysis": (
        "SELECT CASE WHEN is_weekend = 1 THEN 'Weekend' ELSE 'Weekday' END AS day_type, COUNT(*) AS trip_count, ROUND(AVG(total_amount), 2) AS avg_total_amount, ROUND(AVG(trip_distance), 2) AS avg_distance_miles, ROUND(AVG(tip_amount), 2) AS avg_tip FROM taxi_trips GROUP BY is_weekend ORDER BY is_weekend;",
        "q9_weekday_vs_weekend.csv"
    ),
    "Q10 — Payment Type Analysis": (
        "SELECT CASE WHEN payment_type = 1 THEN 'Credit Card' WHEN payment_type = 2 THEN 'Cash' WHEN payment_type = 3 THEN 'No Charge' WHEN payment_type = 4 THEN 'Dispute' ELSE 'Unknown' END AS payment_method, COUNT(*) AS trip_count, ROUND(SUM(total_amount), 2) AS total_revenue, ROUND(AVG(tip_amount), 2) AS avg_tip, ROUND(AVG(total_amount), 2) AS avg_fare FROM taxi_trips GROUP BY payment_type ORDER BY trip_count DESC;",
        "q10_payment_types.csv"
    ),
    "Q11 — High-Volume Zones": (
        "SELECT PULocationID, COUNT(*) AS trip_count, ROUND(AVG(total_amount), 2) AS avg_total_amount, ROUND(AVG(tip_amount), 2) AS avg_tip, ROUND(SUM(total_amount), 2) AS total_revenue FROM taxi_trips GROUP BY PULocationID HAVING COUNT(*) >= (SELECT MAX(10, COUNT(*)/500) FROM taxi_trips) ORDER BY trip_count DESC;",
        "q11_high_volume_zones.csv"
    ),
    "Q12 — Monthly Performance Breakdown": (
        "SELECT pickup_year, pickup_month, COUNT(*) AS trip_count, ROUND(SUM(total_amount), 2) AS total_revenue, ROUND(AVG(trip_distance), 2) AS avg_distance_miles, ROUND(AVG(trip_duration_min), 2) AS avg_duration_min, ROUND(AVG(fare_amount), 2) AS avg_fare FROM taxi_trips GROUP BY pickup_year, pickup_month ORDER BY pickup_year, pickup_month;",
        "q12_monthly_breakdown.csv"
    )
}

print("============================================================")
print("             EXECUTING ALL 12 ANALYTICS QUERIES             ")
print("============================================================")

for name, (sql, filename) in queries.items():
    print(f"\n>>> {name}")
    res = pd.read_sql_query(sql, conn)
    print(res.to_string(index=False))
    
    # Save output file
    out_file_path = os.path.join(output_dir, filename)
    res.to_csv(out_file_path, index=False)

conn.close()

print("\n" + "="*60)
print(f"[SUCCESS] All 12 queries completed in seconds!")
print(f"[INFO] All output CSV files saved to: {output_dir}")
print("="*60)
