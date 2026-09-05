# NYC Yellow Taxi — Big Data Analytics Pipeline

A complete, reproducible Big Data Analytics pipeline using **Apache Hadoop 3.3.6 YARN + MapReduce + Hive 3.1.3** running on Docker, with Python-based visualization.

**Dataset:** NYC Yellow Taxi (Jan–Mar 2026) · ~7.6 million records · 1.1 GB CSV  
**Architecture:** HDFS → Java MapReduce (zone performance + hourly demand + payment tipping behavior) + Hive (12 analytical queries) + Python visualizations

---

## Project Structure

```
bda_capstone2/
├── docker-compose.yml              # 5-container Hadoop+Hive cluster
├── hadoop.env                      # Shared Hadoop env variables
├── requirements.txt                # Python dependencies (pandas, matplotlib, …)
├── run_analysis.sh                 # One-shot: install deps & run visualizations
├── data/
│   ├── csv/
│   │   └── yellow_tripdata_cleaned.csv   # 1.1 GB cleaned dataset
│   └── raw/                        # Source parquet files (gitignored)
├── mapreduce/
│   ├── TaxiMapper.java             # Emits PULocationID → trip metrics
│   ├── TaxiReducer.java            # Aggregates zone-level stats
│   ├── TaxiDriver.java             # Zone job configuration
│   ├── HourlyTaxiMapper.java       # Emits pickup_hour → trip metrics
│   ├── HourlyTaxiReducer.java      # Aggregates hourly stats
│   ├── HourlyTaxiDriver.java       # Hourly job configuration
│   ├── TipBehaviorMapper.java      # Emits composite payment_fare_bucket → tip metrics
│   ├── TipBehaviorReducer.java     # Aggregates tipping stats & zero-tip rate
│   └── TipBehaviorDriver.java      # Tip behavior job configuration
├── hive/
│   ├── taxi_analytics.sql          # All DDL + 12 HiveQL queries
│   ├── run_hive_queries.sh         # Script to run all 12 queries
│   └── conf/                       # Hive/Hadoop XML configs
├── analysis/
│   └── generate_visualizations.py  # Produces 8 charts → visualizations/
├── scripts/
│   └── local_mode/                 # Legacy non-Docker local scripts
├── visualizations/                 # ← Charts live HERE (committed, easy access)
│   ├── hourly_demand_revenue.png
│   ├── top_pickup_zones.png
│   ├── payment_type_share.png
│   ├── weekday_vs_weekend.png
│   ├── monthly_revenue_trend.png
│   ├── zone_speed_vs_fare.png
│   ├── tip_behavior_analysis.png
│   └── ratecode_yield.png
└── results/                        # Pipeline TSV/text outputs (gitignored)
    ├── mapreduce/
    │   ├── zone_performance.tsv
    │   ├── hourly_performance.tsv
    │   └── tip_behavior.tsv
    └── hive/
        └── all_queries_output.txt
```

---

## Prerequisites

| Tool                           | Version |
| ------------------------------ | ------- |
| Docker Desktop                 | ≥ 4.x   |
| Docker Compose                 | ≥ 2.x   |
| ~12 GB RAM available to Docker | —       |
| ~15 GB disk free               | —       |

---

## Step 1 — Start the Cluster

```bash
cd bda2/
docker compose up -d
```

Wait ~30 seconds for NameNode to format and services to start.

**Verify all 5 containers are running:**

```bash
docker compose ps
```

Expected output:

```
taxi-namenode        Up   0.0.0.0:8021->8020, 0.0.0.0:9871->9870
taxi-datanode        Up
taxi-resourcemanager Up   0.0.0.0:8089->8088
taxi-nodemanager     Up
taxi-hive-server     Up   0.0.0.0:9084->9083
```

---

## Step 2 — Health Checks

**HDFS:**

```bash
docker exec taxi-namenode hdfs dfs -ls /
```

**YARN NodeManager:**

```bash
docker exec taxi-resourcemanager yarn node -list
```

Must show at least 1 RUNNING node.

**Hive Metastore:**

```bash
docker exec taxi-hive-server hive -e "SHOW DATABASES;"
```

**Web UIs:**

- HDFS NameNode: http://localhost:9871
- YARN ResourceManager: http://localhost:8089

---

## Step 3 — Upload Dataset to HDFS

Place the cleaned CSV (`yellow_tripdata_cleaned.csv`, ~1.1 GB) in `data/processed/`.

```bash
# Create HDFS directories
docker exec taxi-namenode hdfs dfs -mkdir -p /user/bda/taxi/clean

# Upload the full CSV
docker cp data/processed/yellow_tripdata_cleaned.csv taxi-namenode:/tmp/
docker exec taxi-namenode hdfs dfs -put /tmp/yellow_tripdata_cleaned.csv \
    /user/bda/taxi/clean/

# Verify
docker exec taxi-namenode hdfs dfs -ls /user/bda/taxi/clean/
```

Expected: ~1.1 GB file in HDFS.

---

## Step 4 — Compile MapReduce Jobs

Note: The Hadoop Docker image (`apache/hadoop:3.3.6`) provides Java Runtime (JRE) for executing jobs, but does not include `javac`. MapReduce source code is compiled locally using your host machine's Java compiler and bundled into a single deployment JAR (`taxi-analytics-all.jar`).

### 4.1 Download Hadoop dependencies (one-time setup)

```bash
mkdir -p mapreduce/lib
docker cp taxi-namenode:/opt/hadoop/share/hadoop/common/ mapreduce/lib/common/
docker cp taxi-namenode:/opt/hadoop/share/hadoop/mapreduce/ mapreduce/lib/mapreduce/
```

### 4.2 Compile & package all MapReduce jobs

```bash
# 1. Compile Java files
mkdir -p mapreduce/target/classes
javac --release 8 -cp "mapreduce/lib/common/*:mapreduce/lib/common/lib/*:mapreduce/lib/mapreduce/*:mapreduce/lib/mapreduce/lib/*" -d mapreduce/target/classes mapreduce/*.java

# 2. Package into unified JAR
jar -cvf mapreduce/target/taxi-analytics-all.jar -C mapreduce/target/classes .

# 3. Copy deployment JAR into NameNode container
docker exec taxi-namenode mkdir -p /opt/taxi_mr/
docker cp mapreduce/target/taxi-analytics-all.jar taxi-namenode:/opt/taxi_mr/taxi-analytics-all.jar
```

---

## Step 5 — Run MapReduce Jobs

### 5.1 Job 1: Zone Performance Analysis

Aggregates trip volume, revenue, average fare, distance, duration, speed, and tips across **261 pickup zones**.

```bash
# Remove previous output if exists
docker exec taxi-namenode hdfs dfs -rm -r -f /user/bda/taxi/mapreduce/zone_performance

# Execute Job 1
docker exec taxi-namenode hadoop jar /opt/taxi_mr/taxi-analytics-all.jar TaxiDriver \
  /user/bda/taxi/clean/yellow_tripdata_cleaned.csv \
  /user/bda/taxi/mapreduce/zone_performance

# Save TSV locally
mkdir -p results/mapreduce
docker exec taxi-namenode bash -c "
  echo -e 'PULocationID\ttotal_trips\ttotal_revenue\tavg_fare\tavg_distance\tavg_duration\tavg_speed\tavg_tip'
  hdfs dfs -cat /user/bda/taxi/mapreduce/zone_performance/part-r-00000
" > results/mapreduce/zone_performance.tsv
```

### 5.2 Job 2: Hourly Demand & Revenue Analysis

Aggregates trip metrics across **24 hourly buckets** (00–23) for temporal peak demand analysis.

```bash
# Remove previous output if exists
docker exec taxi-namenode hdfs dfs -rm -r -f /user/bda/taxi/mapreduce/hourly_performance

# Execute Job 2
docker exec taxi-namenode hadoop jar /opt/taxi_mr/taxi-analytics-all.jar HourlyTaxiDriver \
  /user/bda/taxi/clean/yellow_tripdata_cleaned.csv \
  /user/bda/taxi/mapreduce/hourly_performance

# Save TSV locally
mkdir -p results/mapreduce
docker exec taxi-namenode bash -c "
  echo -e 'hour\ttotal_trips\ttotal_revenue\tavg_revenue\tavg_passengers\tavg_duration\tavg_tip\ttip_pct'
  hdfs dfs -cat /user/bda/taxi/mapreduce/hourly_performance/part-r-00000
" > results/mapreduce/hourly_performance.tsv
```

### 5.3 Job 3: Payment Type & Tipping Behavior Binning

Applies **Value-Range Binning** on `fare_amount` combined with `payment_type` to analyze tipping behavior across 6 price brackets.

```bash
# Remove previous output if exists
docker exec taxi-namenode hdfs dfs -rm -r -f /user/bda/taxi/mapreduce/tip_behavior

# Execute Job 3
docker exec taxi-namenode hadoop jar /opt/taxi_mr/taxi-analytics-all.jar TipBehaviorDriver \
  /user/bda/taxi/clean/yellow_tripdata_cleaned.csv \
  /user/bda/taxi/mapreduce/tip_behavior

# Save TSV locally
mkdir -p results/mapreduce
docker exec taxi-namenode bash -c "
  echo -e 'payment_fare_bucket\ttotal_trips\tavg_tip_amount\tavg_tip_pct\tzero_tip_rate\tavg_passengers'
  hdfs dfs -cat /user/bda/taxi/mapreduce/tip_behavior/part-r-00000
" > results/mapreduce/tip_behavior.tsv
```

## Step 7 — Run Hive Analytics

### ⚠️ Critical: Heap Size

Hive runs local MapReduce **inside the CLI JVM**. ORDER BY on 7.6M rows requires 4 GB heap.  
**Always prefix Hive commands with:**

```bash
export HADOOP_CLIENT_OPTS='-Xmx4g'
```

### 7.1 Database & ORC Table Setup (Run Once)

Execute the full script inside the container (which reads `/opt/hive/taxi_analytics.sql` directly without shell escaping issues):

```bash
docker exec taxi-hive-server bash -c "export HADOOP_CLIENT_OPTS='-Xmx4g' && hive --hiveconf hive.execution.engine=mr -f /opt/hive/taxi_analytics.sql"
```

---

## Step 8 — Run Hive Analytics Queries

### 📊 Individual Queries (Run Any Query One-by-One)

**Q1 — Total Taxi Trips**
```bash
docker exec taxi-hive-server bash -c "export HADOOP_CLIENT_OPTS='-Xmx4g' && hive --hiveconf hive.execution.engine=mr --hiveconf hive.cli.print.header=true -e 'USE taxi_analytics; SELECT COUNT(*) AS total_trips FROM taxi_trips;'"
```

**Q2 — Total Revenue**
```bash
docker exec taxi-hive-server bash -c "export HADOOP_CLIENT_OPTS='-Xmx4g' && hive --hiveconf hive.execution.engine=mr --hiveconf hive.cli.print.header=true -e 'USE taxi_analytics; SELECT ROUND(SUM(total_amount),2) AS total_revenue FROM taxi_trips;'"
```

**Q3 — Platform-wide Averages**
```bash
docker exec taxi-hive-server bash -c "export HADOOP_CLIENT_OPTS='-Xmx4g' && hive --hiveconf hive.execution.engine=mr --hiveconf hive.cli.print.header=true -e 'USE taxi_analytics; SELECT ROUND(AVG(fare_amount),2) AS avg_fare, ROUND(AVG(trip_distance),2) AS avg_distance_miles, ROUND(AVG(trip_duration_min),2) AS avg_duration_min, ROUND(AVG(avg_speed_mph),2) AS avg_speed_mph FROM taxi_trips;'"
```

**Q4 — Trip Volume by Pickup Zone (Top 20)**
```bash
docker exec taxi-hive-server bash -c "export HADOOP_CLIENT_OPTS='-Xmx4g' && hive --hiveconf hive.execution.engine=mr --hiveconf hive.cli.print.header=true -e 'USE taxi_analytics; SELECT PULocationID, COUNT(*) AS trip_count FROM taxi_trips GROUP BY PULocationID ORDER BY trip_count DESC LIMIT 20;'"
```

**Q5 — Revenue by Pickup Zone (Top 20)**
```bash
docker exec taxi-hive-server bash -c "export HADOOP_CLIENT_OPTS='-Xmx4g' && hive --hiveconf hive.execution.engine=mr --hiveconf hive.cli.print.header=true -e 'USE taxi_analytics; SELECT PULocationID, COUNT(*) AS trip_count, ROUND(SUM(total_amount),2) AS total_revenue, ROUND(AVG(total_amount),2) AS avg_revenue_per_trip FROM taxi_trips GROUP BY PULocationID ORDER BY total_revenue DESC LIMIT 20;'"
```

**Q6 — Top 10 Highest-Value Trips**
```bash
docker exec taxi-hive-server bash -c "export HADOOP_CLIENT_OPTS='-Xmx4g' && hive --hiveconf hive.execution.engine=mr --hiveconf hive.cli.print.header=true -e 'USE taxi_analytics; SELECT PULocationID, DOLocationID, tpep_pickup_datetime, ROUND(trip_distance,2) AS trip_distance_miles, ROUND(trip_duration_min,2) AS trip_duration_min, ROUND(fare_amount,2) AS fare_amount, ROUND(tip_amount,2) AS tip_amount, ROUND(total_amount,2) AS total_amount FROM taxi_trips ORDER BY total_amount DESC LIMIT 10;'"
```

**Q7 — Trip Distance Statistics**
```bash
docker exec taxi-hive-server bash -c "export HADOOP_CLIENT_OPTS='-Xmx4g' && hive --hiveconf hive.execution.engine=mr --hiveconf hive.cli.print.header=true -e 'USE taxi_analytics; SELECT MAX(trip_distance) AS max_distance_miles, MIN(trip_distance) AS min_distance_miles, ROUND(AVG(trip_distance),2) AS avg_distance_miles, COUNT(*) AS total_trips FROM taxi_trips WHERE trip_distance > 0;'"
```

**Q8 — Trip Demand by Pickup Hour**
```bash
docker exec taxi-hive-server bash -c "export HADOOP_CLIENT_OPTS='-Xmx4g' && hive --hiveconf hive.execution.engine=mr --hiveconf hive.cli.print.header=true -e 'USE taxi_analytics; SELECT pickup_hour, COUNT(*) AS trip_count, ROUND(AVG(trip_duration_min),2) AS avg_duration_min, ROUND(AVG(total_amount),2) AS avg_total_amount FROM taxi_trips GROUP BY pickup_hour ORDER BY pickup_hour;'"
```

**Q9 — Weekday vs Weekend Analysis**
```bash
docker exec taxi-hive-server bash -c "export HADOOP_CLIENT_OPTS='-Xmx4g' && hive --hiveconf hive.execution.engine=mr --hiveconf hive.cli.print.header=true -e 'USE taxi_analytics; SELECT CASE WHEN is_weekend=1 THEN \"Weekend\" ELSE \"Weekday\" END AS day_type, COUNT(*) AS trip_count, ROUND(AVG(total_amount),2) AS avg_total_amount, ROUND(AVG(trip_distance),2) AS avg_distance_miles, ROUND(AVG(tip_amount),2) AS avg_tip FROM taxi_trips GROUP BY is_weekend ORDER BY is_weekend;'"
```

**Q10 — Payment Type Analysis**
```bash
docker exec taxi-hive-server bash -c "export HADOOP_CLIENT_OPTS='-Xmx4g' && hive --hiveconf hive.execution.engine=mr --hiveconf hive.cli.print.header=true -e 'USE taxi_analytics; SELECT CASE WHEN payment_type=1 THEN \"Credit Card\" WHEN payment_type=2 THEN \"Cash\" WHEN payment_type=3 THEN \"No Charge\" WHEN payment_type=4 THEN \"Dispute\" ELSE \"Unknown\" END AS payment_method, COUNT(*) AS trip_count, ROUND(SUM(total_amount),2) AS total_revenue, ROUND(AVG(tip_amount),2) AS avg_tip, ROUND(AVG(total_amount),2) AS avg_fare FROM taxi_trips GROUP BY payment_type ORDER BY trip_count DESC;'"
```

**Q11 — High-Volume Pickup Zones (>10,000 trips)**
```bash
docker exec taxi-hive-server bash -c "export HADOOP_CLIENT_OPTS='-Xmx4g' && hive --hiveconf hive.execution.engine=mr --hiveconf hive.cli.print.header=true -e 'USE taxi_analytics; SELECT PULocationID, COUNT(*) AS trip_count, ROUND(AVG(total_amount),2) AS avg_total_amount, ROUND(AVG(tip_amount),2) AS avg_tip, ROUND(SUM(total_amount),2) AS total_revenue FROM taxi_trips GROUP BY PULocationID HAVING COUNT(*) > 10000 ORDER BY trip_count DESC;'"
```

**Q12 — Monthly Performance**
```bash
docker exec taxi-hive-server bash -c "export HADOOP_CLIENT_OPTS='-Xmx4g' && hive --hiveconf hive.execution.engine=mr --hiveconf hive.cli.print.header=true -e 'USE taxi_analytics; SELECT pickup_year, pickup_month, COUNT(*) AS trip_count, ROUND(SUM(total_amount),2) AS total_revenue, ROUND(AVG(trip_distance),2) AS avg_distance_miles, ROUND(AVG(trip_duration_min),2) AS avg_duration_min, ROUND(AVG(fare_amount),2) AS avg_fare FROM taxi_trips GROUP BY pickup_year, pickup_month ORDER BY pickup_year, pickup_month;'"
```

**Q13 — Ratecode Economic Yield ($/mile)**
```bash
docker exec taxi-hive-server bash -c "export HADOOP_CLIENT_OPTS='-Xmx4g' && hive --hiveconf hive.execution.engine=mr --hiveconf hive.cli.print.header=true -e 'USE taxi_analytics; SELECT CASE WHEN CAST(RatecodeID AS INT)=1 THEN \"Standard Rate\" WHEN CAST(RatecodeID AS INT)=2 THEN \"JFK Airport\" WHEN CAST(RatecodeID AS INT)=3 THEN \"Newark Airport\" WHEN CAST(RatecodeID AS INT)=4 THEN \"Nassau/Westchester\" WHEN CAST(RatecodeID AS INT)=5 THEN \"Negotiated Fare\" WHEN CAST(RatecodeID AS INT)=6 THEN \"Group Ride\" ELSE \"Other/Unknown\" END AS rate_code_description, COUNT(*) AS trip_count, ROUND(SUM(total_amount),2) AS total_revenue, ROUND(AVG(fare_amount),2) AS avg_fare, ROUND(AVG(trip_distance),2) AS avg_distance_miles, ROUND(AVG(avg_speed_mph),2) AS avg_speed_mph, ROUND(AVG(fare_per_mile),2) AS avg_fare_per_mile FROM taxi_trips GROUP BY RatecodeID ORDER BY trip_count DESC;'"
```

**Q14 — Surcharge & Toll Breakdown (Peak vs Off-Peak)**
```bash
docker exec taxi-hive-server bash -c "export HADOOP_CLIENT_OPTS='-Xmx4g' && hive --hiveconf hive.execution.engine=mr --hiveconf hive.cli.print.header=true -e 'USE taxi_analytics; SELECT CASE WHEN pickup_hour BETWEEN 16 AND 19 THEN \"Peak Evening (16-19)\" WHEN pickup_hour BETWEEN 7 AND 9 THEN \"Peak Morning (07-09)\" ELSE \"Off-Peak\" END AS time_period, COUNT(*) AS trip_count, ROUND(SUM(total_amount),2) AS total_revenue, ROUND(SUM(congestion_surcharge),2) AS total_congestion_surcharge, ROUND(SUM(cbd_congestion_fee),2) AS total_cbd_fee, ROUND(SUM(Airport_fee),2) AS total_airport_fee, ROUND(SUM(tolls_amount),2) AS total_tolls FROM taxi_trips GROUP BY CASE WHEN pickup_hour BETWEEN 16 AND 19 THEN \"Peak Evening (16-19)\" WHEN pickup_hour BETWEEN 7 AND 9 THEN \"Peak Morning (07-09)\" ELSE \"Off-Peak\" END ORDER BY trip_count DESC;'"
```

**Q15 — Passenger Count Occupancy Analysis**
```bash
docker exec taxi-hive-server bash -c "export HADOOP_CLIENT_OPTS='-Xmx4g' && hive --hiveconf hive.execution.engine=mr --hiveconf hive.cli.print.header=true -e 'USE taxi_analytics; SELECT CAST(passenger_count AS INT) AS passenger_count, COUNT(*) AS trip_count, ROUND(AVG(trip_distance),2) AS avg_distance_miles, ROUND(AVG(total_amount),2) AS avg_total_amount, ROUND(AVG(tip_amount),2) AS avg_tip_amount, ROUND((SUM(tip_amount)/SUM(fare_amount))*100,2) AS tip_percentage FROM taxi_trips WHERE passenger_count BETWEEN 1 AND 6 GROUP BY CAST(passenger_count AS INT) ORDER BY passenger_count ASC;'"
```

### 🟢 Run Automated Script for All 15 Queries

Or execute all 15 queries automated and write outputs directly into `results/hive/all_queries_output.txt`:

```bash
bash hive/run_hive_queries.sh
```

---

## Step 9 — Generate Visualizations

Produces **8 high-resolution charts** from the MapReduce and Hive outputs, saved to `visualizations/`.

**Prerequisites:** Python 3 with `pandas` and `matplotlib` (install into the project venv):

```bash
source venv/bin/activate       # or: python3 -m venv venv && source venv/bin/activate
pip install pandas matplotlib
```

**Run:**

```bash
python analysis/generate_visualizations.py
# or use the convenience script:
bash run_analysis.sh
```

**Charts produced:**

| File | Description |
| ---- | ----------- |
| `hourly_demand_revenue.png` | Bar + line dual-axis: trip volume & revenue by hour (0–23) |
| `top_pickup_zones.png` | Horizontal bar: top 10 busiest pickup zones |
| `payment_type_share.png` | Donut chart: payment method market share (Q10) |
| `weekday_vs_weekend.png` | Side-by-side bars: trip count & avg spend (Q9) |
| `monthly_revenue_trend.png` | Line chart: Jan–Mar 2026 monthly revenue (Q12) |
| `zone_speed_vs_fare.png` | Scatter: avg speed vs avg fare for all 261 zones |
| `tip_behavior_analysis.png` | Bar chart: credit card tipping % across 6 fare buckets (MapReduce Job 3) |
| `ratecode_yield.png` | Bar chart: revenue yield per mile across rate codes (Hive Q13) |

---

## Results Summary

### MapReduce — Zone Performance (`results/mapreduce/zone_performance.tsv`)

- **261 unique pickup zones** analyzed
- Metrics per zone: trips, revenue, avg fare, avg distance, avg duration, avg speed, avg tip
- Top zone: **Zone 237** (408,094 trips), **Zone 132** top revenue ($31.9M)

### MapReduce — Hourly Performance (`results/mapreduce/hourly_performance.tsv`)

- **24 hourly buckets** (00–23) across all 7.6M trips
- Metrics per hour: trips, total revenue, avg revenue, avg passengers, avg duration, avg tip, tip %
### MapReduce — Tip Behavior (`results/mapreduce/tip_behavior.tsv`)

- **Binned payment & fare brackets** (Credit Card vs Cash across 6 fare ranges)
- Metrics per bucket: total trips, avg tip amount, tip % of base fare, zero-tip rate, avg passengers
- Key Finding: Credit card trips show 14–18% average tipping on standard fares (\$10–\$50), whereas cash payments reflect 100% zero-tip rate in taximeter data (exposing unrecorded cash tips).

### Hive — 15 Analytical Queries (`results/hive/all_queries_output.txt`)

| Query | Result |
| ----- | ------ |
| Q1 Total Trips | **7,637,676** |
| Q2 Total Revenue | **$221,653,148** |
| Q3 Avg Fare / Distance / Duration / Speed | $29.54 / 3.44 mi / 17.3 min / 10.62 mph |
| Q4 Busiest Zone | Zone 237 — 408,094 trips |
| Q5 Top Revenue Zone | Zone 132 (Airport) — $31.9M |
| Q6 Most Expensive Trip | Zone 64→131, $500 total |
| Q7 Distance Range | 0.01 mi – 98.96 mi |
| Q8 Peak Hour | 18:00 (552,506 trips) |
| Q9 Weekday vs Weekend | 5.6M weekday / 2.0M weekend |
| Q10 Payment Split | 87.6% Credit Card |
| Q11 High-volume zones | 57 zones with >10,000 trips |
| Q12 Monthly Trend | Mar highest (2,879,641 trips) |
| Q13 Ratecode Yield | Standard $6.49/mi vs Airport $3.80/mi |
| Q14 Surcharge Impact | Peak Evening revenue leads with $18.2M in fees |
| Q15 Occupancy Efficiency | Single passenger trips represent 72% of total volume |

### Visualizations (`visualizations/`)

8 charts auto-generated by `analysis/generate_visualizations.py`:

- **hourly_demand_revenue.png** — dual-axis bar+line: trip volume & revenue per hour
- **top_pickup_zones.png** — horizontal bar: top 10 busiest zones
- **payment_type_share.png** — donut: 87.6% CC / 11.4% Cash / 1% other
- **weekday_vs_weekend.png** — Weekday 5.63M trips vs Weekend 2.01M trips
- **monthly_revenue_trend.png** — Jan $72.1M → Feb $65.8M → Mar $83.8M
- **zone_speed_vs_fare.png** — scatter of 261 zones coloured by trip volume
- **tip_behavior_analysis.png** — bar chart: credit card tipping % across 6 fare brackets
- **ratecode_yield.png** — bar chart: revenue yield per mile across rate categories

---

## Troubleshooting

### `OutOfMemoryError: Java heap space` in Hive

**Symptom:** Hive fails on Q6 (ORDER BY) or multi-AVG queries.  
**Fix:** Always set heap before Hive CLI:

```bash
export HADOOP_CLIENT_OPTS='-Xmx4g'
```

Hive local-MR runs inside the CLI JVM — child process flags (`-Xmx` via `mapreduce.map.java.opts`) have no effect.

### `MRAppMaster class not found`

**Symptom:** YARN jobs fail with `Could not find or load main class org.apache.hadoop.mapreduce.v2.app.MRAppMaster`  
**Fix:** This project uses `apache/hadoop:3.3.6` which ships consistent MapReduce JARs. Do **not** mix with external Hadoop 2.x JARs (e.g., from Tez).

### HDFS NameNode not starting

```bash
docker compose down -v
docker compose up -d
```

If NameNode keeps failing, the volume may be corrupted. Remove and recreate.

### Hive can't connect to Metastore

```bash
docker compose restart hive-server
```

Wait 30 seconds for the embedded Derby metastore to initialize.

### Hive tables missing after restart

The metastore uses the container's local Derby DB — it persists only while the container is alive. Re-run the CREATE TABLE and CTAS steps after a full restart.

---

## Architecture

```
Yellow Taxi CSV (1.1 GB)
         │
         ▼
       HDFS (/user/bda/taxi/clean/)
         │
    ┌────┴──────────┐
    │               │
    ▼               ▼
MapReduce         Hive
 ┌──┼──┐            │
 │  │  │       taxi_trips_raw (External, CSV)
 ▼  ▼  ▼            │
Zone Hourly Tip taxi_trips (Managed, ORC+Snappy)
Stats Stats Behavior│
 │  │  │            ▼
 └──┼──┘      12 Analytical Queries
    │         (Q1–Q12 results)
    │               │
    └───────┬────────┘
            ▼
   Python Visualizations
   (6 charts → visualizations/)
```

**Docker Services:**

| Container            | Image               | Role                    |
| -------------------- | ------------------- | ----------------------- |
| taxi-namenode        | apache/hadoop:3.3.6 | HDFS NameNode           |
| taxi-datanode        | apache/hadoop:3.3.6 | HDFS DataNode           |
| taxi-resourcemanager | apache/hadoop:3.3.6 | YARN ResourceManager    |
| taxi-nodemanager     | apache/hadoop:3.3.6 | YARN NodeManager        |
| taxi-hive-server     | apache/hive:3.1.3   | HiveServer2 + Metastore |

**Java:** OpenJDK 8 (both Hadoop and Hive containers)  
**Hive execution engine:** MapReduce (local mode)  
**ORC compression:** Snappy

---

## Dataset Schema

| Column                | Type   | Description                         |
| --------------------- | ------ | ----------------------------------- |
| VendorID              | INT    | Taxi vendor (1 or 2)                |
| tpep_pickup_datetime  | STRING | Pickup timestamp                    |
| tpep_dropoff_datetime | STRING | Dropoff timestamp                   |
| passenger_count       | DOUBLE | Number of passengers                |
| trip_distance         | DOUBLE | Trip distance (miles)               |
| PULocationID          | INT    | Pickup zone ID                      |
| DOLocationID          | INT    | Dropoff zone ID                     |
| payment_type          | INT    | 1=CC, 2=Cash, 3=NoCharge, 4=Dispute |
| fare_amount           | DOUBLE | Base fare ($)                       |
| tip_amount            | DOUBLE | Tip ($)                             |
| total_amount          | DOUBLE | Total charged ($)                   |
| trip_duration_min     | DOUBLE | Trip duration (minutes)             |
| pickup_hour           | INT    | Hour of pickup (0–23)               |
| pickup_month          | INT    | Month (1–3)                         |
| pickup_year           | INT    | Year (2026)                         |
| pickup_day_name       | STRING | Day name (Monday, etc.)             |
| is_weekend            | INT    | 1=Weekend, 0=Weekday                |
| avg_speed_mph         | DOUBLE | Average speed (mph)                 |
| fare_per_mile         | DOUBLE | Fare per mile ($)                   |

---

_BDA Capstone Project · NYC Yellow Taxi Analytics · Jan–Mar 2026_
