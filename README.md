# NYC Yellow Taxi — Big Data Analytics Pipeline

A complete, reproducible Big Data Analytics pipeline using **Apache Hadoop 3.3.6 YARN + MapReduce + Hive 3.1.3** running on Docker, with Python-based visualization.

**Dataset:** NYC Yellow Taxi (Jan–Mar 2026) · ~7.6 million records · 1.1 GB CSV  
**Architecture:** HDFS → Java MapReduce (zone analytics + hourly analytics) + Hive (12 analytical queries) + Python visualizations

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
│   └── HourlyTaxiDriver.java       # Hourly job configuration
├── hive/
│   ├── taxi_analytics.sql          # All DDL + 12 HiveQL queries
│   ├── run_hive_queries.sh         # Script to run all 12 queries
│   └── conf/                       # Hive/Hadoop XML configs
├── analysis/
│   └── generate_visualizations.py  # Produces 6 charts → visualizations/
├── scripts/
│   └── local_mode/                 # Legacy non-Docker local scripts
├── visualizations/                 # ← Charts live HERE (committed, easy access)
│   ├── hourly_demand_revenue.png
│   ├── top_pickup_zones.png
│   ├── payment_type_share.png
│   ├── weekday_vs_weekend.png
│   ├── monthly_revenue_trend.png
│   └── zone_speed_vs_fare.png
└── results/                        # Pipeline TSV/text outputs (gitignored)
    ├── mapreduce/
    │   ├── zone_performance.tsv
    │   └── hourly_performance.tsv
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

## Step 4 — Compile & Run MapReduce

### 4.1 Compile (inside namenode)

```bash
docker exec taxi-namenode bash -c "
  cd /opt && mkdir -p taxi_mr/src && \
  hadoop classpath > /dev/null
"

# Copy source files
docker cp mapreduce/TaxiMapper.java taxi-namenode:/opt/taxi_mr/src/
docker cp mapreduce/TaxiReducer.java taxi-namenode:/opt/taxi_mr/src/
docker cp mapreduce/TaxiDriver.java  taxi-namenode:/opt/taxi_mr/src/

# Compile and package
docker exec taxi-namenode bash -c "
  cd /opt/taxi_mr && \
  mkdir -p classes && \
  javac -cp \$(hadoop classpath) -d classes src/*.java && \
  jar -cvfm taxi-analysis.jar /dev/stdin -C classes . << 'EOF'
Manifest-Version: 1.0
Main-Class: TaxiDriver
EOF
"
```

### 4.2 Run MapReduce Job

```bash
# Remove previous output if it exists
docker exec taxi-namenode hdfs dfs -rm -r -f /user/bda/taxi/mapreduce/zone_performance

# Run
docker exec taxi-namenode bash -c "
  hadoop jar /opt/taxi_mr/taxi-analysis.jar \
    /user/bda/taxi/clean/ \
    /user/bda/taxi/mapreduce/zone_performance
"
```

> Takes ~3–5 minutes for 7.6M records.

### 4.3 View MapReduce Output

```bash
docker exec taxi-namenode hdfs dfs -ls /user/bda/taxi/mapreduce/zone_performance/
docker exec taxi-namenode hdfs dfs -cat /user/bda/taxi/mapreduce/zone_performance/part-r-00000 | head -20
```

**Output format (tab-separated):**

```
PULocationID  total_trips  total_revenue  avg_fare  avg_distance  avg_duration  avg_speed  avg_tip
237           408094       8497034.16     19.34     2.71          13.89         12.28      2.72
132           398951       31899115.08    75.26     18.78         57.49         20.15      10.59
...
```

### 4.4 Save Results Locally

```bash
mkdir -p results/mapreduce
docker exec taxi-namenode bash -c "
  echo 'PULocationID\ttotal_trips\ttotal_revenue\tavg_fare\tavg_distance\tavg_duration\tavg_speed\tavg_tip'
  hdfs dfs -cat /user/bda/taxi/mapreduce/zone_performance/part-r-00000
" > results/mapreduce/zone_performance.tsv
```

---

## Step 5 — Compile & Run Hourly MapReduce Job

This second MapReduce job aggregates **24-hour demand, revenue, and tip metrics** across all 7.6M trips.

### 5.1 Copy source files (already done if you ran Step 4)

```bash
docker cp mapreduce/HourlyTaxiMapper.java taxi-namenode:/opt/taxi_mr/src/
docker cp mapreduce/HourlyTaxiReducer.java taxi-namenode:/opt/taxi_mr/src/
docker cp mapreduce/HourlyTaxiDriver.java  taxi-namenode:/opt/taxi_mr/src/
```

### 5.2 Recompile all classes into a combined JAR

```bash
docker exec taxi-namenode bash -c "
  cd /opt/taxi_mr && rm -rf classes && mkdir -p classes && \
  javac -encoding UTF-8 -cp \$(hadoop classpath) -d classes src/*.java && \
  jar -cvf taxi-hourly.jar -C classes/ HourlyTaxiDriver.class \
      -C classes/ HourlyTaxiMapper.class -C classes/ HourlyTaxiReducer.class
"
```

### 5.3 Run Hourly Job

```bash
# Remove previous output if it exists
docker exec taxi-namenode hdfs dfs -rm -r -f /user/bda/taxi/mapreduce/hourly_performance

# Run
docker exec taxi-namenode bash -c "
  hadoop jar /opt/taxi_mr/taxi-hourly.jar HourlyTaxiDriver \
    /user/bda/taxi/clean/yellow_tripdata_cleaned.csv \
    /user/bda/taxi/mapreduce/hourly_performance
"
```

> Takes ~2–3 minutes. Outputs 24 rows (one per hour).

### 5.4 View Output

```bash
docker exec taxi-namenode hdfs dfs -cat /user/bda/taxi/mapreduce/hourly_performance/part-r-00000
```

**Output format (tab-separated):**

```
hour  total_trips  total_revenue  avg_revenue  avg_passengers  avg_duration  avg_tip  tip_pct
18    552506       15702002.89    28.42        1.25            15.89         3.70     20.84%
...
```

### 5.5 Save Results Locally

```bash
mkdir -p results/mapreduce
docker exec taxi-namenode bash -c "
  echo -e 'hour\ttotal_trips\ttotal_revenue\tavg_revenue\tavg_passengers\tavg_duration\tavg_tip\ttip_pct'
  hdfs dfs -cat /user/bda/taxi/mapreduce/hourly_performance/part-r-00000
" > results/mapreduce/hourly_performance.tsv
```

---

## Step 6 — Run Hive Analytics

### ⚠️ Critical: Heap Size

Hive runs local MapReduce **inside the CLI JVM**. ORDER BY on 7.6M rows requires 4 GB heap.  
**Always prefix Hive commands with:**

```bash
export HADOOP_CLIENT_OPTS='-Xmx4g'
```

### 5.1 Copy SQL file into container

```bash
docker cp hive/taxi_analytics.sql taxi-hive-server:/tmp/taxi_analytics.sql
```

### 5.2 Create Database and Tables

```bash
docker exec taxi-hive-server bash -c "
  export HADOOP_CLIENT_OPTS='-Xmx4g'
  hive --hiveconf hive.execution.engine=mr \
       -e \"CREATE DATABASE IF NOT EXISTS taxi_analytics;\"
"
```

**Create external staging table (reads raw CSV from HDFS):**

```bash
docker exec taxi-hive-server bash -c "
  export HADOOP_CLIENT_OPTS='-Xmx4g'
  hive --hiveconf hive.execution.engine=mr -e \"
    USE taxi_analytics;
    CREATE EXTERNAL TABLE IF NOT EXISTS taxi_trips_raw (
        VendorID STRING, tpep_pickup_datetime STRING, tpep_dropoff_datetime STRING,
        passenger_count STRING, trip_distance STRING, RatecodeID STRING,
        store_and_fwd_flag STRING, PULocationID STRING, DOLocationID STRING,
        payment_type STRING, fare_amount STRING, extra STRING, mta_tax STRING,
        tip_amount STRING, tolls_amount STRING, improvement_surcharge STRING,
        total_amount STRING, congestion_surcharge STRING, Airport_fee STRING,
        cbd_congestion_fee STRING, trip_duration_min STRING, pickup_date STRING,
        pickup_hour STRING, pickup_day STRING, pickup_month STRING,
        pickup_year STRING, pickup_dayofweek STRING, pickup_day_name STRING,
        is_weekend STRING, avg_speed_mph STRING, fare_per_mile STRING
    )
    ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
    WITH SERDEPROPERTIES ('separatorChar'=',', 'quoteChar'='\\\"', 'escapeChar'='\\\\')
    STORED AS TEXTFILE
    LOCATION '/user/bda/taxi/clean/'
    TBLPROPERTIES ('skip.header.line.count'='1');
  \"
"
```

**Create managed ORC table (typed, columnar, Snappy-compressed):**

```bash
docker exec taxi-hive-server bash -c "
  export HADOOP_CLIENT_OPTS='-Xmx4g'
  hive --hiveconf hive.execution.engine=mr -e \"
    USE taxi_analytics;
    CREATE TABLE IF NOT EXISTS taxi_trips
    STORED AS ORC
    TBLPROPERTIES ('orc.compress'='SNAPPY')
    AS SELECT
        CAST(VendorID AS INT) AS VendorID,
        tpep_pickup_datetime, tpep_dropoff_datetime,
        CAST(passenger_count AS DOUBLE) AS passenger_count,
        CAST(trip_distance AS DOUBLE) AS trip_distance,
        CAST(RatecodeID AS DOUBLE) AS RatecodeID,
        store_and_fwd_flag,
        CAST(PULocationID AS INT) AS PULocationID,
        CAST(DOLocationID AS INT) AS DOLocationID,
        CAST(payment_type AS INT) AS payment_type,
        CAST(fare_amount AS DOUBLE) AS fare_amount,
        CAST(extra AS DOUBLE) AS extra,
        CAST(mta_tax AS DOUBLE) AS mta_tax,
        CAST(tip_amount AS DOUBLE) AS tip_amount,
        CAST(tolls_amount AS DOUBLE) AS tolls_amount,
        CAST(improvement_surcharge AS DOUBLE) AS improvement_surcharge,
        CAST(total_amount AS DOUBLE) AS total_amount,
        CAST(congestion_surcharge AS DOUBLE) AS congestion_surcharge,
        CAST(Airport_fee AS DOUBLE) AS Airport_fee,
        CAST(cbd_congestion_fee AS DOUBLE) AS cbd_congestion_fee,
        CAST(trip_duration_min AS DOUBLE) AS trip_duration_min,
        pickup_date,
        CAST(pickup_hour AS INT) AS pickup_hour,
        CAST(pickup_day AS INT) AS pickup_day,
        CAST(pickup_month AS INT) AS pickup_month,
        CAST(pickup_year AS INT) AS pickup_year,
        CAST(pickup_dayofweek AS INT) AS pickup_dayofweek,
        pickup_day_name,
        CAST(is_weekend AS INT) AS is_weekend,
        CAST(avg_speed_mph AS DOUBLE) AS avg_speed_mph,
        CAST(fare_per_mile AS DOUBLE) AS fare_per_mile
    FROM taxi_trips_raw
    WHERE VendorID IS NOT NULL AND VendorID != 'VendorID';
  \"
"
```

> CTAS takes ~2 minutes. Creates 7,637,669 rows in ORC format.

### 5.3 Verify Tables

```bash
docker exec taxi-hive-server bash -c "
  export HADOOP_CLIENT_OPTS='-Xmx4g'
  hive -e \"USE taxi_analytics; SHOW TABLES; SELECT COUNT(*) FROM taxi_trips;\"
"
```

Expected: `7637669`

---

## Step 7 — Run All 12 Hive Queries

Run individually (recommended) with the 4 GB heap flag:

```bash
# Quick helper function
hq() {
  docker exec taxi-hive-server bash -c "
    export HADOOP_CLIENT_OPTS='-Xmx4g'
    hive --hiveconf hive.execution.engine=mr \
         --hiveconf hive.cli.print.header=true \
         -e \"USE taxi_analytics; $1\"
  " 2>&1 | grep -v SLF4J | grep -v 'Class path' | grep -v 'Found binding'
}

# Q1
hq "SELECT COUNT(*) AS total_trips FROM taxi_trips;"

# Q2
hq "SELECT ROUND(SUM(total_amount),2) AS total_revenue FROM taxi_trips;"

# Q3
hq "SELECT ROUND(AVG(fare_amount),2) AS avg_fare,
           ROUND(AVG(trip_distance),2) AS avg_distance_miles,
           ROUND(AVG(trip_duration_min),2) AS avg_duration_min,
           ROUND(AVG(avg_speed_mph),2) AS avg_speed_mph
    FROM taxi_trips;"

# Q4 — Trip volume by zone
hq "SELECT PULocationID, COUNT(*) AS trip_count
    FROM taxi_trips GROUP BY PULocationID
    ORDER BY trip_count DESC LIMIT 20;"

# Q5 — Revenue by zone
hq "SELECT PULocationID, COUNT(*) AS trip_count,
           ROUND(SUM(total_amount),2) AS total_revenue
    FROM taxi_trips GROUP BY PULocationID
    ORDER BY total_revenue DESC LIMIT 20;"

# Q6 — Top 10 most expensive trips
hq "SELECT PULocationID, DOLocationID, tpep_pickup_datetime,
           ROUND(fare_amount,2) AS fare, ROUND(total_amount,2) AS total
    FROM taxi_trips ORDER BY total_amount DESC LIMIT 10;"

# Q7 — Distance stats
hq "SELECT MAX(trip_distance) AS max_dist, MIN(trip_distance) AS min_dist,
           ROUND(AVG(trip_distance),2) AS avg_dist
    FROM taxi_trips WHERE trip_distance > 0;"

# Q8 — Demand by hour
hq "SELECT pickup_hour, COUNT(*) AS trips,
           ROUND(AVG(trip_duration_min),2) AS avg_duration
    FROM taxi_trips GROUP BY pickup_hour ORDER BY pickup_hour;"

# Q9 — Weekday vs Weekend
hq "SELECT CASE WHEN is_weekend=1 THEN 'Weekend' ELSE 'Weekday' END AS day_type,
           COUNT(*) AS trips, ROUND(AVG(total_amount),2) AS avg_fare
    FROM taxi_trips GROUP BY is_weekend;"

# Q10 — Payment type
hq "SELECT CASE payment_type
           WHEN 1 THEN 'Credit Card' WHEN 2 THEN 'Cash'
           WHEN 3 THEN 'No Charge'  WHEN 4 THEN 'Dispute'
           ELSE 'Unknown' END AS method,
           COUNT(*) AS trips, ROUND(SUM(total_amount),2) AS revenue
    FROM taxi_trips GROUP BY payment_type ORDER BY trips DESC;"

# Q11 — High-volume zones (HAVING)
hq "SELECT PULocationID, COUNT(*) AS trips,
           ROUND(AVG(total_amount),2) AS avg_fare
    FROM taxi_trips GROUP BY PULocationID
    HAVING COUNT(*) > 10000 ORDER BY trips DESC;"

# Q12 — Monthly breakdown
hq "SELECT pickup_year, pickup_month, COUNT(*) AS trips,
           ROUND(SUM(total_amount),2) AS revenue
    FROM taxi_trips GROUP BY pickup_year, pickup_month
    ORDER BY pickup_year, pickup_month;"
```

Or run the provided script:

```bash
chmod +x hive/run_hive_queries.sh
bash hive/run_hive_queries.sh
```

---

## Step 8 — Generate Visualizations

Produces **6 high-resolution charts** from the MapReduce and Hive outputs, saved to `visualizations/`.

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

---

## Results Summary

### MapReduce — Zone Performance (`results/mapreduce/zone_performance.tsv`)

- **261 unique pickup zones** analyzed
- Metrics per zone: trips, revenue, avg fare, avg distance, avg duration, avg speed, avg tip
- Top zone: **Zone 237** (408,094 trips), **Zone 132** top revenue ($31.9M)

### MapReduce — Hourly Performance (`results/mapreduce/hourly_performance.tsv`)

- **24 hourly buckets** (00–23) across all 7.6M trips
- Metrics per hour: trips, total revenue, avg revenue, avg passengers, avg duration, avg tip, tip %
- Peak hour: **18:00** — 552,506 trips · $15.7M revenue · 20.84% tip rate

### Hive — 12 Analytical Queries (`results/hive/all_queries_output.txt`)

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

### Visualizations (`visualizations/`)

6 charts auto-generated by `analysis/generate_visualizations.py`:

- **hourly_demand_revenue.png** — dual-axis bar+line: trip volume & revenue per hour
- **top_pickup_zones.png** — horizontal bar: top 10 busiest zones
- **payment_type_share.png** — donut: 87.6% CC / 11.4% Cash / 1% other
- **weekday_vs_weekend.png** — Weekday 5.63M trips vs Weekend 2.01M trips
- **monthly_revenue_trend.png** — Jan $72.1M → Feb $65.8M → Mar $83.8M
- **zone_speed_vs_fare.png** — scatter of 261 zones coloured by trip volume

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
 ┌──┴──┐            │
 │     │       taxi_trips_raw (External, CSV)
 ▼     ▼            │
Zone  Hourly   taxi_trips (Managed, ORC+Snappy)
Stats Stats         │
 │     │            ▼
 │     │      12 Analytical Queries
 └──┬──┘      (Q1–Q12 results)
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
