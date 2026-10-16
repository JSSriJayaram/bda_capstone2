# 🚕 NYC Yellow Taxi — Big Data Analytics Platform

> **BDA Capstone Project | 23CSE352** — Full pipeline: HDFS → MapReduce → Hive → Apache HBase → Apache Spark

[![Apache HBase](https://img.shields.io/badge/Apache%20HBase-2.4.x-red?style=flat-square&logo=apache)](https://hbase.apache.org/)
[![Apache Hive](https://img.shields.io/badge/Apache%20Hive-3.1.3-yellow?style=flat-square&logo=apache)](https://hive.apache.org/)
[![Apache Hadoop](https://img.shields.io/badge/Hadoop-3.3.6-blue?style=flat-square&logo=apache)](https://hadoop.apache.org/)
[![Java](https://img.shields.io/badge/Java-8-orange?style=flat-square&logo=java)](https://openjdk.java.net/)
[![Docker](https://img.shields.io/badge/Docker-Compose-2496ED?style=flat-square&logo=docker)](https://docs.docker.com/compose/)

---

## 📋 Project Overview

This project builds a complete big data analytics pipeline for the **NYC Yellow Taxi Trip dataset** (NYC TLC Open Data, approximately 7.6M cleaned rows across January–March 2026). The pipeline progresses through three analytics layers:

| Review       | Technology                         | Focus                                                                           |
| ------------ | ---------------------------------- | ------------------------------------------------------------------------------- |
| Review 1     | HDFS + MapReduce + Hive            | Batch ingestion, distributed computation, SQL analytics                         |
| **Review 2** | **Apache HBase**                   | **NoSQL storage, real-time lookups, server-side filter analytics**              |
| **Review 3** | **Apache Spark + Scala + PySpark** | **RDD processing, exploratory analytics, ML classification, and visualization** |

---

## 🗂️ Repository Structure

```
bda_capstone2/
├── docker-compose.yml                   # All services: Hadoop, Hive, HBase
├── hadoop.env                           # Hadoop environment variables
│
├── hbase/                               # ◀ REVIEW 2: HBase Analytics
│   ├── pom.xml                          # Maven build (HBase 2.4.x client + shade)
│   ├── scripts/
│   │   ├── 00_common_setup.hbaseshell   # Common: create + put + get (all students)
│   │   ├── student1_ddl_ranges.hbaseshell
│   │   ├── student2_payments_reverse.hbaseshell
│   │   ├── student3_distance_mutations.hbaseshell
│   │   ├── student4_pagination_metrics.hbaseshell
│   │   └── run_all_demos.sh             # Master runner: all 4 Java APIs
│   └── src/main/java/com/nyctaxi/hbase/
│       ├── CreateTable.java             # Student 1 — DDL Admin API
│       ├── InsertData.java              # Student 2 — Batch Ingestion API
│       ├── GetAndDelete.java            # Student 3 — Record Lifecycle API
│       └── ScanWithFilter.java          # Student 4 — 5 Server-Side Filters API
│
├── mapreduce/                           # Review 1: MapReduce jobs
├── hive/                                # Review 1: Hive SQL analytics (15 queries)
├── analysis/                            # Visualizations and analytics output
└── preprocessing.ipynb                  # Dataset preprocessing notebook

review3/                                 # REVIEW 3: Spark analytics and ML
├── Dockerfile                            # Reproducible OpenJDK 17 + PySpark environment
├── docker-compose.yml                    # Spark pipeline service
├── run_all.sh                            # Four-stage pipeline orchestrator
├── scala/NycTaxiRDD.scala                # Scala RDD transformations, actions, and caching
├── python/01_preprocessing_eda.py        # PySpark cleaning and exploratory analysis
├── python/02_ml_model.py                 # PySpark MLlib model training and evaluation
├── python/03_visualizations.py           # Charts and executive dashboard
└── output/                               # Generated CSV, JSON, PNG, and prediction artifacts
```

---

## 🗄️ HBase Table Design (Review 2)

### Table: `nyc_taxi`

**Row Key Format:** `{VendorID}_{pickup_datetime_compact}_{PULocationID}`

- Example: `2_20260101005404_239`
- Enables fast **vendor prefix scans** and **time-range scans**
- Prevents **region hot-spotting** (composite key distributes writes)

| Column Family | Qualifiers                                                                         | Purpose                         |
| ------------- | ---------------------------------------------------------------------------------- | ------------------------------- |
| `trip_info`   | passenger_count, trip_distance, PULocationID, DOLocationID, payment_type           | Geographic & capacity analytics |
| `financials`  | fare_amount, tip_amount, total_amount, congestion_surcharge                        | Revenue & payment analytics     |
| `timing`      | pickup_datetime, dropoff_datetime, trip_duration_min, pickup_hour, pickup_day_name | Temporal analytics              |
| `metrics`     | avg_speed_mph, fare_per_mile, is_weekend                                           | Performance dashboard           |

> **Why 4 Column Families?** Each CF is stored in separate HFiles on HDFS. A query against only `financials` reads only that CF's HFiles — skipping `trip_info`, `timing`, and `metrics` entirely. This column-family locality dramatically reduces disk I/O.

---

## 🚀 Quickstart

### Prerequisites

- Docker Desktop installed and running
- The `hbase_docker` image built locally (Java 8 + HBase 2.4.19 + Maven)

### Step 1: Start HBase Container

```bash
docker compose up -d hbase
# Wait ~30 seconds for HBase, ZooKeeper, and RegionServer to start
```

### Step 2: Build the Java Analytics JAR

```bash
docker exec hbase bash -c "cd /root/nyc-taxi-hbase && /opt/maven/bin/mvn package -q"
```

### Step 3: Run Common Setup (create table + insert data)

```bash
# Enter HBase Shell
docker exec -it hbase hbase shell

# Paste contents of: hbase/scripts/00_common_setup.hbaseshell
```

### Step 4: Run Student Java APIs

```bash
# Student 1 — Create Table Schema
docker exec hbase java -cp /root/nyc-taxi-hbase/target/nyc-taxi-hbase-analytics-1.0.jar com.nyctaxi.hbase.CreateTable

# Student 2 — Batch Insert 12 Real Records
docker exec hbase java -cp /root/nyc-taxi-hbase/target/nyc-taxi-hbase-analytics-1.0.jar com.nyctaxi.hbase.InsertData

# Student 3 — Get & Delete Record Lifecycle
docker exec hbase java -cp /root/nyc-taxi-hbase/target/nyc-taxi-hbase-analytics-1.0.jar com.nyctaxi.hbase.GetAndDelete

# Student 4 — 5 Server-Side Filter Analytics (default main)
docker exec hbase java -jar /root/nyc-taxi-hbase/target/nyc-taxi-hbase-analytics-1.0.jar

# OR run all 4 at once:
bash hbase/scripts/run_all_demos.sh
```

---

## 👥 Student Contribution Breakdown (Review 2)

### Common (All Students)

Every student demonstrates the HBase Shell foundation: `create`, `put`, `get`, `describe`, `list`, `count`, `scan`.

---

### 👤 Student 1 — Schema Architecture & DDL

| Component           | Description                                                                                                       |
| ------------------- | ----------------------------------------------------------------------------------------------------------------- |
| **HBase Shell**     | Schema admin: `describe`, `alter`, `disable`, `enable`, `drop`                                                    |
| **Simple Filter**   | `SingleColumnValueFilter` → Credit card trips (`payment_type = 1`)                                                |
| **Advanced Filter** | Composite `SingleColumnValueFilter + RowFilter(regex)` → High-fare Vendor 2 trips                                 |
| **Order/Count**     | Range scan: `STARTROW/STOPROW` for Vendor 1 only; `count` with `INTERVAL => 5`                                    |
| **Java API**        | [`CreateTable.java`](hbase/src/main/java/com/nyctaxi/hbase/CreateTable.java) — Admin API creates table with 4 CFs |

---

### 👤 Student 2 — Batch Ingestion & Payment Analytics

| Component           | Description                                                                                                                  |
| ------------------- | ---------------------------------------------------------------------------------------------------------------------------- |
| **HBase Shell**     | Data ops: `put`, `count`, `scan` with limits                                                                                 |
| **Simple Filter**   | `SingleColumnValueFilter` → Credit card payments (`payment_type = 1`)                                                        |
| **Advanced Filter** | Composite two-condition filter → Zero-tip rides with high total (tipping anomaly)                                            |
| **Order/Count**     | `REVERSED => true` reverse chronological scan; `count` with `CACHE => 10`                                                    |
| **Java API**        | [`InsertData.java`](hbase/src/main/java/com/nyctaxi/hbase/InsertData.java) — Batch `table.put(List<Put>)` of 12 real records |

---

### 👤 Student 3 — Record Lifecycle & Distance Analytics

| Component           | Description                                                                                                                  |
| ------------------- | ---------------------------------------------------------------------------------------------------------------------------- |
| **HBase Shell**     | Mutation ops: `get`, `delete`, `deleteall` with pre/post count audit                                                         |
| **Simple Filter**   | `PrefixFilter('1_')` → All Vendor 1 trips via key prefix (no full scan)                                                      |
| **Advanced Filter** | `SingleColumnValueFilter` with `regexstring:^[1-2]$` → Solo/couple rides, projecting only 2 CFs                              |
| **Order/Count**     | Column-projection scan with `COLUMNS` restriction; before/after deletion count                                               |
| **Java API**        | [`GetAndDelete.java`](hbase/src/main/java/com/nyctaxi/hbase/GetAndDelete.java) — Point Get + Delete + tombstone verification |

---

### 👤 Student 4 — Metrics Projection & Pagination Analytics

| Component           | Description                                                                                                                                  |
| ------------------- | -------------------------------------------------------------------------------------------------------------------------------------------- |
| **HBase Shell**     | Advanced scans: `PageFilter`, `ColumnPrefixFilter`, composite filter scan                                                                    |
| **Simple Filter**   | `PageFilter(3)` → Server-side pagination, first 3 rows                                                                                       |
| **Advanced Filter** | `ColumnPrefixFilter('fare') AND SingleColumnValueFilter` on speed > 15 mph                                                                   |
| **Order/Count**     | Composite `PrefixFilter('2_') AND PageFilter(4)` ordered scan; final `count`                                                                 |
| **Java API**        | [`ScanWithFilter.java`](hbase/src/main/java/com/nyctaxi/hbase/ScanWithFilter.java) — 5 distinct server-side filters (default JAR main class) |

---

## ⚡ Review 3 — Apache Spark Analytics, Scala & Machine Learning

Review 3 extends the batch pipeline with Apache Spark. It uses PySpark DataFrames for cleaning and exploratory analysis, Scala RDDs for low-level distributed processing demonstrations, PySpark MLlib for predictive modeling, and Python visualization libraries for the final dashboard.

### Review 3 Architecture

| Stage                    | Implementation              | Purpose                                                                                                             | Main outputs                                                                 |
| ------------------------ | --------------------------- | ------------------------------------------------------------------------------------------------------------------- | ---------------------------------------------------------------------------- |
| 1. Preprocessing and EDA | PySpark DataFrame           | Profile the raw Parquet data, remove invalid/outlier trips, and derive time, speed, fare-per-mile, and tip features | Cleaned Parquet data, hourly demand, zone statistics, payment/tip statistics |
| 2. RDD analytics         | Scala + Spark RDD           | Demonstrate lineage, partitions, lazy transformations, eager actions, key-value aggregation, and persistence        | Console results for demand, pickup zones, tips, and fare-per-mile analysis   |
| 3. Machine learning      | PySpark MLlib               | Predict whether a trip is high-fare using a reproducible 80/20 split                                                | `ml_results.json`, `lr_predictions.csv`, `rf_predictions.csv`                |
| 4. Visualization         | Python, Matplotlib, Seaborn | Turn aggregate and model results into presentation-ready charts                                                     | Five charts and `dashboard.png`                                              |

### Review 3 Quickstart

Review 3 uses the January 2026 Parquet dataset from `data/raw/` and writes generated artifacts to `review3/output/`. Docker is the recommended, fully tested option (OpenJDK 21 + Python 3.10 + PySpark 4.2.0).

#### 1. Full Pipeline in One Command (Docker)

```bash
# Option A: From inside the review3/ folder
cd review3
docker compose up --build

# Option B: From the repository root
docker compose run --rm spark-review3
```

#### 2. Run Individual Stages via Docker

```bash
cd review3

# Stage 1: Data Preprocessing & EDA (PySpark)
docker run --rm \
  -v "$(pwd)/../data/raw:/app/data:ro" \
  -v "$(pwd)/output:/app/review3/output" \
  -v "$(pwd)/python:/app/review3/python:ro" \
  -v "$(pwd)/scala:/app/review3/scala:ro" \
  -v "$(pwd)/data:/app/review3/data" \
  review3-spark-review3 spark-submit --master "local[*]" --driver-memory 4g python/01_preprocessing_eda.py

# Stage 2: Scala + Spark RDD Processing
docker run --rm \
  -v "$(pwd)/../data/raw:/app/data:ro" \
  -v "$(pwd)/output:/app/review3/output" \
  -v "$(pwd)/python:/app/review3/python:ro" \
  -v "$(pwd)/scala:/app/review3/scala:ro" \
  -v "$(pwd)/data:/app/review3/data" \
  review3-spark-review3 bash -c "printf ':load scala/NycTaxiRDD.scala\n:quit\n' | spark-shell --master 'local[*]' --driver-memory 4g"

# Stage 3: Machine Learning Classification (PySpark MLlib)
docker run --rm \
  -v "$(pwd)/../data/raw:/app/data:ro" \
  -v "$(pwd)/output:/app/review3/output" \
  -v "$(pwd)/python:/app/review3/python:ro" \
  -v "$(pwd)/scala:/app/review3/scala:ro" \
  -v "$(pwd)/data:/app/review3/data" \
  review3-spark-review3 spark-submit --master "local[*]" --driver-memory 4g python/02_ml_model.py

# Stage 4: Visualizations & Dashboard
docker run --rm \
  -v "$(pwd)/../data/raw:/app/data:ro" \
  -v "$(pwd)/output:/app/review3/output" \
  -v "$(pwd)/python:/app/review3/python:ro" \
  -v "$(pwd)/scala:/app/review3/scala:ro" \
  -v "$(pwd)/data:/app/review3/data" \
  review3-spark-review3 python3 python/03_visualizations.py
```

#### 3. Run Locally on Host Machine

```bash
cd review3
pip3 install -r requirements.txt
bash run_all.sh

# Or interactive Scala shell:
spark-shell --driver-memory 4g
:load scala/NycTaxiRDD.scala
```

*(See detailed guide and bug fixes in [`review3/README.md`](review3/README.md))*

### Scala RDD Demonstration

`scala/NycTaxiRDD.scala` loads Parquet through a Spark DataFrame and converts it to an RDD to expose Spark's foundational execution model. It demonstrates:

- `map`, `filter`, `reduceByKey`, `mapValues`, and `flatMap` transformations
- `count`, `take`, `reduce`, and `collect` actions
- Partition inspection and RDD lineage/DAG behavior
- `persist(StorageLevel.MEMORY_AND_DISK)` so the cleaned RDD can be reused across questions
- Three business analyses: hourly demand and top pickup zones, payment type versus tipping, and fare-per-mile by time period and zone

### Machine Learning Pipeline

The ML task is binary classification: label a trip as high-fare when `fare_amount` is above the dataset median. The pipeline uses `VectorAssembler`, `StandardScaler`, and two classifiers:

1. **Logistic Regression** provides a fast, interpretable baseline.
2. **Random Forest** captures non-linear effects such as airport flat fares, congestion thresholds, and rush-hour patterns.

The feature set includes `trip_distance`, `pickup_hour`, `pickup_dayofweek`, `passenger_count`, `PULocationID`, `trip_duration_min`, `is_weekend`, and `avg_speed_mph` when available. Both models use the same seeded 80/20 train-test split and are evaluated with accuracy, weighted precision, weighted recall, F1-score, and ROC-AUC.

The recorded Review 3 benchmark shows Random Forest outperforming Logistic Regression:

| Metric             | Logistic Regression | Random Forest |
| ------------------ | ------------------: | ------------: |
| Accuracy           |              96.57% |    **97.96%** |
| Weighted precision |              0.9674 |    **0.9796** |
| Weighted recall    |              0.9657 |    **0.9796** |
| F1-score           |              0.9657 |    **0.9796** |
| ROC-AUC            |              0.9945 |    **0.9973** |

The Random Forest feature-importance output identifies `trip_duration_min`, `trip_distance`, and `avg_speed_mph` as the leading predictors in the recorded run. Exact values can be regenerated from `review3/output/ml_results.json`.

### Review 3 Generated Artifacts

```text
review3/output/
├── dashboard.png                 # Combined six-panel executive dashboard
├── viz1_hourly_demand.png        # Hourly demand distribution
├── viz2_top_zones.png            # Top pickup zones by trips and revenue
├── viz3_payment_tips.png         # Payment method and tipping behavior
├── viz4_feature_importance.png   # Random Forest feature importance
├── viz5_model_evaluation.png     # ROC, confusion matrices, and metrics
├── hourly_demand.csv             # Hourly aggregate statistics
├── zone_stats.csv                # Pickup-zone aggregate statistics
├── payment_tip_stats.csv         # Payment method statistics
├── ml_results.json               # Metrics, confusion matrices, and feature importance
├── lr_predictions.csv            # Logistic Regression prediction sample
└── rf_predictions.csv            # Random Forest prediction sample
```

---

## 🔑 Key HBase Concepts (Viva Reference)

| Concept                   | Explanation                                                                                     |
| ------------------------- | ----------------------------------------------------------------------------------------------- |
| **Row Key Design**        | Composite `{VendorID}_{datetime}_{PULocationID}` enables prefix scans and prevents hot-spotting |
| **Column Families**       | Stored in separate HFiles — queries only read relevant CFs from disk                            |
| **MemStore**              | In-memory write buffer (128MB). Mutations go WAL → MemStore → HFile                             |
| **Write-Ahead Log (WAL)** | HDFS-backed durability log. Written before MemStore for crash recovery                          |
| **Tombstone Markers**     | `delete` writes a tombstone — row is hidden immediately, physically purged at Major Compaction  |
| **Server-Side Filters**   | Evaluated on RegionServer — only matching rows cross the network to the client                  |
| **HBase vs Hive**         | HBase: millisecond point lookups, real-time mutations. Hive: batch SQL analytics                |

---

## 📊 Dataset

| Field                 | Value                                                                                            |
| --------------------- | ------------------------------------------------------------------------------------------------ |
| **Source**            | [NYC TLC Yellow Taxi Trip Records](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page) |
| **File**              | `yellow_tripdata_cleaned.csv`                                                                    |
| **Volume**            | ~7.6 million cleaned rows (January–March 2026)                                                   |
| **Domain**            | Transportation — NYC Smart City Analytics                                                        |
| **Loaded into HBase** | 12 representative records (real data, not synthetic)                                             |

Review 3 uses the January 2026 Parquet file as its Spark demonstration input and writes its generated analytics and ML artifacts to `review3/output/`.

---

## 🛠️ HBase Restart Commands (if container restarts)

```bash
docker start hbase
docker exec hbase /root/hbase-bin/bin/hbase-daemon.sh start zookeeper && sleep 5
docker exec hbase /root/hbase-bin/bin/hbase-daemon.sh start master && sleep 20
docker exec hbase /root/hbase-bin/bin/hbase-daemon.sh start regionserver
```

---

_Dataset: NYC TLC Open Data | Hadoop 3.3.6 | Hive 3.1.3 | HBase 2.4.19 | Spark 3.5+ | Scala | PySpark | Docker_
