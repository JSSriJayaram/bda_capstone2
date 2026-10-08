# 🚕 NYC Yellow Taxi — Big Data Analytics Platform

> **BDA Capstone Project | 23CSE352** — Full pipeline: HDFS → MapReduce → Hive → Apache HBase

[![Apache HBase](https://img.shields.io/badge/Apache%20HBase-2.4.x-red?style=flat-square&logo=apache)](https://hbase.apache.org/)
[![Apache Hive](https://img.shields.io/badge/Apache%20Hive-3.1.3-yellow?style=flat-square&logo=apache)](https://hive.apache.org/)
[![Apache Hadoop](https://img.shields.io/badge/Hadoop-3.3.6-blue?style=flat-square&logo=apache)](https://hadoop.apache.org/)
[![Java](https://img.shields.io/badge/Java-8-orange?style=flat-square&logo=java)](https://openjdk.java.net/)
[![Docker](https://img.shields.io/badge/Docker-Compose-2496ED?style=flat-square&logo=docker)](https://docs.docker.com/compose/)

---

## 📋 Project Overview

This project builds a complete big data analytics pipeline for the **NYC Yellow Taxi Trip dataset** (NYC TLC Open Data, 7.6M rows, Jan 2026). The pipeline progresses through three analytics layers:

| Review | Technology | Focus |
|--------|-----------|-------|
| Review 1 | HDFS + MapReduce + Hive | Batch ingestion, distributed computation, SQL analytics |
| **Review 2** | **Apache HBase** | **NoSQL storage, real-time lookups, server-side filter analytics** |

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
```

---

## 🗄️ HBase Table Design (Review 2)

### Table: `nyc_taxi`

**Row Key Format:** `{VendorID}_{pickup_datetime_compact}_{PULocationID}`
- Example: `2_20260101005404_239`
- Enables fast **vendor prefix scans** and **time-range scans**
- Prevents **region hot-spotting** (composite key distributes writes)

| Column Family | Qualifiers | Purpose |
|---|---|---|
| `trip_info` | passenger_count, trip_distance, PULocationID, DOLocationID, payment_type | Geographic & capacity analytics |
| `financials` | fare_amount, tip_amount, total_amount, congestion_surcharge | Revenue & payment analytics |
| `timing` | pickup_datetime, dropoff_datetime, trip_duration_min, pickup_hour, pickup_day_name | Temporal analytics |
| `metrics` | avg_speed_mph, fare_per_mile, is_weekend | Performance dashboard |

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
| Component | Description |
|-----------|-------------|
| **HBase Shell** | Schema admin: `describe`, `alter`, `disable`, `enable`, `drop` |
| **Simple Filter** | `SingleColumnValueFilter` → Credit card trips (`payment_type = 1`) |
| **Advanced Filter** | Composite `SingleColumnValueFilter + RowFilter(regex)` → High-fare Vendor 2 trips |
| **Order/Count** | Range scan: `STARTROW/STOPROW` for Vendor 1 only; `count` with `INTERVAL => 5` |
| **Java API** | [`CreateTable.java`](hbase/src/main/java/com/nyctaxi/hbase/CreateTable.java) — Admin API creates table with 4 CFs |

---

### 👤 Student 2 — Batch Ingestion & Payment Analytics
| Component | Description |
|-----------|-------------|
| **HBase Shell** | Data ops: `put`, `count`, `scan` with limits |
| **Simple Filter** | `SingleColumnValueFilter` → Credit card payments (`payment_type = 1`) |
| **Advanced Filter** | Composite two-condition filter → Zero-tip rides with high total (tipping anomaly) |
| **Order/Count** | `REVERSED => true` reverse chronological scan; `count` with `CACHE => 10` |
| **Java API** | [`InsertData.java`](hbase/src/main/java/com/nyctaxi/hbase/InsertData.java) — Batch `table.put(List<Put>)` of 12 real records |

---

### 👤 Student 3 — Record Lifecycle & Distance Analytics
| Component | Description |
|-----------|-------------|
| **HBase Shell** | Mutation ops: `get`, `delete`, `deleteall` with pre/post count audit |
| **Simple Filter** | `PrefixFilter('1_')` → All Vendor 1 trips via key prefix (no full scan) |
| **Advanced Filter** | `SingleColumnValueFilter` with `regexstring:^[1-2]$` → Solo/couple rides, projecting only 2 CFs |
| **Order/Count** | Column-projection scan with `COLUMNS` restriction; before/after deletion count |
| **Java API** | [`GetAndDelete.java`](hbase/src/main/java/com/nyctaxi/hbase/GetAndDelete.java) — Point Get + Delete + tombstone verification |

---

### 👤 Student 4 — Metrics Projection & Pagination Analytics
| Component | Description |
|-----------|-------------|
| **HBase Shell** | Advanced scans: `PageFilter`, `ColumnPrefixFilter`, composite filter scan |
| **Simple Filter** | `PageFilter(3)` → Server-side pagination, first 3 rows |
| **Advanced Filter** | `ColumnPrefixFilter('fare') AND SingleColumnValueFilter` on speed > 15 mph |
| **Order/Count** | Composite `PrefixFilter('2_') AND PageFilter(4)` ordered scan; final `count` |
| **Java API** | [`ScanWithFilter.java`](hbase/src/main/java/com/nyctaxi/hbase/ScanWithFilter.java) — 5 distinct server-side filters (default JAR main class) |

---

## 🔑 Key HBase Concepts (Viva Reference)

| Concept | Explanation |
|---------|-------------|
| **Row Key Design** | Composite `{VendorID}_{datetime}_{PULocationID}` enables prefix scans and prevents hot-spotting |
| **Column Families** | Stored in separate HFiles — queries only read relevant CFs from disk |
| **MemStore** | In-memory write buffer (128MB). Mutations go WAL → MemStore → HFile |
| **Write-Ahead Log (WAL)** | HDFS-backed durability log. Written before MemStore for crash recovery |
| **Tombstone Markers** | `delete` writes a tombstone — row is hidden immediately, physically purged at Major Compaction |
| **Server-Side Filters** | Evaluated on RegionServer — only matching rows cross the network to the client |
| **HBase vs Hive** | HBase: millisecond point lookups, real-time mutations. Hive: batch SQL analytics |

---

## 📊 Dataset

| Field | Value |
|-------|-------|
| **Source** | [NYC TLC Yellow Taxi Trip Records](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page) |
| **File** | `yellow_tripdata_cleaned.csv` |
| **Volume** | ~7.6 million rows (Jan 2026) |
| **Domain** | Transportation — NYC Smart City Analytics |
| **Loaded into HBase** | 12 representative records (real data, not synthetic) |

---

## 🛠️ HBase Restart Commands (if container restarts)

```bash
docker start hbase
docker exec hbase /root/hbase-bin/bin/hbase-daemon.sh start zookeeper && sleep 5
docker exec hbase /root/hbase-bin/bin/hbase-daemon.sh start master && sleep 20
docker exec hbase /root/hbase-bin/bin/hbase-daemon.sh start regionserver
```

---

*Dataset: NYC TLC Open Data | Apache HBase 2.4.19 | Java 8 | Maven 3 | Docker*
