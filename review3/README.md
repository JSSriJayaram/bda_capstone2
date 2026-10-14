# 🚕 NYC Taxi Demand & Fare Intelligence System
## BDA Review 3 — Apache Spark (23CSE352)

> **Case Study:** NYC processes millions of taxi rides daily. We analyze 7.6M trips from January 2026 to find demand patterns, fare drivers, and build a predictive ML model.

---

## 📁 Project Structure

```
review3/
├── data/
│   └── yellow_tripdata_2026-01.parquet  → Raw NYC TLC data (symlink)
│
├── scala/
│   └── NycTaxiRDD.scala                → Scala RDD processing (Q1-Q3)
│
├── python/
│   ├── 01_preprocessing_eda.py          → PySpark data cleaning & EDA
│   ├── 02_ml_model.py                   → PySpark ML (LR + Random Forest)
│   └── 03_visualizations.py             → matplotlib/seaborn charts
│
├── output/                              → Generated charts & results
├── run_all.sh                           → Master pipeline script
└── README.md                            → This file
```

---

## 🚀 How to Run

### Prerequisites
```bash
# Install Apache Spark via Homebrew
brew install apache-spark

# Install Python dependencies
pip3 install pyspark matplotlib seaborn pandas pyarrow
```

### Run Pipeline
```bash
# Step 1: EDA (PySpark)
spark-submit --master "local[*]" --driver-memory 4g python/01_preprocessing_eda.py

# Step 2: Scala RDD (Interactive — run in spark-shell)
spark-shell --driver-memory 4g
# Then in spark-shell:
:load scala/NycTaxiRDD.scala

# Step 3: ML Model (PySpark)
spark-submit --master "local[*]" --driver-memory 4g python/02_ml_model.py

# Step 4: Visualizations
python3 python/03_visualizations.py
```

---

## 🔧 Why Each Tool?

| Tool | What | Why |
|---|---|---|
| **Scala + Spark RDD** | Data ingestion, transformations, aggregations | Spark is written in Scala. Zero JVM serialization overhead. Best for demonstrating RDD concepts (lineage, DAG, partitions). |
| **PySpark DataFrame** | EDA, feature engineering, ML | Python has matplotlib/seaborn for visualization. MLlib Pipeline API is designed for DataFrames. Industry standard for data science. |
| **matplotlib/seaborn** | Charts and dashboard | Python-only libraries. No Scala equivalent exists. |

---

## 📊 Analysis Questions

| # | Question | Tool | Result |
|---|---|---|---|
| Q1 | Peak demand hours & busiest zones? | Scala RDD | Evening rush 5-8 PM; Zone 161 (Midtown) |
| Q2 | Payment type → tipping behavior? | Scala RDD | Credit card tips >> Cash |
| Q3 | Fare-per-mile by time period? | Scala RDD | Night has highest $/mile |
| Q4 | Can we predict high-fare trips? | PySpark ML | Random Forest: ~82% accuracy |

---

## 🤖 ML Model

- **Task:** Binary classification — predict high-fare trips (fare > median)
- **Models:** Logistic Regression (baseline) vs Random Forest (ensemble)
- **Features:** trip_distance, pickup_hour, dayofweek, passenger_count, PULocationID, duration
- **Winner:** Random Forest (handles JFK flat-rate fares, congestion surcharge thresholds)
