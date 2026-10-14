# 🚕 NYC Taxi Demand & Fare Intelligence System
## BDA Review 3 — Apache Spark, Scala & PySpark (23CSE352)

> **Case Study:** New York City Yellow Taxi operational records (~3.7M raw trips / 2.5M clean trips). We analyze spatial-temporal demand dynamics, payment tipping behavior, and deploy predictive Machine Learning models (Logistic Regression vs. Random Forest) on Apache Spark.

---

## ⚡ Quick Start (Zero Setup via Docker)

If you don't want to install Java 21 or Apache Spark on your host machine, you can run the entire pipeline in an isolated container in **one command**:

```bash
# Option A: From inside the review3/ folder
cd review3
docker compose up --build

# Option B: From the repository root (bda2/)
docker compose run --rm spark-review3
```
This automatically builds the environment with OpenJDK 17 + Python 3.10 + PySpark, mounts the raw dataset, executes all 4 pipeline stages, and saves the generated charts to `review3/output/`.

---

## 💻 Quick Start (Running Locally on Host Machine)

### 1. Prerequisites
- **Java**: OpenJDK 17 or OpenJDK 21 *(Note: Java 25 has known Hadoop auth issues)*
  ```bash
  # macOS (Homebrew)
  brew install openjdk@21
  ```
- **Python Dependencies**:
  ```bash
  pip3 install -r requirements.txt
  ```

### 2. Run the Full Pipeline
```bash
cd review3
bash run_all.sh
```

### 3. Run Steps Individually

#### **Step 1: Data Cleaning & Exploratory Data Analysis (PySpark)**
```bash
python3 python/01_preprocessing_eda.py
# OR
spark-submit --master "local[*]" --driver-memory 4g python/01_preprocessing_eda.py
```
*Outputs: Cleans 1.22M outlier records, produces summary statistics, and saves `hourly_demand.csv`, `zone_stats.csv`, and `payment_tip_stats.csv`.*

#### **Step 2: Scala + Spark RDD Processing (Interactive)**
```bash
spark-shell --driver-memory 4g
```
Inside the interactive Scala shell, type:
```scala
:load scala/NycTaxiRDD.scala
```
*Outputs: Demonstrates RDD lineage DAG, partition count, `cache()` in memory/disk, 5 lazy transformations (`map`, `filter`, `reduceByKey`, `mapValues`, `flatMap`), and 4 eager actions (`count`, `take`, `reduce`, `collect`).*

#### **Step 3: Machine Learning Pipeline (PySpark MLlib)**
```bash
python3 python/02_ml_model.py
# OR
spark-submit --master "local[*]" --driver-memory 4g python/02_ml_model.py
```
*Outputs: Trains Logistic Regression vs Random Forest on 80/20 train-test split, computes ROC-AUC, Accuracy, Precision, Recall, F1, and saves `ml_results.json`.*

#### **Step 4: Generate Analytical Charts & Dashboard**
```bash
python3 python/03_visualizations.py
```
*Outputs: Generates 5 high-resolution individual charts and a 6-panel executive dashboard saved to `output/dashboard.png`.*

---

## 📁 Project Directory Structure

```
review3/
├── Dockerfile                         # Isolated container build (Java 17 + PySpark)
├── docker-compose.yml                 # Standalone Docker compose config
├── requirements.txt                   # Python package dependencies
├── run_all.sh                         # Master bash orchestrator script
├── README.md                          # Complete project guide & documentation
│
├── scala/
│   └── NycTaxiRDD.scala               # Scala RDD processing (5 trans, 4 actions, DAG, cache)
│
├── python/
│   ├── 01_preprocessing_eda.py         # PySpark cleaning & exploratory analytics
│   ├── 02_ml_model.py                  # PySpark MLlib (Logistic Regression vs Random Forest)
│   └── 03_visualizations.py            # Matplotlib & Seaborn dashboard generator
│
└── output/                            # Generated artifacts & charts
    ├── dashboard.png                  # Combined 6-panel executive summary
    ├── viz1_hourly_demand.png         # Hourly demand distribution
    ├── viz2_top_zones.png             # Top 10 pickup zones by revenue & trips
    ├── viz3_payment_tips.png          # Payment type vs. tipping behavior
    ├── viz4_feature_importance.png     # Random Forest feature importance rankings
    ├── viz5_model_evaluation.png      # ROC curve, confusion matrices & metrics comparison
    ├── ml_results.json                # Benchmark metrics JSON
    ├── hourly_demand.csv              # Hourly aggregate statistics
    ├── zone_stats.csv                 # Zone aggregate statistics
    ├── payment_tip_stats.csv          # Payment method statistics
    ├── lr_predictions.csv             # Logistic Regression test predictions sample
    └── rf_predictions.csv             # Random Forest test predictions sample
```

---

## 🔧 Architecture & Technology Justification

| Tool | Application Scope | Technical Justification |
|---|---|---|
| **Scala + Spark RDD** | Data ingestion, low-level transformations, lineage & caching | Spark is natively written in Scala. Scala RDDs run on the JVM without serialization overhead. It provides direct access to RDD internals (`toDebugString()`, partition inspection, `persist()`). |
| **PySpark MLlib** | Feature engineering & predictive classification | PySpark DataFrame ML Pipelines (`VectorAssembler` + `StandardScaler`) mirror industry-standard scikit-learn patterns, integrating cleanly with downstream data science tools. |
| **Matplotlib / Seaborn** | Visualization & Dashboard generation | Mature, publication-grade data visualization ecosystem with no direct Scala counterpart. |
| **Docker** | Portability & Containerization | Solves cross-platform JVM incompatibilities (e.g. Java 25 deprecation of Hadoop security methods) by providing a reproducible Debian/OpenJDK environment. |

---

## 📊 Analytical Decisions & Insights

1. **Peak Demand Windows**:
   - Lowest demand occurs at **4:00 AM** (12,260 trips).
   - Demand rises sharply during the morning rush (8:00 AM – 10:00 AM) and peaks during the evening commute (**5:00 PM – 7:00 PM**, reaching **181,715 trips/hr**).
   - *Operational Recommendation:* Reposition fleet vehicles toward Midtown Manhattan (Zones 161, 237, 236) by 4:30 PM.

2. **Digital Payment Impact on Tipping**:
   - **87.0%** of trips are paid via Credit Card, averaging **$3.85 / 18.4%** tip per trip.
   - Cash transactions record negligible tips in digital meter logs due to direct cash handoffs.

3. **Airport Surge**:
   - The highest average fare per hour occurs at **5:00 AM ($29.03)** and **6:00 AM ($25.16)** due to early-morning airport outbound trips (JFK/LaGuardia flat and premium rates).

---

## 🤖 Machine Learning Model Performance

- **Target Problem:** Binary classification predicting whether a trip is high-value (`label = 1` if fare $> \$12.80$ median, else `0`).
- **Input Features (8):** `trip_distance`, `pickup_hour`, `pickup_dayofweek`, `passenger_count`, `PULocationID`, `trip_duration_min`, `is_weekend`, `avg_speed_mph`.
- **Dataset Split:** 80% Training ($2,001,842$ records) / 20% Testing ($500,691$ records).

| Metric | Logistic Regression | Random Forest Classifier | Winner |
|---|---|---|---|
| **Accuracy** | 96.57% | **97.96%** | **Random Forest ✅** |
| **Weighted Precision** | 0.9674 | **0.9796** | **Random Forest ✅** |
| **Weighted Recall** | 0.9657 | **0.9796** | **Random Forest ✅** |
| **F1-Score** | 0.9657 | **0.9796** | **Random Forest ✅** |
| **AUC-ROC** | 0.9945 | **0.9973** | **Random Forest ✅** |

### Top Predictive Features (from Random Forest):
1. **`trip_duration_min`** (69.51% importance) — Meter ticking during traffic delay is the primary fare contributor.
2. **`trip_distance`** (24.49% importance) — Base distance traveled.
3. **`avg_speed_mph`** (5.13% importance) — Highlights congestion conditions.
