# 🚕 NYC Taxi Demand & Fare Intelligence System
## BDA Review 3 — Apache Spark, Scala & PySpark (23CSE352 — 15 Marks)

> **Case Study:** NYC Yellow Taxi operational dataset (~3.7M raw trips / 2.5M clean trips). We analyze spatial-temporal demand dynamics, payment tipping behavior, route speed bottlenecks, and deploy 4 Machine Learning models (2 Regression + 2 Classification) using Apache Spark.

---

## 👥 Student Work Distribution (4 Students — Complete Coverage)

Each student implements a complete, self-contained **Scala + Spark RDD** pipeline satisfying the 15-mark rubric:
- $\ge 3$ Transformations (`map`, `filter`, `reduceByKey`, `mapValues`, `flatMap`)
- $\ge 2$ Actions (`count`, `take`, `reduce`, `collect`)
- Key-Value RDD operations
- Partition count inspection
- RDD Lineage graph & DAG explanation
- In-memory caching with `persist(StorageLevel.MEMORY_AND_DISK)`

| Student | Assigned Topic | Scala RDD Script | ML Model Assignment |
|---|---|---|---|
| **Student 1** | **Temporal Demand Spikes & Hotspots** | [`scala/Student1_SpatialDemandRDD.scala`](file:///Users/apple/Documents/sem7/bda/project/bda2/review3/scala/Student1_SpatialDemandRDD.scala) | **Linear Regression** (Upfront Fare Baseline) |
| **Student 2** | **Revenue Economics & Airport Tolls** | [`scala/Student2_RevenueTollsRDD.scala`](file:///Users/apple/Documents/sem7/bda/project/bda2/review3/scala/Student2_RevenueTollsRDD.scala) | **Random Forest Regressor** (Non-linear Fare Engine) |
| **Student 3** | **Digital Payment & Tipping Dynamics** | [`scala/Student3_TippingBehaviorRDD.scala`](file:///Users/apple/Documents/sem7/bda/project/bda2/review3/scala/Student3_TippingBehaviorRDD.scala) | **Logistic Regression** (Dispatch Classification) |
| **Student 4** | **Traffic Speed & Congestion Surcharges** | [`scala/Student4_CongestionDurationRDD.scala`](file:///Users/apple/Documents/sem7/bda/project/bda2/review3/scala/Student4_CongestionDurationRDD.scala) | **Random Forest Classifier** (Priority Matching) |

---

## ⚡ Quick Start (Zero Setup via Docker)

To run the entire 4-student and 4-model pipeline inside an isolated container:

```bash
# Option A: From inside review3/
cd review3
docker compose up --build

# Option B: From the repository root (bda2/)
docker compose run --rm spark-review3
```
*Outputs are saved automatically to `review3/output/`.*

---

## 💻 Local Execution Guide

### 1. Prerequisites
- **Java**: OpenJDK 17 or OpenJDK 21 (`brew install openjdk@21`)
- **Python Packages**:
  ```bash
  pip3 install -r requirements.txt
  ```

### 2. Run the Full Pipeline in One Command
```bash
cd review3
bash run_all.sh
```

---

## 🎓 Viva Demo Guide: Running Each Student's Scala Script

Launch the interactive Spark shell:
```bash
spark-shell --driver-memory 4g
```

Then load the script for the student being evaluated:

#### **Student 1: Spatial Demand & Hourly Surge**
```scala
:load scala/Student1_SpatialDemandRDD.scala
```
*Demonstrates: Hourly frequency RDD, `reduceByKey`, surge alert `flatMap`, peak window identification (`reduce`), RDD DAG lineage.*

#### **Student 2: Revenue, Tolls & Airport Surcharges**
```scala
:load scala/Student2_RevenueTollsRDD.scala
```
*Demonstrates: Airport route tagging (JFK/LGA/EWR), average toll calculation with `mapValues`, gross revenue aggregation, max revenue route (`reduce`).*

#### **Student 3: Payment Modes & Customer Tipping Behavior**
```scala
:load scala/Student3_TippingBehaviorRDD.scala
```
*Demonstrates: Payment method mapping, credit card vs cash tip accumulation, generous tip event emission (>30%), maximum tip recorded (`reduce`).*

#### **Student 4: Traffic Congestion, Route Speed & Surcharges**
```scala
:load scala/Student4_CongestionDurationRDD.scala
```
*Demonstrates: In-flight velocity calculation (mph), gridlock filtering (<6 mph), hourly congestion surcharge revenue, longest duration trip (`reduce`).*

#### **Master Unified Runner (All 4 Students):**
```scala
:load scala/NycTaxiRDD.scala
```

---

## 🤖 The 4 Machine Learning Models

### Part 1: Regression (Dynamic Upfront Fare Estimation)
- **Business Problem:**
  - *Context:* Riders demand upfront price guarantees before booking. Underquoting fares causes driver cancellations; overquoting hurts conversion.
  - *Target:* `fare_amount` (Continuous USD).
- **Models:**
  1. **Linear Regression (with ElasticNet regularization):** Interpretable linear baseline.
  2. **Random Forest Regressor (40 trees, depth 8):** Captures non-linearities such as JFK flat-rate fares ($70) and congestion surcharge steps.

#### Regression Benchmark Results:
| Metric | Linear Regression | Random Forest Regressor | Winner |
|---|---|---|---|
| **$R^2$ Score** | 0.8668 | **0.8954** | **Random Forest Regressor ✅** |
| **RMSE ($)** | $6.64 | **$5.89** | **Random Forest Regressor ✅** |
| **MAE ($)** | $2.83 | **$1.73** | **Random Forest Regressor ✅** |
| **Training Time** | **2.80s** | 8.20s | Linear Regression (Faster) |

---

### Part 2: Classification (High-Value Ride Dispatch Priority)
- **Business Problem:**
  - *Context:* Drivers waste 35% of their working hours cruising empty for low-fare trips. Dispatch engines classify incoming rides as High-Value (Fare $\ge \$15.00$) to prioritize high-rated drivers and improve driver retention.
  - *Target:* `high_fare_label` (Binary: 1 if Fare $\ge \$15.00$, else 0).
- **Models:**
  3. **Logistic Regression Classifier:** Probabilistic classification baseline.
  4. **Random Forest Classifier (40 trees, depth 8):** Ensemble tree classifier capturing non-linear spatial-temporal boundaries.

#### Classification Benchmark Results:
| Metric | Logistic Regression | Random Forest Classifier | Winner |
|---|---|---|---|
| **Accuracy** | 90.83% | **98.06%** | **Random Forest Classifier ✅** |
| **Weighted Precision** | 0.9204 | **0.9806** | **Random Forest Classifier ✅** |
| **Weighted Recall** | 0.9083 | **0.9806** | **Random Forest Classifier ✅** |
| **F1-Score** | 0.9059 | **0.9805** | **Random Forest Classifier ✅** |
| **AUC-ROC** | 0.9922 | **0.9968** | **Random Forest Classifier ✅** |
| **Training Time** | **2.06s** | 6.68s | Logistic Regression (Faster) |

---

### 📊 Top Predictive Features (from Random Forest Models):
1. **`trip_distance`** (40.52% Regression / 30.41% Classification) — Physical distance is the core fare driver.
2. **`trip_duration_min`** (38.28% Regression / 61.32% Classification) — Manhattan traffic meter ticking drives premium pricing.
3. **`avg_speed_mph`** (15.99% Regression / 6.57% Classification) — Reflects gridlock conditions.
4. **`PULocationID`** (4.46% Regression / 1.49% Classification) — Distinguishes airport pickup zones from outer boroughs.

---

## 📁 Generated Outputs in `review3/output/`

```
output/
├── dashboard.png                      # 6-Panel Master Executive Dashboard
├── viz1_student1_demand.png           # Student 1: Hourly Demand Distribution
├── viz2_student2_revenue.png          # Student 2: Airport vs Non-Airport Economics
├── viz3_student3_tips.png             # Student 3: Payment Method vs Tipping Rates
├── viz4_student4_speed.png            # Student 4: Manhattan Velocity Bottlenecks
├── viz5_regression_models.png         # Regression Models: R² and MAE Comparisons
├── viz6_classification_models.png     # Classification Models: Accuracy, F1, ROC
└── ml_results_4models.json            # Machine-readable benchmarks JSON
```

---

## 🔧 Technology Architecture Justification

- **Scala for Spark RDD**: Spark's native language. Zero JVM serialization overhead; authentic demonstration of low-level RDD internals (`toDebugString`, partitions, `persist`).
- **PySpark for MLlib**: PySpark DataFrames integrate seamlessly with modern Machine Learning pipelines (`VectorAssembler`, `StandardScaler`) and allow direct visualization in Python (`matplotlib`, `seaborn`).
- **Docker**: Encapsulates OpenJDK 17 and Python 3.10 to eliminate host OS environment conflicts.
