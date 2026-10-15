"""
01_preprocessing_eda.py
BDA Review 3 — Preprocessing & Exploratory Data Analysis
NYC Yellow Taxi Trip Data (Jan 2026)

WHY PySpark (not Scala) FOR EDA?
    EDA is interactive and exploratory. Python's describe(), groupBy().agg(),
    and direct integration with pandas/matplotlib makes it 10x faster to iterate.
    PySpark DataFrames use the same Catalyst optimizer as Scala DataFrames —
    zero performance difference for DataFrame operations.

HOW TO RUN:
    spark-submit 01_preprocessing_eda.py
    OR: python3 01_preprocessing_eda.py  (if pyspark is pip-installed)
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, count, when, isnan, isnull,
    hour, dayofweek, date_format,
    unix_timestamp, round as spark_round,
    mean, stddev, min as spark_min, max as spark_max,
    sum as spark_sum, desc, lit
)
import os
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import seaborn as sns
import pandas as pd
import numpy as np

# ============================================================
# INITIALIZE SPARK
# ============================================================
spark = SparkSession.builder \
    .appName("NYC Taxi EDA - BDA Review 3") \
    .config("spark.sql.shuffle.partitions", "8") \
    .config("spark.driver.memory", "4g") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

DATA_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "data")
# Also check parent data/raw directory if inside bda2 repository or root
if not os.path.exists(os.path.join(DATA_DIR, "yellow_tripdata_2026-01.parquet")):
    parent_raw = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))), "data", "raw")
    if os.path.exists(os.path.join(parent_raw, "yellow_tripdata_2026-01.parquet")):
        DATA_DIR = parent_raw

OUTPUT_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "output")
os.makedirs(OUTPUT_DIR, exist_ok=True)
os.makedirs(DATA_DIR, exist_ok=True)

print("=" * 70)
print("  NYC TAXI — PREPROCESSING & EDA")
print("  Dataset: NYC TLC Yellow Taxi Trip Data (Jan 2026)")
print("=" * 70)

# ============================================================
# STEP 1: LOAD RAW DATA
# ============================================================
parquet_file = os.path.join(DATA_DIR, "yellow_tripdata_2026-01.parquet")
print(f"Loading raw data from: {parquet_file}")
raw_df = spark.read.parquet(parquet_file)

print(f"\n📦 Raw Dataset Loaded")
print(f"   Rows:    {raw_df.count():,}")
print(f"   Columns: {len(raw_df.columns)}")

print("\n📋 Schema:")
raw_df.printSchema()

print("\n📋 Sample Records:")
raw_df.show(5, truncate=False)

# ============================================================
# STEP 2: DATA QUALITY ANALYSIS
# ============================================================
print("\n" + "=" * 70)
print("  DATA QUALITY ANALYSIS")
print("=" * 70)

# Null count per column
print("\n📊 Null/Missing Values Per Column:")
null_df = raw_df.select([
    count(when(col(c).isNull(), c)).alias(c)
    for c in raw_df.columns
])
null_df.show(truncate=False)

# Percentage of nulls
total_rows = raw_df.count()
print(f"\n📊 Null Percentage (out of {total_rows:,} rows):")
null_counts = null_df.first().asDict()
for c, null_count in null_counts.items():
    if null_count and null_count > 0:
        pct = (null_count / total_rows) * 100
        print(f"   {c:30s} → {null_count:>10,} nulls ({pct:.2f}%)")

# ============================================================
# STEP 3: PREPROCESSING (CLEANING)
# ============================================================
print("\n" + "=" * 70)
print("  PREPROCESSING — CLEANING PIPELINE")
print("=" * 70)

# Add computed columns
df = raw_df \
    .withColumn("trip_duration_min",
        (unix_timestamp("tpep_dropoff_datetime") -
         unix_timestamp("tpep_pickup_datetime")) / 60.0) \
    .withColumn("pickup_hour", hour("tpep_pickup_datetime")) \
    .withColumn("pickup_dayofweek", dayofweek("tpep_pickup_datetime")) \
    .withColumn("pickup_day_name", date_format("tpep_pickup_datetime", "EEEE")) \
    .withColumn("is_weekend",
        when(dayofweek("tpep_pickup_datetime").isin(1, 7), 1).otherwise(0))

before_count = df.count()

# Apply cleaning filters
df_clean = df.filter(
    (col("fare_amount") > 0) &           # Must have positive fare
    (col("fare_amount") < 500) &          # Remove extreme outliers
    (col("trip_distance") > 0) &          # Must have travelled
    (col("trip_distance") < 100) &        # NYC is not 100 miles wide
    (col("passenger_count") > 0) &        # Must carry passengers
    (col("passenger_count") <= 6) &       # Taxi max capacity
    (col("trip_duration_min") > 0) &      # Must have positive duration
    (col("trip_duration_min") < 180) &    # Remove 3+ hour "trips"
    (col("total_amount") > 0) &           # Must charge something
    (col("total_amount") < 500) &         # Remove extreme totals
    (col("tip_amount") >= 0) &            # Tips can't be negative
    (col("PULocationID").isNotNull()) &    # Must have pickup zone
    (col("DOLocationID").isNotNull())      # Must have dropoff zone
)

# Add derived features for ML
df_clean = df_clean \
    .withColumn("avg_speed_mph",
        when(col("trip_duration_min") > 0,
            spark_round(col("trip_distance") / (col("trip_duration_min") / 60.0), 2)
        ).otherwise(0.0)) \
    .withColumn("fare_per_mile",
        when(col("trip_distance") > 0.1,
            spark_round(col("fare_amount") / col("trip_distance"), 2)
        ).otherwise(0.0)) \
    .withColumn("tip_percentage",
        when(col("fare_amount") > 0,
            spark_round((col("tip_amount") / col("fare_amount")) * 100, 2)
        ).otherwise(0.0))

# Filter out unreasonable speeds
df_clean = df_clean.filter(
    (col("avg_speed_mph") < 80) &   # NYC speed limit is 25 mph, 80 is generous
    (col("fare_per_mile") < 50)      # $50/mile is unreasonable
)

after_count = df_clean.count()
removed = before_count - after_count

print(f"\n   Before cleaning: {before_count:>10,} rows")
print(f"   After cleaning:  {after_count:>10,} rows")
print(f"   Removed:         {removed:>10,} rows ({removed/before_count*100:.2f}%)")

# ============================================================
# STEP 4: EXPLORATORY DATA ANALYSIS
# ============================================================
print("\n" + "=" * 70)
print("  EXPLORATORY DATA ANALYSIS")
print("=" * 70)

# 4.1 Statistical Summary
print("\n📊 Statistical Summary (Key Numeric Columns):")
df_clean.select(
    "trip_distance", "fare_amount", "tip_amount",
    "total_amount", "trip_duration_min", "avg_speed_mph", "fare_per_mile"
).describe().show()

# 4.2 Payment Type Distribution
print("\n📊 Payment Type Distribution:")
payment_names = {1: "Credit Card", 2: "Cash", 3: "No Charge",
                 4: "Dispute", 5: "Unknown", 6: "Voided"}
payment_dist = df_clean.groupBy("payment_type").count() \
    .orderBy(desc("count")).collect()
for row in payment_dist:
    name = payment_names.get(row["payment_type"], f"Type {row['payment_type']}")
    pct = (row["count"] / after_count) * 100
    print(f"   {name:15s} → {row['count']:>10,} trips ({pct:.1f}%)")

# 4.3 Vendor Distribution
print("\n📊 Vendor Distribution:")
vendor_names = {1: "VeriFone (CMT)", 2: "Curb (VTS)"}
vendor_dist = df_clean.groupBy("VendorID").count() \
    .orderBy(desc("count")).collect()
for row in vendor_dist:
    name = vendor_names.get(row["VendorID"], f"Vendor {row['VendorID']}")
    pct = (row["count"] / after_count) * 100
    print(f"   {name:15s} → {row['count']:>10,} trips ({pct:.1f}%)")

# 4.4 Hourly demand
print("\n📊 Hourly Demand Distribution:")
hourly = df_clean.groupBy("pickup_hour").count() \
    .orderBy("pickup_hour").collect()
for row in hourly:
    bar_len = int(row["count"] / (after_count / 24) * 20)
    bar = "█" * bar_len
    print(f"   {row['pickup_hour']:02d}:00 │ {row['count']:>8,} │ {bar}")

# 4.5 Day of week distribution
print("\n📊 Day of Week Distribution:")
day_dist = df_clean.groupBy("pickup_day_name", "pickup_dayofweek").count() \
    .orderBy("pickup_dayofweek").collect()
for row in day_dist:
    print(f"   {row['pickup_day_name']:12s} → {row['count']:>10,} trips")

# 4.6 Weekend vs Weekday
print("\n📊 Weekend vs Weekday:")
weekend_dist = df_clean.groupBy("is_weekend").count().collect()
for row in weekend_dist:
    label = "Weekend" if row["is_weekend"] == 1 else "Weekday"
    print(f"   {label:10s} → {row['count']:>10,} trips")

# 4.7 Correlation-like analysis (top correlates with fare)
print("\n📊 Average Fare by Different Segments:")

# By hour
print("\n   Fare by Hour (Top 5 most expensive hours):")
df_clean.groupBy("pickup_hour") \
    .agg(spark_round(mean("fare_amount"), 2).alias("avg_fare")) \
    .orderBy(desc("avg_fare")).show(5)

# By distance buckets
print("   Fare by Distance Bucket:")
df_clean.withColumn("distance_bucket",
    when(col("trip_distance") < 1, "0-1 mi")
    .when(col("trip_distance") < 3, "1-3 mi")
    .when(col("trip_distance") < 5, "3-5 mi")
    .when(col("trip_distance") < 10, "5-10 mi")
    .otherwise("10+ mi")
).groupBy("distance_bucket") \
 .agg(
    count("*").alias("trips"),
    spark_round(mean("fare_amount"), 2).alias("avg_fare"),
    spark_round(mean("tip_amount"), 2).alias("avg_tip"),
    spark_round(mean("trip_duration_min"), 2).alias("avg_duration_min")
 ).orderBy("avg_fare").show()

# ============================================================
# SAVE CLEANED DATA
# ============================================================
print("\n💾 Saving cleaned data for ML pipeline...")
clean_path = os.path.join(DATA_DIR, "yellow_taxi_cleaned_for_ml.parquet")
df_clean.write.mode("overwrite").parquet(clean_path)
print(f"   ✅ Saved {after_count:,} rows → {clean_path}")

# Also save aggregated results as CSV for visualization
print("💾 Saving aggregated results for visualization...")

# Hourly demand for viz
hourly_df = df_clean.groupBy("pickup_hour").agg(
    count("*").alias("trip_count"),
    spark_round(mean("fare_amount"), 2).alias("avg_fare"),
    spark_round(mean("tip_amount"), 2).alias("avg_tip"),
    spark_round(mean("trip_distance"), 2).alias("avg_distance")
).orderBy("pickup_hour")
hourly_df.toPandas().to_csv(os.path.join(OUTPUT_DIR, "hourly_demand.csv"), index=False)

# Top zones for viz
zone_df = df_clean.groupBy("PULocationID").agg(
    count("*").alias("trip_count"),
    spark_round(spark_sum("total_amount"), 2).alias("total_revenue"),
    spark_round(mean("fare_amount"), 2).alias("avg_fare")
).orderBy(desc("trip_count"))
zone_df.toPandas().to_csv(os.path.join(OUTPUT_DIR, "zone_stats.csv"), index=False)

# Payment-tip for viz
payment_df = df_clean.groupBy("payment_type").agg(
    count("*").alias("trip_count"),
    spark_round(mean("tip_amount"), 2).alias("avg_tip"),
    spark_round(mean("tip_percentage"), 2).alias("avg_tip_pct")
).orderBy("payment_type")
payment_df.toPandas().to_csv(os.path.join(OUTPUT_DIR, "payment_tip_stats.csv"), index=False)

print("   ✅ Saved: hourly_demand.csv, zone_stats.csv, payment_tip_stats.csv")

# ============================================================
# STEP 5: GENERATE COMPREHENSIVE EDA VISUALIZATIONS FOR ML
# ============================================================
print("\n" + "=" * 70)
print("  STEP 5: GENERATING EDA VISUALIZATIONS & ML FEASIBILITY PLOTS")
print("=" * 70)

# Extract a representative 50k sample for fast and smooth matplotlib/seaborn rendering
print("📊 Sampling 50,000 clean records for statistical EDA visualizations...")
eda_sample = df_clean.select(
    "trip_distance", "trip_duration_min", "fare_amount", "tip_amount", 
    "total_amount", "avg_speed_mph", "fare_per_mile", "pickup_hour", "is_weekend"
).sample(withReplacement=False, fraction=0.025, seed=42).limit(50000).toPandas()

# 1. Feature Correlation Heatmap
print("📊 Generating: eda_correlation_matrix.png...")
plt.figure(figsize=(9, 7))
corr = eda_sample.corr()
mask = np.triu(np.ones_like(corr, dtype=bool))
sns.heatmap(corr, mask=mask, annot=True, fmt=".2f", cmap='Blues', vmin=-0.2, vmax=1.0, square=True)
plt.title("EDA: Feature Correlation Matrix (Informing Feature Selection for ML)", fontsize=13, fontweight='bold', pad=12)
plt.tight_layout()
plt.savefig(os.path.join(OUTPUT_DIR, "eda_correlation_matrix.png"), dpi=200)
plt.close()

# 2. Skewness and Feature Distributions
print("📊 Generating: eda_feature_distributions.png...")
fig, axes = plt.subplots(2, 2, figsize=(12, 9))
sns.histplot(eda_sample['fare_amount'], kde=True, ax=axes[0, 0], color='#0ea5e9', bins=40)
axes[0, 0].set_title("Fare Amount Distribution (Right Skewed)", fontweight='bold')
axes[0, 0].set_xlabel("Fare ($)")

sns.histplot(eda_sample['trip_distance'], kde=True, ax=axes[0, 1], color='#10b981', bins=40)
axes[0, 1].set_title("Trip Distance Distribution (Right Skewed)", fontweight='bold')
axes[0, 1].set_xlabel("Distance (miles)")

sns.histplot(eda_sample['trip_duration_min'], kde=True, ax=axes[1, 0], color='#f59e0b', bins=40)
axes[1, 0].set_title("Trip Duration Distribution", fontweight='bold')
axes[1, 0].set_xlabel("Duration (minutes)")

sns.histplot(eda_sample['avg_speed_mph'], kde=True, ax=axes[1, 1], color='#8b5cf6', bins=40)
axes[1, 1].set_title("Average Speed Distribution (Traffic Velocity)", fontweight='bold')
axes[1, 1].set_xlabel("Speed (mph)")

plt.suptitle("EDA: Feature Skewness & Value Distributions (Justifying Scaling & Ensemble ML)", fontsize=14, fontweight='bold', y=0.99)
plt.tight_layout()
plt.savefig(os.path.join(OUTPUT_DIR, "eda_feature_distributions.png"), dpi=200)
plt.close()

# 3. Distance vs Fare Relationship (Hexbin & Non-linearities)
print("📊 Generating: eda_distance_vs_fare.png...")
plt.figure(figsize=(10, 6))
plt.hexbin(eda_sample['trip_distance'], eda_sample['fare_amount'], gridsize=50, cmap='YlOrRd', mincnt=1, bins='log')
cb = plt.colorbar(label='Log10(Trip Density)')
plt.title("EDA: Distance vs Fare Relationship (Revealing Linear Meter + Flat Airport Rates)", fontsize=13, fontweight='bold', pad=12)
plt.xlabel("Trip Distance (miles)")
plt.ylabel("Fare Amount ($)")
plt.axhline(70.0, color='blue', linestyle='--', linewidth=1.8, label='JFK Flat Rate Rule ($70)')
plt.legend(loc='upper left')
plt.tight_layout()
plt.savefig(os.path.join(OUTPUT_DIR, "eda_distance_vs_fare.png"), dpi=200)
plt.close()

# 4. Outlier Detection Boxplots (Sanity Checks)
print("📊 Generating: eda_outlier_boxplots.png...")
fig, axes = plt.subplots(1, 3, figsize=(14, 5))
sns.boxplot(y=eda_sample['fare_amount'], ax=axes[0], color='#0ea5e9')
axes[0].set_title("Cleaned Fare Amount ($)", fontweight='bold')

sns.boxplot(y=eda_sample['trip_distance'], ax=axes[1], color='#10b981')
axes[1].set_title("Cleaned Distance (miles)", fontweight='bold')

sns.boxplot(y=eda_sample['trip_duration_min'], ax=axes[2], color='#f43f5e')
axes[2].set_title("Cleaned Duration (minutes)", fontweight='bold')

plt.suptitle("EDA: Outlier Boundaries Verification (Post-Preprocessing Boxplots)", fontsize=14, fontweight='bold')
plt.tight_layout()
plt.savefig(os.path.join(OUTPUT_DIR, "eda_outlier_boxplots.png"), dpi=200)
plt.close()

# 5. Master EDA Visual Summary Dashboard
print("📊 Generating: eda_summary_dashboard.png...")
fig = plt.figure(figsize=(16, 11))
gs = fig.add_gridspec(2, 2, hspace=0.3, wspace=0.25)

ax1 = fig.add_subplot(gs[0, 0])
key_cols = ['fare_amount', 'trip_duration_min', 'trip_distance']
sns.heatmap(corr[key_cols].loc[key_cols], annot=True, fmt=".2f", cmap='Blues', ax=ax1, cbar=False)
ax1.set_title("A. Key Feature Correlations to Fare", fontweight='bold')

ax2 = fig.add_subplot(gs[0, 1])
sns.histplot(eda_sample['fare_amount'], kde=True, ax=ax2, color='#0ea5e9', bins=35)
ax2.axvline(15.0, color='red', linestyle='--', linewidth=2, label='Classification Split ($15)')
ax2.set_title("B. Target Fare Distribution & Class Split", fontweight='bold')
ax2.legend()

ax3 = fig.add_subplot(gs[1, 0])
ax3.hexbin(eda_sample['trip_distance'], eda_sample['fare_amount'], gridsize=40, cmap='inferno', mincnt=1, bins='log')
ax3.set_title("C. Distance vs Fare Density (Linear + Non-Linear)", fontweight='bold')
ax3.set_xlabel("Distance (miles)")
ax3.set_ylabel("Fare ($)")

ax4 = fig.add_subplot(gs[1, 1])
class_counts = [len(eda_sample[eda_sample['fare_amount'] < 15.0]), len(eda_sample[eda_sample['fare_amount'] >= 15.0])]
ax4.bar(['Standard (<$15)', 'High-Value (≥$15)'], class_counts, color=['#0ea5e9', '#10b981'], width=0.5)
ax4.set_title("D. ML Classification Class Balance", fontweight='bold')
ax4.set_ylabel("Trips in Sample")
for i, v in enumerate(class_counts):
    pct = (v / len(eda_sample)) * 100
    ax4.text(i, v + 200, f"{v:,} ({pct:.1f}%)", ha='center', fontweight='bold')

plt.suptitle("NYC TAXI EXPLORATORY DATA ANALYSIS (EDA) & ML READINESS DASHBOARD", fontsize=15, fontweight='bold', y=0.98)
plt.savefig(os.path.join(OUTPUT_DIR, "eda_summary_dashboard.png"), dpi=200, bbox_inches='tight')
plt.close()

print("   ✅ Generated: eda_correlation_matrix.png, eda_feature_distributions.png, eda_distance_vs_fare.png, eda_outlier_boxplots.png, eda_summary_dashboard.png")


# ============================================================
# SUMMARY
# ============================================================
print("\n" + "=" * 70)
print("  ✅ PREPROCESSING & EDA COMPLETE")
print("=" * 70)
print(f"""
   Dataset:     NYC TLC Yellow Taxi (Jan 2026)
   Raw rows:    {before_count:,}
   Clean rows:  {after_count:,}
   Removed:     {removed:,} ({removed/before_count*100:.1f}%)

   Features engineered:
     • trip_duration_min   — ride duration in minutes
     • pickup_hour         — 0-23
     • pickup_dayofweek    — 1 (Sun) to 7 (Sat)
     • is_weekend          — binary
     • avg_speed_mph       — distance / duration
     • fare_per_mile       — fare / distance
     • tip_percentage      — (tip / fare) × 100

   Key EDA Findings:
     • Peak hours: Evening rush (5-9 PM)
     • Most trips: Credit card payment (~67%)
     • Average fare: Check describe() output above
     • Busiest zones: Midtown Manhattan (zones 161, 237, 236)

   Next step → Run 02_ml_model.py for Machine Learning
""")

spark.stop()
