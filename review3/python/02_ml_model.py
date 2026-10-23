"""
02_ml_model.py
BDA Review 3 — Machine Learning Pipeline (4 Models)
NYC Yellow Taxi Trip Data (Jan 2026)

================================================================
HOW SCALA RDD ANALYTICS FEED INTO THE ML MODELS:
================================================================

STUDENT 1 [Q1 → CLASSIFICATION]:
  Analytics: Hourly trip counts + Top pickup zones (PULocationID)
  → Feature: pickup_hour (when is peak demand?)
  → Feature: is_hotspot_zone (is the pickup in a top-5 busiest zone?)
  → Helps classify: Is this a HIGH-VALUE ride?

STUDENT 2 [Q2 → CLASSIFICATION]:
  Analytics: Payment type vs avg tipping behavior
  → Feature: payment_type (Credit card = high tip = high value)
  → Feature: tip_ratio (tip_amount / fare_amount from Q2 averages)
  → Helps classify: Is this a HIGH-VALUE ride?

STUDENT 3 [Q3 → REGRESSION]:
  Analytics: Avg fare-per-mile by time period (Morning/Afternoon/Evening/Night)
  → Feature: fare_per_mile (derived from Q3's rate analysis)
  → Feature: time_period_code (0=Night,1=Morning,2=Afternoon,3=Evening)
  → Helps predict: What will the total fare_amount be?

STUDENT 4 [Q4 → REGRESSION]:
  Analytics: Hourly avg speed (mph) + congestion surcharge revenue
  → Feature: avg_speed_mph (from Q4's distance/duration per hour)
  → Feature: congestion_surcharge (direct surcharge signal from Q4)
  → Helps predict: What will the total fare_amount be?

MODELS:
  Regression (S3 + S4) → Linear Regression + Random Forest Regressor
  Classification (S1 + S2) → Logistic Regression + Random Forest Classifier

HOW TO RUN:
    python3 python/02_ml_model.py
    OR: spark-submit --driver-memory 4g python/02_ml_model.py
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, when, hour, dayofweek, unix_timestamp
)
from pyspark.ml.feature import VectorAssembler, StandardScaler
from pyspark.ml.regression import LinearRegression, RandomForestRegressor
from pyspark.ml.classification import LogisticRegression, RandomForestClassifier
from pyspark.ml.evaluation import (
    RegressionEvaluator,
    BinaryClassificationEvaluator,
    MulticlassClassificationEvaluator
)
from pyspark.ml import Pipeline
import os
import json
import time

# ================================================================
# INITIALIZE SPARK
# ================================================================
spark = SparkSession.builder \
    .appName("NYC Taxi ML Pipeline — Scala RDD Analytics Driven") \
    .config("spark.sql.shuffle.partitions", "8") \
    .config("spark.driver.memory", "4g") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA_DIR = os.path.join(BASE_DIR, "data")
OUTPUT_DIR = os.path.join(BASE_DIR, "output")
os.makedirs(OUTPUT_DIR, exist_ok=True)

print("=" * 80)
print("  BDA REVIEW 3 — SCALA RDD ANALYTICS-DRIVEN ML PIPELINE (4 MODELS)")
print("  Input: Analytics from Q1(S1), Q2(S2), Q3(S3), Q4(S4) Scala RDD")
print("  Models: 2 Regression (S3+S4) + 2 Classification (S1+S2)")
print("=" * 80)

# ================================================================
# STEP 1: LOAD RAW DATA (same parquet used in NycTaxiRDD.scala)
# ================================================================
candidate_paths = [
    os.path.join(DATA_DIR, "yellow_tripdata_2026-01.parquet"),
    os.path.join(DATA_DIR, "yellow_taxi_cleaned_for_ml.parquet"),
    "/app/data/yellow_tripdata_2026-01.parquet"
]
data_path = next((p for p in candidate_paths if os.path.exists(p)), candidate_paths[0])
print(f"\n📦 Loading dataset from: {data_path}")
raw = spark.read.parquet(data_path)
print(f"   Raw rows loaded: {raw.count():,}")

# ================================================================
# STEP 2: REPLICATE SCALA RDD CLEANING (T1 + T2 from NycTaxiRDD.scala)
# ================================================================
print("\n🔄 Replicating Scala RDD Cleaning (T1: map, T2: filter from NycTaxiRDD.scala)...")

df = raw \
    .withColumn("trip_duration_min",
        (unix_timestamp("tpep_dropoff_datetime") - unix_timestamp("tpep_pickup_datetime")) / 60.0) \
    .withColumn("pickup_hour", hour("tpep_pickup_datetime")) \
    .withColumn("pickup_dayofweek", dayofweek("tpep_pickup_datetime")) \
    .withColumn("is_weekend",
        when(dayofweek("tpep_pickup_datetime").isin(1, 7), 1).otherwise(0)) \
    .withColumn("avg_speed_mph",
        when(col("trip_duration_min") > 0,
             col("trip_distance") / (col("trip_duration_min") / 60.0)
        ).otherwise(0.0)) \
    .filter(
        (col("fare_amount") > 0) & (col("fare_amount") < 500) &
        (col("trip_distance") > 0) & (col("trip_distance") < 100) &
        (col("passenger_count") > 0) &
        (col("trip_duration_min") > 0) & (col("trip_duration_min") < 180) &
        (col("avg_speed_mph") < 80) &
        (col("pickup_hour") >= 0) &
        (col("total_amount") > 0) & (col("total_amount") < 500)
    )

total_clean = df.count()
print(f"   ✅ Clean records after T1+T2 filter: {total_clean:,}")

# ================================================================
# STEP 3: FEATURE ENGINEERING FROM SCALA RDD ANALYTICS (Q1–Q4)
# ================================================================

# ── Q1 (Student 1) → CLASSIFICATION feature: is_hotspot_zone ──────────────
# Q1 Scala: zoneCountRDD.sortBy(-_._2).take(5) — top 5 busiest pickup zones
print("\n📊 [Q1 - Student 1] Computing Top-5 Hotspot Zones (replicating zoneCountRDD)...")
zone_counts = df.groupBy("PULocationID").count().orderBy(col("count").desc())
top5_zones = [row["PULocationID"] for row in zone_counts.limit(5).collect()]
print(f"   Top-5 Busiest Pickup Zones from Q1: {top5_zones}")
df = df.withColumn("is_hotspot_zone",
    when(col("PULocationID").isin(top5_zones), 1).otherwise(0))

# ── Q2 (Student 2) → CLASSIFICATION features: tip_ratio, is_credit_card ───
# Q2 Scala: avgTipByPayment.collect() — credit card users tip more
print("\n📊 [Q2 - Student 2] Computing Tip Ratio (replicating tipByPaymentRDD)...")
df = df \
    .withColumn("tip_ratio",
        when(col("fare_amount") > 0,
             col("tip_amount") / col("fare_amount")).otherwise(0.0)) \
    .withColumn("is_credit_card",
        when(col("payment_type") == 1, 1).otherwise(0))
print("   tip_ratio = tip/fare  |  is_credit_card = (payment_type == 1)")

# ── Q3 (Student 3) → REGRESSION features: fare_per_mile + time_period_code ─
# Q3 Scala: flatMap() → timePeriod + farePerMile, then avgFarePerMile.collect()
print("\n📊 [Q3 - Student 3] Computing Fare-per-Mile & Time Period (replicating timePeriodAndZoneRDD)...")
df = df \
    .withColumn("fare_per_mile",
        when(col("trip_distance") > 0.1,
             col("fare_amount") / col("trip_distance")).otherwise(0.0)) \
    .withColumn("time_period_code",
        when((col("pickup_hour") >= 6)  & (col("pickup_hour") < 12), 1)
        .when((col("pickup_hour") >= 12) & (col("pickup_hour") < 17), 2)
        .when((col("pickup_hour") >= 17) & (col("pickup_hour") < 21), 3)
        .otherwise(0))
print("   fare_per_mile  |  time_period_code: 0=Night,1=Morning,2=Afternoon,3=Evening")

# ── Q4 (Student 4) → REGRESSION features: avg_speed_mph + congestion_flag ──
# Q4 Scala: hourlyTrafficMetrics.collect() — avgSpeedMph & congestion surcharges
print("\n📊 [Q4 - Student 4] Computing Congestion Flag (replicating trafficPairsRDD thresholds)...")
df = df.withColumn("congestion_flag",
    when(col("avg_speed_mph") < 10.0, 1).otherwise(0))
# Handle congestion_surcharge column (may already exist in parquet)
if "congestion_surcharge" not in raw.columns:
    df = df.withColumn("congestion_surcharge", col("trip_distance") * 0.0)
else:
    df = df.withColumn("congestion_surcharge",
        when(col("congestion_surcharge").isNull(), 0.0).otherwise(col("congestion_surcharge")))
print("   congestion_flag: 1 if speed < 10mph (Heavy Congestion from Q4 threshold)")

# ================================================================
# STEP 4: DEFINE TARGETS & FEATURE SETS
# ================================================================
THRESHOLD_FARE = 15.0
df_ml = df \
    .withColumn("fare_target", col("fare_amount").cast("double")) \
    .withColumn("high_fare_label",
        when(col("fare_amount") >= THRESHOLD_FARE, 1.0).otherwise(0.0))

# REGRESSION features: S3 analytics + S4 analytics
REGRESSION_FEATURES = [
    "trip_distance",        # Base
    "trip_duration_min",    # Derived in Scala T1 (map)
    "fare_per_mile",        # [Q3-S3] Fare efficiency rate from flatMap analysis
    "time_period_code",     # [Q3-S3] Morning/Afternoon/Evening/Night encoding
    "avg_speed_mph",        # [Q4-S4] Velocity metric from traffic dynamics
    "congestion_surcharge", # [Q4-S4] Direct surcharge from Q4 hourly analysis
    "congestion_flag",      # [Q4-S4] Heavy congestion binary (speed < 10mph)
    "pickup_hour",          # Temporal context
]

# CLASSIFICATION features: S1 analytics + S2 analytics
CLASSIFICATION_FEATURES = [
    "pickup_hour",          # [Q1-S1] Peak demand hour from hourlyDemand.collect()
    "is_hotspot_zone",      # [Q1-S1] Top-5 busiest zones from zoneCountRDD.take(5)
    "is_weekend",           # Temporal signal
    "payment_type",         # [Q2-S2] Payment method driving tipping behavior
    "is_credit_card",       # [Q2-S2] Credit card = high recorded tip
    "tip_ratio",            # [Q2-S2] Avg tip/fare from tipByPaymentRDD.mapValues()
    "passenger_count",      # Ride occupancy
    "PULocationID",         # Pickup zone (spatial)
]

print("\n📋 FEATURE MAPPING (Scala RDD Analytics → ML Features):")
print("   REGRESSION (S3+S4):     " + ", ".join(REGRESSION_FEATURES))
print("   CLASSIFICATION (S1+S2): " + ", ".join(CLASSIFICATION_FEATURES))

# ================================================================
# STEP 5: PREPARE & SPLIT DATA
# ================================================================
all_feat = list(set(REGRESSION_FEATURES + CLASSIFICATION_FEATURES))
df_prepared = df_ml.select(
    [col(c).cast("double") for c in all_feat] + [col("fare_target"), col("high_fare_label")]
).dropna()

sample_size = min(total_clean, 500_000)
df_sampled = df_prepared.sample(False, float(sample_size) / total_clean, seed=42) \
    if total_clean > sample_size else df_prepared

train_df, test_df = df_sampled.randomSplit([0.8, 0.2], seed=42)
train_count = train_df.count()
test_count  = test_df.count()
print(f"\n📊 Training set: {train_count:,} (80%)  |  Test set: {test_count:,} (20%)")

# ================================================================
# PART 1: REGRESSION MODELS (Student 3 + Student 4)
# ================================================================
print("\n" + "=" * 80)
print("  PART 1: REGRESSION — Upfront Fare Estimation")
print("  Features: Q3 (fare_per_mile, time_period_code) + Q4 (avg_speed_mph, congestion)")
print("=" * 80)

reg_assembler = VectorAssembler(inputCols=REGRESSION_FEATURES, outputCol="raw_reg_feat", handleInvalid="skip")
reg_scaler    = StandardScaler(inputCol="raw_reg_feat", outputCol="reg_features", withStd=True, withMean=False)

print("\n📈 [Model 1/4] Linear Regression  (Baseline — S3+S4 features)")
t0 = time.time()
lr_reg = LinearRegression(featuresCol="reg_features", labelCol="fare_target",
                          maxIter=20, regParam=0.1, elasticNetParam=0.5)
lr_reg_model = Pipeline(stages=[reg_assembler, reg_scaler, lr_reg]).fit(train_df)
lr_reg_preds = lr_reg_model.transform(test_df)
t_lr_reg = time.time() - t0
print(f"   ✅ Trained in {t_lr_reg:.2f}s")

print("\n🌲 [Model 2/4] Random Forest Regressor  (S3+S4 features)")
t0 = time.time()
rf_reg = RandomForestRegressor(featuresCol="reg_features", labelCol="fare_target",
                                numTrees=40, maxDepth=8, seed=42)
rf_reg_model = Pipeline(stages=[reg_assembler, reg_scaler, rf_reg]).fit(train_df)
rf_reg_preds = rf_reg_model.transform(test_df)
t_rf_reg = time.time() - t0
print(f"   ✅ Trained in {t_rf_reg:.2f}s")

eval_rmse = RegressionEvaluator(labelCol="fare_target", predictionCol="prediction", metricName="rmse")
eval_r2   = RegressionEvaluator(labelCol="fare_target", predictionCol="prediction", metricName="r2")
eval_mae  = RegressionEvaluator(labelCol="fare_target", predictionCol="prediction", metricName="mae")

reg_results = {
    "linear_regression": {
        "rmse": float(eval_rmse.evaluate(lr_reg_preds)),
        "r2":   float(eval_r2.evaluate(lr_reg_preds)),
        "mae":  float(eval_mae.evaluate(lr_reg_preds)),
        "training_time_sec": round(t_lr_reg, 2)
    },
    "random_forest_regressor": {
        "rmse": float(eval_rmse.evaluate(rf_reg_preds)),
        "r2":   float(eval_r2.evaluate(rf_reg_preds)),
        "mae":  float(eval_mae.evaluate(rf_reg_preds)),
        "training_time_sec": round(t_rf_reg, 2)
    }
}

print("\n📊 REGRESSION RESULTS:")
print(f"   {'Metric':<16} | {'Linear Regression':<22} | {'Random Forest Regressor'}")
print(f"   {'R² Score':<16} | {reg_results['linear_regression']['r2']:<22.4f} | {reg_results['random_forest_regressor']['r2']:.4f}")
print(f"   {'RMSE ($)':<16} | ${reg_results['linear_regression']['rmse']:<21.2f} | ${reg_results['random_forest_regressor']['rmse']:.2f}")
print(f"   {'MAE ($)':<16} | ${reg_results['linear_regression']['mae']:<21.2f} | ${reg_results['random_forest_regressor']['mae']:.2f}")

# ================================================================
# PART 2: CLASSIFICATION MODELS (Student 1 + Student 2)
# ================================================================
print("\n" + "=" * 80)
print("  PART 2: CLASSIFICATION — High-Value Ride Dispatch")
print("  Features: Q1 (pickup_hour, is_hotspot_zone) + Q2 (payment_type, tip_ratio)")
print(f"  Target: is_high_fare (1 if fare >= ${THRESHOLD_FARE})")
print("=" * 80)

clf_assembler = VectorAssembler(inputCols=CLASSIFICATION_FEATURES, outputCol="raw_clf_feat", handleInvalid="skip")
clf_scaler    = StandardScaler(inputCol="raw_clf_feat", outputCol="clf_features", withStd=True, withMean=False)

print("\n🎯 [Model 3/4] Logistic Regression  (S1+S2 features)")
t0 = time.time()
lr_clf = LogisticRegression(featuresCol="clf_features", labelCol="high_fare_label",
                             maxIter=15, regParam=0.05, elasticNetParam=0.8)
lr_clf_model = Pipeline(stages=[clf_assembler, clf_scaler, lr_clf]).fit(train_df)
lr_clf_preds = lr_clf_model.transform(test_df)
t_lr_clf = time.time() - t0
print(f"   ✅ Trained in {t_lr_clf:.2f}s")

print("\n🌲 [Model 4/4] Random Forest Classifier  (S1+S2 features)")
t0 = time.time()
rf_clf = RandomForestClassifier(featuresCol="clf_features", labelCol="high_fare_label",
                                 numTrees=40, maxDepth=8, seed=42)
rf_clf_model = Pipeline(stages=[clf_assembler, clf_scaler, rf_clf]).fit(train_df)
rf_clf_preds = rf_clf_model.transform(test_df)
t_rf_clf = time.time() - t0
print(f"   ✅ Trained in {t_rf_clf:.2f}s")

def evaluate_classifier(predictions):
    bin_eval   = BinaryClassificationEvaluator(labelCol="high_fare_label",
                     rawPredictionCol="rawPrediction", metricName="areaUnderROC")
    multi_eval = MulticlassClassificationEvaluator(labelCol="high_fare_label",
                     predictionCol="prediction")
    auc_roc   = bin_eval.evaluate(predictions)
    multi_eval.setMetricName("accuracy");         accuracy  = multi_eval.evaluate(predictions)
    multi_eval.setMetricName("weightedPrecision"); precision = multi_eval.evaluate(predictions)
    multi_eval.setMetricName("weightedRecall");    recall    = multi_eval.evaluate(predictions)
    multi_eval.setMetricName("f1");                f1        = multi_eval.evaluate(predictions)
    cm = predictions.groupBy("high_fare_label", "prediction").count().collect()
    tp = sum(r["count"] for r in cm if r["high_fare_label"] == 1.0 and r["prediction"] == 1.0)
    tn = sum(r["count"] for r in cm if r["high_fare_label"] == 0.0 and r["prediction"] == 0.0)
    fp = sum(r["count"] for r in cm if r["high_fare_label"] == 0.0 and r["prediction"] == 1.0)
    fn = sum(r["count"] for r in cm if r["high_fare_label"] == 1.0 and r["prediction"] == 0.0)
    return {"accuracy": accuracy, "precision": precision, "recall": recall,
            "f1": f1, "auc_roc": auc_roc, "tp": tp, "tn": tn, "fp": fp, "fn": fn}

clf_lr_eval = evaluate_classifier(lr_clf_preds)
clf_rf_eval = evaluate_classifier(rf_clf_preds)
clf_lr_eval["training_time_sec"] = round(t_lr_clf, 2)
clf_rf_eval["training_time_sec"] = round(t_rf_clf, 2)

print("\n📊 CLASSIFICATION RESULTS:")
print(f"   {'Metric':<20} | {'Logistic Regression':<20} | {'Random Forest Clf'}")
print(f"   {'Accuracy':<20} | {clf_lr_eval['accuracy']*100:<19.2f}% | {clf_rf_eval['accuracy']*100:.2f}%")
print(f"   {'F1-Score':<20} | {clf_lr_eval['f1']:<20.4f} | {clf_rf_eval['f1']:.4f}")
print(f"   {'AUC-ROC':<20} | {clf_lr_eval['auc_roc']:<20.4f} | {clf_rf_eval['auc_roc']:.4f}")

# ================================================================
# FEATURE IMPORTANCES
# ================================================================
reg_importances = dict(zip(REGRESSION_FEATURES,
    rf_reg_model.stages[-1].featureImportances.toArray()))
clf_importances = dict(zip(CLASSIFICATION_FEATURES,
    rf_clf_model.stages[-1].featureImportances.toArray()))

print("\n📊 FEATURE IMPORTANCES (Which Scala analytics matter most?):")
print("\n   Regression (S3+S4 features):")
for feat, imp in sorted(reg_importances.items(), key=lambda x: -x[1]):
    src = "[Q3-S3]" if feat in ("fare_per_mile","time_period_code") \
          else "[Q4-S4]" if feat in ("avg_speed_mph","congestion_surcharge","congestion_flag") \
          else "[raw]"
    print(f"     {feat:<25} {src:<10}  {imp:.4f}  {'█' * int(imp * 60)}")

print("\n   Classification (S1+S2 features):")
for feat, imp in sorted(clf_importances.items(), key=lambda x: -x[1]):
    src = "[Q1-S1]" if feat in ("pickup_hour","is_hotspot_zone") \
          else "[Q2-S2]" if feat in ("payment_type","tip_ratio","is_credit_card") \
          else "[raw]"
    print(f"     {feat:<25} {src:<10}  {imp:.4f}  {'█' * int(imp * 60)}")

# ================================================================
# SUMMARY TABLE
# ================================================================
print("\n" + "=" * 80)
print("  SCALA RDD ANALYTICS → ML FEATURE BRIDGE (SUMMARY)")
print("=" * 80)
print("""
   Student 1 [Q1 → CLASSIFICATION]
     RDD: hourlyDemand.collect(), zoneCountRDD.sortBy(-_._2).take(5)
     Features: pickup_hour, is_hotspot_zone
     Why: Peak hours + hotspot zones → Higher probability of High-Value ride

   Student 2 [Q2 → CLASSIFICATION]
     RDD: tipByPaymentRDD.reduceByKey(), avgTipByPayment.mapValues()
     Features: payment_type, tip_ratio, is_credit_card
     Why: Credit card + high tip_ratio → classify as High-Value ride (label=1)

   Student 3 [Q3 → REGRESSION]
     RDD: timePeriodAndZoneRDD via flatMap(), avgFarePerMile.collect()
     Features: fare_per_mile, time_period_code
     Why: Evening fare rate × distance → accurate continuous fare prediction

   Student 4 [Q4 → REGRESSION]
     RDD: trafficPairsRDD.reduceByKey(), hourlyTrafficMetrics.mapValues()
     Features: avg_speed_mph, congestion_surcharge, congestion_flag
     Why: Slow speed + surcharge → fare inflated → better regression accuracy
""")

# ================================================================
# SAVE RESULTS
# ================================================================
all_results = {
    "threshold_high_fare": THRESHOLD_FARE,
    "regression_features_S3_S4": REGRESSION_FEATURES,
    "classification_features_S1_S2": CLASSIFICATION_FEATURES,
    "regression": reg_results,
    "classification": {
        "logistic_regression": clf_lr_eval,
        "random_forest_classifier": clf_rf_eval
    },
    "feature_importances": {
        "regression": reg_importances,
        "classification": clf_importances
    }
}

with open(os.path.join(OUTPUT_DIR, "ml_results_4models.json"), "w") as f:
    json.dump(all_results, f, indent=2)

lr_reg_preds.select("fare_target","prediction").limit(5000).toPandas().to_csv(
    os.path.join(OUTPUT_DIR, "regression_lr_sample.csv"), index=False)
rf_reg_preds.select("fare_target","prediction").limit(5000).toPandas().to_csv(
    os.path.join(OUTPUT_DIR, "regression_rf_sample.csv"), index=False)
lr_clf_preds.select("high_fare_label","prediction").limit(5000).toPandas().to_csv(
    os.path.join(OUTPUT_DIR, "classification_lr_sample.csv"), index=False)
rf_clf_preds.select("high_fare_label","prediction").limit(5000).toPandas().to_csv(
    os.path.join(OUTPUT_DIR, "classification_rf_sample.csv"), index=False)

print("💾 Results saved to output/ml_results_4models.json")
print("""
================================================================================
✅ 4-MODEL ML PIPELINE COMPLETE — DRIVEN BY SCALA RDD ANALYTICS
   1. Linear Regression       (S3+S4 features) → RMSE, R², MAE
   2. Random Forest Regressor (S3+S4 features) → RMSE, R², MAE
   3. Logistic Regression     (S1+S2 features) → Accuracy, F1, AUC-ROC
   4. Random Forest Classifier(S1+S2 features) → Accuracy, F1, AUC-ROC
================================================================================
""")

spark.stop()
