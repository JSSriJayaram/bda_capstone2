"""
02_ml_model.py
BDA Review 3 — Machine Learning Pipeline (4 Models)
NYC Yellow Taxi Trip Data (Jan 2026)

============================================================
THE 4 MODELS & BUSINESS PROBLEMS:
============================================================

1. REGRESSION (Continuous Value Prediction):
   Business Problem: Dynamic Upfront Fare Estimation & Revenue Forecasting
   - Problem: Passengers demand transparent, guaranteed upfront pricing before booking.
     Operators must quote accurate prices without undercharging drivers or overcharging riders.
   - Target: fare_amount (Continuous USD)
   - Model 1: Linear Regression (with ElasticNet regularization) — Baseline linear model
   - Model 2: Random Forest Regressor — Ensemble non-linear model capturing flat-rate
     airport trips ($70 JFK), step-function surcharges, and congestion bottlenecks.

2. CLASSIFICATION (Binary Categorization):
   Business Problem: High-Value Ride Dispatch & Driver Retention Optimization
   - Problem: Cruising for fares wastes 35% of driver fuel and time. Dispatch systems
     need to classify trip requests as High-Value (Fare >= $15) to match with drivers strategically.
   - Target: is_high_fare (Binary 0 or 1)
   - Model 3: Logistic Regression Classifier — Probabilistic baseline with sigmoid activation
   - Model 4: Random Forest Classifier — Multi-tree ensemble capturing spatial-temporal boundaries.

HOW TO RUN:
    python3 python/02_ml_model.py
    OR: spark-submit --driver-memory 4g python/02_ml_model.py
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, when, count, mean, round as spark_round
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

# ============================================================
# INITIALIZE SPARK
# ============================================================
spark = SparkSession.builder \
    .appName("NYC Taxi 4-Model ML Pipeline - BDA Review 3") \
    .config("spark.sql.shuffle.partitions", "8") \
    .config("spark.driver.memory", "4g") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

DATA_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "data")
OUTPUT_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "output")
os.makedirs(OUTPUT_DIR, exist_ok=True)

# Candidate paths for cleaned parquet
candidate_clean = [
    os.path.join(DATA_DIR, "yellow_taxi_cleaned_for_ml.parquet"),
    os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))), "review3", "data", "yellow_taxi_cleaned_for_ml.parquet"),
    "/app/data/yellow_taxi_cleaned_for_ml.parquet"
]
clean_path = next((p for p in candidate_clean if os.path.exists(p)), os.path.join(DATA_DIR, "yellow_taxi_cleaned_for_ml.parquet"))

print("=" * 80)
print("  BDA REVIEW 3 — MACHINE LEARNING PIPELINE (4 MODELS)")
print("  Models: 2 Regression + 2 Classification")
print("=" * 80)

# ============================================================
# STEP 1: LOAD CLEANED DATA
# ============================================================
if os.path.exists(clean_path):
    print(f"\n📦 Loading cleaned dataset from: {clean_path}")
    df = spark.read.parquet(clean_path)
else:
    print("\n⚠️ Cleaned data not found. Creating cleaned dataset from raw data...")
    from pyspark.sql.functions import hour, dayofweek, unix_timestamp
    candidate_raw = [
        os.path.join(DATA_DIR, "yellow_tripdata_2026-01.parquet"),
        os.path.join(os.path.dirname(DATA_DIR), "data", "raw", "yellow_tripdata_2026-01.parquet"),
        "/app/data/yellow_tripdata_2026-01.parquet",
        "/Users/s4n/Documents/clg/sem7/bda_capstone2/data/raw/yellow_tripdata_2026-01.parquet"
    ]
    raw_path = next((p for p in candidate_raw if os.path.exists(p)), candidate_raw[0])
    raw = spark.read.parquet(raw_path)
    df = raw.withColumn("trip_duration_min",
            (unix_timestamp("tpep_dropoff_datetime") - unix_timestamp("tpep_pickup_datetime")) / 60.0) \
        .withColumn("pickup_hour", hour("tpep_pickup_datetime")) \
        .withColumn("pickup_dayofweek", dayofweek("tpep_pickup_datetime")) \
        .withColumn("is_weekend", when(dayofweek("tpep_pickup_datetime").isin(1, 7), 1).otherwise(0)) \
        .withColumn("avg_speed_mph",
            when(col("trip_duration_min") > 0, col("trip_distance") / (col("trip_duration_min") / 60.0)).otherwise(0.0)) \
        .filter(
            (col("fare_amount") > 0) & (col("fare_amount") < 500) &
            (col("trip_distance") > 0) & (col("trip_distance") < 100) &
            (col("passenger_count") > 0) & (col("trip_duration_min") > 0) &
            (col("trip_duration_min") < 180) & (col("avg_speed_mph") < 80)
        )

total_rows = df.count()
print(f"   Available Clean Trips: {total_rows:,} rows")

# ============================================================
# STEP 2: PREPARE TARGET VARIABLES & FEATURES
# ============================================================
# Target 1 (Regression): fare_amount (Continuous USD)
# Target 2 (Classification): is_high_fare (Binary: 1 if fare >= $15.00, else 0)
THRESHOLD_FARE = 15.0
df_ml = df.withColumn("fare_target", col("fare_amount").cast("double")) \
          .withColumn("high_fare_label", when(col("fare_amount") >= THRESHOLD_FARE, 1.0).otherwise(0.0))

feature_cols = [
    "trip_distance",
    "trip_duration_min",
    "pickup_hour",
    "pickup_dayofweek",
    "passenger_count",
    "PULocationID",
    "avg_speed_mph",
    "is_weekend"
]

# Ensure valid data
df_prepared = df_ml.select(feature_cols + ["fare_target", "high_fare_label"]).dropna()

# Stratified sampling for responsive training (500k records provides optimal statistical significance)
sample_size = min(total_rows, 500000)
if total_rows > sample_size:
    sample_fraction = float(sample_size) / total_rows
    df_sampled = df_prepared.sample(withReplacement=False, fraction=sample_fraction, seed=42)
else:
    df_sampled = df_prepared

train_df, test_df = df_sampled.randomSplit([0.8, 0.2], seed=42)
train_count = train_df.count()
test_count = test_df.count()

print(f"\n📊 Training Set: {train_count:,} records (80%)")
print(f"📊 Testing Set:  {test_count:,} records (20%)")

# VectorAssembler & StandardScaler pipeline stages
assembler = VectorAssembler(inputCols=feature_cols, outputCol="raw_features", handleInvalid="skip")
scaler = StandardScaler(inputCol="raw_features", outputCol="features", withStd=True, withMean=False)

# ============================================================
# REGRESSION MODELS (Upfront Fare Amount Prediction)
# ============================================================
print("\n" + "=" * 80)
print("  PART 1: REGRESSION MODELS (Upfront Fare Amount Estimation)")
print("  Problem: Predict continuous fare_amount ($) for upfront ride quoting")
print("=" * 80)

# --- Model 1: Linear Regression ---
print("\n📈 [Model 1/4] Training Linear Regression...")
t0 = time.time()
lr_reg = LinearRegression(
    featuresCol="features",
    labelCol="fare_target",
    maxIter=20,
    regParam=0.1,
    elasticNetParam=0.5
)
lr_reg_pipeline = Pipeline(stages=[assembler, scaler, lr_reg])
lr_reg_model = lr_reg_pipeline.fit(train_df)
lr_reg_preds = lr_reg_model.transform(test_df)
t_lr_reg = time.time() - t0
print(f"   ✅ Linear Regression trained in {t_lr_reg:.2f}s")

# --- Model 2: Random Forest Regressor ---
print("\n🌲 [Model 2/4] Training Random Forest Regressor...")
t0 = time.time()
rf_reg = RandomForestRegressor(
    featuresCol="features",
    labelCol="fare_target",
    numTrees=40,
    maxDepth=8,
    seed=42
)
rf_reg_pipeline = Pipeline(stages=[assembler, scaler, rf_reg])
rf_reg_model = rf_reg_pipeline.fit(train_df)
rf_reg_preds = rf_reg_model.transform(test_df)
t_rf_reg = time.time() - t0
print(f"   ✅ Random Forest Regressor trained in {t_rf_reg:.2f}s")

# Evaluate Regression Models
eval_rmse = RegressionEvaluator(labelCol="fare_target", predictionCol="prediction", metricName="rmse")
eval_r2 = RegressionEvaluator(labelCol="fare_target", predictionCol="prediction", metricName="r2")
eval_mae = RegressionEvaluator(labelCol="fare_target", predictionCol="prediction", metricName="mae")

reg_results = {
    "linear_regression": {
        "rmse": float(eval_rmse.evaluate(lr_reg_preds)),
        "r2": float(eval_r2.evaluate(lr_reg_preds)),
        "mae": float(eval_mae.evaluate(lr_reg_preds)),
        "training_time_sec": round(t_lr_reg, 2)
    },
    "random_forest_regressor": {
        "rmse": float(eval_rmse.evaluate(rf_reg_preds)),
        "r2": float(eval_r2.evaluate(rf_reg_preds)),
        "mae": float(eval_mae.evaluate(rf_reg_preds)),
        "training_time_sec": round(t_rf_reg, 2)
    }
}

print("\n📊 REGRESSION EVALUATION RESULTS:")
print("   " + "─" * 60)
print(f"   {'Metric':<15} | {'Linear Regression':<20} | {'Random Forest Regressor':<20}")
print("   " + "─" * 60)
print(f"   {'R² Score':<15} | {reg_results['linear_regression']['r2']:<20.4f} | {reg_results['random_forest_regressor']['r2']:<20.4f}")
print(f"   {'RMSE ($)':<15} | ${reg_results['linear_regression']['rmse']:<19.2f} | ${reg_results['random_forest_regressor']['rmse']:<19.2f}")
print(f"   {'MAE ($)':<15}  | ${reg_results['linear_regression']['mae']:<19.2f} | ${reg_results['random_forest_regressor']['mae']:<19.2f}")
print(f"   {'Train Time':<15} | {reg_results['linear_regression']['training_time_sec']:<18.2f}s | {reg_results['random_forest_regressor']['training_time_sec']:<18.2f}s")
print("   " + "─" * 60)

# ============================================================
# CLASSIFICATION MODELS (High-Value Ride Dispatch)
# ============================================================
print("\n" + "=" * 80)
print("  PART 2: CLASSIFICATION MODELS (High-Value Ride Dispatch Matching)")
print(f"  Problem: Classify trips as High-Value (Fare >= ${THRESHOLD_FARE:.2f}) vs Standard")
print("=" * 80)

# --- Model 3: Logistic Regression Classifier ---
print("\n🎯 [Model 3/4] Training Logistic Regression Classifier...")
t0 = time.time()
lr_clf = LogisticRegression(
    featuresCol="features",
    labelCol="high_fare_label",
    maxIter=15,
    regParam=0.05,
    elasticNetParam=0.8
)
lr_clf_pipeline = Pipeline(stages=[assembler, scaler, lr_clf])
lr_clf_model = lr_clf_pipeline.fit(train_df)
lr_clf_preds = lr_clf_model.transform(test_df)
t_lr_clf = time.time() - t0
print(f"   ✅ Logistic Regression Classifier trained in {t_lr_clf:.2f}s")

# --- Model 4: Random Forest Classifier ---
print("\n🌲 [Model 4/4] Training Random Forest Classifier...")
t0 = time.time()
rf_clf = RandomForestClassifier(
    featuresCol="features",
    labelCol="high_fare_label",
    numTrees=40,
    maxDepth=8,
    seed=42
)
rf_clf_pipeline = Pipeline(stages=[assembler, scaler, rf_clf])
rf_clf_model = rf_clf_pipeline.fit(train_df)
rf_clf_preds = rf_clf_model.transform(test_df)
t_rf_clf = time.time() - t0
print(f"   ✅ Random Forest Classifier trained in {t_rf_clf:.2f}s")

# Evaluate Classification Models
def evaluate_classifier(predictions, model_name):
    bin_eval = BinaryClassificationEvaluator(labelCol="high_fare_label", rawPredictionCol="rawPrediction", metricName="areaUnderROC")
    multi_eval = MulticlassClassificationEvaluator(labelCol="high_fare_label", predictionCol="prediction")
    
    auc_roc = bin_eval.evaluate(predictions)
    multi_eval.setMetricName("accuracy")
    accuracy = multi_eval.evaluate(predictions)
    multi_eval.setMetricName("weightedPrecision")
    precision = multi_eval.evaluate(predictions)
    multi_eval.setMetricName("weightedRecall")
    recall = multi_eval.evaluate(predictions)
    multi_eval.setMetricName("f1")
    f1 = multi_eval.evaluate(predictions)
    
    # Confusion Matrix
    cm = predictions.groupBy("high_fare_label", "prediction").count().collect()
    tp = sum(r["count"] for r in cm if r["high_fare_label"] == 1.0 and r["prediction"] == 1.0)
    tn = sum(r["count"] for r in cm if r["high_fare_label"] == 0.0 and r["prediction"] == 0.0)
    fp = sum(r["count"] for r in cm if r["high_fare_label"] == 0.0 and r["prediction"] == 1.0)
    fn = sum(r["count"] for r in cm if r["high_fare_label"] == 1.0 and r["prediction"] == 0.0)
    
    return {
        "accuracy": accuracy,
        "precision": precision,
        "recall": recall,
        "f1": f1,
        "auc_roc": auc_roc,
        "tp": tp, "tn": tn, "fp": fp, "fn": fn
    }

clf_lr_eval = evaluate_classifier(lr_clf_preds, "Logistic Regression")
clf_rf_eval = evaluate_classifier(rf_clf_preds, "Random Forest Classifier")
clf_lr_eval["training_time_sec"] = round(t_lr_clf, 2)
clf_rf_eval["training_time_sec"] = round(t_rf_clf, 2)

print("\n📊 CLASSIFICATION EVALUATION RESULTS:")
print("   " + "─" * 60)
print(f"   {'Metric':<18} | {'Logistic Regression':<18} | {'Random Forest Clf':<18}")
print("   " + "─" * 60)
print(f"   {'Accuracy':<18} | {clf_lr_eval['accuracy']*100:<17.2f}% | {clf_rf_eval['accuracy']*100:<17.2f}%")
print(f"   {'Weighted Precision':<18} | {clf_lr_eval['precision']:<18.4f} | {clf_rf_eval['precision']:<18.4f}")
print(f"   {'Weighted Recall':<18}    | {clf_lr_eval['recall']:<18.4f} | {clf_rf_eval['recall']:<18.4f}")
print(f"   {'F1-Score':<18}   | {clf_lr_eval['f1']:<18.4f} | {clf_rf_eval['f1']:<18.4f}")
print(f"   {'AUC-ROC':<18}    | {clf_lr_eval['auc_roc']:<18.4f} | {clf_rf_eval['auc_roc']:<18.4f}")
print(f"   {'Train Time':<18} | {clf_lr_eval['training_time_sec']:<17.2f}s | {clf_rf_eval['training_time_sec']:<17.2f}s")
print("   " + "─" * 60)

# ============================================================
# FEATURE IMPORTANCE EXTRACTION
# ============================================================
rf_reg_stage = rf_reg_model.stages[-1]
rf_clf_stage = rf_clf_model.stages[-1]

reg_importances = {feat: float(imp) for feat, imp in zip(feature_cols, rf_reg_stage.featureImportances.toArray())}
clf_importances = {feat: float(imp) for feat, imp in zip(feature_cols, rf_clf_stage.featureImportances.toArray())}

print("\n📊 TOP FEATURE IMPORTANCES (Random Forest Models):")
print("   " + "─" * 65)
print(f"   {'Feature':<22} | {'Regression Imp.':<18} | {'Classification Imp.':<18}")
print("   " + "─" * 65)
for feat in feature_cols:
    print(f"   {feat:<22} | {reg_importances[feat]:<18.4f} | {clf_importances[feat]:<18.4f}")
print("   " + "─" * 65)

# ============================================================
# SAVE METRICS & SAMPLES FOR VISUALIZATION
# ============================================================
all_results = {
    "threshold_high_fare": THRESHOLD_FARE,
    "feature_columns": feature_cols,
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
print(f"\n💾 Saved all metrics to: {os.path.join(OUTPUT_DIR, 'ml_results_4models.json')}")

# Save sample predictions for visualization
print("💾 Saving prediction samples for plotting...")
lr_reg_preds.select("fare_target", "prediction").limit(5000).toPandas().to_csv(
    os.path.join(OUTPUT_DIR, "regression_lr_sample.csv"), index=False
)
rf_reg_preds.select("fare_target", "prediction").limit(5000).toPandas().to_csv(
    os.path.join(OUTPUT_DIR, "regression_rf_sample.csv"), index=False
)
rf_clf_preds.select("high_fare_label", "prediction").limit(5000).toPandas().to_csv(
    os.path.join(OUTPUT_DIR, "classification_rf_sample.csv"), index=False
)
lr_clf_preds.select("high_fare_label", "prediction").limit(5000).toPandas().to_csv(
    os.path.join(OUTPUT_DIR, "classification_lr_sample.csv"), index=False
)

print("""
================================================================================
✅ 4-MODEL MACHINE LEARNING PIPELINE COMPLETE
   1. Linear Regression (Regression Baseline)       → Evaluated (RMSE, R2, MAE)
   2. Random Forest Regressor (Non-linear Engine)   → Evaluated (RMSE, R2, MAE)
   3. Logistic Regression (Classification Baseline) → Evaluated (Acc, F1, ROC)
   4. Random Forest Classifier (Ensemble Predictor) → Evaluated (Acc, F1, ROC)
================================================================================
""")

spark.stop()
