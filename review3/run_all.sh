#!/bin/bash
# ============================================================
# run_all.sh — BDA Review 3 Complete Pipeline
# NYC Yellow Taxi Demand & Fare Intelligence System
# ============================================================
# Usage: bash run_all.sh
# ============================================================

set -e

# Setup PATH for homebrew tools and Python scripts (spark-submit, spark-shell)
eval "$(/opt/homebrew/bin/brew shellenv)" 2>/dev/null || true

# Auto-detect JAVA_HOME if not already exported
if [ -z "$JAVA_HOME" ]; then
    if [ -d "/opt/homebrew/opt/openjdk@21/libexec/openjdk.jdk/Contents/Home" ]; then
        export JAVA_HOME="/opt/homebrew/opt/openjdk@21/libexec/openjdk.jdk/Contents/Home"
    elif command -v /usr/libexec/java_home >/dev/null 2>&1; then
        export JAVA_HOME=$(/usr/libexec/java_home -v 21 2>/dev/null || /usr/libexec/java_home 2>/dev/null)
    elif [ -d "/usr/lib/jvm/java-21-openjdk-amd64" ]; then
        export JAVA_HOME="/usr/lib/jvm/java-21-openjdk-amd64"
    elif [ -d "/usr/lib/jvm/java-17-openjdk-amd64" ]; then
        export JAVA_HOME="/usr/lib/jvm/java-17-openjdk-amd64"
    elif [ -d "/usr/lib/jvm/java-21-openjdk-arm64" ]; then
        export JAVA_HOME="/usr/lib/jvm/java-21-openjdk-arm64"
    elif [ -d "/usr/lib/jvm/java-17-openjdk-arm64" ]; then
        export JAVA_HOME="/usr/lib/jvm/java-17-openjdk-arm64"
    fi
fi

# Add Java and user Python scripts to PATH
if [ -n "$JAVA_HOME" ]; then
    export PATH="$JAVA_HOME/bin:$PATH"
fi
# Use venv if available, otherwise fall back to system Python
VENV_DIR="$(cd "$(dirname "$0")/.." && pwd)/venv"
if [ -d "$VENV_DIR/bin" ]; then
    export PATH="$VENV_DIR/bin:$PATH"
    echo "🐍 Using venv: $VENV_DIR"
else
    echo "⚠️  venv not found at $VENV_DIR — falling back to system Python"
fi

PROJECT_DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$PROJECT_DIR"

# Link dataset if present in parent data/raw/
mkdir -p data
if [ ! -f "data/yellow_tripdata_2026-01.parquet" ]; then
    if [ -f "../data/raw/yellow_tripdata_2026-01.parquet" ]; then
        echo "🔗 Linking dataset from ../data/raw/yellow_tripdata_2026-01.parquet..."
        ln -sf "../data/raw/yellow_tripdata_2026-01.parquet" data/yellow_tripdata_2026-01.parquet
    elif [ -f "/app/data/yellow_tripdata_2026-01.parquet" ]; then
        ln -sf "/app/data/yellow_tripdata_2026-01.parquet" data/yellow_tripdata_2026-01.parquet
    fi
fi

echo "============================================================"
echo "  BDA REVIEW 3 — NYC TAXI ANALYTICS PIPELINE"
echo "  $(date)"
echo "============================================================"

echo ""
echo "📁 Project directory: $PROJECT_DIR"
echo "☕ JAVA_HOME: $JAVA_HOME"
echo "🔥 Spark: $(spark-submit --version 2>&1 | head -1 || echo 'not found')"
echo ""

# ============================================================
# STEP 1: PySpark Preprocessing & EDA
# ============================================================
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "  STEP 1/4: Preprocessing & EDA (PySpark)"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
spark-submit \
    --master "local[*]" \
    --driver-memory 4g \
    python/01_preprocessing_eda.py

echo ""
echo "✅ Step 1 complete — Cleaned data saved"
echo ""

# ============================================================
# STEP 2: Scala RDD Processing (4 Students Individual Scripts)
# ============================================================
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "  STEP 2/4: Scala + Spark RDD Processing (4 Students)"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "  To demo interactively in spark-shell (run per student):"
echo "    spark-shell --driver-memory 4g"
echo ""
echo "    Student 1 (Demand & Hotspots):"
echo "      :load scala/Student1_SpatialDemandRDD.scala"
echo ""
echo "    Student 2 (Revenue & Airport Surcharges):"
echo "      :load scala/Student2_RevenueTollsRDD.scala"
echo ""
echo "    Student 3 (Tipping Dynamics & Payment):"
echo "      :load scala/Student3_TippingBehaviorRDD.scala"
echo ""
echo "    Student 4 (Traffic Congestion & Velocity):"
echo "      :load scala/Student4_CongestionDurationRDD.scala"
echo ""
echo "    Master Pipeline (Runs all in sequence):"
echo "      :load scala/NycTaxiRDD.scala"
echo ""

# ============================================================
# STEP 3: PySpark 4-Model Machine Learning Pipeline
# ============================================================
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "  STEP 3/4: Machine Learning (4 Models: 2 Reg + 2 Clf)"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "  1. Linear Regression         (Upfront Fare Estimation)"
echo "  2. Random Forest Regressor   (Non-linear Fare Estimation)"
echo "  3. Logistic Regression       (High-Value Trip Dispatch)"
echo "  4. Random Forest Classifier  (Multi-tree Dispatch Priority)"
echo ""
python3 python/02_ml_model.py

echo ""
echo "✅ Step 3 complete — All 4 ML models trained and evaluated"
echo ""

# ============================================================
# STEP 4: Visualizations & 6-Panel Dashboard
# ============================================================
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "  STEP 4/4: Visualizations & Executive Dashboard"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
python3 python/03_visualizations.py
if [ -f "python/generate_questions_visualizations.py" ]; then
    python3 python/generate_questions_visualizations.py
fi

echo ""
echo "============================================================"
echo "  ✅ ALL STEPS COMPLETE"
echo "============================================================"
echo ""
echo "  Output files in: $PROJECT_DIR/output/"
ls -la output/
echo ""
echo "  To view dashboard: open output/dashboard.png"
echo ""
