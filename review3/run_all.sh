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
if [ -d "/opt/homebrew/opt/openjdk@21/libexec/openjdk.jdk/Contents/Home" ]; then
    export JAVA_HOME="/opt/homebrew/opt/openjdk@21/libexec/openjdk.jdk/Contents/Home"
else
    export JAVA_HOME=$(/usr/libexec/java_home -v 21 2>/dev/null || /usr/libexec/java_home 2>/dev/null)
fi
export PATH="$JAVA_HOME/bin:/Users/apple/Library/Python/3.9/bin:$PATH"

PROJECT_DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$PROJECT_DIR"

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
# STEP 2: Scala RDD Processing (via spark-shell)
# ============================================================
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "  STEP 2/4: Scala + Spark RDD Processing"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "  To run interactively (recommended for demo):"
echo "    spark-shell --driver-memory 4g"
echo "    :load scala/NycTaxiRDD.scala"
echo ""
echo "  Or run non-interactively:"
echo "    cat scala/NycTaxiRDD.scala | spark-shell --driver-memory 4g"
echo ""

# ============================================================
# STEP 3: PySpark ML Model
# ============================================================
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "  STEP 3/4: Machine Learning (PySpark)"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
spark-submit \
    --master "local[*]" \
    --driver-memory 4g \
    python/02_ml_model.py

echo ""
echo "✅ Step 3 complete — ML models trained and evaluated"
echo ""

# ============================================================
# STEP 4: Visualizations
# ============================================================
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "  STEP 4/4: Visualizations (Python + matplotlib)"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
python3 python/03_visualizations.py

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
