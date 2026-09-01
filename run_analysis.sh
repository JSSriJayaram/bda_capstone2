#!/usr/bin/env bash
# =============================================================
# run_analysis.sh
# NYC Yellow Taxi — Step 6: Visualizations
# Runs generate_visualizations.py using the venv or system Python
# =============================================================
set -e
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"

echo "=== Step 6: Generating Visualizations ==="

# Activate venv if present
if [ -f "$SCRIPT_DIR/venv/bin/activate" ]; then
    source "$SCRIPT_DIR/venv/bin/activate"
fi

pip install -q matplotlib pandas 2>/dev/null || true

python "$SCRIPT_DIR/analysis/generate_visualizations.py"

echo "=== Done. Charts saved to visualizations/ ==="
