#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

echo "=== Medicaid Fraud Signal Detection Engine ==="
python3 "$SCRIPT_DIR/src/main.py" \
    --data-dir "$SCRIPT_DIR/data" \
    --output "$SCRIPT_DIR/fraud_signals.json" \
    "$@"
echo "=== Analysis complete ==="
echo "Output: $SCRIPT_DIR/fraud_signals.json"
