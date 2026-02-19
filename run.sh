#!/usr/bin/env bash
# run.sh — Run the Medicaid Fraud Signal Detection Engine
# Works on Ubuntu 22.04+ and macOS 14+, Python 3.11+
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# Find Python 3.11+
PYTHON=""
for p in python3 python3.12 python3.11; do
    if command -v "$p" &>/dev/null; then
        PYTHON="$p"
        break
    fi
done
if [ -z "${PYTHON}" ]; then
    echo "ERROR: Python 3.11+ required but not found."
    exit 1
fi

echo "=== Medicaid Provider Fraud Signal Detection Engine ==="
echo "Python: ${PYTHON} ($(${PYTHON} --version))"
echo ""

# Check dependencies
if ! "${PYTHON}" -c "import duckdb, requests" 2>/dev/null; then
    echo "ERROR: Required packages not found. Run:"
    echo "  bash ${SCRIPT_DIR}/setup.sh"
    exit 1
fi

# Run setup if OIG CSV doesn't exist
if [ ! -f "${SCRIPT_DIR}/data/UPDATED.csv" ]; then
    echo "Data not found. Running setup..."
    bash "${SCRIPT_DIR}/setup.sh"
fi

# Detect memory and set appropriate limit
TOTAL_MEM_KB=0
if [[ "$(uname)" == "Darwin" ]]; then
    TOTAL_MEM_KB=$(($(sysctl -n hw.memsize) / 1024))
else
    TOTAL_MEM_KB=$(grep MemTotal /proc/meminfo | awk '{print $2}')
fi
TOTAL_MEM_GB=$((TOTAL_MEM_KB / 1024 / 1024))

if [ "${TOTAL_MEM_GB}" -ge 128 ]; then
    MEM_LIMIT="64GB"
elif [ "${TOTAL_MEM_GB}" -ge 64 ]; then
    MEM_LIMIT="32GB"
elif [ "${TOTAL_MEM_GB}" -ge 16 ]; then
    MEM_LIMIT="8GB"
elif [ "${TOTAL_MEM_GB}" -ge 8 ]; then
    MEM_LIMIT="4GB"
else
    MEM_LIMIT="2GB"
fi

echo "System RAM: ~${TOTAL_MEM_GB}GB | DuckDB limit: ${MEM_LIMIT}"
echo ""

# Pass all arguments through
"${PYTHON}" "${SCRIPT_DIR}/detect_fraud.py" --memory-limit "${MEM_LIMIT}" "$@"

# Check output
OUTPUT="${SCRIPT_DIR}/fraud_signals.json"
if [ -f "${OUTPUT}" ]; then
    echo ""
    echo "=== Output Summary ==="
    "${PYTHON}" -c "
import json, os
with open('${OUTPUT}') as f:
    data = json.load(f)
print(f'Generated at:          {data[\"generated_at\"]}')
print(f'Providers scanned:     {data[\"total_providers_scanned\"]:,}')
print(f'Providers flagged:     {data[\"total_providers_flagged\"]:,}')
print(f'Est. overpayment:      \${data[\"total_estimated_overpayment_usd\"]:,.2f}')
print(f'Output file:           ${OUTPUT}')
print(f'Output size:           {os.path.getsize(\"${OUTPUT}\"):,} bytes')
# Signal breakdown
signals = {}
for p in data['flagged_providers']:
    for s in p['signals']:
        signals[s['signal_name']] = signals.get(s['signal_name'], 0) + 1
print()
print('Signal breakdown:')
for name, count in sorted(signals.items(), key=lambda x: -x[1]):
    print(f'  {name:40s} {count:5d} providers')
"
else
    echo "ERROR: fraud_signals.json was not generated."
    exit 1
fi
