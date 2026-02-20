#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DATA_DIR="$SCRIPT_DIR/data"

echo "=== Medicaid Fraud Signal Detection Engine — Setup ==="

echo "[1/4] Installing Python dependencies..."
pip install -r "$SCRIPT_DIR/requirements.txt" --quiet

mkdir -p "$DATA_DIR"

SPENDING_FILE="$DATA_DIR/medicaid_spending.parquet"
if [ ! -f "$SPENDING_FILE" ]; then
    echo "[2/4] Downloading Medicaid Provider Spending data (2.9 GB)..."
    wget -q --show-progress -O "$SPENDING_FILE" \
        "https://stopendataprod.blob.core.windows.net/datasets/medicaid-provider-spending/2026-02-09/medicaid-provider-spending.parquet"
else
    echo "[2/4] Medicaid spending data already exists, skipping."
fi

LEIE_FILE="$DATA_DIR/LEIE.csv"
if [ ! -f "$LEIE_FILE" ]; then
    echo "[3/4] Downloading OIG LEIE Exclusion List..."
    wget -q --show-progress -O "$LEIE_FILE" \
        "https://oig.hhs.gov/exclusions/downloadables/UPDATED.csv"
else
    echo "[3/4] LEIE data already exists, skipping."
fi

NPPES_ZIP="$DATA_DIR/NPPES.zip"
NPPES_SLIM="$DATA_DIR/nppes/nppes_slim.parquet"
NPPES_SLIM_ALT="$DATA_DIR/nppes_slim.parquet"
if [ -f "$NPPES_SLIM" ] || [ -f "$NPPES_SLIM_ALT" ] || ls "$DATA_DIR"/npidata_pfile_*.csv 1>/dev/null 2>&1; then
    echo "[4/4] NPPES data already available, skipping."
else
    if [ ! -f "$NPPES_ZIP" ]; then
        echo "[4/4] Downloading NPPES NPI Registry (~1 GB)..."
        wget -q --show-progress -O "$NPPES_ZIP" \
            "https://download.cms.gov/nppes/NPPES_Data_Dissemination_February_2026_V2.zip"
    fi
    echo "Building slim NPPES parquet (extracts only needed columns, saves ~10 GB disk)..."
    echo "The ingest module will handle extraction automatically on first run."
fi

echo ""
echo "=== Setup complete ==="
ls -lh "$DATA_DIR"
