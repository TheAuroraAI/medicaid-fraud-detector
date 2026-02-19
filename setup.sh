#!/usr/bin/env bash
# setup.sh — Download data files for Medicaid Fraud Signal Detection Engine
# Works on Ubuntu 22.04+ and macOS 14+ with Python 3.11+
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DATA_DIR="${SCRIPT_DIR}/data"
mkdir -p "${DATA_DIR}"

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
echo "Using Python: ${PYTHON} ($(${PYTHON} --version))"

# Install dependencies
echo "Installing Python dependencies..."
${PYTHON} -m pip install -r "${SCRIPT_DIR}/requirements.txt" --quiet 2>/dev/null || \
    ${PYTHON} -m pip install -r "${SCRIPT_DIR}/requirements.txt" --quiet --break-system-packages 2>/dev/null || \
    echo "WARNING: Could not install deps automatically. Install manually: pip install -r requirements.txt"

echo ""
echo "=== Medicaid Fraud Signal Detection Engine — Setup ==="
echo "Data directory: ${DATA_DIR}"
echo ""

# ---- 1. Download OIG LEIE Exclusion List (~15MB CSV) ----
OIG_CSV="${DATA_DIR}/UPDATED.csv"
if [ -f "${OIG_CSV}" ]; then
    echo "[OIG] Exclusion list already downloaded: ${OIG_CSV}"
else
    echo "[OIG] Downloading OIG LEIE Exclusion List..."
    curl -L -o "${OIG_CSV}" \
        "https://oig.hhs.gov/exclusions/downloadables/UPDATED.csv" \
        --progress-bar
    echo "[OIG] Downloaded: $(wc -l < "${OIG_CSV}") lines"
fi

# ---- 2. Download Medicaid Provider Spending Parquet (~2.9GB) ----
PARQUET="${DATA_DIR}/medicaid-provider-spending.parquet"
if [ -f "${PARQUET}" ]; then
    SIZE=$(stat -f%z "${PARQUET}" 2>/dev/null || stat -c%s "${PARQUET}" 2>/dev/null || echo "0")
    if [ "${SIZE}" -gt 100000000 ]; then
        SIZE_MB=$((SIZE / 1024 / 1024))
        echo "[Parquet] Already downloaded: ${PARQUET} (${SIZE_MB} MB)"
    else
        echo "[Parquet] File too small, re-downloading..."
        rm -f "${PARQUET}"
    fi
fi
if [ ! -f "${PARQUET}" ]; then
    echo "[Parquet] Downloading Medicaid provider spending data (~2.9GB)..."
    echo "[Parquet] This may take 10-30 minutes depending on connection speed."
    curl -L -o "${PARQUET}" \
        "https://stopendataprod.blob.core.windows.net/datasets/medicaid-provider-spending/2026-02-09/medicaid-provider-spending.parquet" \
        --progress-bar
    SIZE=$(stat -f%z "${PARQUET}" 2>/dev/null || stat -c%s "${PARQUET}" 2>/dev/null || echo "0")
    SIZE_MB=$((SIZE / 1024 / 1024))
    echo "[Parquet] Downloaded: ${SIZE_MB} MB"
fi

# ---- 3. NPPES NPI Registry ----
NPPES_DIR="${DATA_DIR}/nppes"
NPPES_PARQUET="${NPPES_DIR}/nppes_slim.parquet"
NPPES_ZIP="${NPPES_DIR}/nppes.zip"

if [ -f "${NPPES_PARQUET}" ]; then
    echo "[NPPES] Slim parquet already exists: ${NPPES_PARQUET}"
else
    mkdir -p "${NPPES_DIR}"

    # Check available disk space (need ~10GB temporarily)
    if [[ "$(uname)" == "Darwin" ]]; then
        AVAIL_KB=$(df -k "${DATA_DIR}" | tail -1 | awk '{print $4}')
    else
        AVAIL_KB=$(df --output=avail "${DATA_DIR}" | tail -1 | tr -d ' ')
    fi
    AVAIL_GB=$((AVAIL_KB / 1024 / 1024))
    echo "[NPPES] Available disk: ${AVAIL_GB}GB"

    if [ "${AVAIL_GB}" -lt 3 ]; then
        echo "[NPPES] WARNING: Less than 3GB free. Will use NPPES API for lookups."
        echo "api_fallback" > "${NPPES_DIR}/mode.txt"
    else
        echo "[NPPES] Downloading NPPES zip (~1GB)..."
        curl -L -o "${NPPES_ZIP}" \
            "https://download.cms.gov/nppes/NPPES_Data_Dissemination_February_2026_V2.zip" \
            --progress-bar

        echo "[NPPES] Extracting main CSV from zip..."
        MAIN_CSV=$(unzip -l "${NPPES_ZIP}" 2>/dev/null | grep -oE 'npidata_pfile_[0-9]+-[0-9]+\.csv' | head -1 || true)

        if [ -z "${MAIN_CSV}" ]; then
            echo "[NPPES] Could not find main CSV in zip. Contents:"
            unzip -l "${NPPES_ZIP}" | head -20
            echo "[NPPES] Falling back to API mode."
            echo "api_fallback" > "${NPPES_DIR}/mode.txt"
            rm -f "${NPPES_ZIP}"
        else
            echo "[NPPES] Found: ${MAIN_CSV}"
            unzip -o -j "${NPPES_ZIP}" "${MAIN_CSV}" -d "${NPPES_DIR}/"

            echo "[NPPES] Creating slim parquet (only needed columns)..."
            ${PYTHON} -c "
import duckdb
con = duckdb.connect()
con.execute(\"SET memory_limit='1500MB'\")
con.execute(\"\"\"
    COPY (
        SELECT
            NPI,
            \"Entity Type Code\" AS entity_type_code,
            \"Provider Organization Name (Legal Business Name)\" AS org_name,
            \"Provider Last Name (Legal Name)\" AS last_name,
            \"Provider First Name\" AS first_name,
            \"Provider Business Mailing Address State Name\" AS state,
            \"Provider Business Mailing Address Postal Code\" AS postal_code,
            \"Healthcare Provider Taxonomy Code_1\" AS taxonomy_code,
            \"Provider Enumeration Date\" AS enumeration_date,
            \"Authorized Official Last Name\" AS auth_official_last_name,
            \"Authorized Official First Name\" AS auth_official_first_name
        FROM read_csv('${NPPES_DIR}/${MAIN_CSV}',
            auto_detect=true,
            ignore_errors=true,
            parallel=false)
        WHERE NPI IS NOT NULL
    ) TO '${NPPES_PARQUET}' (FORMAT PARQUET, COMPRESSION ZSTD)
\"\"\")
print('Slim parquet created successfully')
con.close()
"
            echo "[NPPES] Cleaning up large files..."
            rm -f "${NPPES_DIR}/${MAIN_CSV}"
            rm -f "${NPPES_ZIP}"
            echo "parquet" > "${NPPES_DIR}/mode.txt"
        fi
    fi
fi

echo ""
echo "=== Setup Complete ==="
echo "OIG CSV:  $(ls -lh "${OIG_CSV}" 2>/dev/null || echo 'not found')"
echo "Parquet:  $(ls -lh "${PARQUET}" 2>/dev/null || echo 'not found')"
echo "NPPES:    $(cat "${NPPES_DIR}/mode.txt" 2>/dev/null || echo 'not configured')"
echo ""
echo "Run: bash run.sh"
