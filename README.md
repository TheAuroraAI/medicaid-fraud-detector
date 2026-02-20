# Medicaid Fraud Signal Detection Engine

Detects 6 types of fraud signals in Medicaid provider spending data by cross-referencing the HHS spending dataset (227M rows), OIG LEIE exclusion list, and CMS NPPES NPI registry.

Built with DuckDB for memory-efficient out-of-core analytics — runs on machines with as little as 2GB available RAM.

## Quick Start

```bash
# Install dependencies and download data
./setup.sh

# Run analysis
./run.sh
# Output: fraud_signals.json
```

## Fraud Signals Detected

| # | Signal | Severity | Description |
|---|--------|----------|-------------|
| 1 | Excluded Provider | Critical | Provider on OIG exclusion list still billing Medicaid |
| 2 | Billing Outlier | High/Medium | Provider billing exceeds 99th percentile of taxonomy+state peer group |
| 3 | Rapid Escalation | High/Medium | New entity with >200% rolling 3-month billing growth |
| 4 | Workforce Impossibility | High | Organization billing >6 claims/hour (physically impossible volume) |
| 5 | Shared Official | High/Medium | One person controls 5+ NPIs with >$1M combined billing |
| 6 | Geographic Implausibility | Medium | Home health provider with <0.1 beneficiary/claims ratio |

## Architecture

```
src/
  ingest.py    — DuckDB data loading (parquet, CSV, zip streaming)
  signals.py   — All 6 signal implementations as SQL queries
  output.py    — JSON report generation with FCA statute mapping
  main.py      — CLI entry point
tests/
  conftest.py      — Synthetic data fixtures (triggers all 6 signals)
  test_signals.py  — 26 unit tests (4+ per signal)
```

## Data Sources

| Dataset | Size | Format | Source |
|---------|------|--------|--------|
| Medicaid Spending | 2.9 GB | Parquet | HHS STOP |
| OIG LEIE | 15 MB | CSV | oig.hhs.gov |
| NPPES NPI Registry | 1 GB (zip) | CSV → Parquet | CMS |

The tool automatically builds a slim NPPES parquet (~177 MB) from the full 11 GB CSV, extracting only the 11 columns needed.

## Memory Management

DuckDB processes the 2.9 GB parquet file out-of-core without loading it into RAM. Default memory limit is 2 GB.

```bash
# Adjust for your hardware
./run.sh --memory-limit 8GB    # More RAM = faster
./run.sh --memory-limit 1GB    # Constrained environments
./run.sh --no-gpu              # CPU-only (default, GPU not used)
```

## Testing

```bash
pytest tests/ -v
```

All 26 tests use synthetic data fixtures that trigger each signal, verifying detection logic, severity classification, overpayment calculations, and output format compliance.

## Output Schema

See `fraud_signals.json` for the complete output. Each flagged provider includes:
- All signal evidence with severity classification
- Estimated overpayment in USD (per-signal formulas from spec)
- FCA statute references (31 U.S.C. § 3729)
- Actionable next steps for qui tam / FCA lawyers

## Requirements

- Python 3.11+
- DuckDB, Polars, PyArrow (pip-installable)
- Works on Ubuntu 22.04+ and macOS 14+ (Apple Silicon)
