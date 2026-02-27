# Medicaid Fraud Signal Detection Engine v3.0.0

**Based on [TheAuroraAI/medicaid-fraud-detector](https://github.com/TheAuroraAI/medicaid-fraud-detector).** The core architecture, signal design, and competition submission are their work — they deserve the majority of the competition credit. This fork extends their engine with additional signals and refinements.

Detects **19 types of fraud signals** in Medicaid provider spending data by cross-referencing the HHS spending dataset (227M rows), OIG LEIE exclusion list, and CMS NPPES NPI registry.

Built with DuckDB for memory-efficient out-of-core analytics — auto-tunes memory and threads to your hardware (defaults to ~75% of system RAM).

## Quick Start

```bash
# Install dependencies and download data
./setup.sh

# Run analysis (outputs JSON + HTML + FOF network reports)
./run.sh

# Or run with custom options
python3 src/main.py \
    --data-dir data/ \
    --output fraud_signals.json \
    --html fraud_report.html \
    --fof-json fof_network_fraud.json \
    --fof-html fof_network_fraud.html
```

## Fraud Signals Detected

### Core Signals (1–9)

| # | Signal | Severity | Description | Overpayment Methodology |
|---|--------|----------|-------------|------------------------|
| 1 | Excluded Provider | Critical | Provider on OIG exclusion list still billing Medicaid | 100% of post-exclusion payments (42 CFR 1001.1901) |
| 2 | Billing Outlier | High/Medium | Billing exceeds 99th percentile of taxonomy+state peer group | Amount exceeding peer 99th percentile |
| 3 | Rapid Escalation | High/Medium | New entity with >200% rolling 3-month billing growth | Sum of payments during >200% growth months |
| 4 | Workforce Impossibility | High | Organization billing >6 claims/hour (physically impossible) | Excess claims beyond 6/hr threshold x avg cost |
| 5 | Shared Official | High/Medium | One person controls 5–50 NPIs with >$1M combined billing | 10–20% of combined network billing (DOJ data) |
| 6 | Geographic Implausibility | Medium | Home health provider with <10% home-state claims | Excess claims beyond home-state ratio |
| 7 | Address Clustering | High/Medium | 10+ NPIs at same zip code with >$5M combined billing | 15% of combined billing (OIG ghost office data) |
| 8 | Upcoding | High/Medium | Provider billing >80% high-complexity E&M codes vs <30% peer avg | 30% uplift on excess high-level billing |
| 9 | Concurrent Billing | High/Medium | Individual provider billing in 5+ states in same month | 60% of flagged payments (phantom billing) |

### Network & Organized Fraud Signals (10–15)

| # | Signal | Severity | Description | Overpayment Methodology |
|---|--------|----------|-------------|------------------------|
| 10 | Burst Enrollment Network | High | 4+ orgs same quarter/taxonomy/state — coordinated shell registration | Combined billing of burst cohort |
| 11 | Coordinated Billing Ramp | High | 3+ NPIs under same official peak within 3 months — synchronized escalation | Peak-period billing across coordinated entities |
| 12 | Phantom Servicing Hub | High | 1 servicing NPI appears across 5+ billing entities — phantom referrals | Billing attributed to phantom servicing NPI |
| 13 | Network Beneficiary Dilution | High | >50 claims/beneficiary across network — beneficiary recycling | Excess claims beyond normal bene-to-claims ratio |
| 14 | Caregiver Density Anomaly | High | Zip code with anomalous home health billing by individual providers with very few beneficiaries — family caregiver fraud rings | Flagged billing in anomalous zip codes |
| 15 | Phantom Servicing Spread | High | Hub servicing NPI with <p10 beneficiary-to-claims ratio — fabricated services | Excess claims beyond beneficiary capacity |

### Behavioral & Pattern Signals (16–19)

| # | Signal | Severity | Description | Overpayment Methodology |
|---|--------|----------|-------------|------------------------|
| 16 | Repetitive Service Abuse | High/Medium | Claims per beneficiary exceeds p99 — therapy mill or personal care fraud | Excess claims beyond p99 per-beneficiary threshold |
| 17 | Billing Monoculture | High/Medium | >85% of claims from single HCPCS code — organized fraud targeting one procedure | Flagged single-code billing volume |
| 18 | Billing Bust-Out | High | Rapid escalation then collapse to <10% of peak — bust-out lifecycle | Total billing during ramp-and-collapse period |
| 19 | Reimbursement Rate Anomaly | High/Medium | >3x national median per-claim reimbursement — modifier abuse or place-of-service fraud | Excess above 3x median rate |

### COVID-Era Awareness

Signals 3, 4, and 18 include COVID-era adjustments (March 2020 – December 2021). During the PHE period, telehealth and testing surges are expected — these signals downgrade severity from high to medium and annotate findings with COVID context rather than producing false positives.

## Composite Risk Score

Each flagged provider receives a composite risk score (0–100) combining:
- **Signal breadth** — how many different signal types triggered (max 30 pts)
- **Severity weight** — weighted sum of signal severities x signal-type risk (max 40 pts)
- **Overpayment ratio** — estimated overpayment as % of total billing (max 30 pts)

Risk tiers:
- **Critical** (75–100) — immediate investigation priority
- **High** (50–74) — investigate within 30 days
- **Medium** (25–49) — review within 90 days
- **Low** (0–24) — monitor and reassess

## Cross-Signal Correlation Analysis

The engine identifies providers flagged by multiple independent signals — the highest-priority investigation targets. The report includes:
- Provider distribution by signal count
- Most common signal pair co-occurrences
- Multi-signal provider NPI lists for prioritized review

## Legitimate Entity Filtering

Known legitimate entities (large health systems, university hospitals, national labs, school districts, nonprofits) are filtered with higher thresholds to reduce false positives. High-threshold entities (e.g., FQHCs, tribal health, rural critical access hospitals) require elevated evidence before flagging.

## Output Formats

### JSON (`fraud_signals.json`)
Complete structured output including:
- **Methodology documentation** — per-signal methodology, thresholds, and overpayment basis
- **Cross-signal analysis** — provider overlap across signal types
- **Executive summary** — aggregate stats, tier distribution, top states, highest-risk providers
- **Provider records** — signals, risk scores, case narratives, FCA relevance
- All evidence with severity classification and statute references

### HTML Report (`--html report.html`)
Visual dashboard with:
- Executive summary cards (scanned, flagged, overpayment, critical count)
- Risk tier distribution table
- Signal type summary with counts and estimated overpayments
- Top states by flags
- Detailed provider cards with risk score bars, narratives, and FCA guidance

### FOF Network Fraud Reports (`--fof-json`, `--fof-html`)
Dedicated Feeding Our Future-style network fraud analysis:
- Extracts network-related signals (shared officials, burst enrollment, coordinated ramps, phantom hubs)
- Actionability filtering for investigation-ready output
- Standalone JSON and HTML reports

## Architecture

```
src/
  ingest.py    — DuckDB data loading with auto-tuned memory/threads, parquet/CSV/zip streaming,
                 pre-materialized aggregation tables (provider_totals, provider_monthly, etc.)
  signals.py   — All 19 signal implementations as SQL queries + COVID-era awareness + cross-signal analysis
  output.py    — JSON/HTML/FOF report generation, risk scoring, case narratives, legitimate entity
                 filtering, executive summary, methodology documentation
  main.py      — CLI entry point
tests/
  conftest.py      — Synthetic data fixtures (triggers all 19 signals with materialized tables)
  test_signals.py  — 137 unit tests covering all signals, risk scoring, narratives, HTML, edge cases,
                     COVID-era awareness, entity filters, multi-signal providers
  fixtures/        — Additional test fixtures
```

## Data Sources

| Dataset | Size | Format | Source |
|---------|------|--------|--------|
| Medicaid Spending | 2.9 GB | Parquet | HHS STOP |
| OIG LEIE | 15 MB | CSV | oig.hhs.gov |
| NPPES NPI Registry | 1 GB (zip) | CSV → Parquet | CMS |

The tool automatically builds a slim NPPES parquet (~177 MB) from the full 11 GB CSV, extracting only the 11 columns needed.

## Memory Management

DuckDB processes the 2.9 GB parquet file out-of-core without loading it into RAM. The engine auto-detects system RAM and sets the memory limit to ~75% of available memory (minimum 2 GB).

```bash
# Override auto-tuning
./run.sh --memory-limit 8GB    # More RAM = faster
./run.sh --memory-limit 1GB    # Constrained environments
./run.sh --no-gpu              # CPU-only (default, GPU not used)
```

## Testing

```bash
pytest tests/ -v
```

All 137 tests use synthetic data fixtures that trigger each signal, verifying:
- Detection logic and threshold compliance for all 19 signals
- COVID-era severity downgrading for signals 3, 4, and 18
- Severity classification (critical/high/medium)
- Overpayment calculations (per-signal methodology)
- Composite risk scoring and tier assignment
- Case narrative generation
- Executive summary statistics
- HTML report output and XSS safety
- Cross-signal correlation analysis
- Known legitimate entity filtering and high-threshold entity handling
- Edge cases (empty inputs, zero totals, negative values)
- FCA statute references and claim type mappings
- Multi-signal provider handling
- FOF network fraud report generation

## Output Schema

See `fraud_signals.json` for the complete output. Each flagged provider includes:
- All signal evidence with severity classification
- **Composite risk score** (0–100) with tier, contributing factors
- **Plain-English case narrative** summarizing all findings
- Estimated overpayment in USD (per-signal formulas)
- FCA statute references (31 U.S.C. § 3729)
- Actionable next steps for qui tam / FCA lawyers

The report also includes:
- **Methodology documentation** — per-signal description, SQL methodology, overpayment basis, and threshold
- **Cross-signal analysis** — multi-signal provider identification and pair co-occurrence
- **Executive summary** — aggregate statistics, risk tier distribution, top states, highest-risk providers

## Requirements

- Python 3.10+
- DuckDB, Polars, PyArrow (pip-installable)
- Works on Ubuntu 22.04+ and macOS 14+ (Apple Silicon)
