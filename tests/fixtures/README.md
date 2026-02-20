# Test Fixtures

Small synthetic datasets that trigger each of the 6 fraud signals.

## Files

- **spending.csv** — 28 rows simulating Medicaid spending data with known fraud patterns
- **leie.csv** — 1 excluded provider (NPI 2222222222, excluded 2022-01-01)
- **nppes.csv** — 16 providers including individuals, organizations, and shared official groups

## Which Fixtures Trigger Which Signals

| Signal | Trigger NPI | Fixture Detail |
|--------|-------------|----------------|
| 1: Excluded Provider | 2222222222 | In LEIE with exclusion date before claim months |
| 2: Billing Outlier | 3333333333 | $1.1M total vs ~$5K peers in same taxonomy+state |
| 3: Rapid Escalation | 4444444444 | Enumerated 2022-11, billing starts 2023-01, 400% growth |
| 4: Workforce Impossibility | 5555555555 | Org with 5000 claims in one month (~28 claims/hour) |
| 5: Shared Official | 6666666661-5 | 5 NPIs controlled by ROBERT SMITH, >$1M combined |
| 6: Geographic Implausibility | 7777777777 | Home health codes, 500+ claims, 2 beneficiaries |

These same fixtures are loaded programmatically in `conftest.py` for pytest execution.
