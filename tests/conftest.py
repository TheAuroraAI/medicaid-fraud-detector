"""Shared test fixtures for fraud signal tests."""

import os
import pytest
import duckdb


@pytest.fixture
def con():
    """Create a DuckDB connection with synthetic test data."""
    c = duckdb.connect(":memory:")

    # Create spending table with synthetic data
    c.execute("""
        CREATE TABLE spending AS
        SELECT * FROM (VALUES
            -- Normal provider
            ('1111111111', '1111111111', 'G0151', '2023-01-01'::DATE, 10, 50, 5000.0),
            ('1111111111', '1111111111', 'G0151', '2023-02-01'::DATE, 12, 55, 5500.0),
            ('1111111111', '1111111111', '99213', '2023-03-01'::DATE, 15, 60, 6000.0),

            -- Excluded provider (NPI matches LEIE)
            ('2222222222', '2222222222', '99213', '2023-01-01'::DATE, 20, 100, 10000.0),
            ('2222222222', '2222222222', '99213', '2023-06-01'::DATE, 25, 120, 12000.0),
            ('2222222222', '2222222222', '99213', '2023-12-01'::DATE, 30, 150, 15000.0),

            -- High-volume outlier (taxonomy 207Q00000X, state CA)
            ('3333333333', '3333333333', '99213', '2023-01-01'::DATE, 100, 500, 500000.0),
            ('3333333333', '3333333333', '99213', '2023-06-01'::DATE, 120, 600, 600000.0),

            -- Normal peer provider (same taxonomy+state)
            ('3333333334', '3333333334', '99213', '2023-01-01'::DATE, 10, 50, 5000.0),
            ('3333333335', '3333333335', '99213', '2023-01-01'::DATE, 12, 55, 6000.0),
            ('3333333336', '3333333336', '99213', '2023-01-01'::DATE, 11, 52, 5500.0),
            ('3333333337', '3333333337', '99213', '2023-01-01'::DATE, 10, 48, 4800.0),
            ('3333333338', '3333333338', '99213', '2023-01-01'::DATE, 9, 45, 4500.0),

            -- Rapid escalation provider (new entity, billing grows fast)
            ('4444444444', '4444444444', '99213', '2023-01-01'::DATE, 5, 20, 1000.0),
            ('4444444444', '4444444444', '99213', '2023-02-01'::DATE, 10, 40, 2000.0),
            ('4444444444', '4444444444', '99213', '2023-03-01'::DATE, 20, 100, 10000.0),
            ('4444444444', '4444444444', '99213', '2023-04-01'::DATE, 50, 300, 50000.0),
            ('4444444444', '4444444444', '99213', '2023-05-01'::DATE, 100, 700, 150000.0),
            ('4444444444', '4444444444', '99213', '2023-06-01'::DATE, 200, 1500, 400000.0),

            -- Workforce impossibility org (huge claims in one month)
            ('5555555555', '5555555555', '99213', '2023-01-01'::DATE, 50, 200, 20000.0),
            ('5555555555', '5555555555', '99213', '2023-06-01'::DATE, 100, 5000, 500000.0),

            -- Shared official NPIs (5 NPIs controlled by same person)
            ('6666666661', '6666666661', '99213', '2023-01-01'::DATE, 50, 200, 300000.0),
            ('6666666662', '6666666662', '99213', '2023-01-01'::DATE, 40, 180, 250000.0),
            ('6666666663', '6666666663', '99213', '2023-01-01'::DATE, 30, 150, 200000.0),
            ('6666666664', '6666666664', '99213', '2023-01-01'::DATE, 20, 100, 150000.0),
            ('6666666665', '6666666665', '99213', '2023-01-01'::DATE, 10, 80, 150000.0),

            -- Geographic implausibility: home health, many claims, few beneficiaries
            ('7777777777', '7777777777', 'G0151', '2023-06-01'::DATE, 2, 500, 50000.0),
            ('7777777777', '7777777777', 'T1019', '2023-06-01'::DATE, 3, 200, 20000.0),

            -- Address clustering: 10+ NPIs in same zip with high billing
            ('8800000001', '8800000001', '99213', '2023-01-01'::DATE, 20, 100, 600000.0),
            ('8800000002', '8800000002', '99213', '2023-01-01'::DATE, 18, 90, 550000.0),
            ('8800000003', '8800000003', '99213', '2023-01-01'::DATE, 15, 80, 520000.0),
            ('8800000004', '8800000004', '99213', '2023-01-01'::DATE, 12, 70, 510000.0),
            ('8800000005', '8800000005', '99213', '2023-01-01'::DATE, 10, 60, 500000.0),
            ('8800000006', '8800000006', '99213', '2023-01-01'::DATE, 10, 55, 490000.0),
            ('8800000007', '8800000007', '99213', '2023-01-01'::DATE, 10, 50, 480000.0),
            ('8800000008', '8800000008', '99213', '2023-01-01'::DATE, 10, 45, 470000.0),
            ('8800000009', '8800000009', '99213', '2023-01-01'::DATE, 10, 40, 460000.0),
            ('8800000010', '8800000010', '99213', '2023-01-01'::DATE, 10, 35, 450000.0),

            -- Upcoding provider: bills almost all high-level E&M codes
            ('9900000001', '9900000001', '99215', '2023-01-01'::DATE, 30, 90, 90000.0),
            ('9900000001', '9900000001', '99205', '2023-02-01'::DATE, 20, 60, 60000.0),
            ('9900000001', '9900000001', '99213', '2023-03-01'::DATE, 10, 10, 5000.0),
            -- Normal E&M peers (same taxonomy+state TX, 208D00000X)
            ('9900000002', '9900000002', '99213', '2023-01-01'::DATE, 30, 80, 40000.0),
            ('9900000002', '9900000002', '99215', '2023-02-01'::DATE, 5, 10, 10000.0),
            ('9900000003', '9900000003', '99213', '2023-01-01'::DATE, 25, 70, 35000.0),
            ('9900000003', '9900000003', '99215', '2023-02-01'::DATE, 3, 5, 5000.0),
            ('9900000004', '9900000004', '99213', '2023-01-01'::DATE, 20, 60, 30000.0),

            -- Concurrent billing provider: individual billing across 6 different servicing NPIs in different states
            ('9800000001', '9810000001', '99213', '2023-06-01'::DATE, 10, 50, 5000.0),
            ('9800000001', '9810000002', '99213', '2023-06-01'::DATE, 10, 50, 5000.0),
            ('9800000001', '9810000003', '99213', '2023-06-01'::DATE, 10, 50, 5000.0),
            ('9800000001', '9810000004', '99213', '2023-06-01'::DATE, 10, 50, 5000.0),
            ('9800000001', '9810000005', '99213', '2023-06-01'::DATE, 10, 50, 5000.0),
            ('9800000001', '9810000006', '99213', '2023-06-01'::DATE, 10, 50, 5000.0),

            -- Multi-signal provider: hits both outlier and rapid escalation
            ('9700000001', '9700000001', '99213', '2023-01-01'::DATE, 5, 20, 800.0),
            ('9700000001', '9700000001', '99213', '2023-02-01'::DATE, 10, 50, 2000.0),
            ('9700000001', '9700000001', '99213', '2023-03-01'::DATE, 30, 200, 20000.0),
            ('9700000001', '9700000001', '99213', '2023-04-01'::DATE, 60, 500, 80000.0),
            ('9700000001', '9700000001', '99213', '2023-05-01'::DATE, 100, 900, 200000.0),
            ('9700000001', '9700000001', '99213', '2023-06-01'::DATE, 200, 2000, 500000.0),

            -- Signal 10: Burst Enrollment Network — 4 orgs registered same quarter, same taxonomy+state
            ('1010000001', '1010000001', '99213', '2023-01-01'::DATE, 20, 100, 200000.0),
            ('1010000002', '1010000002', '99213', '2023-01-01'::DATE, 15, 80, 180000.0),
            ('1010000003', '1010000003', '99213', '2023-01-01'::DATE, 12, 70, 150000.0),
            ('1010000004', '1010000004', '99213', '2023-01-01'::DATE, 10, 60, 130000.0),

            -- Signal 11: Coordinated Billing Ramp — 3 NPIs under same official, peaks in same window
            -- (uses shared official NPIs 6666666661-5 which already exist, but adding monthly data
            -- so they peak within a 3-month window with >$200K combined peak)
            ('6666666661', '6666666661', '99213', '2023-06-01'::DATE, 50, 200, 250000.0),
            ('6666666662', '6666666662', '99213', '2023-06-01'::DATE, 40, 180, 200000.0),
            ('6666666663', '6666666663', '99213', '2023-06-01'::DATE, 30, 150, 180000.0),

            -- Signal 12: Phantom Servicing Hub — one servicing NPI across 5+ billing entities
            ('1200000001', '1290000000', '99213', '2023-01-01'::DATE, 10, 50, 120000.0),
            ('1200000002', '1290000000', '99213', '2023-01-01'::DATE, 8, 40, 110000.0),
            ('1200000003', '1290000000', '99213', '2023-01-01'::DATE, 7, 35, 100000.0),
            ('1200000004', '1290000000', '99213', '2023-01-01'::DATE, 6, 30, 95000.0),
            ('1200000005', '1290000000', '99213', '2023-01-01'::DATE, 5, 25, 90000.0),

            -- Signal 13: Network Beneficiary Dilution — network with very high claims/bene ratio
            -- Uses 3 NPIs under same official JONES/MARY with absurd claims/bene ratios
            ('1300000001', '1300000001', '99213', '2023-01-01'::DATE, 3, 500, 250000.0),
            ('1300000002', '1300000002', '99213', '2023-01-01'::DATE, 2, 400, 200000.0),
            ('1300000003', '1300000003', '99213', '2023-01-01'::DATE, 2, 300, 150000.0),

            -- Signal 6 (rewritten): Geographic Implausibility — individual registered in WA
            -- but billing via servicing NPIs in CA, TX, FL (no home-state claims)
            ('7777777778', '7780000001', '99213', '2023-01-01'::DATE, 10, 200, 20000.0),
            ('7777777778', '7780000002', '99213', '2023-02-01'::DATE, 10, 200, 20000.0),
            ('7777777778', '7780000003', '99213', '2023-03-01'::DATE, 10, 200, 20000.0),

            -- Signal 14: Caregiver Density Anomaly — 6 individual home health providers
            -- in zip 55501 each serving 1-2 beneficiaries but billing very high amounts
            -- Total for zip: ~$600K (6 providers × ~$100K each = family caregiver fraud ring)
            ('1400000001', '1400000001', 'T1019', '2023-01-01'::DATE, 1, 200, 50000.0),
            ('1400000001', '1400000001', 'T1019', '2023-02-01'::DATE, 1, 210, 52000.0),
            ('1400000002', '1400000002', 'T1019', '2023-01-01'::DATE, 2, 220, 55000.0),
            ('1400000002', '1400000002', 'T1019', '2023-02-01'::DATE, 1, 200, 50000.0),
            ('1400000003', '1400000003', 'G0151', '2023-01-01'::DATE, 1, 180, 45000.0),
            ('1400000003', '1400000003', 'G0151', '2023-02-01'::DATE, 1, 190, 48000.0),
            ('1400000004', '1400000004', 'T1019', '2023-01-01'::DATE, 2, 210, 52000.0),
            ('1400000004', '1400000004', 'G0151', '2023-02-01'::DATE, 1, 180, 45000.0),
            ('1400000005', '1400000005', 'T1019', '2023-01-01'::DATE, 1, 220, 55000.0),
            ('1400000005', '1400000005', 'T1019', '2023-02-01'::DATE, 1, 200, 50000.0),
            ('1400000006', '1400000006', 'G0151', '2023-01-01'::DATE, 1, 190, 48000.0),
            ('1400000006', '1400000006', 'G0151', '2023-02-01'::DATE, 2, 200, 50000.0),
            -- Signal 14: Normal home health in zip 55502 — ~$112K, many beneficiaries per provider
            ('1400000010', '1400000010', 'T1019', '2023-01-01'::DATE, 25, 100, 35000.0),
            ('1400000010', '1400000010', 'T1019', '2023-02-01'::DATE, 20, 90, 30000.0),
            ('1400000011', '1400000011', 'T1019', '2023-01-01'::DATE, 20, 80, 25000.0),
            ('1400000011', '1400000011', 'G0151', '2023-02-01'::DATE, 18, 70, 22000.0),
            -- Signal 14: Normal home health in zip 55503 — ~$130K, many beneficiaries per provider
            ('1400000020', '1400000020', 'T1019', '2023-01-01'::DATE, 30, 100, 40000.0),
            ('1400000020', '1400000020', 'T1019', '2023-02-01'::DATE, 25, 90, 35000.0),
            ('1400000021', '1400000021', 'G0151', '2023-01-01'::DATE, 20, 80, 30000.0),
            ('1400000021', '1400000021', 'G0151', '2023-02-01'::DATE, 18, 70, 25000.0),

            -- Signal 15: Repetitive Service Abuse — NPI 1500000001 bills T1019 250x/bene
            ('1500000001', '1500000001', 'T1019', '2023-01-01'::DATE, 1, 250, 25000.0),
            ('1500000001', '1500000001', 'T1019', '2023-02-01'::DATE, 1, 250, 25000.0),
            -- Signal 15 peers: 10 normal T1019 billers with >200 claims, ~10 claims/bene
            ('1500000002', '1500000002', 'T1019', '2023-01-01'::DATE, 10, 110, 11000.0),
            ('1500000002', '1500000002', 'T1019', '2023-02-01'::DATE, 10, 110, 11000.0),
            ('1500000003', '1500000003', 'T1019', '2023-01-01'::DATE, 9, 105, 10500.0),
            ('1500000003', '1500000003', 'T1019', '2023-02-01'::DATE, 9, 105, 10500.0),
            ('1500000004', '1500000004', 'T1019', '2023-01-01'::DATE, 11, 115, 11500.0),
            ('1500000004', '1500000004', 'T1019', '2023-02-01'::DATE, 11, 115, 11500.0),
            ('1500000005', '1500000005', 'T1019', '2023-01-01'::DATE, 10, 105, 10500.0),
            ('1500000005', '1500000005', 'T1019', '2023-02-01'::DATE, 10, 105, 10500.0),
            ('1500000006', '1500000006', 'T1019', '2023-01-01'::DATE, 8, 110, 11000.0),
            ('1500000006', '1500000006', 'T1019', '2023-02-01'::DATE, 8, 110, 11000.0),
            ('1500000007', '1500000007', 'T1019', '2023-01-01'::DATE, 12, 120, 12000.0),
            ('1500000007', '1500000007', 'T1019', '2023-02-01'::DATE, 12, 120, 12000.0),
            ('1500000008', '1500000008', 'T1019', '2023-01-01'::DATE, 10, 110, 11000.0),
            ('1500000008', '1500000008', 'T1019', '2023-02-01'::DATE, 10, 110, 11000.0),
            ('1500000009', '1500000009', 'T1019', '2023-01-01'::DATE, 9, 105, 10500.0),
            ('1500000009', '1500000009', 'T1019', '2023-02-01'::DATE, 9, 105, 10500.0),
            ('1500000010', '1500000010', 'T1019', '2023-01-01'::DATE, 11, 115, 11500.0),
            ('1500000010', '1500000010', 'T1019', '2023-02-01'::DATE, 11, 115, 11500.0),
            ('1500000011', '1500000011', 'T1019', '2023-01-01'::DATE, 10, 110, 11000.0),
            ('1500000011', '1500000011', 'T1019', '2023-02-01'::DATE, 10, 110, 11000.0),

            -- Signal 16: Billing Monoculture — NPI 1600000001 bills 98% one code
            ('1600000001', '1600000001', '99215', '2023-01-01'::DATE, 20, 490, 245000.0),
            ('1600000001', '1600000001', '99215', '2023-02-01'::DATE, 10, 50, 25000.0),
            ('1600000001', '1600000001', '99213', '2023-01-01'::DATE, 5, 10, 3000.0),

            -- Signal 17: Bust-Out Pattern — NPI 1700000001 ramps then collapses
            ('1700000001', '1700000001', '99213', '2023-01-01'::DATE, 5, 30, 3000.0),
            ('1700000001', '1700000001', '99213', '2023-02-01'::DATE, 10, 80, 8000.0),
            ('1700000001', '1700000001', '99213', '2023-03-01'::DATE, 20, 200, 20000.0),
            ('1700000001', '1700000001', '99213', '2023-04-01'::DATE, 40, 450, 45000.0),
            ('1700000001', '1700000001', '99213', '2023-05-01'::DATE, 60, 700, 70000.0),
            ('1700000001', '1700000001', '99213', '2023-06-01'::DATE, 80, 1200, 120000.0),
            ('1700000001', '1700000001', '99213', '2023-07-01'::DATE, 2, 5, 500.0),
            ('1700000001', '1700000001', '99213', '2023-08-01'::DATE, 1, 3, 300.0),
            ('1700000001', '1700000001', '99213', '2023-09-01'::DATE, 1, 2, 200.0),

            -- Signal 18: Reimbursement Rate Anomaly — NPI 1800000001 bills 99214 at 5x rate
            -- Also need 10+ normal-rate 99214 peers
            ('1800000001', '1800000001', '99214', '2023-01-01'::DATE, 30, 100, 50000.0),
            ('1800000001', '1800000001', '99214', '2023-02-01'::DATE, 30, 100, 50000.0),
            ('1800000002', '1800000002', '99214', '2023-01-01'::DATE, 20, 120, 12000.0),
            ('1800000003', '1800000003', '99214', '2023-01-01'::DATE, 18, 110, 11000.0),
            ('1800000004', '1800000004', '99214', '2023-01-01'::DATE, 22, 130, 13000.0),
            ('1800000005', '1800000005', '99214', '2023-01-01'::DATE, 15, 105, 10500.0),
            ('1800000006', '1800000006', '99214', '2023-01-01'::DATE, 20, 115, 11500.0),
            ('1800000007', '1800000007', '99214', '2023-01-01'::DATE, 17, 108, 10800.0),
            ('1800000008', '1800000008', '99214', '2023-01-01'::DATE, 19, 112, 11200.0),
            ('1800000009', '1800000009', '99214', '2023-01-01'::DATE, 16, 102, 10200.0),
            ('1800000010', '1800000010', '99214', '2023-01-01'::DATE, 21, 125, 12500.0),
            ('1800000011', '1800000011', '99214', '2023-01-01'::DATE, 18, 118, 11800.0),

            -- Signal 19: Phantom Servicing Spread — hub 1900000000 serves 6 billers, 3 benes
            ('1900000001', '1900000000', '99213', '2023-01-01'::DATE, 1, 100, 40000.0),
            ('1900000002', '1900000000', '99213', '2023-01-01'::DATE, 1, 100, 40000.0),
            ('1900000003', '1900000000', '99213', '2023-01-01'::DATE, 1, 100, 40000.0),
            ('1900000004', '1900000000', '99213', '2023-01-01'::DATE, 0, 100, 40000.0),
            ('1900000005', '1900000000', '99213', '2023-01-01'::DATE, 0, 100, 40000.0),
            ('1900000006', '1900000000', '99213', '2023-01-01'::DATE, 0, 100, 40000.0)

        ) AS t(billing_npi, servicing_npi, hcpcs_code, claim_month, unique_beneficiaries, total_claims, total_paid)
    """)

    # Create a view alias for spending
    c.execute("CREATE VIEW spending_view AS SELECT * FROM spending")

    # Create LEIE table
    c.execute("""
        CREATE TABLE leie AS
        SELECT * FROM (VALUES
            ('DOE', 'JOHN', '', '', '', '', '', '2222222222', '', '', '', 'NY', '', '1422a4', '20220101', '', '', '')
        ) AS t(lastname, firstname, midname, busname, general, specialty, upin, npi, dob, address, city, state, zip, excl_type, excl_date_raw, rein_date_raw, waiverdate, wvrstate)
    """)
    # Add parsed dates
    c.execute("""
        ALTER TABLE leie ADD COLUMN excl_date DATE;
        UPDATE leie SET excl_date = TRY_STRPTIME(excl_date_raw, '%Y%m%d');
        ALTER TABLE leie ADD COLUMN rein_date DATE;
        UPDATE leie SET rein_date = CASE WHEN LENGTH(TRIM(rein_date_raw)) = 8 THEN TRY_STRPTIME(rein_date_raw, '%Y%m%d') ELSE NULL END;
    """)

    # Create NPPES table
    c.execute("""
        CREATE TABLE nppes AS
        SELECT * FROM (VALUES
            ('1111111111', '1', NULL, 'Smith', 'Jane', 'CA', '90210', '207Q00000X', '2015-01-15'::DATE, NULL, NULL),
            ('2222222222', '1', NULL, 'Doe', 'John', 'NY', '10001', '207Q00000X', '2018-01-01'::DATE, NULL, NULL),
            ('3333333333', '1', NULL, 'Mega', 'Provider', 'CA', '90210', '207Q00000X', '2010-01-01'::DATE, NULL, NULL),
            ('3333333334', '1', NULL, 'Normal1', 'Doc', 'CA', '90211', '207Q00000X', '2010-01-01'::DATE, NULL, NULL),
            ('3333333335', '1', NULL, 'Normal2', 'Doc', 'CA', '90212', '207Q00000X', '2010-01-01'::DATE, NULL, NULL),
            ('3333333336', '1', NULL, 'Normal3', 'Doc', 'CA', '90213', '207Q00000X', '2010-01-01'::DATE, NULL, NULL),
            ('3333333337', '1', NULL, 'Normal4', 'Doc', 'CA', '90214', '207Q00000X', '2010-01-01'::DATE, NULL, NULL),
            ('3333333338', '1', NULL, 'Normal5', 'Doc', 'CA', '90215', '207Q00000X', '2010-01-01'::DATE, NULL, NULL),
            ('4444444444', '1', NULL, 'Fast', 'Grower', 'TX', '75001', '208000000X', '2022-11-01'::DATE, NULL, NULL),
            ('5555555555', '2', 'MegaCorp Health', NULL, NULL, 'FL', '33101', '251S00000X', '2015-01-01'::DATE, NULL, NULL),
            ('6666666661', '2', 'Shell Corp 1', NULL, NULL, 'NJ', '07001', '261QM1200X', '2018-01-01'::DATE, 'SMITH', 'ROBERT'),
            ('6666666662', '2', 'Shell Corp 2', NULL, NULL, 'NJ', '07002', '261QM1200X', '2018-06-01'::DATE, 'SMITH', 'ROBERT'),
            ('6666666663', '2', 'Shell Corp 3', NULL, NULL, 'NJ', '07003', '261QM1200X', '2019-01-01'::DATE, 'SMITH', 'ROBERT'),
            ('6666666664', '2', 'Shell Corp 4', NULL, NULL, 'NJ', '07004', '261QM1200X', '2019-06-01'::DATE, 'SMITH', 'ROBERT'),
            ('6666666665', '2', 'Shell Corp 5', NULL, NULL, 'NJ', '07005', '261QM1200X', '2020-01-01'::DATE, 'SMITH', 'ROBERT'),
            ('7777777777', '2', 'Home Health LLC', NULL, NULL, 'PA', '19101', '251E00000X', '2016-01-01'::DATE, NULL, NULL),
            -- Address clustering: 10 providers at same zip 11111
            ('8800000001', '2', 'Cluster Corp 1', NULL, NULL, 'NY', '11111', '207Q00000X', '2017-01-01'::DATE, NULL, NULL),
            ('8800000002', '2', 'Cluster Corp 2', NULL, NULL, 'NY', '11111', '207Q00000X', '2017-02-01'::DATE, NULL, NULL),
            ('8800000003', '2', 'Cluster Corp 3', NULL, NULL, 'NY', '11111', '207Q00000X', '2017-03-01'::DATE, NULL, NULL),
            ('8800000004', '2', 'Cluster Corp 4', NULL, NULL, 'NY', '11111', '207Q00000X', '2017-04-01'::DATE, NULL, NULL),
            ('8800000005', '2', 'Cluster Corp 5', NULL, NULL, 'NY', '11111', '207Q00000X', '2017-05-01'::DATE, NULL, NULL),
            ('8800000006', '2', 'Cluster Corp 6', NULL, NULL, 'NY', '11111', '207Q00000X', '2017-06-01'::DATE, NULL, NULL),
            ('8800000007', '2', 'Cluster Corp 7', NULL, NULL, 'NY', '11111', '207Q00000X', '2017-07-01'::DATE, NULL, NULL),
            ('8800000008', '2', 'Cluster Corp 8', NULL, NULL, 'NY', '11111', '207Q00000X', '2017-08-01'::DATE, NULL, NULL),
            ('8800000009', '2', 'Cluster Corp 9', NULL, NULL, 'NY', '11111', '207Q00000X', '2017-09-01'::DATE, NULL, NULL),
            ('8800000010', '2', 'Cluster Corp 10', NULL, NULL, 'NY', '11111', '207Q00000X', '2017-10-01'::DATE, NULL, NULL),
            -- Upcoding provider + peers (taxonomy 208D00000X, state TX)
            ('9900000001', '1', NULL, 'Upcoder', 'Max', 'TX', '77001', '208D00000X', '2015-01-01'::DATE, NULL, NULL),
            ('9900000002', '1', NULL, 'Normal6', 'Doc', 'TX', '77002', '208D00000X', '2015-01-01'::DATE, NULL, NULL),
            ('9900000003', '1', NULL, 'Normal7', 'Doc', 'TX', '77003', '208D00000X', '2015-01-01'::DATE, NULL, NULL),
            ('9900000004', '1', NULL, 'Normal8', 'Doc', 'TX', '77004', '208D00000X', '2015-01-01'::DATE, NULL, NULL),
            -- Concurrent billing: individual provider + 6 servicing NPIs in different states
            ('9800000001', '1', NULL, 'Multi', 'State', 'NY', '10001', '207Q00000X', '2015-01-01'::DATE, NULL, NULL),
            ('9810000001', '1', NULL, 'Serv1', 'Doc', 'NY', '10001', '207Q00000X', '2015-01-01'::DATE, NULL, NULL),
            ('9810000002', '1', NULL, 'Serv2', 'Doc', 'CA', '90001', '207Q00000X', '2015-01-01'::DATE, NULL, NULL),
            ('9810000003', '1', NULL, 'Serv3', 'Doc', 'TX', '75001', '207Q00000X', '2015-01-01'::DATE, NULL, NULL),
            ('9810000004', '1', NULL, 'Serv4', 'Doc', 'FL', '33001', '207Q00000X', '2015-01-01'::DATE, NULL, NULL),
            ('9810000005', '1', NULL, 'Serv5', 'Doc', 'IL', '60001', '207Q00000X', '2015-01-01'::DATE, NULL, NULL),
            ('9810000006', '1', NULL, 'Serv6', 'Doc', 'PA', '19001', '207Q00000X', '2015-01-01'::DATE, NULL, NULL),
            -- Multi-signal provider (rapidly escalating + potentially outlier)
            ('9700000001', '1', NULL, 'Multi', 'Signal', 'CA', '90210', '207Q00000X', '2022-11-01'::DATE, NULL, NULL),
            -- Signal 10: Burst enrollment — 4 orgs same taxonomy(261QR0400X)+state(OH)+quarter(Q1 2023)
            ('1010000001', '2', 'Burst Corp 1', NULL, NULL, 'OH', '44101', '261QR0400X', '2023-01-15'::DATE, NULL, NULL),
            ('1010000002', '2', 'Burst Corp 2', NULL, NULL, 'OH', '44102', '261QR0400X', '2023-02-01'::DATE, NULL, NULL),
            ('1010000003', '2', 'Burst Corp 3', NULL, NULL, 'OH', '44103', '261QR0400X', '2023-02-15'::DATE, NULL, NULL),
            ('1010000004', '2', 'Burst Corp 4', NULL, NULL, 'OH', '44104', '261QR0400X', '2023-03-01'::DATE, NULL, NULL),
            -- Signal 12: Phantom servicing hub — the hub NPI and the 5 billing NPIs
            ('1290000000', '1', NULL, 'Hub', 'Phantom', 'TX', '75001', '207Q00000X', '2015-01-01'::DATE, NULL, NULL),
            ('1200000001', '2', 'Hub Client 1', NULL, NULL, 'TX', '75002', '207Q00000X', '2018-01-01'::DATE, NULL, NULL),
            ('1200000002', '2', 'Hub Client 2', NULL, NULL, 'TX', '75003', '207Q00000X', '2018-02-01'::DATE, NULL, NULL),
            ('1200000003', '2', 'Hub Client 3', NULL, NULL, 'TX', '75004', '207Q00000X', '2018-03-01'::DATE, NULL, NULL),
            ('1200000004', '2', 'Hub Client 4', NULL, NULL, 'TX', '75005', '207Q00000X', '2018-04-01'::DATE, NULL, NULL),
            ('1200000005', '2', 'Hub Client 5', NULL, NULL, 'TX', '75006', '207Q00000X', '2018-05-01'::DATE, NULL, NULL),
            -- Signal 13: Beneficiary dilution — 3 NPIs under same official JONES MARY
            ('1300000001', '2', 'Dilution Corp 1', NULL, NULL, 'GA', '30301', '251E00000X', '2019-01-01'::DATE, 'JONES', 'MARY'),
            ('1300000002', '2', 'Dilution Corp 2', NULL, NULL, 'GA', '30302', '251E00000X', '2019-02-01'::DATE, 'JONES', 'MARY'),
            ('1300000003', '2', 'Dilution Corp 3', NULL, NULL, 'GA', '30303', '251E00000X', '2019-03-01'::DATE, 'JONES', 'MARY'),
            -- Signal 6 (geographic): individual registered in WA, services in CA/TX/FL
            ('7777777778', '1', NULL, 'Geo', 'Fraud', 'WA', '98101', '207Q00000X', '2015-01-01'::DATE, NULL, NULL),
            ('7780000001', '1', NULL, 'ServCA', 'Doc', 'CA', '90001', '207Q00000X', '2015-01-01'::DATE, NULL, NULL),
            ('7780000002', '1', NULL, 'ServTX', 'Doc', 'TX', '75001', '207Q00000X', '2015-01-01'::DATE, NULL, NULL),
            ('7780000003', '1', NULL, 'ServFL', 'Doc', 'FL', '33001', '207Q00000X', '2015-01-01'::DATE, NULL, NULL),
            -- Signal 14: Caregiver density — 6 individual PCA providers in zip 55501 (MN)
            ('1400000001', '1', NULL, 'Caregiver1', 'Family', 'MN', '55501', '374700000X', '2018-01-01'::DATE, NULL, NULL),
            ('1400000002', '1', NULL, 'Caregiver2', 'Family', 'MN', '55501', '374700000X', '2018-02-01'::DATE, NULL, NULL),
            ('1400000003', '1', NULL, 'Caregiver3', 'Family', 'MN', '55501', '374700000X', '2018-03-01'::DATE, NULL, NULL),
            ('1400000004', '1', NULL, 'Caregiver4', 'Family', 'MN', '55501', '374700000X', '2018-04-01'::DATE, NULL, NULL),
            ('1400000005', '1', NULL, 'Caregiver5', 'Family', 'MN', '55501', '374700000X', '2018-05-01'::DATE, NULL, NULL),
            ('1400000006', '1', NULL, 'Caregiver6', 'Family', 'MN', '55501', '374700000X', '2018-06-01'::DATE, NULL, NULL),
            -- Signal 14: Normal home health providers in zip 55502 (MN)
            ('1400000010', '1', NULL, 'NormalHH1', 'Doc', 'MN', '55502', '374700000X', '2015-01-01'::DATE, NULL, NULL),
            ('1400000011', '1', NULL, 'NormalHH2', 'Doc', 'MN', '55502', '374700000X', '2015-01-01'::DATE, NULL, NULL),
            ('1400000012', '1', NULL, 'NormalHH3', 'Doc', 'MN', '55502', '374700000X', '2015-01-01'::DATE, NULL, NULL),
            -- Signal 14: Normal home health in zip 55503 (MN) — 3rd zip for state median
            ('1400000020', '1', NULL, 'NormalHH4', 'Doc', 'MN', '55503', '374700000X', '2015-01-01'::DATE, NULL, NULL),
            ('1400000021', '1', NULL, 'NormalHH5', 'Doc', 'MN', '55503', '374700000X', '2015-01-01'::DATE, NULL, NULL),
            -- Signal 15: Repetitive service abuse NPI + 10 peers
            ('1500000001', '1', NULL, 'Repetitive', 'Biller', 'MN', '55501', '374700000X', '2018-01-01'::DATE, NULL, NULL),
            ('1500000002', '1', NULL, 'Peer1', 'T1019', 'MN', '55502', '374700000X', '2015-01-01'::DATE, NULL, NULL),
            ('1500000003', '1', NULL, 'Peer2', 'T1019', 'MN', '55503', '374700000X', '2015-01-01'::DATE, NULL, NULL),
            ('1500000004', '1', NULL, 'Peer3', 'T1019', 'MN', '55504', '374700000X', '2015-01-01'::DATE, NULL, NULL),
            ('1500000005', '1', NULL, 'Peer4', 'T1019', 'MN', '55505', '374700000X', '2015-01-01'::DATE, NULL, NULL),
            ('1500000006', '1', NULL, 'Peer5', 'T1019', 'MN', '55506', '374700000X', '2015-01-01'::DATE, NULL, NULL),
            ('1500000007', '1', NULL, 'Peer6', 'T1019', 'MN', '55507', '374700000X', '2015-01-01'::DATE, NULL, NULL),
            ('1500000008', '1', NULL, 'Peer7', 'T1019', 'MN', '55508', '374700000X', '2015-01-01'::DATE, NULL, NULL),
            ('1500000009', '1', NULL, 'Peer8', 'T1019', 'MN', '55509', '374700000X', '2015-01-01'::DATE, NULL, NULL),
            ('1500000010', '1', NULL, 'Peer9', 'T1019', 'MN', '55510', '374700000X', '2015-01-01'::DATE, NULL, NULL),
            ('1500000011', '1', NULL, 'Peer10', 'T1019', 'MN', '55511', '374700000X', '2015-01-01'::DATE, NULL, NULL),
            -- Signal 16: Billing monoculture
            ('1600000001', '1', NULL, 'Monoculture', 'Provider', 'IL', '60601', '208100000X', '2015-01-01'::DATE, NULL, NULL),
            -- Signal 17: Bust-out
            ('1700000001', '1', NULL, 'Bustout', 'Provider', 'TX', '75001', '207Q00000X', '2022-01-01'::DATE, NULL, NULL),
            -- Signal 18: Rate anomaly NPI + 10 normal-rate peers
            ('1800000001', '1', NULL, 'RateAnomaly', 'Provider', 'CA', '90210', '207Q00000X', '2015-01-01'::DATE, NULL, NULL),
            ('1800000002', '1', NULL, 'RatePeer1', 'Doc', 'CA', '90211', '207Q00000X', '2015-01-01'::DATE, NULL, NULL),
            ('1800000003', '1', NULL, 'RatePeer2', 'Doc', 'CA', '90212', '207Q00000X', '2015-01-01'::DATE, NULL, NULL),
            ('1800000004', '1', NULL, 'RatePeer3', 'Doc', 'CA', '90213', '207Q00000X', '2015-01-01'::DATE, NULL, NULL),
            ('1800000005', '1', NULL, 'RatePeer4', 'Doc', 'CA', '90214', '207Q00000X', '2015-01-01'::DATE, NULL, NULL),
            ('1800000006', '1', NULL, 'RatePeer5', 'Doc', 'CA', '90215', '207Q00000X', '2015-01-01'::DATE, NULL, NULL),
            ('1800000007', '1', NULL, 'RatePeer6', 'Doc', 'CA', '90216', '207Q00000X', '2015-01-01'::DATE, NULL, NULL),
            ('1800000008', '1', NULL, 'RatePeer7', 'Doc', 'CA', '90217', '207Q00000X', '2015-01-01'::DATE, NULL, NULL),
            ('1800000009', '1', NULL, 'RatePeer8', 'Doc', 'CA', '90218', '207Q00000X', '2015-01-01'::DATE, NULL, NULL),
            ('1800000010', '1', NULL, 'RatePeer9', 'Doc', 'CA', '90219', '207Q00000X', '2015-01-01'::DATE, NULL, NULL),
            ('1800000011', '1', NULL, 'RatePeer10', 'Doc', 'CA', '90220', '207Q00000X', '2015-01-01'::DATE, NULL, NULL),
            -- Signal 19: Phantom servicing spread hub + 6 billing entities
            ('1900000000', '1', NULL, 'SpreadHub', 'Phantom', 'TX', '75001', '207Q00000X', '2015-01-01'::DATE, NULL, NULL),
            ('1900000001', '2', 'Spread Client 1', NULL, NULL, 'TX', '75002', '207Q00000X', '2018-01-01'::DATE, NULL, NULL),
            ('1900000002', '2', 'Spread Client 2', NULL, NULL, 'TX', '75003', '207Q00000X', '2018-02-01'::DATE, NULL, NULL),
            ('1900000003', '2', 'Spread Client 3', NULL, NULL, 'TX', '75004', '207Q00000X', '2018-03-01'::DATE, NULL, NULL),
            ('1900000004', '2', 'Spread Client 4', NULL, NULL, 'TX', '75005', '207Q00000X', '2018-04-01'::DATE, NULL, NULL),
            ('1900000005', '2', 'Spread Client 5', NULL, NULL, 'TX', '75006', '207Q00000X', '2018-05-01'::DATE, NULL, NULL),
            ('1900000006', '2', 'Spread Client 6', NULL, NULL, 'TX', '75007', '207Q00000X', '2018-06-01'::DATE, NULL, NULL)
        ) AS t(npi, entity_type_code, org_name, last_name, first_name, state, zip_code, taxonomy_code, enumeration_date, auth_official_last, auth_official_first)
    """)

    # Pre-compute materialized aggregation tables (matches production ingest)
    c.execute("""
        CREATE TABLE provider_totals AS
        SELECT
            billing_npi AS npi,
            SUM(total_paid) AS total_paid,
            SUM(total_claims) AS total_claims,
            SUM(unique_beneficiaries) AS total_beneficiaries
        FROM spending
        GROUP BY billing_npi
    """)

    c.execute("""
        CREATE TABLE provider_code_totals AS
        SELECT
            billing_npi AS npi,
            hcpcs_code,
            SUM(total_paid) AS total_paid,
            SUM(total_claims) AS total_claims,
            SUM(unique_beneficiaries) AS total_beneficiaries
        FROM spending
        GROUP BY billing_npi, hcpcs_code
    """)

    c.execute("""
        CREATE TABLE provider_monthly AS
        SELECT
            billing_npi AS npi,
            claim_month,
            SUM(total_paid) AS month_paid,
            SUM(total_claims) AS month_claims,
            SUM(unique_beneficiaries) AS month_beneficiaries
        FROM spending
        GROUP BY billing_npi, claim_month
    """)

    c.execute("""
        CREATE TABLE spending_em AS
        SELECT billing_npi, hcpcs_code, total_claims, total_paid
        FROM spending
        WHERE hcpcs_code IN (
            '99201','99202','99203','99204','99205',
            '99211','99212','99213','99214','99215',
            '99221','99222','99223',
            '99231','99232','99233',
            '99241','99242','99243','99244','99245',
            '99251','99252','99253','99254','99255'
        )
    """)

    c.execute("""
        CREATE TABLE spending_hh AS
        SELECT billing_npi, hcpcs_code, claim_month,
               total_claims, unique_beneficiaries, total_paid
        FROM spending
        WHERE hcpcs_code IN (
            'G0151','G0152','G0153','G0154','G0155','G0156','G0157','G0158',
            'G0159','G0160','G0161','G0162','G0299','G0300',
            'S9122','S9123','S9124','T1019','T1020','T1021','T1022'
        )
    """)

    c.execute("""
        CREATE TABLE org_worker_monthly AS
        SELECT
            s.billing_npi AS npi,
            s.claim_month,
            COUNT(DISTINCT s.servicing_npi) AS distinct_workers
        FROM spending s
        JOIN nppes n ON s.billing_npi = n.npi
        WHERE n.entity_type_code = '2'
          AND s.servicing_npi IS NOT NULL
          AND TRIM(s.servicing_npi) != ''
        GROUP BY s.billing_npi, s.claim_month
    """)

    c.execute("""
        CREATE TABLE servicing_hub_totals AS
        SELECT
            servicing_npi,
            billing_npi,
            SUM(total_paid) AS total_paid,
            SUM(total_claims) AS total_claims,
            SUM(unique_beneficiaries) AS total_beneficiaries
        FROM spending
        WHERE servicing_npi IS NOT NULL
          AND servicing_npi != billing_npi
        GROUP BY servicing_npi, billing_npi
    """)

    # Signal 14: hh_zip_totals — pre-materialized home health zip aggregation
    c.execute("""
        CREATE TABLE hh_zip_totals AS
        SELECT
            n.zip_code,
            n.state,
            n.npi,
            n.entity_type_code,
            COALESCE(n.org_name, n.first_name || ' ' || n.last_name) AS provider_name,
            SUM(sh.total_paid) AS hh_paid,
            SUM(sh.total_claims) AS hh_claims,
            SUM(sh.unique_beneficiaries) AS hh_beneficiaries
        FROM spending_hh sh
        JOIN nppes n ON sh.billing_npi = n.npi
        WHERE n.zip_code IS NOT NULL AND TRIM(n.zip_code) != ''
        GROUP BY n.zip_code, n.state, n.npi, n.entity_type_code,
                 COALESCE(n.org_name, n.first_name || ' ' || n.last_name)
    """)

    c.execute("""
        CREATE TABLE serv_state_monthly AS
        SELECT
            s.billing_npi AS npi,
            s.claim_month,
            n.state,
            SUM(s.total_paid) AS month_paid,
            SUM(s.total_claims) AS month_claims
        FROM spending s
        JOIN nppes n ON s.servicing_npi = n.npi
        WHERE n.state IS NOT NULL AND TRIM(n.state) != ''
        GROUP BY s.billing_npi, s.claim_month, n.state
    """)

    # Census ZCTA demographics for signal 14 enrichment
    c.execute("""
        CREATE TABLE census_zcta AS
        SELECT * FROM (VALUES
            ('55501', 12000, 1800, 1200, 3600),
            ('55502', 45000, 8100, 5400, 9000),
            ('55503', 30000, 5500, 3000, 6000)
        ) AS t(zcta, total_population, population_65_plus, disability_count, poverty_count)
    """)

    return c
