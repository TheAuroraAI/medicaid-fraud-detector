"""Data loading and joining module using DuckDB for memory-efficient processing."""

import os
import glob
import duckdb


def _auto_memory_limit() -> str:
    """Detect system RAM and return ~50% as DuckDB memory limit."""
    try:
        import subprocess
        result = subprocess.run(
            ["sysctl", "-n", "hw.memsize"], capture_output=True, text=True
        )
        total_bytes = int(result.stdout.strip())
        limit_gb = max(2, total_bytes * 3 // (1024 ** 3) // 4)  # 75% of RAM
        return f"{limit_gb}GB"
    except Exception:
        try:
            with open("/proc/meminfo") as f:
                for line in f:
                    if line.startswith("MemTotal:"):
                        kb = int(line.split()[1])
                        limit_gb = max(2, kb * 3 // (1024 * 1024) // 4)  # 75% of RAM
                        return f"{limit_gb}GB"
        except Exception:
            pass
    return "2GB"


def _auto_threads() -> int:
    """Detect available CPU cores."""
    try:
        return os.cpu_count() or 4
    except Exception:
        return 4


def get_connection(data_dir: str, memory_limit: str = "2GB") -> duckdb.DuckDBPyConnection:
    """Create a DuckDB connection with appropriate settings for large data."""
    con = duckdb.connect(":memory:")
    effective_memory = memory_limit if memory_limit != "2GB" else _auto_memory_limit()
    effective_threads = _auto_threads()
    con.execute(f"SET memory_limit = '{effective_memory}'")
    con.execute(f"SET threads = {effective_threads}")
    con.execute("SET enable_progress_bar = true")
    con.execute("SET enable_object_cache = true")
    con.execute("SET preserve_insertion_order = false")
    print(f"  DuckDB config: memory_limit={effective_memory}, threads={effective_threads}")
    return con


def load_spending(con: duckdb.DuckDBPyConnection, data_dir: str) -> None:
    """Load Medicaid spending parquet as a materialized table.

    Materializing avoids re-reading and re-parsing the 2.7GB parquet
    file on every signal query.  TRY_STRPTIME runs once at load time.
    """
    path = os.path.join(data_dir, "medicaid_spending.parquet")
    if not os.path.exists(path):
        raise FileNotFoundError(f"Spending data not found: {path}")
    con.execute(f"""
        CREATE TABLE spending AS
        SELECT
            BILLING_PROVIDER_NPI_NUM AS billing_npi,
            SERVICING_PROVIDER_NPI_NUM AS servicing_npi,
            HCPCS_CODE AS hcpcs_code,
            TRY_STRPTIME(CLAIM_FROM_MONTH, '%Y-%m') AS claim_month,
            TOTAL_UNIQUE_BENEFICIARIES AS unique_beneficiaries,
            TOTAL_CLAIMS AS total_claims,
            TOTAL_PAID AS total_paid
        FROM read_parquet('{path}')
    """)


def load_leie(con: duckdb.DuckDBPyConnection, data_dir: str) -> None:
    """Load OIG LEIE exclusion list."""
    path = os.path.join(data_dir, "LEIE.csv")
    if not os.path.exists(path):
        raise FileNotFoundError(f"LEIE data not found: {path}")
    con.execute(f"""
        CREATE TABLE leie AS
        SELECT
            LASTNAME AS lastname,
            FIRSTNAME AS firstname,
            MIDNAME AS midname,
            BUSNAME AS busname,
            NPI AS npi,
            STATE AS state,
            EXCLTYPE AS excl_type,
            EXCLDATE AS excl_date_raw,
            REINDATE AS rein_date_raw,
            CASE
                WHEN LENGTH(TRIM(EXCLDATE)) = 8
                THEN TRY_STRPTIME(EXCLDATE, '%Y%m%d')
                ELSE NULL
            END AS excl_date,
            CASE
                WHEN LENGTH(TRIM(REINDATE)) = 8
                THEN TRY_STRPTIME(REINDATE, '%Y%m%d')
                ELSE NULL
            END AS rein_date
        FROM read_csv('{path}', header=true, auto_detect=true, all_varchar=true)
    """)


def load_nppes(con: duckdb.DuckDBPyConnection, data_dir: str) -> None:
    """Load NPPES NPI Registry (only required columns).

    Supports three data sources in priority order:
    1. Pre-built slim parquet (nppes/ subdirectory or nppes_slim.parquet)
    2. Full NPPES CSV (npidata_pfile_*.csv)
    3. Streaming extraction from NPPES.zip (avoids 11GB disk extraction)
    """
    # Option 1: Slim parquet (fastest, smallest)
    slim_parquet = os.path.join(data_dir, "nppes", "nppes_slim.parquet")
    if not os.path.exists(slim_parquet):
        slim_parquet = os.path.join(data_dir, "nppes_slim.parquet")

    if os.path.exists(slim_parquet):
        print(f"  Using slim parquet: {slim_parquet}")
        con.execute(f"""
            CREATE TABLE nppes AS
            SELECT
                NPI AS npi,
                entity_type_code,
                org_name,
                last_name,
                first_name,
                state,
                postal_code AS zip_code,
                taxonomy_code,
                CASE
                    WHEN enumeration_date IS NOT NULL AND LENGTH(TRIM(enumeration_date)) >= 8
                    THEN TRY_STRPTIME(enumeration_date, '%m/%d/%Y')
                    ELSE NULL
                END AS enumeration_date,
                auth_official_last_name AS auth_official_last,
                auth_official_first_name AS auth_official_first
            FROM read_parquet('{slim_parquet}')
        """)
        return

    # Option 2: Full CSV
    pattern = os.path.join(data_dir, "npidata_pfile_*.csv")
    matches = glob.glob(pattern)
    if matches:
        path = matches[0]
        print(f"  Using full CSV: {path}")
        con.execute(f"""
            CREATE TABLE nppes AS
            SELECT
                "NPI" AS npi,
                "Entity Type Code" AS entity_type_code,
                "Provider Organization Name (Legal Business Name)" AS org_name,
                "Provider Last Name (Legal Name)" AS last_name,
                "Provider First Name" AS first_name,
                "Provider Business Practice Location Address State Name" AS state,
                "Provider Business Practice Location Address Postal Code" AS zip_code,
                "Healthcare Provider Taxonomy Code_1" AS taxonomy_code,
                CASE
                    WHEN LENGTH(TRIM("Provider Enumeration Date")) >= 10
                    THEN TRY_STRPTIME("Provider Enumeration Date", '%m/%d/%Y')
                    ELSE NULL
                END AS enumeration_date,
                "Authorized Official Last Name" AS auth_official_last,
                "Authorized Official First Name" AS auth_official_first
            FROM read_csv('{path}',
                header=true,
                auto_detect=true,
                all_varchar=true,
                columns={{
                    'NPI': 'VARCHAR',
                    'Entity Type Code': 'VARCHAR',
                    'Provider Organization Name (Legal Business Name)': 'VARCHAR',
                    'Provider Last Name (Legal Name)': 'VARCHAR',
                    'Provider First Name': 'VARCHAR',
                    'Provider Business Practice Location Address State Name': 'VARCHAR',
                    'Provider Business Practice Location Address Postal Code': 'VARCHAR',
                    'Healthcare Provider Taxonomy Code_1': 'VARCHAR',
                    'Provider Enumeration Date': 'VARCHAR',
                    'Authorized Official Last Name': 'VARCHAR',
                    'Authorized Official First Name': 'VARCHAR'
                }}
            )
        """)
        return

    # Option 3: Stream from zip
    zip_path = os.path.join(data_dir, "NPPES.zip")
    if os.path.exists(zip_path):
        print(f"  Building slim parquet from zip (saves disk space)...")
        slim_out = os.path.join(data_dir, "nppes_slim.parquet")
        _build_slim_parquet_from_zip(zip_path, slim_out)
        load_nppes(con, data_dir)  # Recurse — will find slim parquet
        return

    raise FileNotFoundError(
        f"NPPES data not found. Provide one of: "
        f"nppes/nppes_slim.parquet, npidata_pfile_*.csv, or NPPES.zip in {data_dir}"
    )


def _build_slim_parquet_from_zip(zip_path: str, output_path: str) -> None:
    """Extract only needed columns from NPPES zip into a slim parquet file."""
    import subprocess
    import tempfile

    # Stream CSV from zip, pipe to DuckDB for column selection
    con = duckdb.connect(":memory:")
    con.execute("SET memory_limit = '1GB'")

    # Use unzip -p to pipe CSV to stdout, then DuckDB reads it
    # DuckDB can read from a named pipe or temporary file
    with tempfile.NamedTemporaryFile(suffix=".csv", delete=False) as tmp:
        tmp_path = tmp.name

    try:
        # Find the CSV filename inside the zip
        result = subprocess.run(
            ["unzip", "-l", zip_path],
            capture_output=True, text=True
        )
        csv_name = None
        for line in result.stdout.split("\n"):
            if "npidata_pfile_" in line and line.strip().endswith(".csv"):
                csv_name = line.strip().split()[-1]
                break

        if not csv_name:
            raise FileNotFoundError("npidata_pfile CSV not found in zip")

        # Extract to slim parquet using DuckDB pipe read
        # This approach streams without full extraction
        print(f"    Extracting {csv_name} → slim parquet...")
        subprocess.run(
            ["unzip", "-p", zip_path, csv_name],
            stdout=open(tmp_path, "w"),
            check=True,
        )

        con.execute(f"""
            COPY (
                SELECT
                    "NPI",
                    "Entity Type Code" AS entity_type_code,
                    "Provider Organization Name (Legal Business Name)" AS org_name,
                    "Provider Last Name (Legal Name)" AS last_name,
                    "Provider First Name" AS first_name,
                    "Provider Business Practice Location Address State Name" AS state,
                    "Provider Business Practice Location Address Postal Code" AS postal_code,
                    "Healthcare Provider Taxonomy Code_1" AS taxonomy_code,
                    "Provider Enumeration Date" AS enumeration_date,
                    "Authorized Official Last Name" AS auth_official_last_name,
                    "Authorized Official First Name" AS auth_official_first_name
                FROM read_csv('{tmp_path}', header=true, auto_detect=true, all_varchar=true)
            ) TO '{output_path}' (FORMAT PARQUET, COMPRESSION ZSTD)
        """)
        print(f"    Slim parquet saved: {output_path}")
    finally:
        os.unlink(tmp_path)
        con.close()


def _materialize_aggregations(con: duckdb.DuckDBPyConnection) -> None:
    """Pre-compute aggregation tables reused by multiple signals.

    provider_totals  — per-NPI billing totals (signals 2, 5, 7, report output)
    provider_monthly — per-NPI per-month totals (signals 3, 4)
    spending_em      — E&M code subset of spending (signal 8 upcoding)
    spending_hh      — home health HCPCS subset (signal 6 geographic)
    serv_state_monthly — billing_npi × claim_month × servicing state (signal 9)
    """
    import time

    t0 = time.time()
    print("  Pre-computing provider_totals...")
    con.execute("""
        CREATE TABLE provider_totals AS
        SELECT
            billing_npi AS npi,
            SUM(total_paid) AS total_paid,
            SUM(total_claims) AS total_claims,
            SUM(unique_beneficiaries) AS total_beneficiaries
        FROM spending
        GROUP BY billing_npi
    """)

    print("  Pre-computing provider_code_totals (signals 15, 16, 18)...")
    con.execute("""
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

    print("  Pre-computing provider_monthly...")
    con.execute("""
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

    print("  Pre-computing spending_em (E&M codes)...")
    con.execute("""
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

    print("  Pre-computing spending_hh (home health codes)...")
    con.execute("""
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

    print("  Pre-computing serv_state_monthly (signal 9)...")
    con.execute("""
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

    print("  Pre-computing org_worker_monthly (signal 4 workforce scaling)...")
    con.execute("""
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

    print("  Pre-computing servicing_hub_totals (signal 12)...")
    con.execute("""
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

    print("  Pre-computing hh_zip_totals (signal 14 caregiver density)...")
    con.execute("""
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

    elapsed = time.time() - t0
    pt_count = con.execute("SELECT COUNT(*) FROM provider_totals").fetchone()[0]
    pm_count = con.execute("SELECT COUNT(*) FROM provider_monthly").fetchone()[0]
    print(f"  provider_totals: {pt_count:,} providers, provider_monthly: {pm_count:,} rows ({elapsed:.1f}s)")


def load_all(data_dir: str, memory_limit: str = "2GB") -> duckdb.DuckDBPyConnection:
    """Load all three datasets and return the connection."""
    con = get_connection(data_dir, memory_limit)

    print("Loading Medicaid spending data...")
    load_spending(con, data_dir)

    print("Loading OIG LEIE exclusion list...")
    load_leie(con, data_dir)

    print("Loading NPPES NPI registry...")
    load_nppes(con, data_dir)

    # Print summary stats (fast now that spending is a table)
    spending_count = con.execute("SELECT COUNT(*) FROM spending").fetchone()[0]
    leie_count = con.execute("SELECT COUNT(*) FROM leie").fetchone()[0]
    nppes_count = con.execute("SELECT COUNT(*) FROM nppes").fetchone()[0]

    print(f"  Spending rows: {spending_count:,}")
    print(f"  LEIE entries:  {leie_count:,}")
    print(f"  NPPES entries: {nppes_count:,}")

    print("Materializing aggregation tables...")
    _materialize_aggregations(con)

    # Optional: Census ACS ZCTA data for caregiver density signal enrichment
    load_census_zcta(con, data_dir)

    return con


def load_census_zcta(con: duckdb.DuckDBPyConnection, data_dir: str) -> None:
    """Load Census ACS ZCTA-level demographics if available.

    Expected CSV columns: zcta, total_population, population_65_plus,
    disability_count, poverty_count.

    If the file is missing the signal still works — it just uses
    state-median comparisons instead of demographic-adjusted expectations.
    """
    path = os.path.join(data_dir, "census_zcta.csv")
    if not os.path.exists(path):
        print("  Census ZCTA data not found (optional) — skipping demographic enrichment")
        return

    print(f"  Loading Census ZCTA demographics: {path}")
    con.execute(f"""
        CREATE TABLE census_zcta AS
        SELECT
            CAST(zcta AS VARCHAR) AS zcta,
            CAST(total_population AS INTEGER) AS total_population,
            CAST(population_65_plus AS INTEGER) AS population_65_plus,
            CAST(disability_count AS INTEGER) AS disability_count,
            CAST(poverty_count AS INTEGER) AS poverty_count
        FROM read_csv('{path}', header=true, auto_detect=true)
    """)
    row_count = con.execute("SELECT COUNT(*) FROM census_zcta").fetchone()[0]
    print(f"  Census ZCTA entries: {row_count:,}")
