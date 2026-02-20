"""Data loading and joining module using DuckDB for memory-efficient processing."""

import os
import glob
import duckdb


def get_connection(data_dir: str, memory_limit: str = "2GB") -> duckdb.DuckDBPyConnection:
    """Create a DuckDB connection with appropriate settings for large data."""
    con = duckdb.connect(":memory:")
    con.execute(f"SET memory_limit = '{memory_limit}'")
    con.execute("SET threads = 4")
    con.execute("SET enable_progress_bar = true")
    return con


def load_spending(con: duckdb.DuckDBPyConnection, data_dir: str) -> None:
    """Register Medicaid spending parquet as a view."""
    path = os.path.join(data_dir, "medicaid_spending.parquet")
    if not os.path.exists(path):
        raise FileNotFoundError(f"Spending data not found: {path}")
    con.execute(f"""
        CREATE VIEW spending AS
        SELECT
            BILLING_PROVIDER_NPI_NUM AS billing_npi,
            SERVICING_PROVIDER_NPI_NUM AS servicing_npi,
            HCPCS_CODE AS hcpcs_code,
            CLAIM_FROM_MONTH AS claim_month,
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
                    WHEN enumeration_date IS NOT NULL
                    THEN TRY_CAST(enumeration_date AS DATE)
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


def load_all(data_dir: str, memory_limit: str = "2GB") -> duckdb.DuckDBPyConnection:
    """Load all three datasets and return the connection."""
    con = get_connection(data_dir, memory_limit)

    print("Loading Medicaid spending data...")
    load_spending(con, data_dir)

    print("Loading OIG LEIE exclusion list...")
    load_leie(con, data_dir)

    print("Loading NPPES NPI registry...")
    load_nppes(con, data_dir)

    # Print summary stats
    spending_count = con.execute("SELECT COUNT(*) FROM spending").fetchone()[0]
    leie_count = con.execute("SELECT COUNT(*) FROM leie").fetchone()[0]
    nppes_count = con.execute("SELECT COUNT(*) FROM nppes").fetchone()[0]

    print(f"  Spending rows: {spending_count:,}")
    print(f"  LEIE entries:  {leie_count:,}")
    print(f"  NPPES entries: {nppes_count:,}")

    return con
