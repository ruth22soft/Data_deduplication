"""
pipeline/ingest.py
──────────────────
Low-memory ingestion helpers built on DuckDB SQL.
Uses a view over parquet files so data is scanned lazily.
"""

import duckdb
from loguru import logger


def create_source_view(con: duckdb.DuckDBPyConnection, ledger_path: str, view_name: str = "ledger_raw") -> str:
    """Create a lazy DuckDB view over parquet files with a derived source_date."""
    con.execute(f"""
        CREATE OR REPLACE VIEW {view_name} AS
        SELECT
            *,
            regexp_extract(filename, '(\\d{{4}}-\\d{{2}}-\\d{{2}})', 1) AS source_date
        FROM read_parquet('{ledger_path}/**/*.parquet', filename=true)
    """)

    total_rows = con.execute(f"SELECT COUNT(*) FROM {view_name}").fetchone()[0]
    date_folders = con.execute(
        f"SELECT COUNT(DISTINCT source_date) FROM {view_name}"
    ).fetchone()[0]
    logger.success(f"Loaded {total_rows:,} rows from {date_folders} date folders")
    return view_name


def export_table_to_parquet(
    con: duckdb.DuckDBPyConnection,
    table_or_view: str,
    output_file: str,
) -> None:
    """Write result to parquet directly from DuckDB to keep memory usage low."""
    con.execute(
        f"COPY (SELECT * FROM {table_or_view}) TO '{output_file}' (FORMAT PARQUET, COMPRESSION 'snappy')"
    )