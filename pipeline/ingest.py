# FILE: pipeline/ingest.py
# PURPOSE: Open all parquet files, combine into one table
import duckdb, os
from pathlib import Path

def load_all(ledger_path):
    con = duckdb.connect()
    df = con.execute(f"""
        SELECT *,
          regexp_extract(filename,
            '(\\d{{4}}-\\d{{2}}-\\d{{2}})', 1)
            AS source_date
        FROM read_parquet(
          '{ledger_path}/**/*.parquet',
          filename=true)
    """).df()
    return df