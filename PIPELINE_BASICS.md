# Pipeline Basics

This is a practical introduction to how this project pipeline works.

## What This Pipeline Does

The pipeline reads many parquet files from dated folders, analyzes duplicates, validates data quality, and writes a combined parquet output.

### Input

- Host folder: `data/enat_ledger/`
- Expected structure: nested date folders containing parquet files

### Output

- `output/enat_ledger_combined.parquet`
- `output/duplicate_analysis.json`

## High-Level Flow

The main entrypoint is `pipeline/run_all.py`.

1. **Schema check**
- Confirms all parquet files have consistent column layout.

2. **Ingest**
- Creates a DuckDB source view over all parquet files.
- Adds `source_date` extracted from folder/file path.

3. **Duplicate analysis (read-only)**
- Computes duplicate rate and distribution.
- Saves JSON summary report.

4. **Duplicate detection in flow**
- Logs duplicate counts and affected dates.
- Does **not** remove duplicates by default.

5. **Validation checks**
- Mandatory key null checks.
- Row-count sanity checks by date.

6. **Export**
- Writes combined parquet to `output/` using snappy compression.

## Why DuckDB Is Used

DuckDB handles large parquet scans and aggregations efficiently with lower Python memory overhead.

Benefits in this project:
- Better stability on large datasets.
- SQL-based transformations and validations.
- Direct parquet export from query results.

## Run Commands You Should Use

### Run full pipeline

```bash
docker compose run --rm --build pipeline python -m pipeline.run_all
```

### Alternative full run

```bash
docker compose up --build
```

### Run duplicate analysis only

```bash
docker compose run --rm pipeline python pipeline/analyze_duplicates.py
```

### Open shell inside container (debug)

```bash
docker compose run --rm pipeline bash
```

## Common Command Pitfalls

### Wrong

```bash
docker compose run --rm pipeline run_all.py
```

Reason: `run_all.py` is not an executable in `PATH`.

### Wrong

```bash
docker compose run --rm pipeline python run_all.py
```

Reason: path is wrong (`/app/run_all.py` does not exist).

### Correct

```bash
docker compose run --rm pipeline python -m pipeline.run_all
```

## Duplicate Removal Toggle

- File: `pipeline/dedupe.py`
- Duplicate removal logic is present but commented.
- Keep it commented for detection-only mode.
- Uncomment when business rules confirm you want to collapse duplicates.

## Quick Verification Checklist

1. Build/run command exits with code `0`.
2. `output/duplicate_analysis.json` exists.
3. `output/enat_ledger_combined.parquet` exists.
4. Logs show all steps completed.

## Typical Daily Workflow

1. Place/refresh input parquet files under `data/enat_ledger/`.
2. Run full pipeline.
3. Inspect duplicate report.
4. Validate output parquet.
5. (Optional) Enable dedupe block later if required.
