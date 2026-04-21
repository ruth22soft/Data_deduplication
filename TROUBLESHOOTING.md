# Troubleshooting Guide

This file captures what failed, what worked, and why we changed the pipeline design.

## Quick Status

- Current run command that works:
  - `docker compose run --rm --build pipeline python -m pipeline.run_all`
- Final run result:
  - Completed successfully (exit code `0`)
- Output files generated:
  - `output/duplicate_analysis.json`
  - `output/enat_ledger_combined.parquet`

## Main Issues We Hit

### 1) Dockerfile not found during compose build

**Symptom**
- `failed to solve: failed to read dockerfile: open Dockerfile: no such file or directory`

**Root cause**
- Compose was building from project root but Dockerfile lives in `Data_validation/`.

**Fix**
- Updated `docker-compose.yml` build config to:
  - `context: .`
  - `dockerfile: Data_validation/Dockerfile`

---

### 2) Wrong runtime commands for pipeline entrypoint

**Symptoms**
- `python: can't open file '/app/run_all.py'`
- `exec: "run_all.py": executable file not found in $PATH`
- `ModuleNotFoundError: No module named 'pipeline'`

**Root cause**
- `run_all.py` was invoked with the wrong path or as a plain executable.
- Package-style imports need module execution mode.

**Fix**
- Use module mode:
  - `python -m pipeline.run_all`
- Updated Docker CMD in `Data_validation/Dockerfile` to module mode.

---

### 3) Container killed with exit code 137

**Symptom**
- Run started, then exited with code `137` during ingest/export.

**Root cause**
- Memory pressure (OOM-like behavior): original flow loaded all data into pandas and performed heavy operations.

**Fix**
- Refactored pipeline to low-memory DuckDB SQL-first approach:
  - Avoid full-table pandas DataFrame materialization.
  - Use DuckDB views and SQL aggregates.
  - Export parquet directly from DuckDB.
  - Removed expensive full `ORDER BY` in final export.
  - Added DuckDB runtime settings:
    - `PRAGMA temp_directory='/tmp'`
    - `PRAGMA threads=2`

---

### 4) Duplicate key mismatch (`transaction_id` vs `entry_id`)

**Symptom**
- Binder error about missing `transaction_id` column.

**Root cause**
- Dataset schema uses `entry_id` but duplicate logic initially expected `transaction_id`.

**Fix**
- Added key auto-detection fallback (`transaction_id` -> `entry_id`) in dedupe/analysis modules.

## What Worked

- Schema validation across all parquet files.
- Duplicate analysis report generation.
- Duplicate detection summary by date.
- Null checks and row-count checks.
- Final parquet output write.

## Final Working Commands

### Full run (recommended)

```bash
docker compose run --rm --build pipeline python -m pipeline.run_all
```

### Service-mode run

```bash
docker compose up --build
```

### Analysis only

```bash
docker compose run --rm pipeline python pipeline/analyze_duplicates.py
```

## How to Debug Quickly

1. Check logs:

```bash
docker compose logs -f pipeline
```

2. Open shell inside container:

```bash
docker compose run --rm pipeline bash
```

3. Verify output files:

```bash
ls -lah output
```

## Optional Duplicate Removal (Disabled by Default)

- File: `pipeline/dedupe.py`
- Removal block is intentionally commented.
- Uncomment only when you want to keep one record per unique key (latest by `source_date`).

## Notes

- A one-off container created with `docker compose run --rm ...` is removed on exit.
- Results persist because `./output` is mounted from host to container path `/app/output`.
