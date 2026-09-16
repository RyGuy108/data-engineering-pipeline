# Operational health monitoring

The health report checks the existing local warehouse and recent run history. It
does not create tables, repair records, advance checkpoints, start Spark, access
the weather API, or send notifications. It can be collected by a scheduler or
another monitoring system as a JSON document.

`weather_pipeline.monitoring.check_health(data_dir, settings, now=None)` returns
`status`, `checked_at`, `checks`, `stations`, `counts`, and `thresholds`. Each check
has a `name`, `status`, `message`, and optional `details`. The overall status is the
highest severity found: `healthy`, `warning`, or `critical`. Unavailable databases,
including write locks and damaged files, return actionable critical reports.
Configuration errors raise `ValueError` so invalid monitoring settings cannot
silently report success. All report timestamps are UTC; `now` can be supplied as
a timezone-aware datetime for a reproducible check.

## Thresholds

Add the following table to `settings.toml` to override the defaults:

```toml
[health]
max_observation_age_minutes = 180
max_run_age_minutes = 120
max_rejection_pct = 5
```

Values must be finite positive numbers, not booleans or strings. The rejection
percentage cannot exceed 100. Observation and checkpoint timestamps up to five
minutes ahead of the checker are tolerated for clock skew; later timestamps are
critical. Thresholds should accommodate the collection schedule and normal NWS
reporting delays.

## What the checks mean

| Check | Meaning and response |
| --- | --- |
| Warehouse | Missing, locked, unreadable, or structurally incomplete warehouses are critical. Bootstrap with a live run; retry after active writers finish; restore or rebuild damaged files from archived raw batches. |
| Records | An initialized warehouse with zero observations is critical. Inspect API responses and rejected records. |
| Station observations | Every configured station must have an observation within the freshness threshold. A fresh station cannot disguise a missing or stale station. |
| Live checkpoints | For every configured station, both the checkpoint update time and watermark must be within the run threshold. Missing or stale checkpoints are critical. Inspect scheduling and perform a live refresh. |
| Rejection rate | Inspect the most recently completed nonempty batch referenced by current live checkpoints. Rejected / input × 100 above the threshold is a warning. Backfill batches are excluded. No eligible nonempty batch produces an unavailable-rate warning. |
| Missing measurements | Informational: missing temperature, humidity, and wind values / (3 × accepted records) × 100. Optional missing values remain null and do not count as rejected records. |
| Recent failures | Failed processing or ingestion in the last 24 hours is critical until a later live checkpoint advancement or successful retry of the same failed batch is observed. Inspect the recorded error. |
| Event-log integrity | Malformed records or future event timestamps produce a warning. Inspect the event file and system clock. |

An exact batch replay does not update `source_checkpoints.updated_at`. Backfills
do not advance live checkpoints. Therefore a recent replay or backfill cannot
make a stopped live pipeline appear current. A successful replay of the *same*
failed batch can resolve that batch's failure, for example after repairing a
dashboard export, while live freshness is still checked independently.

An ingestion failure has no committed batch and is resolved only by later live
checkpoint advancement. A failure after warehouse commit remains visible until
the operation recovers or newer live work succeeds.

## Bounds and limitations

Warehouse checks use aggregate queries rather than fetching observation records.
Recent run failures are checked within the last 1 MiB and at most 2,000 event
records in `events.jsonl`. Reports disclose whether the history was truncated and
how many records were examined. This is a bounded operational check, not an audit
of every historical failure. A missing event file means no recent events are
available; warehouse and checkpoint checks still run.

The warehouse does not persist every historical batch's live/backfill mode.
Quality monitoring therefore uses batches referenced by current checkpoints,
which have proven live provenance. If the latest live checkpoints reference only
empty batches, the report warns that a rejection rate is unavailable instead of
using an unrelated backfill or presenting a misleading zero-percent rate.

DuckDB permits one writer process. Health opens the database read-only and can be
temporarily unavailable during a separate active writer; collect it after the
scheduled run finishes, or retry. No automatic destructive repair is attempted.
