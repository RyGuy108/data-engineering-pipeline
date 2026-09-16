# Operations and recovery

## Inspect a run

Use `weather-pipeline status`, `weather-pipeline health`, `data/events.jsonl`, the batch report, and `dashboard_quality.csv`. Successful batch details are in `etl_runs`. Failures are in the event log; the warehouse success table intentionally contains only committed batches. `data/scheduler.json` records the worker's last cycle, phase and next run time. Health checks are read-only; a replay cannot make stale live processing appear current. See [monitoring](monitoring.md) for thresholds and limits.

## API failure

The client uses bounded retries for transient errors and respects a capped Retry-After delay. Pagination must stay on the same NWS observation endpoint and preserve the original window. A repeated cursor, unsafe next link, incomplete capped page, or failed station causes the entire ingestion to fail. No manifest is published for a partial fetch, and the checkpoint remains unchanged. Run the command again after the API recovers.

## Transform or warehouse failure

The raw manifest is already saved. Inspect its path in the event log, fix the cause, and use `weather-pipeline replay <manifest-path>`. Failed warehouse transactions roll back observations, dimensions, the version ledger, success audit, and checkpoints together. A retry processes the same immutable input.

Each processing attempt uses a separate Parquet/report directory. A changed replay that collides with an existing batch ID therefore cannot overwrite artifacts referenced by the successful audit. The canonical batch report is written for a newly committed batch or repaired if missing after an identical validated replay. Failed attempts are retained for diagnosis.

For manifests produced by ingestion, processing verifies the saved SHA-256 digest before starting Spark and verifies the input count before loading. Truncated or altered archives fail before any checkpoint advances. Restore the original bytes from a backup; do not edit the digest merely to bypass a corruption failure.

## Crash after commit

If a process stops after warehouse commit but before export, replay the manifest or run `weather-pipeline export`. An identical committed batch is a no-op at the warehouse. Its business counts and totals stay unchanged. Failed export does not undo a committed warehouse load.

## Backfill and long downtime

Explicit `--start`/`--end` windows are backfills and never advance the live checkpoint. Use timezone-aware values within the supported recent window. Older data can only be replayed if it was retained locally or fetched using an additional historical source adapter.

If a live checkpoint is more than seven days behind, choose a recovery policy explicitly. Preserve the existing warehouse. One safe option is to use a new data directory to collect recent data while retaining the old warehouse for a later, documented migration. The pipeline does not silently skip an unavailable historical gap.

## Local scheduling

`weather-pipeline schedule --interval-minutes 60 --with-dashboard` runs immediately, then on a fixed cadence measured with a monotonic clock. Processing time is included in the interval. If a run overruns its next slot, missed slots are skipped so runs never overlap and recovery does not trigger a burst of catch-up jobs. Failures are recorded and the next scheduled slot retries from committed checkpoints. Ctrl+C stops the foreground process; SIGTERM requests a stop after the current cycle. Waiting is interruptible. The last cycle determines the exit code when `--max-runs` is supplied.

While waiting, the scheduler also checks its published UTC deadline at least every 30 seconds of running time. If the computer or Docker pauses its elapsed-time clock and UTC becomes overdue, one catch-up cycle runs after execution resumes and establishes a fresh cadence. `clock_catchup_count` records this adjustment; a forward clock correction can also trigger it. Moving UTC backwards cannot postpone the monotonic deadline. The worker cannot execute while the computer or Docker is suspended.

The Compose worker is configured for hourly runs and was started during deployment verification. `sh scripts/compose.sh ps` shows its current state; `sh scripts/compose.sh stop` pauses it. A separate scheduler lifetime lock prevents two workers sharing the data directory. The per-operation pipeline lock still allows backups between cycles. Restarting a worker begins an immediate run.

The local lock prevents concurrent run, replay, or export operations in the same data directory. Read status while a writer is active may encounter DuckDB's file lock; retry after the writer finishes.

## Storage and deployment

Use `weather-pipeline backup --output-dir backups` for a verified snapshot. It coordinates the pipeline lock with a read-only DuckDB lock, validates referenced raw data, and checks every archived file. Restore only into an absent or empty directory using `weather-pipeline restore <archive> --destination <new-directory>`. Restored manifests and warehouse lineage paths are relocated without changing batch fingerprints. See [backup/restore](backup-restore.md).

Raw batches, reports and backups currently have no retention policy and will grow. A production cloud deployment would additionally need managed object storage, warehouse credentials, external alerting, retention, and a tested schema-migration strategy. This deployment uses local Docker and has no paid cloud resources.
