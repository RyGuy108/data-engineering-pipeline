# Backup and restore

Create a backup before changing the pipeline or moving its data. These commands
run locally, require no API calls or Spark session, and do not delete earlier
backups. Run them from the project directory with its virtual environment.

```sh
.venv/bin/weather-pipeline --data-dir data backup --output-dir backups
.venv/bin/weather-pipeline restore backups/weather-backup-TIMESTAMP-ID.zip --destination restored-data
.venv/bin/weather-pipeline --data-dir restored-data status
```

Use the actual `archive_path` printed by the backup command. The restore target
must be absent or empty. An existing warehouse is never overwritten. When
switching to recovered data, pass the recovered directory explicitly to each
pipeline or scheduler command, or update the deployment's data directory.

## What is preserved

The ZIP snapshot includes `warehouse.duckdb`, raw observations and manifests,
processed output from committed batches and retained attempts, reports, dashboard
exports, Tableau files, the original event log, and any other regular files in
the data directory. Empty directories are preserved. `scheduler.json`, when
present, is retained as historical operational metadata; restore starts no jobs.

The pipeline and scheduler lock files, temporary and partial files, Spark's
`_temporary` directory, `.venv`, `.runtime`, and `__pycache__` are excluded. A
backup directory located inside the data directory is excluded from its own
snapshot. Prefer a backup directory outside the data tree and copy successful
archives to separate storage for protection from a device failure.

A backup takes the pipeline's writer lock and holds a read-only DuckDB connection
through copying and verification. It checks that committed batches' raw and
processed locations are present, verifies raw input digests, records each file's
size and SHA-256 digest, and rereads every ZIP payload before atomically publishing
the archive. The source warehouse and raw files remain unchanged. The command
reports the archive's SHA-256 digest as well.

If ingestion, replay, export, or another pipeline operation is active, backup
fails rather than copying a partial operation. Retry after that operation exits.
If `warehouse.duckdb.wal` exists, close the warehouse writer normally and retry.
The backup command does not checkpoint or repair the source in place. A crashed
writer may require a normal DuckDB open and close to recover/checkpoint its WAL
before backup. Do not remove the WAL manually: it can contain committed data.

## Restore guarantees

Restore validates ZIP member names, regular-file types, unique members, sizes,
and SHA-256 checksums before publishing anything. It rejects traversal paths,
symbolic links, extra or missing members, incompatible manifests, missing batch
references, and damaged archives. Extraction and database changes happen in a
private sibling staging directory. A failure removes that staging directory and
leaves an absent or empty destination unchanged. Publication uses one directory
rename on the same filesystem. A stable, hidden sibling `.weather-restore-*.lock`
file serializes simultaneous restores; it is operational metadata, not recovered
data.

Only location fields are changed in the recovered copy:

- Raw manifests' `raw_path` and existing `manifest_path` values point into the
  restored directory.
- `etl_runs.raw_path`, `curated_path`, and `rejected_path` point to the restored
  raw and processed files.
- Observation facts, checkpoint values, batch IDs, and input fingerprints stay
  unchanged. Fingerprints already exclude the two manifest location fields, so
  a committed archived batch remains an idempotent replay after relocation.

Original `events.jsonl` bytes are preserved, including historical source paths.
Those paths describe what happened at the original location. Use restored raw
manifests and warehouse lineage columns for current file access. Embedded content
inside other artifacts, such as old HTML reports or standalone Tableau connection
paths, is not rewritten; regenerate dashboard exports/workbooks from the restored
warehouse when needed. No scheduler or external dashboard refresh is started.

Archives are integrity checked, not encrypted or cryptographically signed. Keep
them in trusted storage with suitable access controls. The format requires all
raw and processed references to live inside the source data directory; backups
of externally linked artifacts are refused. Use the same locked project
requirements to restore this warehouse; this command is not a DuckDB version
migration tool.

## Recovery check

After restoring, inspect `status` and compare its observation count and checkpoint
values with the source or a recorded prior status. Replay a restored manifest:

```sh
.venv/bin/weather-pipeline --data-dir restored-data replay restored-data/raw/BATCH_ID/manifest.json
```

For a previously committed batch, loading should report `already_loaded` and
leave business facts and checkpoints unchanged. Replay runs PySpark and creates
new attempt reports and exports, so the whole data directory is not expected to
remain byte-identical after that check.

Automated tests cover successful warehouse replay from relocated Parquet, source
byte preservation, checksum and truncation failures, missing references, failure
after staged rebasing, unsafe ZIP paths, symbolic links, duplicate members,
existing destination protection, source locks, and transient-file exclusions.

## Python interfaces

`backup_data(data_dir, backup_root) -> dict` returns `status="backed_up"`,
`archive_path`, `file_count`, `source_bytes`, `archive_bytes`, and `sha256`.

`restore_backup(archive_path, destination) -> dict` returns `status="restored"`,
`archive_path`, `data_dir`, `file_count`, `rebased_manifests`, `committed_batches`,
and `observation_count`. Both functions acquire their own locks; callers must
not wrap them in another pipeline lock.
