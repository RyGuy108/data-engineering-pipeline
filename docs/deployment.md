# Local operational deployment

The pipeline runs as a Docker Compose worker, with Python 3.12, Java 17, PySpark 4.0.1 and the pinned Python dependencies in `requirements.lock`. The local development environment uses Java 21; both runtimes have processed real observations successfully.

## Start, inspect and stop

From the project directory:

```sh
sh scripts/compose.sh build
sh scripts/compose.sh up -d
sh scripts/compose.sh ps
sh scripts/compose.sh logs --tail 100
sh scripts/compose.sh stop
```

The project-specific service is `weather-data-pipeline-pipeline-1`. The wrapper uses the project directory even when called from elsewhere and normalizes `WEATHER_DATA_DIR` to an absolute path. It binds only that data directory plus read-only settings; it publishes no network ports and does not mount the Docker socket.

To inspect health inside the running container:

```sh
sh scripts/compose.sh exec pipeline weather-pipeline --data-dir "$PWD/data" health
```

If you use a different data directory, supply its same absolute path. The default data directory is mounted at the same location inside and outside Docker. Avoid bypassing the wrapper when using existing host-created data; the container-default `/app/data` location would create a different lineage-path convention.

## Worker behavior

- Runs immediately on startup and then every 60 minutes on a fixed cadence.
- Reads only the necessary source window plus the configured overlap.
- Commits warehouse data/checkpoints together, exports CSV snapshots, rebuilds Tableau, and audits the package.
- Reports errors, keeps its committed checkpoint, and retries at the next scheduled slot.
- Skips missed schedule slots when work overruns; there are no overlapping cycles.
- Checks UTC deadlines in waits of at most 30 seconds so an overdue worker catches up once after a host/VM pause, then resumes its normal cadence. A backwards UTC correction does not delay the monotonic schedule.
- Records `scheduler.json` with the phase and next run time. This file is operational metadata, not proof that a saved PID is still alive.
- Responds to SIGTERM by stopping after the current cycle; Compose allows 90 seconds before forced shutdown. A forced stop can require recovery from raw data, while the transaction protects committed warehouse state.

Docker must remain running. Sleep, shutdown, unavailable networking or a stopped container interrupt collection. If downtime exceeds the source's seven-day recent-history window, follow the explicit gap-recovery procedure in the runbook.

## Resource and health settings

The service is limited to two CPUs and two GiB of memory. Container logs rotate at 10 MiB with three files. Data and reports are not automatically deleted.

Docker health checks run every 60 seconds, with a 15-second timeout, three retries and a 180-second initial grace period. CLI health returns zero only for healthy data; stale/missing stations, live checkpoint delays and unresolved errors surface as nonzero results. Defaults are configurable in `[health]` of `settings.toml`.

`restart: unless-stopped` restarts a process that exits, subject to Docker's behavior. An unhealthy health status by itself does not restart the container. The scheduler continues trying on its next slot; diagnose the health report and logs. No external notifications are sent.

## Backups and relocation

Run backups while the writer is idle between cycles, or stop the service first. The backup command fails clearly if a writer or uncheckpointed WAL prevents a consistent snapshot. Restore into a separate directory and verify it before switching `WEATHER_DATA_DIR`. Stop the original worker before starting a restored copy against the same logical workload.

Historical event logs and scheduler metadata remain unchanged in a restored snapshot; replay and new activity create new records. Restoring a backup does not start a worker.

## Updating code

```sh
sh scripts/compose.sh stop
sh scripts/compose.sh build
sh scripts/compose.sh up -d
```

Use the test suite before rebuilding. A change to transformation semantics requires a deliberate rebuild/migration of existing warehouse data, as documented in the data-quality rules; changing a batch ID does not force recalculation of unchanged source payloads.

References: [Compose health checks](https://docs.docker.com/reference/compose-file/services/#healthcheck), [restart policy](https://docs.docker.com/reference/compose-file/services/#restart), [Python monotonic time](https://docs.python.org/3/library/time.html#time.monotonic).
