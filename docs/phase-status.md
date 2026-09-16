# Project phase status

The local project is implemented and verified, including native Tableau Desktop
2026.2.2 rendering on September 15, 2026. The final native check found and fixed
a workbook format error and incorrect percentage formatting. The corrected
dashboard, data comparison, native image, and sign-off are retained in
`artifacts/native-validation/20260915-corrected/`.

This status follows the original nine-phase project outline. Changing live counts
and worker state belong in dated [verification records](verification.md); inspect
`weather-pipeline status`, `weather-pipeline health`, and Compose status for the
current deployment.

| Original phase | Status | Delivered evidence |
| --- | --- | --- |
| 1. Define the use case | Implemented and verified | Real NWS observations for KAUS, KORD, and KJFK; temperature, observation volume, and measurement-completeness questions in the [README](../README.md). Source window and reporting limits are explicit. |
| 2. Design architecture and data model | Implemented and verified | Raw JSONL → PySpark → Parquet → DuckDB → Tableau architecture; [documented star schema](data-model.md), row grain, keys, units, null meaning, lineage, and correction policy. |
| 3. Build raw ingestion | Implemented and verified | Paginated API collection, bounded retries, immutable raw files and manifests, input checksums, and replay without another API call. Live-source results and failure tests are recorded in [verification](verification.md). |
| 4. Implement PySpark transformations | Implemented and verified | Explicit schemas, UTC timestamps, measurement-unit conversion, typed Parquet, and deterministic duplicate selection. See [quality rules](data-quality.md) and the real Spark integration test. |
| 5. Add data-quality validation | Implemented and verified | Required-field, invalid-value, duplicate, station, and timestamp checks; optional missing measurements remain null; rejected rows retain original input and reason codes. Per-run counts reconcile. |
| 6. Implement incremental, idempotent loads | Implemented and verified | Initial load and overlapping incremental windows; stable fact keys; ordered correction handling; atomic fact/audit/checkpoint commits. Exact replay, rollback, retry, and stale-input protection are tested. |
| 7. Schedule and prove reliability | Implemented and verified locally | Hourly Docker worker, run events, station-level [health checks](monitoring.md), bounded retries, historical replay, and verified [backup/restore](backup-restore.md). [Deployment controls](deployment.md) explain startup, inspection, and shutdown. |
| 8. Build the dashboard | Implemented and verified natively | All three charts render in Tableau. Counts, completeness, native hourly exports, and all three data-source row counts match the frozen reference. A full dashboard image was exported directly from Tableau; see [validation](dashboard-validation.md). |
| 9. Package the project | Implemented and verified locally | The [project walkthrough](project-walkthrough.md), isolated offline demo, installable wheel, checksum-verified source ZIP, and CI workflow are supplied. The compatibility fix is version 0.2.1; all 210 tests pass. Original 0.2.0 artifacts and their dated Linux evidence are retained. Hosted CI runs on pushes, pull requests, and manual dispatches; see [verification](verification.md). |

## Verified dashboard

Open `artifacts/native-validation/20260915-corrected/weather_observatory.twbx`
for the exact snapshot checked in Tableau Desktop 2026.2.2, or
`data/tableau/weather_observatory.twbx` for the latest hourly snapshot. Select
**Weather Observatory**. Live counts continue changing as new data arrives.
The original September 14 frozen workbook failed native loading and is retained
as failure evidence; use the corrected package.

## Scope decisions

DuckDB and local Docker satisfy this project's analytical warehouse and scheduled
execution needs. A cloud deployment, hosted BI refresh, external alert delivery,
and automatic backup retention have not been configured. They are separate
extensions, not hidden prerequisites for running the local project.

MongoDB and Cassandra remain optional. The current workload has no distinct
access pattern requiring another database, so adding one would not close an
unfinished core phase. Transformation versioning and warehouse schema migration
are also future extensions; follow the documented fresh-warehouse replay process
when changing transformation semantics.
