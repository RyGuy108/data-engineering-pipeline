# Five-minute project walkthrough

Use this walkthrough to demonstrate the engineering behavior and its evidence.
The weather data comes from the National Weather Service; intentionally malformed
demo probes are labeled and retained only as rejected records. They are not
presented as source observations.

Prepare the isolated demo from the project directory after following the
[environment setup](../README.md#quick-start):

```sh
weather-pipeline demo --output-dir artifacts/demo
```

The command uses captured real input and runs locally without calling the weather
API. Its output directory is separate from the hourly worker's `data/` directory.
The destination must be absent or empty. If `artifacts/demo/` already contains the
prepared example, review it directly or reproduce the run with
`--output-dir artifacts/demo-review` and use that directory's reports.
Treat the five minutes below as presentation pacing, not a pipeline runtime
benchmark. Run preparation before presenting.

Start with the generated local report at `artifacts/demo/report.md`; its
machine-readable evidence is at `artifacts/demo/report.json`. The isolated native
workbook is at `artifacts/demo/tableau/weather_observatory.twbx`. Generated
artifacts are intentionally excluded from Git.
The live dashboard remains in `data/tableau/` and represents a different snapshot.

| Time | Show | Explain and verify |
| --- | --- | --- |
| 0:00–0:40 | [Architecture](../README.md#architecture) and a raw observation envelope | The same path used for live API responses preserves original feature JSON, station ID, ingestion time, and batch identity. A saved manifest identifies the input for replay. |
| 0:40–1:30 | Demo quality report, curated Parquet, and rejected-record output | Spark validates and normalizes the records. Missing optional measurements remain null. Valid duplicate keys are reduced deterministically; malformed probes are rejected with reasons. Confirm `input = accepted + rejected + duplicates`. |
| 1:30–2:15 | [Data model](data-model.md) and the demo warehouse | One fact row represents one station at one UTC observation timestamp. Station and date dimensions support analytics. The version ledger orders corrections; batch audits and station checkpoints capture committed progress. |
| 2:15–3:30 | Demo replay, failure, and incremental evidence | Compare the before/after warehouse snapshots for exact replay: facts, dimensions, version ledger, audit rows, and checkpoints must remain unchanged. The injected precommit failure must also preserve that state. Recovery loads the overlapping followup batch, adding only its new records and advancing checkpoints after commit. |
| 3:30–4:30 | Generated Tableau package and its audit | The dashboard contains hourly temperature, station observation counts, and measurement completeness. Audit results compare packaged rows, keys, nulls, and measures with the warehouse. Open the workbook in Tableau when available; a passing audit alone does not establish visual correctness. |
| 4:30–5:00 | [Operations](deployment.md), [recovery](backup-restore.md), and [phase status](phase-status.md) | Explain hourly scheduling, station freshness checks, restoring into a separate directory, the completed native Tableau result, and the local single-writer scope. |

## Useful reviewer checks

Use the demo warehouse to answer these questions in SQL. These checks make the
model and the dashboard's measures inspectable without relying on screenshots.

```sql
-- No duplicate business keys should be returned.
SELECT station_id, observed_at, count(*) AS copies
FROM fact_observation
GROUP BY station_id, observed_at
HAVING count(*) > 1;

-- Show committed progress; replay does not add another committed batch.
SELECT batch_id, input_count, accepted_count, rejected_count,
       duplicate_count, inserted_count, updated_count, unchanged_count
FROM etl_runs
ORDER BY completed_at, batch_id;

-- Match the station counts and measurement completeness in the dashboard.
SELECT station_id, count(*) AS observations,
       100.0 * (3 * count(*) - sum(missing_measurement_count))
           / (3 * count(*)) AS present_measurements_pct
FROM dashboard_observations
GROUP BY station_id
ORDER BY station_id;
```

Demonstrate live operations separately, from the default project directory:

```sh
weather-pipeline status
weather-pipeline health
weather-pipeline audit-dashboard
sh scripts/compose.sh ps
```

These inspect current live data and the worker; an old captured demo is not
expected to pass present-day freshness thresholds. If a scheduled write is active,
wait for its completion before reading or auditing the DuckDB snapshot.

## Claims the evidence supports

- Real public API ingestion and real local Spark transformations are exercised.
- Incremental loading is shown by new and overlapping input; idempotency is shown
  by unchanged warehouse state after replay, not merely by stable row counts.
- Rejected data remains inspectable, and measurement absence remains distinct
  from invalid data and duplicate input.
- The warehouse-to-dashboard audit verifies snapshot data and structure. Native
  Tableau rendering remains a separate, explicitly recorded check.

The [verification record](verification.md) distinguishes actual live runs from
deterministic tests. No throughput benchmark, distributed deployment, always-on
availability guarantee, or hosted dashboard is claimed.

## Shareable handoff

The final 0.2.1 release files are under `artifacts/release/0.2.1/`:

- `weather-data-pipeline-0.2.1-source.zip` contains the portable project source,
  documentation, tests, and captured demo inputs.
- `weather_data_pipeline-0.2.1-py3-none-any.whl` is the installable Python package.

Use the source bundle for the full project walkthrough; a wheel installs the
package, not the surrounding repository documentation and deployment files.
Neither artifact includes the local live warehouse, backups, virtual environment,
or Java installation. The CI workflow runs in the standalone GitHub repository.
Consult the [release verification record](verification.md) for checks that were
actually executed.
