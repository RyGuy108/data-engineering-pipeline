# Reproducible offline demonstration

Run the complete Spark → DuckDB → Tableau workflow with a small recording of real NWS observations. The demonstration makes no API requests and does not read or modify the live `data/` directory. It writes a separate warehouse, raw batches, quality reports, extracts, workbook, and verification evidence into the directory you choose.

```sh
weather-pipeline demo --output-dir artifacts/demo
```

The output directory must be absent or empty. Choose a new directory for each run; an existing nonempty directory, file, or destination symlink is refused before Spark starts. The command requires the project's installed Python dependencies and Java 17 or 21, matching the supported transformation runtimes. Dependency installation can require a network connection; the demonstration itself does not. On a source checkout, the project runtime is detected if available. An installed wheel includes the recording and works from an unrelated working directory.

## What it proves

1. **Initial loading:** Transform 12 real observations from Austin, New York, and Chicago. A repeated delivery of one payload is deduplicated. Four naturally unavailable measurements remain null.
2. **Quality validation:** Quarantine three explicitly labeled synthetic probes: a missing station identifier, relative humidity of 101%, and a malformed observation timestamp. The probes preserve rejection reasons and never enter accepted weather facts.
3. **Exact replay:** Process the same initial manifest again. The loader reports `already_loaded`; all six warehouse tables, including facts, load timestamps, the latest-seen version ledger, run records, and checkpoints, remain identical. Dashboard CSV bytes also remain identical.
4. **Incremental loading with overlap:** Build the next windows from the saved station checkpoints with the normal two-hour overlap setting. The recorded followup contains three already loaded observations and six actual observations later than the first checkpoint.
5. **Failure and recovery:** Inject an error immediately before committing the incremental load. Verify complete database rollback and unchanged extracts. Replay that same incremental manifest successfully, add only six new observations, and advance all checkpoints after commit.
6. **Warehouse and dashboard reconciliation:** Verify 18 unique final observations, three stations, two committed batches, and accepted payload hashes matching the public recording. Export and audit a packaged Tableau workbook.

One Spark session runs all four processing attempts. This is a small correctness demonstration; it does not measure production scale or present current weather.

## Evidence to review

The output directory contains:

| Artifact | Purpose |
|---|---|
| `report.md` | Readable checklist with the result of every demonstration assertion. |
| `report.json` | Machine-readable checks, counts, provenance, phase outcomes, and output paths. |
| `warehouse.duckdb` | Isolated warehouse containing the final 18 recorded observations. |
| `raw/` | Initial and followup manifests and replayable input envelopes. |
| `processed/` and `reports/` | Curated and rejected Parquet files, quality reports, and separate attempt evidence. |
| `events.jsonl` | Two initial successes, the intentional failure, and successful recovery. |
| `exports/` | Observation, daily, and quality dashboard CSV extracts. |
| `tableau/weather_observatory.twbx` | Packaged dashboard using this demonstration's recorded data. |
| `tableau/audit.json` and `tableau/audit.md` | Automated workbook/data reconciliation. |

The intentional failed attempt is expected evidence. The overall report should still say `passed`; a failed demonstration check produces a nonzero command exit. An unexpected runtime error writes a failed report with its error, then exits unsuccessfully. Keep the directory to inspect evidence and use a new empty destination after fixing the cause.

Open the generated `.twbx` in Tableau to verify native chart rendering. The automated audit checks data and workbook structure; it does not claim native Tableau visual verification. The dates shown are historical, and the deliberate failure and rejected probes appear in the isolated demonstration's audit history.

## Recording provenance

The fixture is shipped as `weather_pipeline/examples/recorded-nws-sample.json` in both the source tree and installed package. It contains exact archived NWS observation payload strings, individual SHA-256 hashes, original public API identifiers, fetched timestamps, station metadata, request windows, source batch identifiers, and original source archive checksums. Local workstation paths are excluded.

The initial subset comes from the batch fetched at `2026-09-13T22:48:52.877116Z`, with source window end `2026-09-13T22:48:52.876605Z`. The followup subset comes from the batch fetched at `2026-09-13T23:10:58.232781Z`, with source window end `2026-09-13T23:10:58.232165Z`. These are representative subsets, not complete API responses or complete station history. Source archive checksums identify the original larger capture; per-payload checksums verify the shipped observations.

The warehouse source label is `recorded_nws_demo`. Accepted payloads remain unmodified. Runtime batch identifiers and envelope provenance identify this replay separately from the original live ingestion. Only rejected probes alter payload content, and each includes a `demo_synthetic_probe` label retained in its rejection evidence. The duplicate probe adds an envelope label while keeping its original weather payload unchanged.
