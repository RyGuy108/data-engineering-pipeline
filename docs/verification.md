# Verification record

Verified locally on September 13, 2026 using Python 3.12.14, PySpark 4.0.1, Java 21 (project-local Eclipse Temurin runtime), and DuckDB 1.4.0. The public source was `https://api.weather.gov`.

## Live source and warehouse

| Operation | Result |
|---|---|
| Initial load: preceding 24 hours, KAUS/KJFK/KORD | 838 observations accepted and inserted; zero rejected; zero within-batch duplicates |
| Missing optional measurements in initial load | 41 missing fields across temperature/humidity/wind, retained as null |
| Exact replay of initial immutable manifest | 0 inserts, 0 updates, 838 unchanged; status `already_loaded` |
| Subsequent incremental fetch with two-hour overlap | 78 observations accepted: 15 new, 63 unchanged, 0 updates |
| Warehouse after incremental run | 853 observations, 3 stations, 2 committed batches |
| Incremental station coverage | All three stations present; no station stale by the 120-minute threshold; no internal gap over 120 minutes |

Initial batch: `20260913T224852877116Z-ec4a6964738c4c8abfe25b204d02ef12`.

Incremental batch: `20260913T231058232781Z-fc28d3267b034f3b8b5a785c58966c4d`.

All three live station checkpoints reached `2026-09-13T23:10:58.232165Z`. The latest observation in that snapshot was `2026-09-13T23:00:00Z`. These are observed verification results, not promises about future source availability.

## Automated checks

The core suite passed 53 tests. Tests include a real Spark-to-DuckDB integration scenario, not only mocks: full warehouse table snapshots and exact exported CSV bytes stay unchanged after identical replay; an injected pre-commit failure rolls back; retry recovers; newer corrections update; stale versions do not overwrite them; a changed batch-ID collision preserves the original committed Parquet and report artifacts. API responses are mocked in unit tests to exercise pagination and failures deterministically.

The additional five native Tableau tests passed, for 58 distinct passing tests in total. They check package contents, relative data paths, worksheet/field references, weighted completeness calculations, empty snapshots, and missing required columns. Native rendering requires Tableau and is documented separately in the dashboard notes.

The original workbook passed the official Tableau 2026.1 structural schema check using `scripts/validate_tableau_schema.py`. The published schema's missing namespace imports were resolved as described in the dashboard notes. On September 15, native opening exposed a format-selection error despite this earlier result; the correction is recorded below. Final source/test/helper lint checks passed at the time of this original check.

## Operational phases verified

On September 13, 2026 local time (September 14 UTC), the expanded suite passed **175 tests in 25.40 seconds**, and all source, test and helper lint checks passed.

| Check | Observed result |
|---|---|
| Docker image build | Succeeded with pinned Python dependencies and Java 17 |
| Finite scheduled Docker cycle | 100 accepted inputs: 35 inserts, 1 source correction, 64 unchanged; dashboard rebuilt and audited |
| Persistent hourly worker | `weather-data-pipeline-pipeline-1` running and Docker reports healthy |
| First persistent-worker cycle | 66 accepted overlapping observations, 0 inserts/updates, 66 unchanged |
| Current warehouse snapshot | 888 observations, 3 stations, 4 committed batches |
| Dashboard-to-warehouse audit | 37 of 37 checks passed, including packaged rows, keys, nulls, measures and snapshot metadata |
| Operational health | Healthy; all configured stations and live checkpoints current |
| Verified recovery snapshot | 39 files, 9,989,707 source bytes; ZIP checksum verified |
| Restore to separate directory | 853 observations and 2 committed batches restored; raw/warehouse references rebased |
| Replay after restore | Original 838-row batch returned `already_loaded`: 0 inserts, 0 updates; restored warehouse remained at 853 observations and dashboard audit passed |

The recovery snapshot preceded the later Docker loads, which is why it contains 853 observations while the active warehouse contains 888. The source directory was not replaced during recovery testing. `data-recovery-check` remains available as the verified restored copy.

Recovery archive: `backups/weather-backup-20260914T000354179906Z-bbfd0ba8.zip`.

SHA-256: `21ea9e62cc623bec815003e04aa097182517e6a1f25cd43304d3293de24f58ce`.

At the final deployment inspection, the worker had completed its first cycle and was waiting for the next run at `2026-09-14T01:06:33Z` (8:06 p.m. America/Chicago on September 13). This records the inspected state; use Compose status and `data/scheduler.json` for current state. The worker will keep running hourly while Docker and the computer remain available. Control instructions are in [deployment.md](deployment.md).

## Remaining environment validation

- Native Tableau verification was pending at this September 13 checkpoint. It was completed on September 15 as recorded below. The `.twbx` is a local packaged workbook, not a published dashboard URL.
- No cloud deployment or external alerting service is configured. The local Docker scheduler is running; health results are available through Docker and the CLI.

## Release and repeatable demo verification — September 14, 2026

The release adds an isolated offline demonstration, packaged public-source input,
CI configuration, and portable source/wheel builders. Accepted demo measurements
are unchanged recorded NWS payloads. Three labeled malformed probes demonstrate
rejection; none become accepted weather observations.

The final expanded suite passed **201 tests in 26.82 seconds**, with all source,
test, and helper lint checks passing. Dependency compatibility also passed.
The machine-readable test result is `artifacts/test-results.xml`.

Both the local demonstration and the installed-package demonstration passed
**20 evidence checks and all 37 Tableau reconciliation checks**. Each produced
18 observations for three stations in two committed batches. Exact replay
preserved all six warehouse tables and exported CSV bytes; the injected
precommit failure preserved the same state; recovery inserted six new
observations and retained three overlapping observations unchanged.

Evidence is available in `artifacts/demo/report.md` and `report.json`. The
installed-package evidence is in `artifacts/wheel-smoke-final/demo/report.json`.
These outputs are separate from the live warehouse and hourly worker.

The wheel was installed with `--no-index --no-deps` in a disposable Linux
container with networking disabled. The container used the already installed
pinned dependencies and Java 17. It mounted only the release directory and an
empty results directory, changed to `/tmp`, and verified imports came from the
installed package. The image's `/app` source directory was hidden behind an
empty temporary filesystem. The complete demo loaded its recording through packaged
resources. This verifies operation without the developer checkout or a live API;
it does not claim an offline installation of Python, Java, and all dependencies
on an unprepared machine.

The final live inspection also caught a waiting worker with a UTC deadline of
15:57 and no refresh by 17:17 UTC. Its earlier successful cycles had long wall-time
gaps and no reported failures, consistent with host/VM suspension. The scheduler
previously waited only on a monotonic clock. The release now checks both the
monotonic and UTC deadlines in waits of at most 30 seconds, performs one overdue
catch-up, and resumes a normal cadence. Regression tests cover a paused monotonic
clock, a backwards UTC correction, and a forward jump of both clocks without a
burst of jobs. Existing graceful-stop and duplicate-worker tests still pass.

After rebuilding and updating the existing Docker worker, its immediate run
succeeded at `2026-09-14T18:18:21Z`. The inspected warehouse contained **1,573
observations, three stations, and 13 committed batches**, with observations through
18:00 UTC and checkpoints at 18:18 UTC. All 37 live dashboard audit checks passed,
and both the CLI health report and Docker reported healthy.
The next scheduled run was 19:18 UTC. These counts are a dated snapshot; the
worker continues collecting new observations while Docker and the computer run.

The source ZIP includes source, documentation, tests, deployment configuration,
CI, and the public demo recording. Its allowlist excludes live data, backups,
credentials, and local runtime installations. The archive contains an individual
file checksum manifest and has a separate SHA-256 sidecar.

The GitHub Actions workflow is supplied for a standalone repository. No remote
repository was published and no hosted workflow was run. Native Tableau visual
verification was pending at this September 14 checkpoint.

## Native Tableau compatibility correction — September 15, 2026

The original frozen package (`936121e5611b7f9abb549f3c99e3b39212a896e4f37af24aa3764d109749f85d`)
failed to open in Tableau Desktop 2026.2.2 with error D2E8DA72. The generator had
declared workbook format 26.1 with `ManifestByVersion`. Installed Tableau examples
use format 18.1 with explicit feature flags. The generator now uses this proven
format, fixes the one-decimal percentage pattern to `p0.0%`, and explicitly shows
all station bar labels. The hourly worker includes the correction.

The corrected frozen package SHA-256 is
`17b17e28f1884665dd1f26bace161932fc3335069799620e9f6719e82ee6889c`.
All three embedded CSV files are byte-identical to the previously audited snapshot.
This lets the original warehouse reconciliation support the same data while the
new structure and native checks assess the repaired workbook. It is not a claim
that the frozen package matches the newer live warehouse.

Native Tableau opened the corrected package without repair or missing-file prompts.
All three charts, the station legend, timestamps, axes, labels, and completeness
note render correctly. Counts are KAUS 595, KJFK 447, KORD 589; completeness is
96.8%, 99.6%, and 99.7%. Tableau's View Data reports 1,631 observation rows,
nine daily rows, and 14 quality rows. All six selected hourly temperature values
match the reference within 0.00000001 °C in a native worksheet export.

Evidence is under `artifacts/native-validation/20260915-corrected/`:
`dashboard-native.png` is a 2400×1700 image exported directly from Tableau,
`hourly-tableau.csv` contains Tableau's calculated marks, and `native-signoff.json`
records the native checks. The original failing package remains unchanged.

The expanded regression suite passed **210 tests in 37.73 seconds**, and Ruff
passed. New regressions reject the original unsupported declaration even when all
CSV hashes still agree, and detect hidden labels and incorrect percentage formats.
After the Docker rebuild and graceful worker restart, installed generator/audit
source hashes match the checkout. The next successful live cycle at 16:25:39 UTC
passed **41 of 41 dashboard checks**, with Docker and CLI health both healthy.
That later live snapshot contains 2,374 observations across three stations, through
16:10 UTC. These changing live counts are separate from the frozen native packet.

## Final 0.2.1 release acceptance — September 16, 2026

The corrected source and wheel are under `artifacts/release/0.2.1/`. The release
report records their SHA-256 digests, sizes, archive audits, and installed-package
evidence. Every source ZIP manifest digest matches its member, and every archived
source file matches the final checkout. The wheel contains only the expected
application, recording, and package metadata files.

The final wheel was installed into a new environment in a Linux ARM64 container
with networking disabled and the `/app` checkout hidden. Its distribution and
runtime versions both reported `0.2.1`; all installed package bytes matched the
wheel. The complete recorded-data demo passed 20 evidence checks and the generated
dashboard passed all 41 reconciliation checks. Its result also reports workbook
format `18.1` and the native verification application accurately.

The full suite passed 210 tests before the final informational metadata correction.
The directly affected Tableau and demo tests then passed 12 of 12, and Ruff remained
clean. The final 0.2.1 image then completed its first live cycle successfully:
88 inputs were accepted, with 18 inserts, three corrections, 67 unchanged rows,
and no rejects or duplicates. The worker had completed 53 batches, the warehouse
contained 3,132 observations across all three stations through 13:20 UTC, the
current dashboard passed all 41 audit checks, and health was `healthy`. These live
counts are dated and will continue changing while the worker runs.
