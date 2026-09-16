# Dashboard release validation

The Tableau package is a snapshot of the warehouse at generation time. Every
`weather-pipeline dashboard` run now creates the workbook, audits it against the
current warehouse, and saves `data/tableau/audit.json` and
`data/tableau/audit.md`. A failed audit makes dashboard generation fail.
`weather-pipeline run --with-dashboard` also refreshes and audits the dashboard
after a successful load.

Run the audit alone to check an existing package without refreshing it:

```sh
weather-pipeline audit-dashboard
weather-pipeline audit-dashboard /path/to/weather_observatory.twbx
```

The command prints structured JSON and returns a nonzero exit status on failure.
It accepts a `.twb` with its adjacent `Data/` folder as well as a portable `.twbx`.
The audit itself never initializes a warehouse, creates warehouse tables, or
updates the database. All warehouse queries and reference CSV exports share one
read-only transaction, so the comparison uses a consistent snapshot. Temporary
reference exports are removed after the audit.

## Automated evidence

| Check | What a passing result establishes |
| --- | --- |
| Package membership | Exactly one expected workbook and all three CSV sources are present; no extra archive members are extracted. |
| Document format | Workbook and datasource versions use document format `18.1`, with explicit `SheetIdentifierTracking` and `SortTagCleanup` flags; the rejected `26.1` / `ManifestByVersion` combination is absent. |
| Source schema and connections | Generated data sources have the expected columns and types, use relative `Data/` paths, and contain no connection credentials. |
| Current snapshot bytes | Each packaged CSV has the same SHA-256 digest as a freshly exported, deterministically ordered warehouse view. |
| Counts and business keys | Row counts agree; observation station/time keys, daily station/date keys, and quality batch keys are unique and non-null. |
| Missing values | Null counts agree for every column; missing weather measurements are preserved as nulls. |
| Numeric measures | Sums, averages, minima, and maxima agree for every numeric column in all three data sources. Small floating-point aggregation differences use a tolerance of `1e-10`; byte comparison remains exact. |
| Worksheets and formulas | The expected temperature, observation-count, and field-completeness sheets exist, with the intended shelves, aggregations, hour truncation and completeness calculation. Percentage format is `p0.0%`, and bar labels are explicitly enabled for all marks without culling. |
| Dashboard | The Weather Observatory dashboard includes the expected sheet zones; its count, station total, and freshness header agree with the warehouse snapshot. |

The exact-byte check covers textual fields and changes that aggregate checks
alone could miss. A valid older package fails after any change to the exported
warehouse data, including a correction that leaves the observation count
unchanged or a newly recorded ETL run. A renamed station can leave all numeric
measures unchanged and still fail byte reconciliation. Line-ending or CSV-format
changes also fail this strict generated-artifact check; regenerate with the
project command to restore the canonical package.

The three exported views are `dashboard_observations`, `dashboard_daily`, and
`dashboard_quality`. Their full expected and packaged profiles appear in the
JSON report. The Markdown report summarizes the result and identifies any
failed checks. Successful reports contain aggregates, hashes, and validation results.

## Historical schema check and its limits

The September 13, 2026 build passed a structural check against Tableau's published
2026.1 XSD, with temporary definitions for its two missing namespace imports.
That historical result did **not** establish Desktop compatibility. On September
15, Tableau Desktop 2026.2.2 rejected the original workbook with error `D2E8DA72`,
despite its earlier schema pass. The original workbook declared the application-like
version `26.1` and used `ManifestByVersion`; native opening exposed incompatible
schema feature selection.

The corrected generator uses Tableau's **document format `18.1`**, which is
distinct from the Desktop application release, and explicit
`SheetIdentifierTracking` and `SortTagCleanup` flags. This matches the format
conventions in the installed application's bundled workbooks. It also corrects
the percentage format from `p1%` to `p0.0%` and explicitly enables bar labels.
These corrections were checked in Tableau itself; their data exports remain
byte-for-byte identical to the frozen original snapshot.

`scripts/validate_tableau_schema.py` is retained only to reproduce the historical
structural check using a separately downloaded
[`twb_2026.1.0.xsd`](https://raw.githubusercontent.com/tableau/tableau-document-schemas/main/schemas/2026_1/twb_2026.1.0.xsd)
from Tableau's [schema repository](https://github.com/tableau/tableau-document-schemas):

```sh
python scripts/validate_tableau_schema.py /path/to/historical_workbook.twb /path/to/twb_2026.1.0.xsd
```

It needs `xmllint`, supplies local definitions for two imports omitted by that
published schema, and rejects workbooks using attributes from those auxiliary
namespaces. It does not download or modify either input. A pass means agreement
with the supplied XSD only. This helper is **not a dashboard acceptance gate**;
do not change a natively working workbook merely to satisfy the older XSD.

## Native verification on September 15, 2026

The corrected frozen workbook opened in **Tableau Desktop 2026.2.2** and all three
charts rendered. The station bars showed **595 / 447 / 589** observations and
**96.8% / 99.6% / 99.7%** completeness for **KAUS / KJFK / KORD**, respectively.
Six noon UTC temperature marks, covering September 13 and 14 at all three
stations, matched the reference values within `1e-8` °C using Tableau's native
**Worksheet → Export → Data** output. Native View Data also confirmed the
observation, daily, and quality sources contained **1,631**, **9**, and **14**
rows. The native acceptance checklist for this corrected package is complete.

The local packet at `artifacts/native-validation/20260915-corrected/` contains
the reviewed workbook, 2400 × 1700 native dashboard image, native hourly data
export, and data and mark reconciliation. Generated evidence is intentionally
excluded from Git and retained with the local release records.
The corrected package SHA-256 is
`17b17e28f1884665dd1f26bace161932fc3335069799620e9f6719e82ee6889c`.
These results apply to that frozen package and the tested Desktop version;
subsequent generated snapshots need their own audit and any appropriate native
checks. Local evidence artifacts are separate from the source-only release ZIP.

<a id="native-visual-check-remains-open"></a>

## Repeat the native acceptance check

Automated reconciliation cannot observe marks, legends, labels, or layout. Use
this procedure when accepting a new package or changing the generator:

1. Open the generated `.twbx` in Tableau Desktop and select **Weather Observatory**.
2. Confirm all three sheets render, the station color legend is readable, and the
   full title, refresh timestamps, axes, labels, and completeness note fit.
3. Inspect the data sources: no missing-file prompt should appear, and the
   observation, daily, and quality data should all be available.
4. Compare each station's count and completeness to the SQL below. Inspect a few
   hourly temperature marks against the second query. Tableau's temperature
   averages ignore null values; completeness uses all observation rows.
5. Save native screenshots or a Tableau image export and record the Tableau
   version, package hash, audit time, per-check results, and overall outcome.
   Retain native data exports when using them to verify mark values. A screenshot
   of another rendering surface does not count as Tableau validation.

```sql
SELECT station_id, count(*) AS observations,
       100.0 * (3 * count(*) - sum(missing_measurement_count))
           / (3 * count(*)) AS present_measurements_pct
FROM dashboard_observations
GROUP BY station_id
ORDER BY station_id;

SELECT station_id, date_trunc('hour', observed_at) AS observation_hour_utc,
       avg(temperature_c) AS average_temperature_c
FROM dashboard_observations
GROUP BY station_id, observation_hour_utc
ORDER BY observation_hour_utc, station_id;
```

Keep the native acceptance record separate from the automated audit. The audit's
JSON field `native_visual_validation` deliberately remains `not_performed` even
after a separate native review, because the audit function cannot observe the
Tableau application.
