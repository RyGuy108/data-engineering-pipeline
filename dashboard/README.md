# Weather Observatory — Tableau dashboard

Generate the dashboard from the loaded warehouse:

```sh
weather-pipeline dashboard
```

The default outputs are `data/tableau/weather_observatory.twb` and
`data/tableau/weather_observatory.twbx`. The `.twbx` contains the native workbook
and all three warehouse CSV exports; it is portable. The `.twb` uses relative
connections to the adjacent `Data/` directory. Keep them together when moving it.

Open the `.twbx` in **Tableau Desktop** and select the **Weather
Observatory** dashboard. Native verification used Desktop **2026.2.2**. It contains three native, editable worksheets:

| Worksheet | Meaning |
| --- | --- |
| Hourly temperature (°C) | Mean of non-null observation temperatures within each UTC hour and station. |
| Observations by station | Count of warehouse observations per station, after business-key deduplication. |
| Present measurements (%) | Present temperature, humidity and wind fields divided by three possible fields per observation, grouped by station. |

Completeness is calculated from individual observation records, so it remains
properly weighted across days with different observation counts. It is **field
completeness**, not the fraction of expected observation periods received. Empty
measurements remain null and do not become zero-degree or calm-wind readings.

Station colors distinguish the temperature series. Hover over native marks to
inspect their values. The dashboard header records the latest observation and
warehouse load timestamps in UTC. The `Weather daily` and `Weather quality` data
sources are included for further analysis; the three displayed sheets use the
observation-level source.

## Refresh and customize

Run the pipeline to load new data, then run `weather-pipeline dashboard` again
and reopen the regenerated package. A previously opened `.twbx` contains its own
data snapshot and does not follow changes in the warehouse automatically.
Regeneration replaces the generated workbook, package and copied CSVs. Save any
manual Tableau edits under another name first.

CSV connections and local packaged workbooks are supported by Tableau. Public
publishing is optional; Tableau creates an extract when saving to Tableau Public.
No workbook has been published by this project. See Tableau's documentation on
[packaged workbooks](https://help.tableau.com/current/pro/desktop/en-us/save_savework_packagedworkbooks.htm)
and [Tableau Public](https://help.tableau.com/current/pro/desktop/en-us/publish_workbooks_tableaupublic.htm).

## Verification status

The corrected dashboard was opened and visually verified in Tableau Desktop
2026.2.2 on September 15, 2026. All three charts render, station counts and
completeness labels match the reference, all three data sources load, and all
140 hourly temperature marks match the independently calculated frozen values.

The verified package and native image/data exports are retained in
`artifacts/native-validation/20260915-corrected/`. The latest hourly package is
`data/tableau/weather_observatory.twbx`; its counts change as the worker runs.
See the [native verification record](../docs/dashboard-validation.md) for the
package hash, evidence, and instructions to repeat the check.

The original package passed a published 2026.1 XSD check but failed native
opening with D2E8DA72. The generator now uses workbook document format `18.1`
and explicit feature flags, as verified in the installed application. The
old schema helper is retained for historical reproduction only; it is not an
acceptance gate for the corrected workbook. Regression checks now reject the
old declaration and incorrect percentage/label settings.
