# Data-quality rules

Validation happens in PySpark before warehouse loading. Raw data remains available for investigation. Rejected records are written with machine-readable reasons; they are not silently dropped or converted to zero.

| Check | Behavior |
|---|---|
| Malformed JSON or envelope | Quarantine the original input line |
| Missing batch ID, station ID, ingestion timestamp, or feature JSON | Reject |
| Missing/invalid observation timestamp, or missing/mismatched source station | Reject |
| Unknown station | Reject instead of creating an accidental dimension member |
| Observation later than the fixed batch end plus ten minutes | Reject as a future timestamp |
| Numeric string, boolean, object, non-finite number, or unsupported measurement unit | Reject non-null invalid measurements |
| Temperature outside −100 to 60 °C | Reject |
| Relative humidity outside 0 to 100% | Reject |
| Sustained wind outside 0 to 500 km/h | Reject |
| Invalid coordinate types or latitude/longitude bounds | Reject |
| Optional null temperature, humidity, or wind | Accept and count missing fields |
| Multiple valid rows with the same station/timestamp | Keep a deterministic winner; count remaining rows as duplicates |
| Empty or stale station feed, or long observation gap | Surface coverage signals; irregular reporting means this is not proof of a lost source event |

Temperature units supported: Celsius, Fahrenheit, Kelvin. Wind units supported: km/h, m/s, knots, miles/hour. Humidity is a percentage. Thresholds are explicit project rules rather than authoritative meteorological quality certifications. Source quality-control flags remain in raw features; the current rules do not implement the full NWS quality-control taxonomy.

Every run reconciles `input_count = accepted_count + rejected_count + duplicate_count`. Deduplication follows validation, so an invalid duplicate is a rejected input rather than a valid duplicate. `missing_measurement_count` counts null measurement fields among accepted unique observations (0–3 per row).

Coverage signals compare each configured station with the batch's accepted observations. An absent station is flagged, and data more than 120 minutes behind the fixed batch end is considered stale. Gaps exceeding 120 minutes inside a batch are signals for investigation. The API does not guarantee a uniform reporting interval, so missing-event counts cannot be inferred reliably. No external alert service is configured.

The warehouse independently verifies required fields, uniqueness, foreign-key membership, quality-count reconciliation, and missing-field totals before committing. Data rejected by Spark is preserved before advancing a successful checkpoint. To apply a changed transformation to existing data, rebuild into a fresh data directory/warehouse by replaying the archived manifests. A committed batch cannot change its logical output, and the current fact update policy uses the source payload hash; changing only the batch ID does not cause recalculation of unchanged raw payloads. Explicit transformation versioning and schema migration are future extensions.
