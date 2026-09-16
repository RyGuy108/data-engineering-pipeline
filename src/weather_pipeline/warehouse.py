"""Transactional local analytical warehouse and portable dashboard extracts.

All database timestamps are UTC, represented as timezone-free TIMESTAMP values.
The observation grain is one station and observation timestamp. A version ledger
tracks newer sightings of unchanged payloads without rewriting business facts.
"""

from __future__ import annotations

import hashlib
import json
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any
from uuid import uuid4

import duckdb


_COUNTS = (
    "input_count", "accepted_count", "rejected_count", "duplicate_count",
    "missing_measurement_count",
)

_SCHEMA = """
CREATE TABLE IF NOT EXISTS dim_station (
    station_id VARCHAR PRIMARY KEY,
    station_name VARCHAR NOT NULL,
    latitude DOUBLE,
    longitude DOUBLE,
    metadata_ingested_at TIMESTAMP NOT NULL
);
CREATE TABLE IF NOT EXISTS dim_date (
    observation_date DATE PRIMARY KEY,
    year SMALLINT NOT NULL,
    month SMALLINT NOT NULL,
    day SMALLINT NOT NULL,
    iso_day_of_week SMALLINT NOT NULL
);
CREATE TABLE IF NOT EXISTS fact_observation (
    station_id VARCHAR NOT NULL REFERENCES dim_station(station_id),
    observed_at TIMESTAMP NOT NULL,
    observation_date DATE NOT NULL REFERENCES dim_date(observation_date),
    temperature_c DOUBLE,
    humidity_pct DOUBLE,
    wind_kph DOUBLE,
    text_description VARCHAR,
    latitude DOUBLE,
    longitude DOUBLE,
    payload_hash VARCHAR NOT NULL,
    source_ingested_at TIMESTAMP NOT NULL,
    batch_id VARCHAR NOT NULL,
    loaded_at TIMESTAMP NOT NULL,
    PRIMARY KEY (station_id, observed_at)
);
CREATE TABLE IF NOT EXISTS observation_versions (
    station_id VARCHAR NOT NULL,
    observed_at TIMESTAMP NOT NULL,
    source_ingested_at TIMESTAMP NOT NULL,
    payload_hash VARCHAR NOT NULL,
    PRIMARY KEY (station_id, observed_at)
);
CREATE TABLE IF NOT EXISTS etl_runs (
    batch_id VARCHAR PRIMARY KEY,
    input_fingerprint VARCHAR NOT NULL,
    source VARCHAR NOT NULL,
    window_start TIMESTAMP NOT NULL,
    window_end TIMESTAMP NOT NULL,
    source_ingested_at TIMESTAMP NOT NULL,
    completed_at TIMESTAMP NOT NULL,
    input_count BIGINT NOT NULL,
    accepted_count BIGINT NOT NULL,
    rejected_count BIGINT NOT NULL,
    duplicate_count BIGINT NOT NULL,
    missing_measurement_count BIGINT NOT NULL,
    inserted_count BIGINT NOT NULL,
    updated_count BIGINT NOT NULL,
    unchanged_count BIGINT NOT NULL,
    raw_path VARCHAR NOT NULL,
    curated_path VARCHAR NOT NULL,
    rejected_path VARCHAR NOT NULL
);
CREATE TABLE IF NOT EXISTS source_checkpoints (
    station_id VARCHAR PRIMARY KEY,
    watermark TIMESTAMP NOT NULL,
    batch_id VARCHAR NOT NULL,
    updated_at TIMESTAMP NOT NULL
);
CREATE OR REPLACE VIEW dashboard_observations AS
SELECT
    f.station_id, s.station_name, d.observation_date, f.observed_at,
    f.temperature_c, f.humidity_pct, f.wind_kph, f.text_description,
    coalesce(f.latitude, s.latitude) AS latitude,
    coalesce(f.longitude, s.longitude) AS longitude,
    (CAST(f.temperature_c IS NULL AS INTEGER)
     + CAST(f.humidity_pct IS NULL AS INTEGER)
     + CAST(f.wind_kph IS NULL AS INTEGER)) AS missing_measurement_count,
    f.batch_id, f.source_ingested_at, f.loaded_at
FROM fact_observation f
JOIN dim_station s USING (station_id)
JOIN dim_date d USING (observation_date);
CREATE OR REPLACE VIEW dashboard_daily AS
SELECT observation_date, station_id, station_name,
    count(*) AS observation_count,
    avg(temperature_c) AS avg_temperature_c,
    min(temperature_c) AS min_temperature_c,
    max(temperature_c) AS max_temperature_c,
    avg(humidity_pct) AS avg_humidity_pct,
    avg(wind_kph) AS avg_wind_kph,
    sum(missing_measurement_count)::BIGINT AS missing_measurement_count,
    100.0 * (3 * count(*) - sum(missing_measurement_count))
        / (3 * count(*)) AS measurement_completeness_pct,
    max(observed_at) AS latest_observed_at
FROM dashboard_observations
GROUP BY observation_date, station_id, station_name;
CREATE OR REPLACE VIEW dashboard_quality AS
SELECT batch_id, source, window_start, window_end, source_ingested_at,
    completed_at, input_count, accepted_count, rejected_count,
    duplicate_count, missing_measurement_count,
    inserted_count, updated_count, unchanged_count,
    100.0 * rejected_count / nullif(input_count, 0) AS rejection_pct
FROM etl_runs;
"""


def _connect(db_path: str | Path) -> duckdb.DuckDBPyConnection:
    path = Path(db_path).expanduser().resolve()
    path.parent.mkdir(parents=True, exist_ok=True)
    connection = duckdb.connect(str(path))
    connection.execute("SET TimeZone = 'UTC'")
    connection.execute(_SCHEMA)
    return connection


def _utc(value: str | datetime) -> datetime:
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00")) if isinstance(value, str) else value
    if not isinstance(parsed, datetime) or parsed.tzinfo is None:
        raise ValueError("Manifest timestamps must include a timezone")
    return parsed.astimezone(timezone.utc).replace(tzinfo=None)


def _json_default(value: Any) -> str:
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    if isinstance(value, Path):
        return str(value)
    raise TypeError(f"Cannot serialize {type(value).__name__}")


def _input_fingerprint(connection, manifest: dict, result: dict) -> str:
    """Fingerprint logical rows, not nondeterministic Parquet byte layouts.

    Location-only paths are excluded so an identical archived batch can be
    replayed from a different directory. The raw input itself is hashed.
    """
    payload = {
        "manifest": {k: v for k, v in manifest.items() if k not in {"raw_path", "manifest_path"}},
        "report": {key: result["report"][key] for key in _COUNTS},
    }
    digest = hashlib.sha256(json.dumps(
        payload, sort_keys=True, separators=(",", ":"),
        default=_json_default, allow_nan=False,
    ).encode())
    with Path(manifest["raw_path"]).open("rb") as raw:
        for chunk in iter(lambda: raw.read(1024 * 1024), b""):
            digest.update(chunk)
    cursor = connection.execute(
        "SELECT to_json(i) FROM incoming i ORDER BY station_id, observed_at"
    )
    while rows := cursor.fetchmany(10_000):
        for (row,) in rows:
            digest.update(b"\n")
            digest.update(row.encode())
    return digest.hexdigest()


def _stage(connection, manifest: dict, result: dict) -> None:
    parquet = Path(result["curated_path"]).expanduser().resolve()
    files = sorted(str(path) for path in parquet.rglob("*.parquet")) if parquet.is_dir() else [str(parquet)]
    if not files:
        raise ValueError("Curated output must contain a schema-bearing Parquet file, even when empty")
    connection.execute("""
        CREATE TEMP TABLE incoming AS
        SELECT station_id::VARCHAR AS station_id,
            observed_at::TIMESTAMP AS observed_at,
            temperature_c::DOUBLE AS temperature_c,
            humidity_pct::DOUBLE AS humidity_pct,
            wind_kph::DOUBLE AS wind_kph,
            text_description::VARCHAR AS text_description,
            latitude::DOUBLE AS latitude,
            longitude::DOUBLE AS longitude,
            payload_hash::VARCHAR AS payload_hash,
            source_ingested_at::TIMESTAMP AS source_ingested_at
        FROM read_parquet(?)
    """, [files])
    report = result["report"]
    if any(not isinstance(report[k], int) or isinstance(report[k], bool) or report[k] < 0 for k in _COUNTS):
        raise ValueError("Quality counts must be nonnegative integers")
    if report["input_count"] != sum(report[k] for k in ("accepted_count", "rejected_count", "duplicate_count")):
        raise ValueError("Input count must equal accepted + rejected + duplicate counts")
    if connection.execute("SELECT count(*) FROM incoming").fetchone()[0] != report["accepted_count"]:
        raise ValueError("Accepted count does not match curated rows")
    if connection.execute("""
        SELECT count(*) FROM incoming
        WHERE station_id IS NULL OR trim(station_id) = '' OR observed_at IS NULL
            OR payload_hash IS NULL OR trim(payload_hash) = '' OR source_ingested_at IS NULL
    """).fetchone()[0]:
        raise ValueError("Curated rows contain missing required fields")
    if connection.execute("""
        SELECT count(*) FROM (
            SELECT station_id, observed_at FROM incoming
            GROUP BY station_id, observed_at HAVING count(*) > 1
        )
    """).fetchone()[0]:
        raise ValueError("Curated rows contain duplicate business keys")
    missing = connection.execute("""
        SELECT coalesce(sum(CAST(temperature_c IS NULL AS INTEGER)
            + CAST(humidity_pct IS NULL AS INTEGER)
            + CAST(wind_kph IS NULL AS INTEGER)), 0) FROM incoming
    """).fetchone()[0]
    if missing != report["missing_measurement_count"]:
        raise ValueError("Missing measurement count does not match curated rows")
    stations = manifest["stations"]
    station_ids = [station["station_id"] for station in stations]
    if len(set(station_ids)) != len(station_ids) or not station_ids:
        raise ValueError("Manifest must have unique station metadata")
    if any(not isinstance(station_id, str) or not station_id.strip() for station_id in station_ids):
        raise ValueError("Manifest station IDs must be nonempty strings")
    known = set(station_ids)
    if any(row[0] not in known for row in connection.execute("SELECT DISTINCT station_id FROM incoming").fetchall()):
        raise ValueError("Curated station has no station metadata in the manifest")
    if _utc(manifest["start"]) >= _utc(manifest["end"]):
        raise ValueError("Manifest start must precede end")
    _utc(manifest["ingested_at"])
    if not isinstance(manifest["advance_checkpoint"], bool):
        raise ValueError("advance_checkpoint must be a boolean")
    if not manifest["windows"]:
        raise ValueError("Manifest must contain ingestion windows")
    for window in manifest["windows"]:
        if window["station_id"] not in known:
            raise ValueError("Checkpoint station has no station metadata")
        if _utc(window["start"]) >= _utc(window["end"]):
            raise ValueError("Window start must precede end")


def load_batch(
    db_path: str | Path,
    manifest: dict,
    result: dict,
    fail_before_commit: bool = False,
) -> dict:
    """Load a validated batch atomically; identical committed batch IDs are no-ops.

    ``unchanged_count`` includes unchanged payloads and stale versions skipped.
    A newer unchanged payload updates only the version ledger, keeping facts
    stable while preventing intermediate stale versions from overwriting them.
    """
    if not isinstance(manifest["batch_id"], str) or not manifest["batch_id"].strip():
        raise ValueError("Batch ID must be a nonempty string")
    connection = _connect(db_path)
    try:
        _stage(connection, manifest, result)
        fingerprint = _input_fingerprint(connection, manifest, result)
        connection.execute("BEGIN TRANSACTION")
        try:
            previous = connection.execute(
                "SELECT input_fingerprint FROM etl_runs WHERE batch_id = ?", [manifest["batch_id"]]
            ).fetchone()
            if previous:
                if previous[0] != fingerprint:
                    raise ValueError(f"Batch ID collision: {manifest['batch_id']} has different inputs")
                connection.execute("COMMIT")
                return {"batch_id": manifest["batch_id"], "status": "already_loaded", "inserted_count": 0,
                        "updated_count": 0, "unchanged_count": result["report"]["accepted_count"]}
            connection.execute("""
                CREATE TEMP TABLE incoming_stations (
                    station_id VARCHAR, station_name VARCHAR, latitude DOUBLE,
                    longitude DOUBLE, metadata_ingested_at TIMESTAMP
                )
            """)
            connection.executemany("INSERT INTO incoming_stations VALUES (?, ?, ?, ?, ?)", [
                (station["station_id"], station.get("name") or station["station_id"],
                 station.get("latitude"), station.get("longitude"), _utc(manifest["ingested_at"]))
                for station in manifest["stations"]
            ])
            connection.execute("""
                MERGE INTO dim_station d USING incoming_stations s USING (station_id)
                WHEN MATCHED AND s.metadata_ingested_at >= d.metadata_ingested_at THEN
                    UPDATE SET station_name = s.station_name, latitude = s.latitude,
                        longitude = s.longitude, metadata_ingested_at = s.metadata_ingested_at
                WHEN NOT MATCHED THEN INSERT BY NAME
            """)
            connection.execute("""
                INSERT INTO dim_date
                SELECT DISTINCT observed_at::DATE, year(observed_at), month(observed_at),
                    day(observed_at), isodow(observed_at)
                FROM incoming
                ON CONFLICT DO NOTHING
            """)
            connection.execute("""
                CREATE TEMP TABLE eligible AS
                SELECT i.* FROM incoming i
                LEFT JOIN observation_versions v USING (station_id, observed_at)
                WHERE v.station_id IS NULL
                    OR i.source_ingested_at > v.source_ingested_at
                    OR (i.source_ingested_at = v.source_ingested_at AND i.payload_hash >= v.payload_hash)
            """)
            inserted, updated = connection.execute("""
                SELECT count(*) FILTER (WHERE f.station_id IS NULL),
                    count(*) FILTER (WHERE f.station_id IS NOT NULL AND e.payload_hash <> f.payload_hash)
                FROM eligible e LEFT JOIN fact_observation f USING (station_id, observed_at)
            """).fetchone()
            unchanged = result["report"]["accepted_count"] - inserted - updated
            connection.execute("""
                MERGE INTO fact_observation f
                USING (SELECT *, observed_at::DATE AS observation_date,
                    ?::VARCHAR AS batch_id, current_timestamp::TIMESTAMP AS loaded_at FROM eligible) s
                USING (station_id, observed_at)
                WHEN MATCHED AND s.payload_hash <> f.payload_hash THEN
                    UPDATE SET temperature_c = s.temperature_c, humidity_pct = s.humidity_pct,
                        wind_kph = s.wind_kph, text_description = s.text_description,
                        latitude = s.latitude, longitude = s.longitude, payload_hash = s.payload_hash,
                        source_ingested_at = s.source_ingested_at, batch_id = s.batch_id,
                        loaded_at = s.loaded_at
                WHEN NOT MATCHED THEN INSERT BY NAME
            """, [manifest["batch_id"]])
            connection.execute("""
                MERGE INTO observation_versions v
                USING (SELECT station_id, observed_at, source_ingested_at, payload_hash FROM eligible) s
                USING (station_id, observed_at)
                WHEN MATCHED THEN UPDATE SET source_ingested_at = s.source_ingested_at, payload_hash = s.payload_hash
                WHEN NOT MATCHED THEN INSERT BY NAME
            """)
            connection.execute("""
                INSERT INTO etl_runs VALUES (
                    ?, ?, ?, ?, ?, ?, current_timestamp::TIMESTAMP,
                    ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
                )
            """, [manifest["batch_id"], fingerprint, manifest["source"], _utc(manifest["start"]),
                  _utc(manifest["end"]), _utc(manifest["ingested_at"]),
                  *(result["report"][key] for key in _COUNTS), inserted, updated, unchanged,
                  str(manifest["raw_path"]), str(result["curated_path"]), str(result["rejected_path"])])
            if manifest["advance_checkpoint"]:
                for window in manifest["windows"]:
                    connection.execute("""
                        INSERT INTO source_checkpoints VALUES (?, ?, ?, current_timestamp::TIMESTAMP)
                        ON CONFLICT (station_id) DO UPDATE SET watermark = excluded.watermark,
                            batch_id = excluded.batch_id, updated_at = excluded.updated_at
                        WHERE excluded.watermark > source_checkpoints.watermark
                    """, [window["station_id"], _utc(window["end"]), manifest["batch_id"]])
            if fail_before_commit:
                raise RuntimeError("Injected failure before warehouse commit")
            connection.execute("COMMIT")
        except Exception:
            connection.execute("ROLLBACK")
            raise
        return {"batch_id": manifest["batch_id"], "status": "loaded", "inserted_count": inserted,
                "updated_count": updated, "unchanged_count": unchanged}
    finally:
        connection.close()


def get_checkpoints(db_path: str | Path) -> dict[str, str]:
    connection = _connect(db_path)
    try:
        return {station: watermark.replace(tzinfo=timezone.utc).isoformat()
                for station, watermark in connection.execute(
                    "SELECT station_id, watermark FROM source_checkpoints ORDER BY station_id"
                ).fetchall()}
    finally:
        connection.close()


def warehouse_summary(db_path: str | Path) -> dict:
    connection = _connect(db_path)
    try:
        earliest, latest, loaded = connection.execute(
            "SELECT min(observed_at), max(observed_at), max(loaded_at) FROM fact_observation"
        ).fetchone()
        def utc_string(value):
            return value.replace(tzinfo=timezone.utc).isoformat() if value else None

        return {
            "observation_count": connection.execute("SELECT count(*) FROM fact_observation").fetchone()[0],
            "station_count": connection.execute("SELECT count(*) FROM dim_station").fetchone()[0],
            "completed_batch_count": connection.execute("SELECT count(*) FROM etl_runs").fetchone()[0],
            "earliest_observed_at": utc_string(earliest),
            "latest_observed_at": utc_string(latest),
            "latest_fact_loaded_at": utc_string(loaded),
            "checkpoints": {station: utc_string(watermark) for station, watermark in connection.execute(
                "SELECT station_id, watermark FROM source_checkpoints ORDER BY station_id"
            ).fetchall()},
        }
    finally:
        connection.close()


def export_dashboard(db_path: str | Path, output_dir: str | Path) -> dict:
    """Export the three dashboard views from one consistent database snapshot.

    Each file is atomically replaced after all queries succeed. CSV timestamps
    are UTC; missing numeric measurements remain empty cells, never zeroes.
    """
    output = Path(output_dir).expanduser().resolve()
    output.mkdir(parents=True, exist_ok=True)
    connection = _connect(db_path)
    staged: list[tuple[Path, Path]] = []
    try:
        connection.execute("BEGIN TRANSACTION")
        for view, order in (
            ("dashboard_observations", "observed_at, station_id"),
            ("dashboard_daily", "observation_date, station_id"),
            ("dashboard_quality", "completed_at, batch_id"),
        ):
            destination = output / f"{view}.csv"
            temporary = output / f".{view}.{uuid4().hex}.tmp"
            staged.append((temporary, destination))
            connection.execute(
                f"COPY (SELECT * FROM {view} ORDER BY {order}) TO ? (FORMAT CSV, HEADER TRUE)",
                [str(temporary)],
            )
        connection.execute("COMMIT")
        for temporary, destination in staged:
            temporary.replace(destination)
        return {destination.stem: str(destination) for _, destination in staged}
    finally:
        connection.close()
        for temporary, _ in staged:
            temporary.unlink(missing_ok=True)
