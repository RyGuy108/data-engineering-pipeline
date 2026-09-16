"""Strict Spark transformations for NWS observation batches.

Measurement nulls represent unavailable observations, never zero. Invalid records
are quarantined before deduplication; duplicates of otherwise valid records are
counted separately. All timestamps and output business keys use UTC.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Iterable

from pyspark.sql import Column, SparkSession, Window, functions as F, types as T


ENVELOPE_SCHEMA = T.StructType(
    [T.StructField(name, T.StringType(), True) for name in
     ("batch_id", "ingested_at", "station_id", "raw_json", "_corrupt_record")]
)
CURATED_COLUMNS = (
    "station_id", "observed_at", "temperature_c", "humidity_pct", "wind_kph",
    "text_description", "latitude", "longitude", "payload_hash", "source_ingested_at",
)
ISO_TIMESTAMP = r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(\.\d{1,9})?(Z|[+-]\d{2}:\d{2})$"
NUMERIC_TYPE = r"^(BIGINT|DOUBLE|FLOAT|DECIMAL\([0-9]+,[0-9]+\))$"


def create_spark() -> SparkSession:
    """Create a small, reproducible local Spark session (requires Java 17+)."""
    spark = (
        SparkSession.builder.master("local[2]")
        .appName("weather-data-pipeline")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark


def _variant(path: str, column: str = "_payload") -> Column:
    return F.try_variant_get(F.col(column), path, "variant")


def _present(value: Column) -> Column:
    return value.isNotNull() & ~F.is_variant_null(value)


def _kind(value: Column) -> Column:
    return F.schema_of_variant(value)


def _string(path: str) -> Column:
    return F.try_variant_get(F.col("_payload"), path, "string")


def _number(path: str) -> Column:
    return F.try_variant_get(F.col("_payload"), path, "double")


def _invalid_number(path: str) -> Column:
    value = _variant(path)
    number = _number(path)
    return _present(value) & (
        ~_kind(value).rlike(NUMERIC_TYPE)
        | number.isNull() | F.isnan(number) | (F.abs(number) == F.lit(float("inf")))
    )


def _invalid_string(path: str) -> Column:
    value = _variant(path)
    return _present(value) & (_kind(value) != "STRING")


def _invalid_object(path: str) -> Column:
    value = _variant(path)
    return _present(value) & ~_kind(value).startswith("OBJECT<")


def _reason(condition: Column, name: str) -> Column:
    return F.when(condition, F.lit(name))


def _measurement(name: str, unit_factors: dict[str, tuple[float, float]]) -> tuple[Column, list[Column]]:
    """Return a normalized number and checks for its source value and unit."""
    path = f"$.properties.{name}"
    value_path = f"{path}.value"
    unit_path = f"{path}.unitCode"
    value = _number(value_path)
    unit = _string(unit_path)
    normalized = F.lit(None).cast("double")
    for unit_name, (factor, offset) in unit_factors.items():
        normalized = F.when(unit == unit_name, value * factor + offset).otherwise(normalized)
    reasons = [
        _reason(_invalid_object(path), f"invalid_{name}_object"),
        _reason(_invalid_number(value_path), f"invalid_{name}_value"),
        _reason(_invalid_string(unit_path), f"invalid_{name}_unit_type"),
        _reason(
            _present(_variant(value_path))
            & (unit.isNull() | ~unit.isin(list(unit_factors))),
            f"unsupported_{name}_unit",
        ),
    ]
    return normalized, reasons


def _station_coverage(curated, station_ids: list[str], reference: datetime) -> list[dict]:
    """Report freshness/gap signals, not proof that the source lost observations.

    NWS cadence varies by station. A station absent from this batch has unknown
    freshness; consult its warehouse history before treating it as an outage.
    """
    window = Window.partitionBy("station_id").orderBy("observed_at")
    observations = curated.withColumn("_previous_observed_at", F.lag("observed_at").over(window))
    summaries = observations.groupBy("station_id").agg(
        F.count("*").alias("accepted_count"),
        F.max("observed_at").alias("_latest"),
        F.sum(F.when(
            F.col("observed_at").cast("double") - F.col("_previous_observed_at").cast("double") > 120 * 60,
            1,
        ).otherwise(0)).alias("gap_count"),
    ).select(
        "station_id", "accepted_count", "gap_count",
        F.date_format("_latest", "yyyy-MM-dd'T'HH:mm:ss.SSSSSS'Z'").alias("latest_observed_at"),
        (F.greatest(F.lit(0.0), F.lit(reference.timestamp()) - F.col("_latest").cast("double")) / 60).alias("minutes_behind_reference"),
    )
    by_station = {row.station_id: row.asDict() for row in summaries.collect()}
    result = []
    for station in station_ids:
        coverage = by_station.get(station, {
            "station_id": station, "accepted_count": 0, "gap_count": 0,
            "latest_observed_at": None, "minutes_behind_reference": None,
        })
        coverage["missing_in_batch"] = coverage["accepted_count"] == 0
        coverage["stale"] = (
            coverage["minutes_behind_reference"] is not None
            and coverage["minutes_behind_reference"] > 120
        )
        result.append(coverage)
    return result


def transform_batch(
    spark: SparkSession,
    raw_path: str | Path,
    output_dir: str | Path,
    station_ids: Iterable[str],
    reference_time: str,
) -> dict:
    """Validate, normalize and deterministically deduplicate one raw NDJSON batch.

    ``missing_measurement_count`` is the total null measurement fields (0–3 per
    accepted observation) after deduplication. Temperature, humidity and wind
    bounds are respectively [-100, 60] °C, [0, 100] %, and [0, 500] km/h.
    Observation timestamps may be at most ten minutes after the manifest end.
    Every input line is accepted, rejected or counted as a valid duplicate.
    Station coverage flags absent stations, observations older than two hours,
    and within-batch gaps over two hours as signals for investigation.
    """
    allowed_stations = sorted(set(station_ids))
    if not allowed_stations:
        raise ValueError("station_ids must contain at least one station")
    try:
        reference = datetime.fromisoformat(reference_time.replace("Z", "+00:00"))
    except (TypeError, ValueError) as exc:
        raise ValueError("reference_time must be an ISO timestamp with a timezone") from exc
    if reference.tzinfo is None:
        raise ValueError("reference_time must include a timezone")
    future_limit = reference.astimezone(timezone.utc) + timedelta(minutes=10)
    spark.conf.set("spark.sql.session.timeZone", "UTC")
    destination = Path(output_dir).resolve()
    destination.mkdir(parents=True, exist_ok=True)
    curated_path = str(destination / "curated")
    rejected_path = str(destination / "rejected")

    # Keep the original line as well as raw_json so malformed envelopes remain
    # inspectable. The explicit schema also makes a completely empty batch valid.
    frame = (
        spark.read.text(str(Path(raw_path).resolve()))
        .select(
            F.col("value").alias("raw_envelope"),
            F.from_json("value", ENVELOPE_SCHEMA).alias("_envelope"),
            F.try_parse_json("value").alias("_envelope_json"),
        )
        .select("raw_envelope", "_envelope_json", "_envelope.*")
        .withColumn("_payload", F.try_parse_json("raw_json"))
    )
    temperature, temperature_reasons = _measurement("temperature", {
        "wmoUnit:degC": (1.0, 0.0),
        "wmoUnit:degF": (5.0 / 9.0, -32.0 * 5.0 / 9.0),
        "wmoUnit:K": (1.0, -273.15),
    })
    humidity, humidity_reasons = _measurement("relativeHumidity", {"wmoUnit:percent": (1.0, 0.0)})
    wind, wind_reasons = _measurement("windSpeed", {
        "wmoUnit:km_h-1": (1.0, 0.0),
        "wmoUnit:m_s-1": (3.6, 0.0),
        "wmoUnit:kn": (1.852, 0.0),
        "wmoUnit:mi_h-1": (1.609344, 0.0),
    })
    frame = frame.withColumns({
        "observed_at": F.try_to_timestamp(_string("$.properties.timestamp")),
        "source_ingested_at": F.try_to_timestamp("ingested_at"),
        "temperature_c": temperature,
        "humidity_pct": humidity,
        "wind_kph": wind,
        "text_description": _string("$.properties.textDescription"),
        "longitude": _number("$.geometry.coordinates[0]"),
        "latitude": _number("$.geometry.coordinates[1]"),
        "payload_hash": F.sha2("raw_json", 256),
        "_source_station": F.regexp_extract(
            _string("$.properties.station"),
            r"^https?://api\.weather\.gov/stations/([A-Za-z0-9_-]+)/?$", 1,
        ),
    })
    reasons = [
        _reason(
            F.col("_corrupt_record").isNotNull()
            | F.col("_envelope_json").isNull()
            | ~_kind(F.col("_envelope_json")).startswith("OBJECT<"),
            "invalid_envelope",
        ),
    ]
    for name in ("batch_id", "station_id", "ingested_at", "raw_json"):
        reasons.extend([
            _reason(F.col(name).isNull() | (F.trim(F.col(name)) == ""), f"missing_{name}"),
            _reason(
                _present(_variant(f"$.{name}", "_envelope_json"))
                & (_kind(_variant(f"$.{name}", "_envelope_json")) != "STRING"),
                f"invalid_{name}_type",
            ),
        ])
    reasons.extend([
        _reason(~F.col("station_id").isin(allowed_stations), "unknown_station"),
        _reason(
            F.col("_payload").isNull() | F.is_variant_null("_payload")
            | ~_kind(F.col("_payload")).startswith("OBJECT<"),
            "invalid_payload",
        ),
        _reason(_invalid_object("$.properties"), "invalid_properties"),
        _reason(_invalid_string("$.properties.station"), "invalid_source_station_type"),
        _reason(
            F.col("_source_station").isNull() | (F.col("_source_station") == ""),
            "missing_or_invalid_source_station",
        ),
        _reason(
            (F.col("_source_station") != "") & (F.col("_source_station") != F.col("station_id")),
            "source_station_mismatch",
        ),
        _reason(
            F.col("observed_at").isNull()
            | ~_string("$.properties.timestamp").rlike(ISO_TIMESTAMP)
            | _invalid_string("$.properties.timestamp"),
            "invalid_observed_at",
        ),
        _reason(F.col("observed_at") > F.lit(future_limit), "future_observed_at"),
        _reason(
            F.col("source_ingested_at").isNull() | ~F.col("ingested_at").rlike(ISO_TIMESTAMP),
            "invalid_ingested_at",
        ),
        _reason(_invalid_string("$.properties.textDescription"), "invalid_text_description"),
        *temperature_reasons, *humidity_reasons, *wind_reasons,
        _reason(~F.col("temperature_c").between(-100.0, 60.0), "temperature_out_of_range"),
        _reason(~F.col("humidity_pct").between(0.0, 100.0), "humidity_out_of_range"),
        _reason(~F.col("wind_kph").between(0.0, 500.0), "wind_out_of_range"),
        _reason(_invalid_object("$.geometry"), "invalid_geometry"),
        _reason(
            _present(_variant("$.geometry.coordinates")) & (
                ~_kind(_variant("$.geometry.coordinates")).startswith("ARRAY<")
                | _number("$.geometry.coordinates[0]").isNull()
                | _number("$.geometry.coordinates[1]").isNull()
            ),
            "invalid_coordinates",
        ),
        _reason(_invalid_number("$.geometry.coordinates[0]"), "invalid_longitude"),
        _reason(_invalid_number("$.geometry.coordinates[1]"), "invalid_latitude"),
        _reason(~F.col("longitude").between(-180.0, 180.0), "longitude_out_of_range"),
        _reason(~F.col("latitude").between(-90.0, 90.0), "latitude_out_of_range"),
    ])
    classified = frame.withColumn("reasons", F.filter(F.array(*reasons), lambda reason: reason.isNotNull())).persist()
    ranked = None
    try:
        rejected = classified.where(F.size("reasons") > 0).select(
            "batch_id", "station_id", "ingested_at", "raw_json", "raw_envelope", "reasons",
        )
        rejected_count = rejected.count()
        rejected.write.mode("overwrite").parquet(rejected_path)
        window = Window.partitionBy("station_id", "observed_at").orderBy(
            F.col("source_ingested_at").desc(), F.col("payload_hash").desc(),
        )
        ranked = (
            classified.where(F.size("reasons") == 0)
            .select(*CURATED_COLUMNS)
            .withColumn("_rank", F.row_number().over(window))
            .persist()
        )
        missing = sum(F.col(name).isNull().cast("long") for name in
                      ("temperature_c", "humidity_pct", "wind_kph"))
        stats = ranked.agg(
            F.count("*").alias("valid_count"),
            F.sum(F.when(F.col("_rank") == 1, 1).otherwise(0)).alias("accepted_count"),
            F.sum(F.when(F.col("_rank") == 1, missing).otherwise(0)).alias("missing_measurement_count"),
        ).first()
        accepted_count = int(stats.accepted_count or 0)
        valid_count = int(stats.valid_count)
        curated = ranked.where(F.col("_rank") == 1).select(*CURATED_COLUMNS)
        curated.write.mode("overwrite").parquet(curated_path)
        coverage = _station_coverage(curated, allowed_stations, reference)
        return {
            "curated_path": curated_path,
            "rejected_path": rejected_path,
            "report": {
                "input_count": valid_count + rejected_count,
                "accepted_count": accepted_count,
                "rejected_count": rejected_count,
                "duplicate_count": valid_count - accepted_count,
                "missing_measurement_count": int(stats.missing_measurement_count or 0),
                "station_coverage": coverage,
                "empty_station_count": sum(station["missing_in_batch"] for station in coverage),
                "stale_station_count": sum(station["stale"] for station in coverage),
            },
        }
    finally:
        if ranked is not None:
            ranked.unpersist()
        classified.unpersist()
