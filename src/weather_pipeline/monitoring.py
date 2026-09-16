"""Read-only, bounded operational health checks for the local weather pipeline."""

from __future__ import annotations

import json
import math
from datetime import datetime, timedelta, timezone
from pathlib import Path

import duckdb


_DEFAULTS = {
    "max_observation_age_minutes": 180,
    "max_run_age_minutes": 120,
    "max_rejection_pct": 5,
}
_CLOCK_SKEW_MINUTES = 5
_EVENT_TAIL_BYTES = 1024 * 1024
_EVENT_TAIL_LINES = 2000
_SEVERITY = {"healthy": 0, "warning": 1, "critical": 2}


def _utc(value):
    if isinstance(value, str):
        value = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if not isinstance(value, datetime):
        raise ValueError("Expected a timestamp")
    return value.replace(tzinfo=timezone.utc) if value.tzinfo is None else value.astimezone(timezone.utc)


def _iso(value):
    return _utc(value).isoformat() if value is not None else None


def _age(now, value):
    return round((now - _utc(value)).total_seconds() / 60, 2) if value is not None else None


def _thresholds(settings):
    overrides = settings.get("health", {})
    if not isinstance(overrides, dict):
        raise ValueError("health must be a settings table")
    values = {}
    for key, default in _DEFAULTS.items():
        value = overrides.get(key, default)
        if (isinstance(value, bool) or not isinstance(value, (int, float))
                or not math.isfinite(value) or value <= 0):
            raise ValueError(f"health.{key} must be a finite positive number")
        if key == "max_rejection_pct" and value > 100:
            raise ValueError("health.max_rejection_pct cannot exceed 100")
        values[key] = value
    return values


def _event_tail(path):
    """Read at most 1 MiB and inspect at most 2,000 complete recent events."""
    if not path.exists():
        return [], {"events_checked": 0, "truncated": False, "invalid_events": 0}
    with path.open("rb") as handle:
        size = handle.seek(0, 2)
        offset = max(0, size - _EVENT_TAIL_BYTES)
        handle.seek(offset)
        content = handle.read(_EVENT_TAIL_BYTES)
    lines = content.splitlines()
    # Starting in the middle of a UTF-8 record is safe: discard that fragment.
    if offset and lines:
        lines = lines[1:]
    truncated = bool(offset) or len(lines) > _EVENT_TAIL_LINES
    events, invalid = [], 0
    for line in lines[-_EVENT_TAIL_LINES:]:
        try:
            event = json.loads(line)
            if not isinstance(event, dict) or not isinstance(event.get("at"), str):
                raise ValueError("Invalid event")
            parsed = datetime.fromisoformat(event["at"].replace("Z", "+00:00"))
            if parsed.tzinfo is None:
                raise ValueError("Event timestamp has no timezone")
            event["_time"] = _utc(parsed)
            events.append(event)
        except (ValueError, TypeError, UnicodeDecodeError):
            invalid += 1
    return events, {"events_checked": len(events), "truncated": truncated,
                    "invalid_events": invalid, "max_bytes": _EVENT_TAIL_BYTES,
                    "max_events": _EVENT_TAIL_LINES}


def check_health(data_dir, settings, now=None) -> dict:
    """Return a JSON-safe report; never initialize, repair, or modify the warehouse.

    Configuration errors raise ValueError. Operational failures are represented
    by critical/warning checks, including a missing, locked, or corrupt database.
    ``now`` is an optional timezone-aware datetime, primarily for reproducible checks.
    """
    thresholds = _thresholds(settings)
    expected = settings.get("stations")
    if (not isinstance(expected, list) or not expected
            or any(not isinstance(s, str) or not s.strip() for s in expected)
            or len(expected) != len(set(expected))):
        raise ValueError("stations must be a nonempty list of unique station IDs")
    now = now or datetime.now(timezone.utc)
    if not isinstance(now, datetime) or now.tzinfo is None:
        raise ValueError("now must be a timezone-aware datetime")
    now = _utc(now)
    data_dir = Path(data_dir).expanduser().resolve()
    report = {"status": "healthy", "checked_at": _iso(now), "checks": [],
              "stations": [], "counts": {}, "thresholds": thresholds}

    def add(name, status, message, details=None):
        check = {"name": name, "status": status, "message": message}
        if details is not None:
            check["details"] = details
        report["checks"].append(check)
        if _SEVERITY[status] > _SEVERITY[report["status"]]:
            report["status"] = status

    db_path = data_dir / "warehouse.duckdb"
    connection = None
    try:
        if not db_path.is_file():
            add("warehouse", "critical", "Warehouse is missing. Run the pipeline once to bootstrap it.",
                {"path": str(db_path)})
            return report
        connection = duckdb.connect(str(db_path), read_only=True)
        connection.execute("BEGIN TRANSACTION")
        counts = connection.execute("""
            SELECT (SELECT count(*) FROM fact_observation),
                   (SELECT count(*) FROM dim_station), (SELECT count(*) FROM etl_runs)
        """).fetchone()
        observations = dict(connection.execute("""
            SELECT station_id, max(observed_at) FROM fact_observation GROUP BY station_id
        """).fetchall())
        checkpoints = {row[0]: row[1:] for row in connection.execute("""
            SELECT station_id, watermark, updated_at, batch_id FROM source_checkpoints
        """).fetchall()}
        quality = connection.execute("""
            SELECT r.batch_id, r.completed_at, r.input_count, r.accepted_count,
                r.rejected_count, r.duplicate_count, r.missing_measurement_count
            FROM etl_runs r
            WHERE r.batch_id IN (
                SELECT batch_id FROM source_checkpoints WHERE station_id IN (SELECT unnest(?))
            )
              AND r.accepted_count + r.rejected_count > 0
            ORDER BY r.completed_at DESC, r.batch_id DESC LIMIT 1
        """, [expected]).fetchone()
        connection.execute("COMMIT")
    except (duckdb.Error, OSError) as exc:
        add("warehouse", "critical",
            "Warehouse is unavailable. If a load is running, retry after it finishes; otherwise "
            "check the database path and restore or rebuild a damaged warehouse from archived batches.",
            {"path": str(db_path), "error": str(exc)[:1000]})
        return report
    finally:
        if connection is not None:
            connection.close()

    report["counts"] = dict(zip(
        ("observation_count", "station_count", "completed_batch_count"), counts, strict=True
    ))
    report["counts"]["expected_station_count"] = len(expected)
    add("warehouse", "healthy", "Warehouse is readable.", report["counts"])
    if counts[0] == 0:
        add("warehouse_records", "critical", "Warehouse contains no observations. Inspect ingestion and rejected records.")
    future_limit = now + timedelta(minutes=_CLOCK_SKEW_MINUTES)
    for station in expected:
        observed = observations.get(station)
        watermark, updated, batch_id = checkpoints.get(station, (None, None, None))
        station_report = {
            "station_id": station, "latest_observed_at": _iso(observed),
            "observation_age_minutes": _age(now, observed),
            "checkpoint_watermark": _iso(watermark), "checkpoint_updated_at": _iso(updated),
            "checkpoint_age_minutes": _age(now, watermark),
            "live_run_age_minutes": _age(now, updated), "live_batch_id": batch_id,
        }
        report["stations"].append(station_report)
        observation_status = "healthy"
        message = f"{station} observations are current."
        if observed is None:
            observation_status, message = "critical", f"{station} has no observations. Inspect its API response and rejected records."
        elif _utc(observed) > future_limit:
            observation_status, message = "critical", f"{station} has future observations. Check source timestamps and system clock."
        elif _age(now, observed) > thresholds["max_observation_age_minutes"]:
            observation_status, message = "critical", f"{station} observations are stale. Inspect the source and run a live refresh."
        add(f"observations:{station}", observation_status, message, station_report)

        checkpoint_status = "healthy"
        message = f"{station} has a recent live run and checkpoint."
        if watermark is None:
            checkpoint_status, message = "critical", f"{station} has no live checkpoint. Run a refresh without explicit historical dates."
        elif _utc(watermark) > future_limit or _utc(updated) > future_limit:
            checkpoint_status, message = "critical", f"{station} has a future checkpoint. Check the system clock and stored checkpoint before continuing."
        elif (_age(now, updated) > thresholds["max_run_age_minutes"]
              or _age(now, watermark) > thresholds["max_run_age_minutes"]):
            checkpoint_status, message = "critical", f"{station} live processing is behind. Inspect the scheduler and run a live refresh."
        add(f"checkpoint:{station}", checkpoint_status, message, station_report)

    if quality is None:
        add("rejection_rate", "warning",
            "No nonempty batch is referenced by the live checkpoints. Inspect ingestion; a rejection rate is unavailable.")
    else:
        batch_id, completed, total, accepted, rejected, duplicates, missing = quality
        rejection_pct = 100.0 * rejected / total if total else None
        missing_pct = 100.0 * missing / (3 * accepted) if accepted else None
        details = {"batch_id": batch_id, "completed_at": _iso(completed),
                   "input_count": total, "accepted_count": accepted, "rejected_count": rejected,
                   "duplicate_count": duplicates, "rejection_pct": rejection_pct,
                   "missing_measurement_count": missing, "missing_measurement_pct": missing_pct}
        bad = rejection_pct is not None and rejection_pct > thresholds["max_rejection_pct"]
        add("rejection_rate", "warning" if bad else "healthy",
            "Recent live batch rejection rate exceeds the limit. Inspect its rejected-record report."
            if bad else "Recent live batch rejection rate is within the limit.", details)
        add("missing_measurements", "healthy",
            "Missing optional measurements are reported separately; they are not rejected or replaced with zero.",
            {"batch_id": batch_id, "missing_measurement_count": missing,
             "missing_measurement_pct": missing_pct, "measurement_slots": 3 * accepted})

    try:
        events, event_details = _event_tail(data_dir / "events.jsonl")
    except OSError as exc:
        add("recent_failures", "warning", "Cannot read recent run events. Check access to events.jsonl.",
            {"error": str(exc)[:1000]})
        return report
    valid_events = [event for event in events if event["_time"] <= future_limit]
    # Only checkpoint advancement proves successful live work. A recent replay
    # or backfill success must never clear an ingestion failure or stale live run.
    live_success = max((_utc(row[1]) for station, row in checkpoints.items()
                        if station in expected and _utc(row[1]) <= future_limit), default=None)
    successes = {}
    for event in valid_events:
        if event.get("status") == "succeeded" and isinstance(event.get("batch_id"), str):
            batch = event["batch_id"]
            successes[batch] = max(event["_time"], successes.get(batch, event["_time"]))
    unresolved = []
    for event in valid_events:
        if event.get("status") not in {"failed", "ingestion_failed"}:
            continue
        if event["_time"] < now - timedelta(hours=24):
            continue
        resolved = live_success is not None and live_success > event["_time"]
        if event.get("status") == "failed" and isinstance(event.get("batch_id"), str):
            resolved = resolved or successes.get(event["batch_id"], event["_time"]) > event["_time"]
        if not resolved:
            unresolved.append(event)
    event_details["unresolved_failure_count"] = len(unresolved)
    if unresolved:
        latest = max(unresolved, key=lambda event: event["_time"])
        event_details["latest_failure"] = {
            "at": latest["at"], "status": latest["status"], "batch_id": latest.get("batch_id"),
            "error": str(latest.get("error", "Unknown failure"))[:1000],
        }
    add("recent_failures", "critical" if unresolved else "healthy",
        "A recent failure has not been followed by recovery. Inspect the latest error and retry the affected operation."
        if unresolved else "No unresolved failures found in the bounded recent event history.", event_details)
    future_count = len(events) - len(valid_events)
    if event_details["invalid_events"] or future_count:
        add("event_log", "warning", "Recent events contain invalid or future timestamps. Inspect events.jsonl and the system clock.",
            {"invalid_events": event_details["invalid_events"], "future_events": future_count})
    return report
