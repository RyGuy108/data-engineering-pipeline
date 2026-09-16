"""Orchestration, replay, checkpoint windows, and local single-writer locking."""

from __future__ import annotations

import fcntl
import hashlib
import json
import os
import sys
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
from pathlib import Path
from uuid import uuid4


def parse_time(value: str) -> datetime:
    result = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if result.tzinfo is None:
        raise ValueError("Timestamps must include a timezone, for example 2026-09-13T00:00:00Z")
    return result.astimezone(timezone.utc)


def configure_runtime() -> None:
    """Prefer the optional project runtime; never change the system Java installation."""
    project = Path(__file__).resolve().parents[2]
    candidates = [project / ".runtime/java21/Contents/Home", project / ".runtime/java21"]
    if "JAVA_HOME" not in os.environ:
        for candidate in candidates:
            if (candidate / "bin/java").exists():
                os.environ["JAVA_HOME"] = str(candidate)
                break
    os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
    os.environ.setdefault("SPARK_LOCAL_IP", "127.0.0.1")


def build_windows(stations, checkpoints, end, initial_hours=24, overlap_hours=2, start=None):
    if initial_hours <= 0 or overlap_hours < 0:
        raise ValueError("initial_hours must be positive and overlap_hours cannot be negative")
    end = parse_time(end) if isinstance(end, str) else end
    if end.tzinfo is None:
        raise ValueError("End must have a timezone")
    fixed_start = parse_time(start) if isinstance(start, str) else start
    windows = []
    for station in stations:
        checkpoint = checkpoints.get(station)
        beginning = fixed_start or (
            parse_time(checkpoint) - timedelta(hours=overlap_hours)
            if checkpoint else end - timedelta(hours=initial_hours)
        )
        if beginning >= end:
            raise ValueError(f"Start must precede end for {station}; checkpoint may be in the future")
        windows.append({"station_id": station, "start": beginning.isoformat(), "end": end.isoformat()})
    return windows


@contextmanager
def pipeline_lock(data_dir):
    data_dir = Path(data_dir).resolve()
    data_dir.mkdir(parents=True, exist_ok=True)
    with (data_dir / ".pipeline.lock").open("a+") as lock:
        try:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as exc:
            raise RuntimeError("Another pipeline operation is using this data directory") from exc
        try:
            yield
        finally:
            fcntl.flock(lock, fcntl.LOCK_UN)


def write_json(path, value):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    temp = path.with_suffix(path.suffix + ".tmp")
    temp.write_text(json.dumps(value, indent=2, default=str) + "\n")
    temp.replace(path)


def log_event(data_dir, event):
    with (Path(data_dir) / "events.jsonl").open("a") as handle:
        handle.write(json.dumps({"at": datetime.now(timezone.utc).isoformat(), **event}, default=str) + "\n")


def verify_raw_input(manifest):
    """Check the archived input against its ingest-time digest before advancing state."""
    expected = manifest.get("raw_sha256")
    if expected is not None:
        digest = hashlib.sha256()
        with Path(manifest["raw_path"]).open("rb") as handle:
            for chunk in iter(lambda: handle.read(1024 * 1024), b""):
                digest.update(chunk)
        if digest.hexdigest() != expected:
            raise ValueError("Raw input checksum mismatch: restore the original archived batch before replay")


def refresh_dashboard(data_dir, exports=None):
    """Create a current package and reconcile its snapshot before reporting success."""
    from weather_pipeline.dashboard_audit import audit_dashboard, render_audit_markdown
    from weather_pipeline.tableau import build_workbook
    from weather_pipeline.warehouse import export_dashboard

    data_dir = Path(data_dir).resolve()
    db_path = data_dir / "warehouse.duckdb"
    exports = exports or export_dashboard(db_path, data_dir / "exports")
    workbook = build_workbook(exports, data_dir / "tableau")
    audit = audit_dashboard(db_path, workbook["twbx_path"])
    audit_path = data_dir / "tableau" / "audit.json"
    write_json(audit_path, audit)
    (data_dir / "tableau" / "audit.md").write_text(render_audit_markdown(audit))
    if audit["status"] != "passed":
        raise RuntimeError(f"Dashboard audit failed; inspect {audit_path}")
    return {**workbook, "audit_path": str(audit_path), "audit_status": audit["status"]}


def process_manifest(manifest, data_dir, spark=None, fail_before_commit=False, with_dashboard=False):
    from weather_pipeline.transform import create_spark, transform_batch
    from weather_pipeline.warehouse import export_dashboard, load_batch

    data_dir = Path(data_dir).resolve()
    batch_id = manifest["batch_id"]
    # A manifest can be user-supplied on replay; it must not escape the output directory.
    if not batch_id or Path(batch_id).name != batch_id or batch_id in {".", ".."}:
        raise ValueError("Invalid batch_id")
    configure_runtime()
    owns_spark = spark is None
    attempt_id = uuid4().hex
    try:
        verify_raw_input(manifest)
        spark = spark or create_spark()
        result = transform_batch(
            spark, manifest["raw_path"], data_dir / "processed" / batch_id / attempt_id,
            [station["station_id"] for station in manifest["stations"]], manifest["end"],
        )
        if "record_count" in manifest and result["report"]["input_count"] != manifest["record_count"]:
            raise ValueError("Raw input record count differs from the ingestion manifest")
        report_path = data_dir / "reports" / batch_id / f"{attempt_id}.json"
        write_json(report_path, result["report"])
        loaded = load_batch(data_dir / "warehouse.duckdb", manifest, result,
                            fail_before_commit=fail_before_commit)
        canonical_report = data_dir / "reports" / f"{batch_id}.json"
        if loaded["status"] == "loaded" or not canonical_report.exists():
            write_json(canonical_report, result["report"])
        exports = export_dashboard(data_dir / "warehouse.duckdb", data_dir / "exports")
        outcome = {"batch_id": batch_id, "manifest_path": manifest.get("manifest_path"),
                   "report_path": str(report_path),
                   "quality": result["report"], "load": loaded, "exports": exports}
        if with_dashboard:
            outcome["dashboard"] = refresh_dashboard(data_dir, exports)
        log_event(data_dir, {"status": "succeeded", **outcome})
        return outcome
    except Exception as exc:
        log_event(data_dir, {"status": "failed", "batch_id": batch_id,
                             "error": str(exc), "manifest_path": manifest.get("manifest_path")})
        raise
    finally:
        if owns_spark and spark is not None:
            spark.stop()


def run_pipeline(data_dir, settings, start=None, end=None, hours=None, with_dashboard=False):
    from weather_pipeline.ingestion import NWSClient, ingest_batch
    from weather_pipeline.warehouse import get_checkpoints

    data_dir = Path(data_dir).resolve()
    if hours is not None and hours <= 0:
        raise ValueError("hours must be positive")
    with pipeline_lock(data_dir):
        try:
            windows = build_windows(
                settings["stations"], get_checkpoints(data_dir / "warehouse.duckdb"),
                parse_time(end) if end else datetime.now(timezone.utc),
                initial_hours=hours if hours is not None else settings.get("initial_hours", 24),
                overlap_hours=settings.get("overlap_hours", 2), start=start,
            )
            with NWSClient(user_agent=os.environ.get("NWS_USER_AGENT", settings["user_agent"])) as client:
                manifest = ingest_batch(client, data_dir / "raw", windows,
                                        advance_checkpoint=start is None and end is None)
        except Exception as exc:
            log_event(data_dir, {"status": "ingestion_failed", "error": str(exc)})
            raise
        return process_manifest(manifest, data_dir, with_dashboard=with_dashboard)
