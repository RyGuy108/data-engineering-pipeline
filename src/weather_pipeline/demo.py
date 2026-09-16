"""An isolated, offline demonstration using archived public NWS observations."""

from __future__ import annotations

import copy
import hashlib
import json
from datetime import datetime, timezone
from importlib.resources import files
from pathlib import Path

import duckdb

from weather_pipeline.pipeline import (
    build_windows,
    configure_runtime,
    pipeline_lock,
    process_manifest,
    refresh_dashboard,
    write_json,
)
from weather_pipeline.transform import create_spark
from weather_pipeline.warehouse import get_checkpoints, warehouse_summary


_TABLES = (
    "fact_observation", "dim_station", "dim_date", "observation_versions",
    "etl_runs", "source_checkpoints",
)
_PROBES = {
    "missing_station": "missing_station_id",
    "invalid_humidity": "humidity_out_of_range",
    "invalid_timestamp": "invalid_observed_at",
}


def _sample() -> dict:
    resource = files("weather_pipeline").joinpath("examples/recorded-nws-sample.json")
    sample = json.loads(resource.read_text(encoding="utf-8"))
    if sample["schema_version"] != 1 or sample["source"] != "recorded_nws_demo":
        raise ValueError("Unsupported recorded demonstration sample")
    for batch in sample["batches"].values():
        for record in batch["records"]:
            if hashlib.sha256(record["raw_json"].encode()).hexdigest() != record["payload_sha256"]:
                raise ValueError("Recorded observation payload checksum mismatch")
    return sample


def _write_batch(destination: Path, sample: dict, name: str, windows=None) -> dict:
    batch = sample["batches"][name]
    source = next(item for item in sample["source_batches"]
                  if item["batch_id"] == batch["source_batch_id"])
    batch_id = f"recorded-demo-{name}"
    envelopes = [{
        "batch_id": batch_id,
        "station_id": row["station_id"],
        "ingested_at": row["ingested_at"],
        "raw_json": row["raw_json"],
        "original_batch_id": row["batch_id"],
    } for row in batch["records"]]
    if name == "initial":
        # Duplicate delivery leaves the real payload untouched. The three other
        # probes deliberately violate rules and must never reach accepted facts.
        envelopes.append({**envelopes[0], "demo_probe": "duplicate_delivery"})
        for probe in _PROBES:
            envelope = copy.deepcopy(envelopes[0])
            payload = json.loads(envelope["raw_json"])
            payload["demo_synthetic_probe"] = probe
            if probe == "missing_station":
                envelope["station_id"] = None
            elif probe == "invalid_humidity":
                payload["properties"]["relativeHumidity"] = {
                    "value": 101, "unitCode": "wmoUnit:percent",
                }
            else:
                payload["properties"]["timestamp"] = "not-a-timestamp"
            envelope["raw_json"] = json.dumps(payload, sort_keys=True, separators=(",", ":"))
            envelope["demo_probe"] = probe
            envelopes.append(envelope)
    raw_path = destination / "raw" / batch_id / "observations.ndjson"
    raw_path.parent.mkdir(parents=True)
    raw_path.write_text("".join(json.dumps(row, sort_keys=True) + "\n" for row in envelopes))
    windows = windows or source["windows"]
    manifest = {
        "batch_id": batch_id,
        "source": "recorded_nws_demo",
        "ingested_at": source["ingested_at"],
        "start": min(window["start"] for window in windows),
        "end": source["end"],
        "advance_checkpoint": True,
        "raw_path": str(raw_path),
        "raw_sha256": hashlib.sha256(raw_path.read_bytes()).hexdigest(),
        "record_count": len(envelopes),
        "stations": source["stations"],
        "windows": windows,
        "recording_provenance": {
            "source_batch_id": source["batch_id"],
            "source_raw_sha256": source["raw_sha256"],
            "original_fetched_at": source["ingested_at"],
            "original_window_end": source["end"],
            "subset_of_source": True,
            "synthetic_rejected_probes": list(_PROBES) if name == "initial" else [],
        },
        "manifest_path": str(raw_path.with_name("manifest.json")),
    }
    write_json(manifest["manifest_path"], manifest)
    return manifest


def _snapshot(db_path: Path) -> dict:
    # Compare all persistent table values, including load timestamps and audit
    # records. File bytes can change while a DuckDB database remains unchanged.
    with duckdb.connect(str(db_path), read_only=True) as connection:
        return {table: connection.execute(f"SELECT * FROM {table} ORDER BY ALL").fetchall()
                for table in _TABLES}


def _snapshot_digest(snapshot: dict) -> str:
    return hashlib.sha256(json.dumps(snapshot, default=str, sort_keys=True).encode()).hexdigest()


def _exports_snapshot(exports: dict) -> dict:
    return {name: hashlib.sha256(Path(path).read_bytes()).hexdigest()
            for name, path in exports.items()}


def _check(report: dict, name: str, actual, expected, detail: str) -> None:
    report["checks"].append({
        "name": name, "status": "passed" if actual == expected else "failed",
        "actual": actual, "expected": expected, "detail": detail,
    })


def render_demo_markdown(report: dict) -> str:
    """Render portable evidence, with the historical/synthetic boundary explicit."""
    lines = [
        "# Recorded weather pipeline demonstration", "",
        f"Status: **{report['status'].upper()}**", "",
        "This offline run uses exact public NWS observation payloads recorded on "
        "September 13, 2026. It does not represent current weather or source freshness.", "",
        "Three deliberately invalid synthetic probes are quarantined. A duplicate delivery "
        "reuses a real payload. Every accepted weather measurement comes from the recording.", "",
    ]
    if report.get("summary"):
        summary = report["summary"]
        lines.extend([
            f"Final warehouse: **{summary['observation_count']} observations**, "
            f"**{summary['station_count']} stations**, "
            f"**{summary['completed_batch_count']} committed batches**.", "",
        ])
    lines.extend(["| Check | Result | Evidence |", "|---|---|---|"])
    for check in report["checks"]:
        detail = check["detail"].replace("|", "\\|").replace("\n", " ")
        lines.append(f"| {check['name']} | {check['status']} | {detail} |")
    lines.extend(["", "## Artifacts", "", "- Full evidence: [report.json](report.json)",
                  "- Warehouse: `warehouse.duckdb`", "- Rejected probes: `processed/`",
                  "- CSV extracts: `exports/`"])
    if report.get("dashboard"):
        lines.extend([
            "- Tableau workbook: [weather_observatory.twbx](tableau/weather_observatory.twbx)",
            "- Workbook/data reconciliation: [audit.md](tableau/audit.md)", "",
            "The workbook audit checks data and workbook structure. Rendering in Tableau "
            "still requires opening the workbook in Tableau and inspecting the charts.",
        ])
    if report.get("error"):
        lines.extend(["", f"Run error: {report['error']}"])
    return "\n".join(lines) + "\n"


def run_demo(destination: str | Path) -> dict:
    """Run real Spark/warehouse replay, rollback and recovery in a fresh directory.

    Existing nonempty directories and destination symlinks are refused before
    loading Spark or writing files. Operational failures save a failed report
    and propagate; evidence mismatches return a report with ``status=failed``.
    """
    requested = Path(destination).expanduser()
    if requested.is_symlink():
        raise ValueError("Demo destination must not be a symlink")
    destination = requested.resolve()
    if destination.exists() and (not destination.is_dir() or any(destination.iterdir())):
        raise ValueError("Demo destination must be absent or an empty directory")
    sample = _sample()
    destination.mkdir(parents=True, exist_ok=True)
    report = {
        "schema_version": 1, "status": "running", "source": sample["source"],
        "started_at": datetime.now(timezone.utc).isoformat(),
        "output_dir": str(destination), "network_required": False,
        "recording": {
            "description": sample["description"],
            "source_batches": sample["source_batches"],
            "accepted_weather_is_synthetic": False,
            "synthetic_rejected_probes": _PROBES,
        },
        "checks": [], "phases": {},
    }
    spark = None
    try:
        with pipeline_lock(destination):
            configure_runtime()
            spark = create_spark()
            db_path = destination / "warehouse.duckdb"
            initial_manifest = _write_batch(destination, sample, "initial")
            initial = process_manifest(initial_manifest, destination, spark=spark)
            report["phases"]["initial"] = initial
            expected_initial = len(sample["batches"]["initial"]["records"])
            _check(report, "Initial real observations loaded", initial["load"]["inserted_count"],
                   expected_initial, f"Inserted {expected_initial} archived NWS observations.")
            for label, count, expected in (
                ("Duplicate delivery detected", "duplicate_count", 1),
                ("Invalid and missing required values rejected", "rejected_count", len(_PROBES)),
            ):
                _check(report, label, initial["quality"][count], expected,
                       f"{count} = {initial['quality'][count]} (expected {expected}).")
            expected_missing = sum(
                json.loads(row["raw_json"])["properties"].get(field, {}).get("value") is None
                for row in sample["batches"]["initial"]["records"]
                for field in ("temperature", "relativeHumidity", "windSpeed")
            )
            _check(report, "Real missing measurements preserved", initial["quality"]["missing_measurement_count"],
                   expected_missing, f"Preserved {expected_missing} unavailable measurement fields as nulls.")
            with duckdb.connect(str(db_path), read_only=True) as connection:
                rejected_path = connection.execute(
                    "SELECT rejected_path FROM etl_runs WHERE batch_id = ?",
                    [initial_manifest["batch_id"]],
                ).fetchone()[0]
                rejected_rows = connection.execute(
                    "SELECT raw_json, reasons FROM read_parquet(?)", [f"{rejected_path}/*.parquet"],
                ).fetchall()
            observed_probes = {
                json.loads(payload).get("demo_synthetic_probe"): reasons
                for payload, reasons in rejected_rows
            }
            report["rejected_probe_reasons"] = observed_probes
            _check(report, "Synthetic probes quarantined with correct reasons",
                   len(observed_probes) == len(_PROBES) and all(
                       reason in observed_probes.get(probe, []) for probe, reason in _PROBES.items()
                   ), True, "Rejected required station, humidity 101%, and malformed timestamp probes.")

            initial_snapshot = _snapshot(db_path)
            initial_exports = _exports_snapshot(initial["exports"])
            initial_checkpoints = get_checkpoints(db_path)
            replay = process_manifest(initial_manifest, destination, spark=spark)
            report["phases"]["replay"] = replay
            _check(report, "Exact replay is a no-op", replay["load"]["status"], "already_loaded",
                   "The committed batch ID is recognized with zero inserts or updates.")
            _check(report, "Replay preserves all warehouse state", _snapshot(db_path),
                   initial_snapshot, "All six tables, facts, load timestamps and checkpoints match exactly.")
            # Keep compact fingerprints in the persisted report rather than copies
            # of all table rows (which include runtime-dependent datetimes/paths).
            for field in ("actual", "expected"):
                report["checks"][-1][field] = _snapshot_digest(report["checks"][-1][field])
            _check(report, "Replay preserves CSV extracts", _exports_snapshot(replay["exports"]),
                   initial_exports, "The three dashboard extracts are byte-for-byte unchanged.")

            followup_source = next(item for item in sample["source_batches"]
                                   if item["batch_id"] == sample["batches"]["followup"]["source_batch_id"])
            windows = build_windows(
                [item["station_id"] for item in sample["station_metadata"]],
                initial_checkpoints, followup_source["end"], overlap_hours=2,
            )
            followup_manifest = _write_batch(destination, sample, "followup", windows)
            report["incremental_windows"] = windows
            injected_error = None
            try:
                process_manifest(followup_manifest, destination, spark=spark, fail_before_commit=True)
            except RuntimeError as exc:
                if str(exc) != "Injected failure before warehouse commit":
                    raise
                injected_error = str(exc)
            _check(report, "Precommit failure exercised", injected_error,
                   "Injected failure before warehouse commit", "The loader failed after its writes and before commit.")
            _check(report, "Failure rolls back every warehouse table", _snapshot(db_path),
                   initial_snapshot, "Facts, dimensions, version ledger, run ledger and checkpoints match the prior commit.")
            for field in ("actual", "expected"):
                report["checks"][-1][field] = _snapshot_digest(report["checks"][-1][field])
            _check(report, "Failure preserves previous dashboard extracts",
                   _exports_snapshot(initial["exports"]), initial_exports,
                   "Failed loading does not replace the last successful CSV snapshot.")

            recovered = process_manifest(followup_manifest, destination, spark=spark)
            report["phases"]["recovered_incremental"] = recovered
            initial_keys = {(row["station_id"], json.loads(row["raw_json"])["properties"]["timestamp"])
                            for row in sample["batches"]["initial"]["records"]}
            followup_keys = {(row["station_id"], json.loads(row["raw_json"])["properties"]["timestamp"])
                             for row in sample["batches"]["followup"]["records"]}
            expected_new = len(followup_keys - initial_keys)
            expected_overlap = len(followup_keys & initial_keys)
            _check(report, "Recovery inserts only newer observations", recovered["load"]["inserted_count"],
                   expected_new, f"Recovery adds {expected_new} actual later NWS observations.")
            _check(report, "Incremental overlap remains unchanged", recovered["load"]["unchanged_count"],
                   expected_overlap, f"The overlap contains {expected_overlap} existing observations.")
            _check(report, "Incremental overlap does not rewrite facts", recovered["load"]["updated_count"],
                   0, "Unchanged overlapping payloads cause zero fact updates.")
            expected_end = datetime.fromisoformat(followup_manifest["end"].replace("Z", "+00:00")).isoformat()
            _check(report, "Checkpoints advance after recovery", get_checkpoints(db_path),
                   {station: expected_end for station in initial_checkpoints},
                   f"All three stations advance to recorded window end {expected_end}.")
            with duckdb.connect(str(db_path), read_only=True) as connection:
                actual_payloads = {row[0] for row in connection.execute(
                    "SELECT payload_hash FROM fact_observation"
                ).fetchall()}
                duplicate_keys = connection.execute("""
                    SELECT count(*) FROM (
                        SELECT station_id, observed_at FROM fact_observation
                        GROUP BY station_id, observed_at HAVING count(*) > 1
                    )
                """).fetchone()[0]
            expected_payloads = {row["payload_sha256"] for batch in sample["batches"].values()
                                 for row in batch["records"]}
            _check(report, "Accepted payloads match the real recording", sorted(actual_payloads),
                   sorted(expected_payloads), "Every accepted payload hash matches an unmodified archived NWS record.")
            _check(report, "Final business keys are unique", duplicate_keys, 0,
                   "No station/timestamp key occurs more than once.")
            report["summary"] = warehouse_summary(db_path)
            _check(report, "Final observation total", report["summary"]["observation_count"],
                   len(initial_keys | followup_keys), "The final count is the union of both recorded subsets.")
            _check(report, "Only successful batches committed", report["summary"]["completed_batch_count"],
                   2, "The replay and failed attempt do not create committed run records.")
            report["dashboard"] = refresh_dashboard(destination, recovered["exports"])
            _check(report, "Tableau workbook reconciles to the warehouse", report["dashboard"]["audit_status"],
                   "passed", "The packaged dashboard passes its automated data and structure audit.")
            report["status"] = "passed" if all(item["status"] == "passed" for item in report["checks"]) else "failed"
    except Exception as exc:
        report["status"] = "failed"
        report["error"] = f"{type(exc).__name__}: {exc}"
        raise
    finally:
        report["completed_at"] = datetime.now(timezone.utc).isoformat()
        write_json(destination / "report.json", report)
        (destination / "report.md").write_text(render_demo_markdown(report))
        if spark is not None:
            spark.stop()
    return report
