"""Release-gate regressions: current warehouse versus exported Tableau package."""

import csv
import hashlib
import io
import json
import xml.etree.ElementTree as ET
import zipfile
from pathlib import Path

import duckdb
import pytest

from test_warehouse import make_batch
from weather_pipeline.dashboard_audit import audit_dashboard, render_audit_markdown
from weather_pipeline.tableau import SCHEMAS, WORKBOOK_NAME, build_workbook
from weather_pipeline.warehouse import export_dashboard, load_batch


@pytest.fixture
def snapshot(tmp_path):
    db = tmp_path / "warehouse.duckdb"
    load_batch(db, *make_batch(tmp_path, rows=[{}, {
        "observed_at": "2026-09-12T01:30:00", "temperature_c": None,
        "humidity_pct": 80.0, "wind_kph": None, "payload_hash": "bbb",
        "text_description": "",
    }]))
    exports = export_dashboard(db, tmp_path / "exports")
    workbook = build_workbook(exports, tmp_path / "tableau")
    return db, workbook


def rewrite_package(path, mutate):
    with zipfile.ZipFile(path) as archive:
        files = {name: archive.read(name) for name in archive.namelist()}
    mutate(files)
    with zipfile.ZipFile(path, "w", zipfile.ZIP_DEFLATED) as archive:
        for name, content in files.items():
            archive.writestr(name, content)


def mutate_csv(files, key, mutate):
    name = f"Data/{key}.csv"
    reader = csv.DictReader(io.StringIO(files[name].decode()))
    rows = list(reader)
    mutate(rows)
    output = io.StringIO(newline="")
    writer = csv.DictWriter(output, fieldnames=reader.fieldnames, lineterminator="\n")
    writer.writeheader()
    writer.writerows(rows)
    files[name] = output.getvalue().encode()


def checks(report):
    return {check["name"]: check for check in report["checks"]}


@pytest.mark.parametrize("extension", ["twb_path", "twbx_path"])
def test_valid_snapshot_reconciles_all_sources_without_changing_warehouse(snapshot, extension):
    db, workbook = snapshot
    before = hashlib.sha256(db.read_bytes()).hexdigest()
    report = audit_dashboard(db, workbook[extension])
    assert report["status"] == "passed", report
    assert report["native_visual_validation"] == "not_performed"
    assert set(report["sources"]) == set(SCHEMAS)
    source = report["sources"]["dashboard_observations"]
    assert source["warehouse"] == source["workbook"]
    assert source["workbook"]["row_count"] == source["workbook"]["distinct_business_keys"] == 2
    assert source["workbook"]["null_counts"]["temperature_c"] == 1
    assert source["workbook"]["null_counts"]["text_description"] == 0
    assert source["workbook"]["numeric_metrics"]["humidity_pct"]["avg"] == 65
    assert source["workbook"]["numeric_metrics"]["missing_measurement_count"]["sum"] == 2
    assert hashlib.sha256(db.read_bytes()).hexdigest() == before
    json.dumps(report, allow_nan=False)
    assert "not" in report["validation_note"] or "remain required" in render_audit_markdown(report)
    assert "Automated audit: PASSED" in render_audit_markdown(report)


def test_later_correction_detects_stale_package_even_without_row_count_change(snapshot, tmp_path):
    db, workbook = snapshot
    load_batch(db, *make_batch(tmp_path, "corrected", rows=[{
        "temperature_c": 30.0, "payload_hash": "ccc",
    }], ingested_at="2026-09-12T04:00:00+00:00"))
    report = audit_dashboard(db, workbook["twbx_path"])
    assert report["status"] == "failed"
    result = checks(report)
    assert result["dashboard_observations.row_count"]["status"] == "passed"
    assert result["dashboard_observations.current_snapshot_bytes"]["status"] == "failed"
    assert result["dashboard_observations.numeric_metrics"]["status"] == "failed"
    assert result["dashboard_quality.row_count"]["status"] == "failed"
    assert result["dashboard_snapshot_header"]["status"] == "failed"


def test_duplicate_observation_fails_unique_business_key_check(snapshot):
    db, workbook = snapshot
    rewrite_package(workbook["twbx_path"], lambda files: mutate_csv(files, "dashboard_observations", lambda rows: rows.append(rows[0])))
    report = audit_dashboard(db, workbook["twbx_path"])
    assert report["status"] == "failed"
    assert checks(report)["dashboard_observations.unique_nonnull_business_keys"]["status"] == "failed"


def test_null_replaced_with_zero_fails_null_counts(snapshot):
    db, workbook = snapshot
    rewrite_package(workbook["twbx_path"], lambda files: mutate_csv(files, "dashboard_observations", lambda rows: rows[1].update(temperature_c="0")))
    report = audit_dashboard(db, workbook["twbx_path"])
    assert report["status"] == "failed"
    assert checks(report)["dashboard_observations.null_counts"]["status"] == "failed"


def test_changed_station_label_detected_when_numeric_aggregates_still_agree(snapshot):
    db, workbook = snapshot
    rewrite_package(workbook["twbx_path"], lambda files: files.__setitem__("Data/dashboard_observations.csv", files["Data/dashboard_observations.csv"].replace(b"Chicago", b"Altered")))
    report = audit_dashboard(db, workbook["twbx_path"])
    assert report["status"] == "failed"
    result = checks(report)
    assert result["dashboard_observations.numeric_metrics"]["status"] == "passed"
    assert result["dashboard_observations.current_snapshot_bytes"]["status"] == "failed"


@pytest.mark.parametrize("member", ["Data/dashboard_observations.csv", "Data/dashboard_daily.csv", "Data/dashboard_quality.csv", WORKBOOK_NAME])
def test_missing_packaged_member_fails(snapshot, member):
    db, workbook = snapshot
    rewrite_package(workbook["twbx_path"], lambda files: files.pop(member))
    report = audit_dashboard(db, workbook["twbx_path"])
    assert report["status"] == "failed"
    assert checks(report)["package_members"]["status"] == "failed"


def test_missing_external_csv_fails(snapshot):
    db, workbook = snapshot
    (Path(workbook["twb_path"]).parent / "Data/dashboard_daily.csv").unlink()
    report = audit_dashboard(db, workbook["twb_path"])
    assert report["status"] == "failed"
    assert checks(report)["audit_completed"]["error_type"] == "FileNotFoundError"


def test_native_rejected_format_fails_even_when_every_csv_matches(snapshot):
    db, workbook = snapshot

    def restore_rejected_format(files):
        root = ET.fromstring(files[WORKBOOK_NAME])
        root.set("version", "26.1")
        root.set("original-version", "26.1")
        for source in root.findall("./datasources/datasource"):
            source.set("version", "26.1")
        manifest = root.find("document-format-change-manifest")
        manifest.clear()
        ET.SubElement(manifest, "ManifestByVersion")
        ET.SubElement(ET.SubElement(root, "explain-data"), "explanation-types")
        files[WORKBOOK_NAME] = ET.tostring(root)

    rewrite_package(workbook["twbx_path"], restore_rejected_format)
    report = audit_dashboard(db, workbook["twbx_path"])
    result = checks(report)
    assert report["status"] == "failed"
    assert result["supported_workbook_format"]["status"] == "failed"
    assert result["explicit_format_features"]["status"] == "failed"
    for source in SCHEMAS:
        assert result[f"{source}.current_snapshot_bytes"]["status"] == "passed"


@pytest.mark.parametrize("mutation, failed_check", [
    (lambda root: root.set("version", "26.1"), "supported_workbook_format"),
    (lambda root: root.set("original-version", "26.1"), "supported_workbook_format"),
    (lambda root: root.find("./datasources/datasource").set("version", "26.1"), "supported_workbook_format"),
    (lambda root: root.find("document-format-change-manifest").clear(), "explicit_format_features"),
    (lambda root: ET.SubElement(root.find("document-format-change-manifest"), "ManifestByVersion"), "explicit_format_features"),
    (lambda root: ET.SubElement(root, "explain-data"), "explicit_format_features"),
    (lambda root: root.find("./datasources/datasource/connection").set("directory", "/tmp/private"), "portable_connections_without_credentials"),
    (lambda root: root.find("./datasources/datasource/connection").set("password", "secret"), "portable_connections_without_credentials"),
    (lambda root: root.find("./worksheets").remove(root.find("./worksheets/worksheet")), "expected_worksheets"),
    (lambda root: root.find("./datasources/datasource/column[@name='[MeasurementCompleteness]']/calculation").set("formula", "0.5"), "formula.[MeasurementCompleteness]"),
    (lambda root: root.find("./datasources/datasource/column[@name='[MeasurementCompleteness]']").set("default-format", "p1%"), "formula.[MeasurementCompleteness]"),
    (lambda root: root.find("./worksheets/worksheet[@name='Observations by station']/table/panes/pane/style/style-rule/format[@attr='mark-labels-show']").set("value", "false"), "worksheet.Observations by station.visible_labels"),
    (lambda root: root.find("./dashboards/dashboard").set("name", "Incorrect"), "expected_dashboard_and_sheet_zones"),
])
def test_damaged_structure_is_reported(snapshot, mutation, failed_check):
    db, workbook = snapshot
    def edit(files):
        root = ET.fromstring(files[WORKBOOK_NAME])
        mutation(root)
        files[WORKBOOK_NAME] = ET.tostring(root)
    rewrite_package(workbook["twbx_path"], edit)
    report = audit_dashboard(db, workbook["twbx_path"])
    assert report["status"] == "failed"
    assert checks(report)[failed_check]["status"] == "failed"
    assert '"secret"' not in json.dumps(report)


def test_missing_warehouse_is_not_created(tmp_path):
    db = tmp_path / "absent.duckdb"
    report = audit_dashboard(db, tmp_path / "absent.twbx")
    assert report["status"] == "failed"
    assert not db.exists()


def test_empty_warehouse_snapshot_is_valid(tmp_path):
    db = tmp_path / "warehouse.duckdb"
    exports = export_dashboard(db, tmp_path / "exports")
    workbook = build_workbook(exports, tmp_path / "tableau")
    report = audit_dashboard(db, workbook["twbx_path"])
    assert report["status"] == "passed", report
    assert report["sources"]["dashboard_observations"]["workbook"]["row_count"] == 0


def test_header_uses_csv_fractional_timestamp_spelling(snapshot, tmp_path):
    db, _ = snapshot
    with duckdb.connect(str(db)) as connection:
        connection.execute("UPDATE fact_observation SET loaded_at = TIMESTAMP '2026-09-12 04:00:00.123450'")
    workbook = build_workbook(export_dashboard(db, tmp_path / "fresh-exports"), tmp_path / "fresh-tableau")
    assert "04:00:00.12345 UTC" in Path(workbook["twb_path"]).read_text()
    report = audit_dashboard(db, workbook["twbx_path"])
    assert report["status"] == "passed", report


def test_invalid_numeric_cell_fails_without_raising(snapshot):
    db, workbook = snapshot
    rewrite_package(workbook["twbx_path"], lambda files: mutate_csv(files, "dashboard_observations", lambda rows: rows[0].update(temperature_c="invalid")))
    report = audit_dashboard(db, workbook["twbx_path"])
    assert report["status"] == "failed"
    assert checks(report)["audit_completed"]["status"] == "failed"


def test_unexpected_zip_member_is_never_extracted(snapshot, tmp_path):
    db, workbook = snapshot
    rewrite_package(workbook["twbx_path"], lambda files: files.update({"../unexpected.txt": b"not extracted"}))
    report = audit_dashboard(db, workbook["twbx_path"])
    assert report["status"] == "failed"
    assert checks(report)["package_members"]["status"] == "failed"
    assert not (tmp_path / "unexpected.txt").exists()
