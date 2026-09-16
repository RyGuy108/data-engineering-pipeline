import csv
import xml.etree.ElementTree as ET
import zipfile
from pathlib import Path

import pytest

from weather_pipeline.tableau import COMPLETENESS_FORMULA, SCHEMAS, SHEETS, build_workbook


@pytest.fixture
def exports(tmp_path):
    result = {}
    for name, schema in SCHEMAS.items():
        path = tmp_path / f"{name}.csv"
        with path.open("w", newline="", encoding="utf-8") as handle:
            writer = csv.DictWriter(handle, fieldnames=list(schema))
            writer.writeheader()
            if name == "dashboard_observations":
                for station, observed in (("KAUS", "2026-09-13 10:00:00"), ("KORD", "2026-09-13 11:00:00")):
                    writer.writerow({"station_id": station, "observed_at": observed, "loaded_at": "2026-09-13 12:00:00", "station_name": "Station & weather <test>", "missing_measurement_count": 1})
        result[name] = str(path)
    return result


def test_portable_native_package_contains_exact_export_bytes(exports, tmp_path):
    output = tmp_path / "dashboard"
    result = build_workbook(exports, output)
    with zipfile.ZipFile(result["twbx_path"]) as archive:
        assert archive.testzip() is None
        assert set(archive.namelist()) == {"weather_observatory.twb", *(f"Data/{key}.csv" for key in SCHEMAS)}
        assert archive.read("weather_observatory.twb") == Path(result["twb_path"]).read_bytes()
        for key, path in exports.items():
            assert archive.read(f"Data/{key}.csv") == Path(path).read_bytes()
    root = ET.parse(result["twb_path"]).getroot()
    for connection in root.findall("./datasources/datasource/connection"):
        assert connection.attrib["class"] == "textscan"
        assert not Path(connection.attrib["directory"]).is_absolute()
        assert (output / connection.attrib["directory"] / connection.attrib["filename"]).is_file()
    # Desktop 2026.2.2 rejects application-style 26.1 + ManifestByVersion.
    assert root.attrib["version"] == root.attrib["original-version"] == "18.1"
    assert {source.get("version") for source in root.findall("./datasources/datasource")} == {"18.1"}
    assert {node.tag for node in root.find("document-format-change-manifest")} == {"SheetIdentifierTracking", "SortTagCleanup"}
    assert result["workbook_format"] == "18.1"
    assert result["native_verified_with"] == "Tableau Desktop 2026.2.2"
    assert "each new data snapshot" in result["validation_note"]


def test_all_sheet_and_field_references_resolve(exports, tmp_path):
    result = build_workbook(exports, tmp_path / "dashboard")
    root = ET.parse(result["twb_path"]).getroot()
    source_columns = {source.attrib["name"]: {column.attrib["name"] for column in source.findall("column")} for source in root.findall("./datasources/datasource")}
    assert {sheet.attrib["name"] for sheet in root.findall("./worksheets/worksheet")} == set(SHEETS)
    for sheet in root.findall("./worksheets/worksheet"):
        dependencies = sheet.find("./table/view/datasource-dependencies")
        source = dependencies.attrib["datasource"]
        for instance in dependencies.findall("column-instance"):
            assert instance.attrib["column"] in source_columns[source]
        instance_names = {instance.attrib["name"] for instance in dependencies.findall("column-instance")}
        for shelf in ("rows", "cols"):
            reference = sheet.find(f"./table/{shelf}").text
            assert reference.startswith(f"[{source}].")
            assert reference[len(source) + 3:] in instance_names
        for encoding in sheet.findall("./table/panes/pane/encodings/*"):
            reference = encoding.attrib["column"]
            assert reference[len(source) + 3:] in instance_names
    for zone in root.findall("./dashboards/dashboard/zones//zone"):
        if "name" in zone.attrib:
            assert zone.attrib["name"] in SHEETS
    identifiers = [node.attrib["uuid"] for node in root.findall(".//simple-id")]
    assert len(identifiers) == len(set(identifiers)) == 4


def test_completeness_uses_observation_weighted_fraction(exports, tmp_path):
    result = build_workbook(exports, tmp_path / "dashboard")
    root = ET.parse(result["twb_path"]).getroot()
    calc = root.find("./datasources/datasource/column[@name='[MeasurementCompleteness]']")
    assert calc.find("calculation").attrib["formula"] == COMPLETENESS_FORMULA
    assert "SUM([missing_measurement_count])" in COMPLETENESS_FORMULA
    assert "3.0 * COUNT([station_id])" in COMPLETENESS_FORMULA
    assert calc.attrib["default-format"] == "p0.0%"
    assert "AVG([measurement_completeness_pct])" not in Path(result["twb_path"]).read_text()
    assert result["summary"] == {"observation_count": 2, "station_count": 2, "latest_observed_at": "2026-09-13 11:00:00", "latest_loaded_at": "2026-09-13 12:00:00"}


def test_missing_source_or_required_columns_fail_before_output(exports, tmp_path):
    output = tmp_path / "dashboard"
    with pytest.raises(ValueError, match="Missing dashboard export"):
        build_workbook({}, output)
    Path(exports["dashboard_daily"]).write_text("station_id\nKAUS\n")
    with pytest.raises(ValueError, match="missing required columns"):
        build_workbook(exports, output)
    assert not output.exists()


def test_empty_snapshot_and_regeneration(exports, tmp_path):
    with Path(exports["dashboard_observations"]).open("w", newline="") as handle:
        csv.writer(handle).writerow(list(SCHEMAS["dashboard_observations"]))
    output = tmp_path / "dashboard"
    first = build_workbook(exports, output)
    before = Path(first["twb_path"]).read_bytes()
    second = build_workbook(exports, output)
    assert Path(second["twb_path"]).read_bytes() == before
    assert second["summary"]["observation_count"] == 0
    assert "no observations" in before.decode()
    assert not list(output.glob(".tableau-*"))
