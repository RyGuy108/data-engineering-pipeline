"""Reconcile a generated Tableau snapshot against a read-only warehouse snapshot.

Passing these checks proves artifact structure and data agreement, not native
Tableau rendering. No warehouse schema initialization or writes are performed.
"""

from __future__ import annotations

import csv
import hashlib
import math
import tempfile
import xml.etree.ElementTree as ET
import zipfile
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any

import duckdb

from .tableau import COMPLETENESS_FORMULA, SCHEMAS, SHEETS, SOURCE_ID, WORKBOOK_NAME

_ORDERS = {
    "dashboard_observations": "observed_at, station_id",
    "dashboard_daily": "observation_date, station_id",
    "dashboard_quality": "completed_at, batch_id",
}
_KEYS = {
    "dashboard_observations": ("station_id", "observed_at"),
    "dashboard_daily": ("observation_date", "station_id"),
    "dashboard_quality": ("batch_id",),
}
_TYPES = {"string": "VARCHAR", "date": "DATE", "datetime": "TIMESTAMP", "real": "DOUBLE", "integer": "BIGINT"}
_NOTE = "Automated data and structure checks only. Opening and visual verification in Tableau Desktop remain required."


def _json_safe(value: Any) -> Any:
    if isinstance(value, (date, datetime)):
        return value.isoformat()
    if isinstance(value, dict):
        return {key: _json_safe(item) for key, item in value.items()}
    if isinstance(value, (tuple, list)):
        return [_json_safe(item) for item in value]
    if isinstance(value, float) and not math.isfinite(value):
        return str(value)
    return value


def _equivalent(left: Any, right: Any) -> bool:
    if isinstance(left, dict) and isinstance(right, dict):
        return left.keys() == right.keys() and all(_equivalent(left[k], right[k]) for k in left)
    if isinstance(left, (tuple, list)) and isinstance(right, (tuple, list)):
        return len(left) == len(right) and all(_equivalent(a, b) for a, b in zip(left, right))
    if isinstance(left, float) and isinstance(right, (int, float)):
        return math.isfinite(left) and math.isfinite(right) and math.isclose(left, right, rel_tol=1e-10, abs_tol=1e-10)
    return left == right


def _check(report: dict, name: str, passed: bool, **details: Any) -> None:
    report["checks"].append({"name": name, "status": "passed" if passed else "failed", **_json_safe(details)})
    if not passed:
        report["status"] = "failed"


def _profile(connection, key: str, relation: str, parameters: list | None = None) -> dict:
    keys = _KEYS[key]
    expressions = ["count(*)", f"count(DISTINCT ({', '.join(keys)}))",
                   f"count(*) FILTER (WHERE {' OR '.join(field + ' IS NULL' for field in keys)})"]
    fields = list(SCHEMAS[key])
    expressions.extend(f'count(*) FILTER (WHERE "{field}" IS NULL)' for field in fields)
    numeric = [field for field, datatype in SCHEMAS[key].items() if datatype in ("real", "integer")]
    for field in numeric:
        expressions.extend(f'{aggregate}("{field}")' for aggregate in ("sum", "avg", "min", "max"))
    values = iter(connection.execute(f"SELECT {', '.join(expressions)} FROM {relation}", parameters or []).fetchone())
    result = {"row_count": next(values), "distinct_business_keys": next(values), "null_business_keys": next(values)}
    result["null_counts"] = {field: next(values) for field in fields}
    result["numeric_metrics"] = {field: {aggregate: next(values) for aggregate in ("sum", "avg", "min", "max")} for field in numeric}
    return result


def _validate_structure(report: dict, root: ET.Element) -> None:
    _check(report, "workbook_root", root.tag == "workbook")
    sources = root.findall("./datasources/datasource")
    # Tableau's application release is not its persisted workbook version.
    # The earlier 26.1/ManifestByVersion declaration passed the external XSD
    # check but failed to open in Desktop 2026.2.2 with error D2E8DA72.
    _check(report, "supported_workbook_format", root.get("version") == "18.1"
           and root.get("original-version") == "18.1"
           and all(source.get("version") == "18.1" for source in sources))
    manifests = root.findall("document-format-change-manifest")
    features = [] if len(manifests) != 1 else [node.tag for node in manifests[0]]
    _check(report, "explicit_format_features", len(features) == 2
           and set(features) == {"SheetIdentifierTracking", "SortTagCleanup"}
           and not root.findall(".//ManifestByVersion")
           and not root.findall("./explain-data"))
    expected_ids = {f"textscan.{key}" for key in SCHEMAS}
    _check(report, "expected_data_sources", len(sources) == len(expected_ids) and {node.get("name") for node in sources} == expected_ids)
    all_connections = root.findall(".//connection")
    portable = len(all_connections) == len(SCHEMAS)
    for source in sources:
        key = (source.get("name") or "").removeprefix("textscan.")
        connections = source.findall("connection")
        if key not in SCHEMAS or len(connections) != 1:
            portable = False
            continue
        connection = connections[0]
        portable &= connection.attrib == {
            "class": "textscan", "directory": "Data", "filename": f"{key}.csv", "server": "",
        }
        headers = connection.findall("./relation/columns/column")
        declared = [(column.get("name"), column.get("datatype")) for column in headers]
        source_fields = {column.get("name"): column.get("datatype") for column in source.findall("column")}
        declared_fields = all(source_fields.get(f"[{field}]") == datatype for field, datatype in SCHEMAS[key].items())
        relations = connection.findall("relation")
        declared_relation = len(relations) == 1 and relations[0].attrib == {
            "name": f"{key}.csv", "table": f"[{key}#csv]", "type": "table",
        }
        _check(report, f"{key}.declared_schema", declared == list(SCHEMAS[key].items()) and declared_fields and declared_relation)
    forbidden = {"password", "username", "user-name", "access-token", "token", "secret", "connection-string"}
    secret_free = not any(attribute.lower().split("}")[-1] in forbidden for node in root.iter() for attribute in node.attrib)
    _check(report, "portable_connections_without_credentials", bool(portable and secret_free))

    sheets = root.findall("./worksheets/worksheet")
    _check(report, "expected_worksheets", len(sheets) == len(SHEETS) and {node.get("name") for node in sheets} == set(SHEETS))
    expected_instances = {
        SHEETS[0]: ("[avg:temperature_c:qk]", "[temperature_c]", "Avg", "[none:ObservationHour:qk]"),
        SHEETS[1]: ("[cnt:station_id:qk]", "[station_id]", "Count", "[none:station_id:nk]"),
        SHEETS[2]: ("[usr:MeasurementCompleteness:qk]", "[MeasurementCompleteness]", "User", "[none:station_id:nk]"),
    }
    for sheet in sheets:
        name = sheet.get("name")
        if name not in expected_instances:
            continue
        instance, column, derivation, dimension = expected_instances[name]
        dependencies = sheet.find("./table/view/datasource-dependencies")
        valid = dependencies is not None and dependencies.get("datasource") == SOURCE_ID
        instances = [] if dependencies is None else dependencies.findall("column-instance")
        valid &= any(node.get("name") == instance and node.get("column") == column and node.get("derivation") == derivation for node in instances)
        rows = sheet.findtext("./table/rows")
        cols = sheet.findtext("./table/cols")
        expected_rows, expected_cols = (instance, dimension) if name == SHEETS[0] else (dimension, instance)
        valid &= rows == f"[{SOURCE_ID}].{expected_rows}" and cols == f"[{SOURCE_ID}].{expected_cols}"
        _check(report, f"worksheet.{name}.aggregation", bool(valid))
        if name != SHEETS[0]:
            panes = sheet.findall("./table/panes/pane")
            visible = bool(panes)
            for pane in panes:
                formats = {node.get("attr"): node.get("value") for node in pane.findall("./style/style-rule[@element='mark']/format")}
                visible &= formats.get("mark-labels-show") == "true" and formats.get("mark-labels-cull") == "false"
                visible &= formats.get("mark-labels-mode") == "all"
                text = pane.find("./encodings/text")
                visible &= text is not None and text.get("column") == f"[{SOURCE_ID}].{instance}"
            _check(report, f"worksheet.{name}.visible_labels", bool(visible))

    formulas = {"[ObservationHour]": "DATETRUNC('hour', [observed_at])", "[MeasurementCompleteness]": COMPLETENESS_FORMULA}
    for field, formula in formulas.items():
        columns = root.findall(f".//column[@name='{field}']")
        valid = bool(columns) and all(node.find("calculation") is not None and node.find("calculation").get("formula") == formula for node in columns)
        if field == "[MeasurementCompleteness]":
            valid &= all(node.get("default-format") == "p0.0%" for node in columns)
        _check(report, f"formula.{field}", bool(valid))
    allowed_formulas = set(formulas.values())
    _check(report, "only_expected_calculations", all(node.get("formula") in allowed_formulas for node in root.findall(".//calculation")))
    dashboards = root.findall("./dashboards/dashboard")
    valid_dashboard = len(dashboards) == 1 and dashboards[0].get("name") == "Weather Observatory"
    if valid_dashboard:
        names = {node.get("name") for node in dashboards[0].findall("./zones//zone") if node.get("name")}
        valid_dashboard &= names == set(SHEETS)
    _check(report, "expected_dashboard_and_sheet_zones", bool(valid_dashboard))


def _read_artifact(path: Path, destination: Path, report: dict) -> tuple[ET.Element, dict[str, Path]]:
    expected = {WORKBOOK_NAME, *(f"Data/{key}.csv" for key in SCHEMAS)}
    data = {}
    if path.suffix.lower() == ".twbx":
        with zipfile.ZipFile(path) as archive:
            names = archive.namelist()
            valid_names = len(names) == len(expected) and set(names) == expected
            _check(report, "package_members", valid_names, expected=sorted(expected), actual=sorted(names))
            if not valid_names:
                raise ValueError("Package must contain exactly the generated workbook and three expected CSVs")
            if archive.testzip() is not None:
                raise ValueError("Package failed its ZIP integrity check")
            xml = archive.read(WORKBOOK_NAME)
            for key in SCHEMAS:
                target = destination / f"{key}.csv"
                target.write_bytes(archive.read(f"Data/{key}.csv"))
                data[key] = target
    elif path.suffix.lower() == ".twb":
        xml = path.read_bytes()
        for key in SCHEMAS:
            source = path.parent / "Data" / f"{key}.csv"
            if not source.resolve().is_relative_to(path.parent.resolve()):
                raise ValueError("CSV reference escapes the workbook directory")
            target = destination / f"{key}.csv"
            target.write_bytes(source.read_bytes())
            data[key] = target
        _check(report, "local_data_files", True)
    else:
        raise ValueError("Workbook must be a .twb or .twbx file")
    if b"<!DOCTYPE" in xml.upper() or b"<!ENTITY" in xml.upper():
        raise ValueError("Workbook must not include DTD or entity declarations")
    root = ET.fromstring(xml)
    _validate_structure(report, root)
    return root, data


def audit_dashboard(db_path: str | Path, workbook_path: str | Path) -> dict:
    """Return JSON-safe evidence for a .twb/.twbx against the current warehouse.

    Exact CSV SHA-256 agreement detects any changed export bytes, including a
    stale snapshot. Typed profiles independently reconcile counts, business-key
    uniqueness, null counts, and every numeric measure. All warehouse reads use
    a single read-only transaction, and CSV parsing uses explicit warehouse types.
    Failure is returned as status='failed', including missing or damaged inputs.
    """
    db_path = Path(db_path).expanduser().resolve()
    workbook_path = Path(workbook_path).expanduser().resolve()
    report = {
        "status": "passed", "audited_at": datetime.now(timezone.utc).isoformat(),
        "warehouse_path": str(db_path), "workbook_path": str(workbook_path),
        "native_visual_validation": "not_performed", "validation_note": _NOTE,
        "checks": [], "sources": {},
    }
    try:
        if not db_path.is_file():
            raise ValueError("Warehouse does not exist; the audit never initializes a warehouse")
        with tempfile.TemporaryDirectory(prefix="weather-dashboard-audit-") as temporary:
            directory = Path(temporary)
            root, data = _read_artifact(workbook_path, directory, report)
            with duckdb.connect(str(db_path), read_only=True) as connection:
                connection.execute("SET TimeZone = 'UTC'")
                connection.execute("BEGIN TRANSACTION")
                for key, path in data.items():
                    with path.open(newline="", encoding="utf-8") as handle:
                        headers = next(csv.reader(handle), [])
                    valid_headers = headers == list(SCHEMAS[key])
                    _check(report, f"{key}.csv_headers", valid_headers)
                    if not valid_headers:
                        continue
                    expected_path = directory / f"expected_{key}.csv"
                    connection.execute(f"COPY (SELECT * FROM {key} ORDER BY {_ORDERS[key]}) TO ? (FORMAT CSV, HEADER TRUE)", [str(expected_path)])
                    expected_hash = hashlib.sha256(expected_path.read_bytes()).hexdigest()
                    actual_hash = hashlib.sha256(path.read_bytes()).hexdigest()
                    _check(report, f"{key}.current_snapshot_bytes", expected_hash == actual_hash, warehouse_sha256=expected_hash, workbook_sha256=actual_hash)
                    expected_profile = _profile(connection, key, key)
                    csv_types = {field: _TYPES[datatype] for field, datatype in SCHEMAS[key].items()}
                    actual_profile = _profile(connection, key, "read_csv(?, columns = ?, header = true, auto_detect = false, strict_mode = true, allow_quoted_nulls = false)", [str(path), csv_types])
                    report["sources"][key] = _json_safe({"warehouse": expected_profile, "workbook": actual_profile})
                    for measure in ("row_count", "distinct_business_keys", "null_counts", "numeric_metrics"):
                        _check(report, f"{key}.{measure}", _equivalent(expected_profile[measure], actual_profile[measure]))
                    valid_keys = all(profile["null_business_keys"] == 0 and profile["distinct_business_keys"] == profile["row_count"] for profile in (expected_profile, actual_profile))
                    _check(report, f"{key}.unique_nonnull_business_keys", valid_keys)
                count, stations, latest, loaded = connection.execute("SELECT count(*), count(DISTINCT station_id), max(observed_at), max(loaded_at) FROM dashboard_observations").fetchone()
                report["warehouse_summary"] = _json_safe({"observation_count": count, "station_count": stations, "latest_observed_at": latest, "latest_loaded_at": loaded})
                # Match the exported timestamp spelling, which omits trailing fractional
                # zeroes; datetime.__str__ always renders six fractional digits.
                latest_text = loaded_text = ""
                with (directory / "expected_dashboard_observations.csv").open(newline="", encoding="utf-8") as handle:
                    for row in csv.DictReader(handle):
                        latest_text = max(latest_text, row["observed_at"])
                        loaded_text = max(loaded_text, row["loaded_at"])
                latest_text = latest_text or "no observations"
                loaded_text = loaded_text or "no completed loads"
                description = f"National Weather Service · {count:,} observations · {stations} stations\nLatest observation: {latest_text} UTC · Warehouse refreshed: {loaded_text} UTC"
                dashboard_texts = [node.text for node in root.findall("./dashboards/dashboard/zones//formatted-text/run")]
                _check(report, "dashboard_snapshot_header", description in dashboard_texts)
                connection.execute("COMMIT")
    except (OSError, ValueError, UnicodeError, csv.Error, zipfile.BadZipFile, ET.ParseError, duckdb.Error, RuntimeError) as error:
        _check(report, "audit_completed", False, error_type=type(error).__name__, error=str(error))
    return report


def render_audit_markdown(report: dict) -> str:
    """Render concise release evidence while keeping native validation explicit."""
    passed = sum(check["status"] == "passed" for check in report["checks"])
    lines = ["# Dashboard validation report", "", f"**Automated audit: {report['status'].upper()}**",
             "", f"Audited at: {report['audited_at']}", "",
             f"Workbook: `{Path(report['workbook_path']).name}`", "",
             f"Checks passed: {passed}/{len(report['checks'])}", "", report["validation_note"], "",
             "| Data source | Warehouse rows | Packaged rows | Distinct packaged keys |", "| --- | ---: | ---: | ---: |"]
    for key, profiles in report["sources"].items():
        expected, actual = profiles["warehouse"], profiles["workbook"]
        lines.append(f"| {key} | {expected['row_count']} | {actual['row_count']} | {actual['distinct_business_keys']} |")
    lines.extend(["", "## Checks", "", "| Check | Result |", "| --- | --- |"])
    for check in report["checks"]:
        lines.append(f"| {check['name']} | {check['status']} |")
    errors = [check.get("error") for check in report["checks"] if check.get("error")]
    if errors:
        lines.extend(["", "Errors:", "", *(f"- {error}" for error in errors)])
    return "\n".join(lines) + "\n"
