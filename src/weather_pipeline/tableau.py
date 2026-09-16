"""Generate a native Tableau workbook and a portable package of warehouse CSVs.

The XML uses Tableau's 18.1 workbook format with explicit feature flags,
verified in Tableau Desktop 2026.2.2. The document format version is separate
from the application release. Structural checks cannot replace native opening.
"""

from __future__ import annotations

import copy
import csv
import os
import shutil
import tempfile
import uuid
import xml.etree.ElementTree as ET
import zipfile
from pathlib import Path
from typing import Any

VERSION = "18.1"
WORKBOOK_NAME = "weather_observatory.twb"
SOURCE_ID = "textscan.dashboard_observations"
SHEETS = ("Hourly temperature (°C)", "Observations by station", "Present measurements (%)")
COMPLETENESS_FORMULA = "IF COUNT([station_id]) = 0 THEN NULL ELSE 1.0 - SUM([missing_measurement_count]) / (3.0 * COUNT([station_id])) END"
SCHEMAS = {
    "dashboard_observations": {
        "station_id": "string", "station_name": "string", "observation_date": "date",
        "observed_at": "datetime", "temperature_c": "real", "humidity_pct": "real",
        "wind_kph": "real", "text_description": "string", "latitude": "real",
        "longitude": "real", "missing_measurement_count": "integer", "batch_id": "string",
        "source_ingested_at": "datetime", "loaded_at": "datetime",
    },
    "dashboard_daily": {
        "observation_date": "date", "station_id": "string", "station_name": "string",
        "observation_count": "integer", "avg_temperature_c": "real", "min_temperature_c": "real",
        "max_temperature_c": "real", "avg_humidity_pct": "real", "avg_wind_kph": "real",
        "missing_measurement_count": "integer", "measurement_completeness_pct": "real",
        "latest_observed_at": "datetime",
    },
    "dashboard_quality": {
        "batch_id": "string", "source": "string", "window_start": "datetime", "window_end": "datetime",
        "source_ingested_at": "datetime", "completed_at": "datetime", "input_count": "integer",
        "accepted_count": "integer", "rejected_count": "integer", "duplicate_count": "integer",
        "missing_measurement_count": "integer", "inserted_count": "integer", "updated_count": "integer",
        "unchanged_count": "integer", "rejection_pct": "real",
    },
}
CAPTIONS = {
    "station_id": "Station", "station_name": "Station name", "temperature_c": "Temperature (°C)",
    "humidity_pct": "Relative humidity (%)", "wind_kph": "Wind speed (km/h)",
    "missing_measurement_count": "Missing measurement fields", "observed_at": "Observation time (UTC)",
    "ObservationHour": "Observation hour (UTC)", "MeasurementCompleteness": "Present measurements (%)",
}


def _sub(parent: ET.Element, tag: str, text: str | None = None, **attrs: str) -> ET.Element:
    node = ET.SubElement(parent, tag, attrs)
    node.text = text
    return node


def _column(name: str, datatype: str, formula: str | None = None) -> ET.Element:
    numeric = datatype in ("real", "integer")
    node = ET.Element("column", {
        "name": f"[{name}]", "datatype": datatype,
        "role": "measure" if numeric else "dimension",
        "type": "quantitative" if numeric else ("ordinal" if datatype in ("date", "datetime") else "nominal"),
        "caption": CAPTIONS.get(name, name.replace("_", " ").capitalize()),
    })
    if name == "MeasurementCompleteness":
        node.set("default-format", "p0.0%")
    if formula:
        _sub(node, "calculation", **{"class": "tableau", "formula": formula})
    return node


def _source(parent: ET.Element, key: str, headers: list[str]) -> dict[str, ET.Element]:
    source = _sub(parent, "datasource", caption=key.replace("dashboard_", "Weather ").replace("_", " "), inline="true", name=f"textscan.{key}", version=VERSION)
    connection = _sub(source, "connection", **{"class": "textscan", "directory": "Data", "filename": f"{key}.csv", "server": ""})
    relation = _sub(connection, "relation", name=f"{key}.csv", table=f"[{key}#csv]", type="table")
    columns = _sub(relation, "columns", **{"character-set": "UTF-8", "header": "yes", "locale": "en_US", "separator": ","})
    for ordinal, name in enumerate(headers):
        _sub(columns, "column", datatype=SCHEMAS[key].get(name, "string"), name=name, ordinal=str(ordinal))
    _sub(source, "aliases", enabled="yes")
    result = {}
    for name in headers:
        result[name] = _column(name, SCHEMAS[key].get(name, "string"))
        source.append(result[name])
    if key == "dashboard_observations":
        for name, datatype, formula in (
            ("ObservationHour", "datetime", "DATETRUNC('hour', [observed_at])"),
            ("MeasurementCompleteness", "real", COMPLETENESS_FORMULA),
        ):
            result[name] = _column(name, datatype, formula)
            source.append(result[name])
    return result


def _ref(instance: str) -> str:
    return f"[{SOURCE_ID}].[{instance}]"


def _identifier(parent: ET.Element, name: str) -> None:
    value = uuid.uuid5(uuid.NAMESPACE_URL, f"weather-observatory/{name}")
    _sub(parent, "simple-id", uuid="{" + str(value).upper() + "}")


def _sheet(parent: ET.Element, name: str, source_columns: dict[str, ET.Element], metric: str, derivation: str, prefix: str, trend: bool = False) -> None:
    sheet = _sub(parent, "worksheet", name=name)
    table = _sub(sheet, "table")
    view = _sub(table, "view")
    sources = _sub(view, "datasources")
    _sub(sources, "datasource", name=SOURCE_ID, caption="Weather observations")
    dependencies = _sub(view, "datasource-dependencies", datasource=SOURCE_ID)
    used = {"station_id", metric}
    if trend:
        used.update({"ObservationHour", "observed_at"})
    if metric == "MeasurementCompleteness":
        used.add("missing_measurement_count")
    for field in sorted(used):
        dependencies.append(copy.deepcopy(source_columns[field]))
    _sub(dependencies, "column-instance", column="[station_id]", derivation="None", name="[none:station_id:nk]", pivot="key", type="nominal")
    metric_instance = f"{prefix}:{metric}:qk"
    _sub(dependencies, "column-instance", column=f"[{metric}]", derivation=derivation, name=f"[{metric_instance}]", pivot="key", type="quantitative")
    if trend:
        _sub(dependencies, "column-instance", column="[ObservationHour]", derivation="None", name="[none:ObservationHour:qk]", pivot="key", type="quantitative")
    _sub(view, "aggregation", value="true")
    _sub(table, "style")
    pane = _sub(_sub(table, "panes"), "pane")
    _sub(_sub(pane, "view"), "breakdown", value="auto")
    _sub(pane, "mark", **{"class": "Line" if trend else "Bar"})
    encodings = _sub(pane, "encodings")
    _sub(encodings, "color", column=_ref("none:station_id:nk"))
    if not trend:
        _sub(encodings, "text", column=_ref(metric_instance))
        mark_style = _sub(_sub(pane, "style"), "style-rule", element="mark")
        _sub(mark_style, "format", attr="mark-labels-show", value="true")
        _sub(mark_style, "format", attr="mark-labels-mode", value="all")
        _sub(mark_style, "format", attr="mark-labels-cull", value="false")
    _sub(table, "rows", _ref(metric_instance) if trend else _ref("none:station_id:nk"))
    _sub(table, "cols", _ref("none:ObservationHour:qk") if trend else _ref(metric_instance))
    _identifier(sheet, "sheet/" + name)


def _zone_style(zone: ET.Element, background: str = "#ffffff") -> None:
    style = _sub(zone, "zone-style")
    for attr, value in (("border-style", "none"), ("border-width", "0"), ("margin", "8"), ("background-color", background)):
        _sub(style, "format", attr=attr, value=value)


def _dashboard(root: ET.Element, summary: dict[str, Any]) -> None:
    dashboard = _sub(_sub(root, "dashboards"), "dashboard", name="Weather Observatory")
    _sub(dashboard, "style")
    _sub(dashboard, "size", maxheight="850", maxwidth="1200", minheight="850", minwidth="1200")
    zones = _sub(dashboard, "zones")
    layout = _sub(zones, "zone", id="0", x="0", y="0", w="100000", h="100000", **{"type-v2": "layout-basic"})
    title = _sub(layout, "zone", id="1", x="2000", y="1000", w="96000", h="6500", **{"type-v2": "text"})
    _sub(_sub(title, "formatted-text"), "run", "Weather Observatory", **{"bold": "true", "fontcolor": "#142c45", "fontsize": "26"})
    _zone_style(title)
    subtitle = _sub(layout, "zone", id="2", x="2000", y="8000", w="96000", h="7000", **{"type-v2": "text"})
    latest = summary["latest_observed_at"] or "no observations"
    completed = summary["latest_loaded_at"] or "no completed loads"
    description = f"National Weather Service · {summary['observation_count']:,} observations · {summary['station_count']} stations\nLatest observation: {latest} UTC · Warehouse refreshed: {completed} UTC"
    _sub(_sub(subtitle, "formatted-text"), "run", description, fontcolor="#4b6177", fontsize="11")
    _zone_style(subtitle)
    for id_, name, x, y, w, h in (
        (3, SHEETS[0], 2000, 16000, 96000, 39000),
        (4, SHEETS[1], 2000, 61000, 47000, 32000),
        (5, SHEETS[2], 51000, 61000, 47000, 32000),
    ):
        zone = _sub(layout, "zone", id=str(id_), name=name, x=str(x), y=str(y), w=str(w), h=str(h), **{"show-title": "true"})
        _zone_style(zone)
    legend = _sub(layout, "zone", id="7", name=SHEETS[0], x="2000", y="56000", w="96000", h="4000", param=_ref("none:station_id:nk"), **{"type-v2": "color", "pane-specification-id": "0", "leg-item-layout": "horz", "show-title": "false"})
    _zone_style(legend)
    note = _sub(layout, "zone", id="6", x="2000", y="94000", w="96000", h="5000", **{"type-v2": "text"})
    _sub(_sub(note, "formatted-text"), "run", "Completeness = present temperature, humidity and wind fields ÷ 3 fields per observation. It does not measure missing observation periods.", fontsize="10", fontcolor="#4b6177")
    _zone_style(note)
    _zone_style(layout, "#eef3f8")
    _identifier(dashboard, "dashboard")
    windows = _sub(root, "windows")
    for name in SHEETS:
        window = _sub(windows, "window", name=name, **{"class": "worksheet"})
        _sub(window, "cards")
        _sub(window, "viewpoint")
    window = _sub(windows, "window", name="Weather Observatory", maximized="true", **{"class": "dashboard"})
    viewpoints = _sub(window, "viewpoints")
    for name in SHEETS:
        _sub(viewpoints, "viewpoint", name=name)
    _sub(window, "active", id="3")


def build_workbook(exports: dict, output_dir: Path) -> dict:
    """Build .twb + .twbx and local Data/ copies from one warehouse export.

    All data references are relative. Regeneration replaces these generated
    artifacts, so save manually customized workbooks under another name.
    """
    headers = {}
    paths = {}
    summary = {"observation_count": 0, "station_count": 0, "latest_observed_at": None, "latest_loaded_at": None}
    for key in SCHEMAS:
        if key not in exports:
            raise ValueError(f"Missing dashboard export: {key}")
        source_path = Path(exports[key]).expanduser().resolve()
        paths[key] = source_path
        with source_path.open(newline="", encoding="utf-8-sig") as handle:
            reader = csv.DictReader(handle)
            headers[key] = list(reader.fieldnames or [])
            missing = set(SCHEMAS[key]) - set(headers[key])
            if missing:
                raise ValueError(f"{key} is missing required columns: {', '.join(sorted(missing))}")
            if len(headers[key]) != len(set(headers[key])):
                raise ValueError(f"{key} has duplicate CSV headers")
            if key == "dashboard_observations":
                stations = set()
                for row in reader:
                    summary["observation_count"] += 1
                    stations.add(row["station_id"])
                    for source, destination in (("observed_at", "latest_observed_at"), ("loaded_at", "latest_loaded_at")):
                        if row[source] and (summary[destination] is None or row[source] > summary[destination]):
                            summary[destination] = row[source]
                summary["station_count"] = len(stations)
    output_dir = Path(output_dir).expanduser().resolve()
    output_dir.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(prefix=".tableau-", dir=output_dir) as temp:
        staging = Path(temp)
        data_dir = staging / "Data"
        data_dir.mkdir()
        for key, path in paths.items():
            shutil.copyfile(path, data_dir / f"{key}.csv")
        root = ET.Element("workbook", {
            "version": VERSION, "original-version": VERSION, "source-build": "0.0.0 (0000.0.0.0)",
            "source-platform": "mac", "xmlns:user": "http://www.tableausoftware.com/xml/user",
        })
        manifest = _sub(root, "document-format-change-manifest")
        for feature in ("SheetIdentifierTracking", "SortTagCleanup"):
            _sub(manifest, feature)
        sources = _sub(root, "datasources")
        fields = {}
        for key in SCHEMAS:
            fields[key] = _source(sources, key, headers[key])
        sheets = _sub(root, "worksheets")
        _sheet(sheets, SHEETS[0], fields["dashboard_observations"], "temperature_c", "Avg", "avg", trend=True)
        _sheet(sheets, SHEETS[1], fields["dashboard_observations"], "station_id", "Count", "cnt")
        _sheet(sheets, SHEETS[2], fields["dashboard_observations"], "MeasurementCompleteness", "User", "usr")
        _dashboard(root, summary)
        ET.indent(root, space="  ")
        workbook = staging / WORKBOOK_NAME
        ET.ElementTree(root).write(workbook, encoding="utf-8", xml_declaration=True)
        package = staging / "weather_observatory.twbx"
        with zipfile.ZipFile(package, "w", compression=zipfile.ZIP_DEFLATED) as archive:
            archive.write(workbook, workbook.name)
            for data in sorted(data_dir.iterdir()):
                archive.write(data, f"Data/{data.name}")
        (output_dir / "Data").mkdir(exist_ok=True)
        for data in data_dir.iterdir():
            os.replace(data, output_dir / "Data" / data.name)
        os.replace(workbook, output_dir / workbook.name)
        os.replace(package, output_dir / package.name)
    return {
        "twb_path": str(output_dir / WORKBOOK_NAME),
        "twbx_path": str(output_dir / "weather_observatory.twbx"),
        "workbook_format": VERSION,
        "native_verified_with": "Tableau Desktop 2026.2.2",
        "worksheet_count": len(SHEETS),
        "summary": summary,
        "validation_note": (
            "The workbook generator was verified in Tableau Desktop 2026.2.2. "
            "Review each new data snapshot natively when it needs its own visual acceptance."
        ),
    }
