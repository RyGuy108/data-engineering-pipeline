"""Reject incomplete distributions and state that should stay out of releases."""

import importlib.util
import json
import subprocess
import sys
import zipfile
from pathlib import Path

import pytest


SCRIPT = Path(__file__).resolve().parents[1] / "scripts" / "check_release.py"
SPEC = importlib.util.spec_from_file_location("check_release", SCRIPT)
release = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(release)


@pytest.fixture
def wheel_files():
    files = {f"weather_pipeline/{name}.py": b"" for name in release.REQUIRED_MODULES}
    files[release.SAMPLE] = b'{"features": [{"type": "Feature"}]}'
    prefix = "weather_data_pipeline-0.1.0.dist-info"
    files[f"{prefix}/METADATA"] = b"Name: weather-data-pipeline\nVersion: 0.1.0\n"
    files[f"{prefix}/WHEEL"] = b"Wheel-Version: 1.0\n"
    files[f"{prefix}/RECORD"] = b""
    files[f"{prefix}/entry_points.txt"] = (
        b"[console_scripts]\nweather-pipeline = weather_pipeline.cli:main\n"
    )
    return files


def write_wheel(tmp_path, files):
    path = tmp_path / "weather_data_pipeline-0.1.0-py3-none-any.whl"
    with zipfile.ZipFile(path, "w") as archive:
        for name, contents in files.items():
            archive.writestr(name, contents)
    return path


def test_expected_package_passes(tmp_path, wheel_files):
    result = release.audit_wheel(write_wheel(tmp_path, wheel_files))
    assert result["status"] == "passed"
    assert result["errors"] == []
    assert result["file_count"] == len(wheel_files)


@pytest.mark.parametrize("filename", [release.SAMPLE, "weather_pipeline/demo.py"])
def test_missing_demo_component_fails(tmp_path, wheel_files, filename):
    del wheel_files[filename]
    result = release.audit_wheel(write_wheel(tmp_path, wheel_files))
    assert result["status"] == "failed"
    assert any(filename in error for error in result["errors"])


@pytest.mark.parametrize("contents", [b"{broken", b"{}", b"[]"])
def test_invalid_or_empty_sample_fails(tmp_path, wheel_files, contents):
    wheel_files[release.SAMPLE] = contents
    assert release.audit_wheel(write_wheel(tmp_path, wheel_files))["status"] == "failed"


@pytest.mark.parametrize("filename", [
    "weather_pipeline/.env", "data/warehouse.duckdb", "../outside.txt", "/absolute.txt",
])
def test_runtime_state_and_unsafe_paths_fail(tmp_path, wheel_files, filename):
    wheel_files[filename] = b"do not ship"
    result = release.audit_wheel(write_wheel(tmp_path, wheel_files))
    assert result["status"] == "failed"
    assert any(filename in error for error in result["errors"])


def test_wrong_entry_point_fails(tmp_path, wheel_files):
    wheel_files["weather_data_pipeline-0.1.0.dist-info/entry_points.txt"] = (
        b"[console_scripts]\nweather-pipeline = absent_package:main\n"
    )
    result = release.audit_wheel(write_wheel(tmp_path, wheel_files))
    assert result["status"] == "failed"
    assert any("console" in error or "point to" in error for error in result["errors"])


def test_developer_path_fails(tmp_path, wheel_files):
    wheel_files["weather_pipeline/demo.py"] = b'LOCAL_DATA = "/Users/developer/data/"'
    result = release.audit_wheel(write_wheel(tmp_path, wheel_files))
    assert result["status"] == "failed"
    assert any("Developer home path" in error for error in result["errors"])


def test_invalid_archive_returns_failure(tmp_path):
    path = tmp_path / "broken.whl"
    path.write_text("not a zip")
    assert release.audit_wheel(path)["status"] == "failed"


def test_cli_returns_nonzero_for_invalid_release(tmp_path, wheel_files):
    del wheel_files[release.SAMPLE]
    wheel = write_wheel(tmp_path, wheel_files)
    result = subprocess.run(
        [sys.executable, str(SCRIPT), str(wheel)], capture_output=True, text=True, check=False,
    )
    assert result.returncode == 1
    assert json.loads(result.stdout)["status"] == "failed"
