import hashlib
import importlib.util
import json
import zipfile
from pathlib import Path

import pytest


module_path = Path(__file__).resolve().parents[1] / "scripts/build_source_release.py"
spec = importlib.util.spec_from_file_location("source_release", module_path)
release = importlib.util.module_from_spec(spec)
spec.loader.exec_module(release)


def project(tmp_path):
    root = tmp_path / "project"
    root.mkdir()
    for name in release.ROOT_FILES:
        (root / name).write_text("example\n")
    (root / "pyproject.toml").write_text('[project]\nname = "weather-data-pipeline"\nversion = "0.2.0"\n')
    for name in ("src/weather_pipeline/demo.py", "src/weather_pipeline/examples/recorded-nws-sample.json",
                 ".github/workflows/ci.yml"):
        path = root / name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text("example\n")
    return root


def test_release_excludes_local_data_and_verifies_all_source_hashes(tmp_path):
    root = project(tmp_path)
    (root / ".env").write_text("SECRET=must-not-ship")
    (root / "data").mkdir()
    (root / "data/warehouse.duckdb").write_bytes(b"private local data")
    result = release.build_source_release(root, tmp_path / "output")
    with zipfile.ZipFile(result["archive_path"]) as archive:
        names = archive.namelist()
        assert not any(name.endswith("/.env") or "/data/" in name for name in names)
        manifest = json.loads(archive.read("weather-data-pipeline-0.2.0/RELEASE-MANIFEST.json"))
        for entry in manifest["files"]:
            assert hashlib.sha256(archive.read(f"weather-data-pipeline-0.2.0/{entry['path']}")).hexdigest() == entry["sha256"]


def test_source_release_is_reproducible_and_rejects_symlink_content(tmp_path):
    root = project(tmp_path)
    one = release.build_source_release(root, tmp_path / "one")
    two = release.build_source_release(root, tmp_path / "two")
    assert one["sha256"] == two["sha256"]
    link = root / "README.md"
    link.unlink()
    link.symlink_to(root / "settings.toml")
    with pytest.raises(ValueError, match="symbolic link"):
        release.build_source_release(root, tmp_path / "bad")
    assert not (tmp_path / "bad").exists()
