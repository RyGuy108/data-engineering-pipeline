"""Snapshot recovery and replay contracts using DuckDB, without Spark or network."""

import hashlib
import json
import stat
import zipfile
from pathlib import Path

import duckdb
import pytest

from weather_pipeline import backup
from weather_pipeline.pipeline import pipeline_lock
from weather_pipeline.warehouse import load_batch


def digest(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


@pytest.fixture
def populated_data(tmp_path):
    data = tmp_path / "data"
    raw = data / "raw" / "batch-1"
    curated = data / "processed" / "batch-1" / "attempt-1" / "curated"
    rejected = curated.parent / "rejected"
    for directory in (raw, curated, rejected, data / "reports", data / "exports", data / "tableau"):
        directory.mkdir(parents=True, exist_ok=True)
    raw_path = raw / "observations.ndjson"
    raw_path.write_text('{"observation": "fixture"}\n')
    manifest = {
        "batch_id": "batch-1", "source": "nws", "ingested_at": "2026-09-12T02:00:00Z",
        "start": "2026-09-12T00:00:00Z", "end": "2026-09-12T02:00:00Z",
        "raw_path": str(raw_path), "manifest_path": str(raw / "manifest.json"),
        "raw_sha256": digest(raw_path), "record_count": 1,
        "stations": [{"station_id": "KORD", "name": "Chicago", "latitude": 41.98, "longitude": -87.9}],
        "windows": [{"station_id": "KORD", "start": "2026-09-12T00:00:00Z", "end": "2026-09-12T02:00:00Z"}],
        "advance_checkpoint": True,
    }
    Path(manifest["manifest_path"]).write_text(json.dumps(manifest))
    with duckdb.connect() as connection:
        connection.execute("""
            CREATE TABLE fixture AS SELECT 'KORD' AS station_id,
                TIMESTAMP '2026-09-12 01:00:00' AS observed_at, 20.0 AS temperature_c,
                50.0 AS humidity_pct, 12.0 AS wind_kph, 'Clear' AS text_description,
                41.98 AS latitude, -87.9 AS longitude, 'aaa' AS payload_hash,
                TIMESTAMP '2026-09-12 02:00:00' AS source_ingested_at
        """)
        connection.execute("COPY fixture TO ? (FORMAT PARQUET)", [str(curated / "part.parquet")])
        connection.execute("COPY (SELECT * FROM fixture WHERE FALSE) TO ? (FORMAT PARQUET)",
                           [str(rejected / "part.parquet")])
    result = {"curated_path": str(curated), "rejected_path": str(rejected), "report": {
        "input_count": 1, "accepted_count": 1, "rejected_count": 0,
        "duplicate_count": 0, "missing_measurement_count": 0,
    }}
    (data / "reports" / "batch-1.json").write_text(json.dumps(result["report"]))
    (data / "events.jsonl").write_text(json.dumps({"status": "succeeded", "raw_path": str(raw_path)}) + "\n")
    (data / "exports" / "dashboard.csv").write_text("station_id\nKORD\n")
    (data / "tableau" / "dashboard.twbx").write_bytes(b"sample dashboard")
    (data / "empty-directory").mkdir()
    load_batch(data / "warehouse.duckdb", manifest, result)
    return data, manifest, result


def query(data, sql):
    with duckdb.connect(str(data / "warehouse.duckdb"), read_only=True) as connection:
        return connection.execute(sql).fetchall()


def make_archive(populated_data, tmp_path):
    return Path(backup.backup_data(populated_data[0], tmp_path / "backups")["archive_path"])


def rewrite_archive(archive, output, edits=None, remove=(), update_checksums=False, extra=()):
    edits = edits or {}
    with zipfile.ZipFile(archive) as source:
        contents = {info.filename: source.read(info) for info in source.infolist()}
    for name in remove:
        contents.pop(name)
    contents.update(edits)
    if update_checksums:
        manifest = json.loads(contents["backup-manifest.json"])
        manifest["files"] = [entry for entry in manifest["files"] if "data/" + entry["path"] in contents]
        for entry in manifest["files"]:
            content = contents["data/" + entry["path"]]
            entry.update(size=len(content), sha256=hashlib.sha256(content).hexdigest())
        contents["backup-manifest.json"] = json.dumps(manifest).encode()
    with zipfile.ZipFile(output, "w") as target:
        for name, content in contents.items():
            target.writestr(name, content)
        for info, content in extra:
            target.writestr(info, content)
    return output


def test_backup_preserves_source_and_restored_batch_replays_exactly(populated_data, tmp_path):
    data, original_manifest, result = populated_data
    before_files = {str(path.relative_to(data)): digest(path) for path in data.rglob("*") if path.is_file()}
    before_facts = query(data, "SELECT * FROM fact_observation")
    before_runs = query(data, "SELECT batch_id, input_fingerprint FROM etl_runs")
    before_checkpoints = query(data, "SELECT * FROM source_checkpoints")
    archive = make_archive(populated_data, tmp_path)
    assert digest(archive)
    assert all(digest(data / name) == checksum for name, checksum in before_files.items())
    destination = tmp_path / "restored"
    outcome = backup.restore_backup(archive, destination)
    assert outcome["observation_count"] == outcome["committed_batches"] == outcome["rebased_manifests"] == 1
    assert (destination / "empty-directory").is_dir()
    restored_manifest = json.loads((destination / "raw/batch-1/manifest.json").read_text())
    assert restored_manifest["raw_path"] == str(destination / "raw/batch-1/observations.ndjson")
    assert restored_manifest["manifest_path"] == str(destination / "raw/batch-1/manifest.json")
    assert {k: v for k, v in restored_manifest.items() if k not in {"raw_path", "manifest_path"}} == {
        k: v for k, v in original_manifest.items() if k not in {"raw_path", "manifest_path"}}
    assert (destination / "events.jsonl").read_bytes() == (data / "events.jsonl").read_bytes()
    assert query(destination, "SELECT * FROM fact_observation") == before_facts
    assert query(destination, "SELECT batch_id, input_fingerprint FROM etl_runs") == before_runs
    assert query(destination, "SELECT * FROM source_checkpoints") == before_checkpoints
    raw, curated, rejected = query(destination, "SELECT raw_path, curated_path, rejected_path FROM etl_runs")[0]
    assert all(Path(path).exists() and Path(path).is_relative_to(destination) for path in (raw, curated, rejected))
    replay_result = {**result, "curated_path": curated, "rejected_path": rejected}
    assert load_batch(destination / "warehouse.duckdb", restored_manifest, replay_result)["status"] == "already_loaded"
    assert query(destination, "SELECT * FROM fact_observation") == before_facts
    assert query(destination, "SELECT * FROM source_checkpoints") == before_checkpoints


def test_empty_destination_is_supported(populated_data, tmp_path):
    archive = make_archive(populated_data, tmp_path)
    destination = tmp_path / "empty"
    destination.mkdir()
    assert backup.restore_backup(archive, destination)["status"] == "restored"


def test_backup_skips_transient_runtime_files_and_its_own_subtree(populated_data):
    data = populated_data[0]
    for directory in (data / ".venv", data / ".runtime", data / "raw/.interrupted.partial", data / "processed/_temporary"):
        directory.mkdir()
        (directory / "ignored").write_text("transient")
    (data / "exports/unpublished.tmp").write_text("transient")
    (data / ".scheduler.lock").write_text("transient")
    backup_root = data / "backups"
    first = Path(backup.backup_data(data, backup_root)["archive_path"])
    second = Path(backup.backup_data(data, backup_root)["archive_path"])
    assert first.exists() and second.exists()
    with zipfile.ZipFile(second) as archive:
        assert all(not any(part in name for part in (
            ".pipeline.lock", ".scheduler.lock", ".venv", ".runtime", ".partial", "_temporary", ".tmp", "backups/",
        )) for name in archive.namelist())


def test_backup_refuses_active_pipeline_and_does_not_publish(populated_data, tmp_path):
    with pipeline_lock(populated_data[0]):
        with pytest.raises(RuntimeError, match="Another pipeline"):
            make_archive(populated_data, tmp_path)
    assert not list((tmp_path / "backups").glob("*"))


def test_backup_refuses_live_wal_and_symlinks(populated_data, tmp_path):
    data = populated_data[0]
    wal = data / "warehouse.duckdb.wal"
    wal.write_bytes(b"uncheckpointed")
    with pytest.raises(RuntimeError, match="WAL exists"):
        make_archive(populated_data, tmp_path)
    assert wal.read_bytes() == b"uncheckpointed"
    wal.unlink()
    (data / "link").symlink_to(data / "events.jsonl")
    with pytest.raises(ValueError, match="nonregular"):
        make_archive(populated_data, tmp_path)
    assert not list((tmp_path / "backups").glob("*"))


@pytest.mark.parametrize("destination_exists", [False, True])
def test_checksum_failure_leaves_destination_untouched(populated_data, tmp_path, destination_exists):
    archive = make_archive(populated_data, tmp_path)
    damaged = rewrite_archive(archive, tmp_path / "damaged.zip", {
        "data/exports/dashboard.csv": b"station_id\nFAIL\n",
    })
    destination = tmp_path / "restored"
    if destination_exists:
        destination.mkdir()
    with pytest.raises(ValueError, match="checksum mismatch"):
        backup.restore_backup(damaged, destination)
    assert destination.exists() == destination_exists
    assert not destination.exists() or not list(destination.iterdir())
    assert not list(tmp_path.glob(".restored.restore-*"))


def test_incomplete_restore_fails_before_publish(populated_data, tmp_path):
    archive = make_archive(populated_data, tmp_path)
    incomplete = rewrite_archive(archive, tmp_path / "incomplete.zip",
                                 remove=["data/raw/batch-1/observations.ndjson"], update_checksums=True)
    destination = tmp_path / "restored"
    with pytest.raises(ValueError, match="missing referenced data"):
        backup.restore_backup(incomplete, destination)
    assert not destination.exists()
    assert not list(tmp_path.glob(".restored.restore-*"))


def test_missing_committed_manifest_is_rejected_by_backup_and_restore(populated_data, tmp_path):
    archive = make_archive(populated_data, tmp_path)
    incomplete = rewrite_archive(archive, tmp_path / "no-manifest.zip",
                                 remove=["data/raw/batch-1/manifest.json"], update_checksums=True)
    with pytest.raises(ValueError, match="missing the matching raw manifest"):
        backup.restore_backup(incomplete, tmp_path / "restored")
    Path(populated_data[1]["manifest_path"]).unlink()
    with pytest.raises(ValueError, match="missing the matching raw manifest"):
        make_archive(populated_data, tmp_path)
    assert not (tmp_path / "restored").exists()
    assert len(list((tmp_path / "backups").glob("*.zip"))) == 1


def test_rejects_external_data_reference_before_publish(populated_data, tmp_path):
    archive = make_archive(populated_data, tmp_path)
    manifest = {**populated_data[1], "raw_path": str(tmp_path / "outside.ndjson")}
    changed = rewrite_archive(archive, tmp_path / "external.zip", {
        "data/raw/batch-1/manifest.json": json.dumps(manifest).encode(),
    }, update_checksums=True)
    with pytest.raises(ValueError, match="outside its source directory"):
        backup.restore_backup(changed, tmp_path / "restored")
    assert not (tmp_path / "restored").exists()


def test_failure_after_rebasing_keeps_destination_absent(populated_data, tmp_path, monkeypatch):
    archive = make_archive(populated_data, tmp_path)
    original = backup._prepare_restored_data

    def fail_after_rebase(*args):
        original(*args)
        raise RuntimeError("injected restore failure")

    monkeypatch.setattr(backup, "_prepare_restored_data", fail_after_rebase)
    with pytest.raises(RuntimeError, match="injected restore failure"):
        backup.restore_backup(archive, tmp_path / "restored")
    assert not (tmp_path / "restored").exists()
    assert not list(tmp_path.glob(".restored.restore-*"))


@pytest.mark.parametrize("unsafe", ["../escape", "/absolute", "data/../escape", "data\\escape", "C:/escape", "data//escape"])
def test_rejects_unsafe_archive_paths(populated_data, tmp_path, unsafe):
    archive = make_archive(populated_data, tmp_path)
    dangerous = rewrite_archive(archive, tmp_path / "dangerous.zip", extra=[(unsafe, b"payload")])
    with pytest.raises(ValueError, match="Unsafe archive path"):
        backup.restore_backup(dangerous, tmp_path / "restored")
    assert not (tmp_path / "restored").exists()


def test_rejects_symbolic_link_and_duplicate_members(populated_data, tmp_path):
    archive = make_archive(populated_data, tmp_path)
    info = zipfile.ZipInfo("data/link")
    info.create_system = 3
    info.external_attr = (stat.S_IFLNK | 0o777) << 16
    linked = rewrite_archive(archive, tmp_path / "linked.zip", extra=[(info, b"../../target")])
    with pytest.raises(ValueError, match="unsupported member"):
        backup.restore_backup(linked, tmp_path / "restored")
    with pytest.warns(UserWarning, match="Duplicate name"):
        duplicate = rewrite_archive(archive, tmp_path / "duplicate.zip", extra=[("data/events.jsonl", b"again")])
    with pytest.raises(ValueError, match="Duplicate archive member"):
        backup.restore_backup(duplicate, tmp_path / "restored")


def test_rejects_nonempty_or_symlink_destination(populated_data, tmp_path):
    archive = make_archive(populated_data, tmp_path)
    data = populated_data[0]
    before = digest(data / "warehouse.duckdb")
    with pytest.raises(ValueError, match="absent or empty"):
        backup.restore_backup(archive, data)
    assert digest(data / "warehouse.duckdb") == before
    link = tmp_path / "linked-destination"
    link.symlink_to(data, target_is_directory=True)
    with pytest.raises(ValueError, match="symbolic link"):
        backup.restore_backup(archive, link)


def test_rejects_archive_without_member_or_with_invalid_zip(populated_data, tmp_path):
    archive = make_archive(populated_data, tmp_path)
    incomplete = rewrite_archive(archive, tmp_path / "incomplete.zip", remove=["data/events.jsonl"])
    with pytest.raises(ValueError, match="Missing or incomplete"):
        backup.restore_backup(incomplete, tmp_path / "restored")
    broken = tmp_path / "broken.zip"
    broken.write_bytes(archive.read_bytes()[:100])
    with pytest.raises(ValueError, match="ZIP is corrupt"):
        backup.restore_backup(broken, tmp_path / "restored")
    assert not (tmp_path / "restored").exists()
