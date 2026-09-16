"""Verified portable snapshots with atomic publication and safe staged restore."""

from __future__ import annotations

import fcntl
import hashlib
import json
import os
import re
import shutil
import stat
import tempfile
import zipfile
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path, PurePosixPath
from uuid import uuid4

import duckdb

from weather_pipeline.pipeline import pipeline_lock, verify_raw_input

_MANIFEST = "backup-manifest.json"
_EXCLUDED = {".pipeline.lock", ".scheduler.lock", ".venv", ".runtime", "__pycache__", "_temporary"}
_HASH = re.compile(r"[0-9a-f]{64}")


def _digest(path):
    value = hashlib.sha256()
    with Path(path).open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            value.update(chunk)
    return value.hexdigest()


def _relative_name(value):
    if not isinstance(value, str) or not value or "\\" in value or "\x00" in value:
        raise ValueError("Unsafe archive path")
    path = PurePosixPath(value)
    if (path.is_absolute() or path.as_posix() != value
            or any(part in {".", ".."} or ":" in part for part in path.parts)):
        raise ValueError(f"Unsafe archive path: {value}")
    return path


def _inventory(data_dir, backup_root):
    files, directories = [], []
    for current, subdirectories, names in os.walk(data_dir, followlinks=False):
        current = Path(current)
        for name in list(subdirectories):
            path = current / name
            if (name in _EXCLUDED or name.endswith((".tmp", ".partial"))
                    or path == backup_root):
                subdirectories.remove(name)
                continue
            if path.is_symlink():
                raise ValueError(f"Snapshot refuses symbolic links: {path}")
            directories.append(path.relative_to(data_dir).as_posix())
        for name in names:
            path = current / name
            if name in _EXCLUDED or name.endswith((".tmp", ".partial")):
                continue
            if not stat.S_ISREG(path.lstat().st_mode):
                raise ValueError(f"Snapshot refuses nonregular files: {path}")
            files.append(path)
    return sorted(files), sorted(directories)


def _read_archive_manifest(archive):
    infos = archive.infolist()
    seen = set()
    for info in infos:
        # This format contains only regular file members; directories are metadata.
        _relative_name(info.filename)
        mode = stat.S_IFMT(info.external_attr >> 16)
        if info.filename in seen:
            raise ValueError(f"Duplicate archive member: {info.filename}")
        if info.is_dir() or mode not in {0, stat.S_IFREG} or info.flag_bits & 1:
            raise ValueError(f"Archive contains an unsupported member: {info.filename}")
        seen.add(info.filename)
    if _MANIFEST not in seen:
        raise ValueError("Backup manifest is missing")
    try:
        manifest = json.loads(archive.read(_MANIFEST))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ValueError("Backup manifest is invalid JSON") from exc
    if not isinstance(manifest, dict) or manifest.get("format_version") != 1:
        raise ValueError("Unsupported backup manifest version")
    source = manifest.get("source_data_dir")
    if not isinstance(source, str) or not Path(source).is_absolute():
        raise ValueError("Backup source_data_dir must be absolute")
    files, directories = manifest.get("files"), manifest.get("directories")
    if not isinstance(files, list) or not isinstance(directories, list):
        raise ValueError("Backup manifest must list files and directories")
    expected = {_MANIFEST}
    paths = set()
    for entry in files:
        if not isinstance(entry, dict):
            raise ValueError("Invalid file metadata in backup manifest")
        name = _relative_name(entry.get("path")).as_posix()
        if name in paths:
            raise ValueError("Duplicate path in backup manifest")
        paths.add(name)
        size, checksum = entry.get("size"), entry.get("sha256")
        if (not isinstance(size, int) or isinstance(size, bool) or size < 0
                or not isinstance(checksum, str) or not _HASH.fullmatch(checksum)):
            raise ValueError("Invalid file size or checksum in backup manifest")
        member = "data/" + name
        expected.add(member)
        if member not in seen or archive.getinfo(member).file_size != size:
            raise ValueError(f"Missing or incomplete backup member: {name}")
    directory_paths = set()
    for name in directories:
        name = _relative_name(name).as_posix()
        if name in directory_paths or name in paths:
            raise ValueError("Duplicate or conflicting directory in backup manifest")
        directory_paths.add(name)
    for name in paths | directory_paths:
        if any(parent.as_posix() in paths for parent in PurePosixPath(name).parents):
            raise ValueError("Archive file conflicts with a parent directory")
    if expected != seen:
        raise ValueError("Archive members do not match the backup manifest")
    if "warehouse.duckdb" not in paths:
        raise ValueError("Backup does not contain warehouse.duckdb")
    return manifest


def _verify_archive(archive, manifest, staging=None):
    if staging is not None:
        for name in manifest["directories"]:
            (staging / name).mkdir(parents=True, exist_ok=True)
    for entry in manifest["files"]:
        checksum = hashlib.sha256()
        destination = staging / entry["path"] if staging is not None else None
        if destination is not None:
            destination.parent.mkdir(parents=True, exist_ok=True)
        with archive.open("data/" + entry["path"]) as source:
            target = destination.open("xb") if destination is not None else None
            try:
                for chunk in iter(lambda: source.read(1024 * 1024), b""):
                    checksum.update(chunk)
                    if target is not None:
                        target.write(chunk)
                if target is not None:
                    target.flush()
                    os.fsync(target.fileno())
            finally:
                if target is not None:
                    target.close()
        if checksum.hexdigest() != entry["sha256"]:
            raise ValueError(f"Backup checksum mismatch: {entry['path']}")


def _sync_directory(directory):
    descriptor = os.open(directory, os.O_RDONLY)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def backup_data(data_dir, backup_root) -> dict:
    """Snapshot a stopped writer's data without changing its warehouse or raw files.

    A shared DuckDB read lock and the pipeline's exclusive application lock are
    held until the snapshot is verified. A remaining WAL requires normal writer
    shutdown first; backup never checkpoints or repairs the source in place.
    """
    data_dir = Path(data_dir).expanduser().resolve()
    backup_root = Path(backup_root).expanduser().resolve()
    if not data_dir.is_dir() or not (data_dir / "warehouse.duckdb").is_file():
        raise ValueError("Data directory must contain warehouse.duckdb")
    if backup_root == data_dir:
        raise ValueError("Backup directory must differ from the data directory")
    backup_root.mkdir(parents=True, exist_ok=True)
    archive_path = backup_root / (
        "weather-backup-" + datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S%fZ")
        + "-" + uuid4().hex[:8] + ".zip"
    )
    temporary = archive_path.with_suffix(".zip.tmp")
    try:
        with pipeline_lock(data_dir):
            database = data_dir / "warehouse.duckdb"
            if database.is_symlink():
                raise ValueError("Snapshot refuses a symbolic-link warehouse")
            if database.with_suffix(".duckdb.wal").exists():
                raise RuntimeError("Warehouse WAL exists; close the warehouse writer normally, then retry backup")
            with duckdb.connect(str(database), read_only=True) as connection:
                connection.execute("SELECT count(*) FROM etl_runs").fetchone()
                files, directories = _inventory(data_dir, backup_root)
                included = {path.relative_to(data_dir).as_posix() for path in files} | set(directories)
                raw_manifests = {}
                for path in files:
                    if path.name == "manifest.json" and path.is_relative_to(data_dir / "raw"):
                        document = json.loads(path.read_text())
                        batch_id = document.get("batch_id")
                        if not isinstance(batch_id, str) or not batch_id or batch_id in raw_manifests:
                            raise ValueError("Raw manifests must have unique, nonempty batch IDs")
                        _, referenced = _rebase_path(document.get("raw_path"), data_dir, data_dir, data_dir)
                        if referenced.relative_to(data_dir).as_posix() not in included:
                            raise ValueError("Backup excludes a raw manifest's input")
                        if "manifest_path" in document:
                            _, referenced_manifest = _rebase_path(document["manifest_path"], data_dir, data_dir, data_dir)
                            if referenced_manifest != path:
                                raise ValueError("Raw manifest references a different manifest file")
                        verify_raw_input(document)
                        raw_manifests[batch_id] = document
                for batch_id, *paths in connection.execute(
                    "SELECT batch_id, raw_path, curated_path, rejected_path FROM etl_runs"
                ).fetchall():
                    for value in paths:
                        _, referenced = _rebase_path(value, data_dir, data_dir, data_dir)
                        if referenced.relative_to(data_dir).as_posix() not in included:
                            raise ValueError(f"Backup excludes referenced data: {value}")
                    if batch_id not in raw_manifests or raw_manifests[batch_id]["raw_path"] != paths[0]:
                        raise ValueError(f"Backup is missing the matching raw manifest for committed batch: {batch_id}")
                entries = []
                with zipfile.ZipFile(temporary, "x", compression=zipfile.ZIP_DEFLATED,
                                     compresslevel=6, allowZip64=True) as archive:
                    for path in files:
                        before = path.stat()
                        relative = path.relative_to(data_dir).as_posix()
                        checksum = _digest(path)
                        archive.write(path, "data/" + relative)
                        after = path.stat()
                        if (before.st_size, before.st_mtime_ns, before.st_ino) != (
                                after.st_size, after.st_mtime_ns, after.st_ino):
                            raise RuntimeError(f"Source changed during backup: {relative}")
                        entries.append({"path": relative, "size": after.st_size, "sha256": checksum})
                    manifest = {
                        "format_version": 1,
                        "created_at": datetime.now(timezone.utc).isoformat(),
                        "source_data_dir": str(data_dir), "files": entries,
                        "directories": directories,
                    }
                    archive.writestr(_MANIFEST, json.dumps(manifest, indent=2, sort_keys=True) + "\n")
                with zipfile.ZipFile(temporary) as archive:
                    checked = _read_archive_manifest(archive)
                    _verify_archive(archive, checked)
                with temporary.open("rb") as handle:
                    os.fsync(handle.fileno())
                temporary.rename(archive_path)
                _sync_directory(backup_root)
        return {"status": "backed_up", "archive_path": str(archive_path),
                "file_count": len(entries), "source_bytes": sum(item["size"] for item in entries),
                "archive_bytes": archive_path.stat().st_size, "sha256": _digest(archive_path)}
    finally:
        temporary.unlink(missing_ok=True)


@contextmanager
def _restore_lock(destination):
    # A stable sibling lock serializes restores without populating the target.
    # Never unlink a lock file: a waiting process could still hold its old inode.
    lock_name = ".weather-restore-" + hashlib.sha256(str(destination).encode()).hexdigest()[:16] + ".lock"
    with (destination.parent / lock_name).open("a+") as handle:
        try:
            fcntl.flock(handle, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as exc:
            raise RuntimeError("Another restore is using this destination") from exc
        try:
            yield
        finally:
            fcntl.flock(handle, fcntl.LOCK_UN)


def _require_empty_destination(destination):
    if destination.is_symlink():
        raise ValueError("Restore destination must not be a symbolic link")
    if destination.exists() and (not destination.is_dir() or any(destination.iterdir())):
        raise ValueError("Restore destination must be absent or empty; existing data is never overwritten")


def _rebase_path(value, old_root, destination, staging):
    if not isinstance(value, str) or not Path(value).is_absolute():
        raise ValueError("Restored data references must be absolute paths inside the original data directory")
    original = Path(value)
    if ".." in original.parts:
        raise ValueError("Restored data reference contains a parent traversal")
    try:
        relative = original.relative_to(old_root)
    except ValueError as exc:
        raise ValueError(f"Backup references data outside its source directory: {value}") from exc
    if not relative.parts or not (staging / relative).exists():
        raise ValueError(f"Backup is missing referenced data: {value}")
    return str(destination / relative), staging / relative


def _prepare_restored_data(staging, manifest, destination):
    old_root = Path(manifest["source_data_dir"])
    manifests = {}
    for path in sorted((staging / "raw").rglob("manifest.json")):
        document = json.loads(path.read_text())
        raw_path, staged_raw = _rebase_path(document.get("raw_path"), old_root, destination, staging)
        verify_raw_input({**document, "raw_path": str(staged_raw)})
        document["raw_path"] = raw_path
        if "manifest_path" in document:
            document["manifest_path"], staged_manifest = _rebase_path(
                document["manifest_path"], old_root, destination, staging)
            if staged_manifest != path:
                raise ValueError("Raw manifest references a different manifest file")
        path.write_text(json.dumps(document, indent=2, sort_keys=True) + "\n")
        batch_id = document.get("batch_id")
        if not isinstance(batch_id, str) or not batch_id or batch_id in manifests:
            raise ValueError("Raw manifests must have unique, nonempty batch IDs")
        manifests[batch_id] = document
    with duckdb.connect(str(staging / "warehouse.duckdb")) as connection:
        connection.execute("BEGIN TRANSACTION")
        rows = connection.execute(
            "SELECT batch_id, raw_path, curated_path, rejected_path FROM etl_runs"
        ).fetchall()
        for batch_id, *paths in rows:
            rebased = [_rebase_path(value, old_root, destination, staging)[0] for value in paths]
            if batch_id not in manifests or manifests[batch_id]["raw_path"] != rebased[0]:
                raise ValueError(f"Backup is missing the matching raw manifest for committed batch: {batch_id}")
            connection.execute(
                "UPDATE etl_runs SET raw_path = ?, curated_path = ?, rejected_path = ? WHERE batch_id = ?",
                [*rebased, batch_id],
            )
        connection.execute("COMMIT")
        connection.execute("CHECKPOINT")
        observations = connection.execute("SELECT count(*) FROM fact_observation").fetchone()[0]
    return {"rebased_manifests": len(manifests), "committed_batches": len(rows),
            "observation_count": observations}


def restore_backup(archive_path, destination) -> dict:
    """Verify and rebase in staging, then atomically publish to an empty target.

    Event logs remain the byte-identical historical audit trail. Only location
    fields in raw manifests and etl_runs change; batch fingerprints do not.
    """
    archive_path = Path(archive_path).expanduser().resolve()
    # Preserve the last component until the symlink check has happened.
    requested = Path(destination).expanduser().absolute()
    if requested.is_symlink():
        raise ValueError("Restore destination must not be a symbolic link")
    destination = requested.resolve()
    _require_empty_destination(destination)
    if not archive_path.is_file():
        raise ValueError("Backup archive does not exist")
    destination.parent.mkdir(parents=True, exist_ok=True)
    staging = None
    with _restore_lock(destination):
        try:
            _require_empty_destination(destination)
            staging = Path(tempfile.mkdtemp(prefix="." + destination.name + ".restore-",
                                            dir=destination.parent))
            with zipfile.ZipFile(archive_path) as archive:
                manifest = _read_archive_manifest(archive)
                _verify_archive(archive, manifest, staging)
            summary = _prepare_restored_data(staging, manifest, destination)
            # fsync files modified during rebasing before publishing the tree.
            for path in staging.rglob("*"):
                if path.is_file():
                    with path.open("rb") as handle:
                        os.fsync(handle.fileno())
            for directory in sorted((p for p in staging.rglob("*") if p.is_dir()),
                                    key=lambda p: len(p.parts), reverse=True):
                _sync_directory(directory)
            _sync_directory(staging)
            _require_empty_destination(destination)
            staging.rename(destination)
            staging = None
            _sync_directory(destination.parent)
            return {"status": "restored", "archive_path": str(archive_path),
                    "data_dir": str(destination), "file_count": len(manifest["files"]), **summary}
        except zipfile.BadZipFile as exc:
            raise ValueError(f"Backup ZIP is corrupt: {exc}") from exc
        finally:
            if staging is not None:
                shutil.rmtree(staging)
