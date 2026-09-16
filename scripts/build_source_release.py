"""Create an allowlisted, deterministic source ZIP without local runtime data."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import stat
import tempfile
import tomllib
import zipfile
from pathlib import Path


ROOT_FILES = (
    "README.md", "pyproject.toml", "requirements.lock", "settings.toml",
    "Dockerfile", "compose.yaml", ".gitignore", ".dockerignore",
)
PATTERNS = (
    "src/**/*.py", "src/weather_pipeline/examples/*.json", "tests/**/*.py",
    "scripts/*.py", "scripts/*.sh", "docs/*.md", "dashboard/README.md",
    ".github/workflows/*.yml", ".github/workflows/*.yaml",
)


def collect_source_files(project_dir):
    project_dir = Path(project_dir).resolve()
    paths = {project_dir / name for name in ROOT_FILES}
    for pattern in PATTERNS:
        paths.update(project_dir.glob(pattern))
    files = {}
    for path in sorted(paths):
        if not path.is_file():
            raise ValueError(f"Required release file is missing: {path.name}")
        relative = path.relative_to(project_dir)
        if "__pycache__" in relative.parts:
            continue
        # Reject symlinks in both the file and parent components.
        current = path
        while current != project_dir:
            if current.is_symlink():
                raise ValueError(f"Release file must not be a symbolic link: {relative}")
            current = current.parent
        files[relative.as_posix()] = path.read_bytes()
    required = {
        "src/weather_pipeline/demo.py",
        "src/weather_pipeline/examples/recorded-nws-sample.json",
        ".github/workflows/ci.yml",
    }
    missing = required - files.keys()
    if missing:
        raise ValueError(f"Release components are missing: {', '.join(sorted(missing))}")
    return files


def build_source_release(project_dir, output_dir):
    project_dir = Path(project_dir).resolve()
    output_dir = Path(output_dir).resolve()
    files = collect_source_files(project_dir)
    metadata = tomllib.loads(files["pyproject.toml"].decode())["project"]
    version = metadata["version"]
    if not version or any(character not in "0123456789abcdefghijklmnopqrstuvwxyz.-" for character in version):
        raise ValueError("Project version cannot be used as an archive filename")
    prefix = f"weather-data-pipeline-{version}"
    manifest = {
        "format_version": 1, "project": metadata["name"], "version": version,
        "contents": "Source, tests, documentation, and a recorded public NWS demo fixture",
        "excluded": ["live data", "backups", "local runtimes", "credentials", "Git metadata"],
        "files": [{"path": name, "bytes": len(content),
                   "sha256": hashlib.sha256(content).hexdigest()}
                  for name, content in sorted(files.items())],
    }
    files["RELEASE-MANIFEST.json"] = (json.dumps(manifest, indent=2, sort_keys=True) + "\n").encode()
    output_dir.mkdir(parents=True, exist_ok=True)
    destination = output_dir / f"{prefix}-source.zip"
    temporary = None
    try:
        with tempfile.NamedTemporaryFile(dir=output_dir, suffix=".zip.tmp", delete=False) as handle:
            temporary = Path(handle.name)
        with zipfile.ZipFile(temporary, "w", compression=zipfile.ZIP_DEFLATED) as archive:
            for name, content in sorted(files.items()):
                entry = zipfile.ZipInfo(f"{prefix}/{name}", date_time=(1980, 1, 1, 0, 0, 0))
                entry.create_system = 3
                entry.external_attr = (stat.S_IFREG | 0o644) << 16
                entry.compress_type = zipfile.ZIP_DEFLATED
                archive.writestr(entry, content)
        with zipfile.ZipFile(temporary) as archive:
            if archive.testzip() is not None:
                raise ValueError("Release ZIP integrity check failed")
            for entry in manifest["files"]:
                actual = archive.read(f"{prefix}/{entry['path']}")
                if hashlib.sha256(actual).hexdigest() != entry["sha256"]:
                    raise ValueError(f"Release checksum mismatch: {entry['path']}")
        # Atomic replacement only touches this generated version's release file.
        os.replace(temporary, destination)
        checksum = hashlib.sha256(destination.read_bytes()).hexdigest()
        destination.with_suffix(".zip.sha256").write_text(f"{checksum}  {destination.name}\n")
        return {"status": "built", "archive_path": str(destination), "version": version,
                "file_count": len(files), "bytes": destination.stat().st_size, "sha256": checksum}
    finally:
        if temporary is not None:
            temporary.unlink(missing_ok=True)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--project-dir", type=Path, default=Path(__file__).resolve().parents[1])
    parser.add_argument("--output-dir", type=Path, default=Path("artifacts/release"))
    args = parser.parse_args()
    print(json.dumps(build_source_release(args.project_dir, args.output_dir), indent=2))


if __name__ == "__main__":
    main()
