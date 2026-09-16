#!/usr/bin/env python3
"""Inspect a built wheel before installing it in the separate release smoke test."""

import argparse
import configparser
import json
import re
import zipfile
from pathlib import Path, PurePosixPath


PACKAGE = "weather_pipeline"
SAMPLE = f"{PACKAGE}/examples/recorded-nws-sample.json"
REQUIRED_MODULES = {
    "__init__", "__main__", "cli", "demo", "ingestion", "pipeline", "transform",
    "warehouse", "tableau", "dashboard_audit", "scheduler", "monitoring", "backup",
}
METADATA_FILES = {"METADATA", "WHEEL", "entry_points.txt", "top_level.txt", "RECORD"}


def audit_wheel(path: Path) -> dict:
    """Check release contents without extracting or importing the archive.

    This catches missing package data and accidental runtime state in releases.
    The separate CI installation exercises imports and actual pipeline behavior.
    """
    errors = []
    try:
        with zipfile.ZipFile(path) as archive:
            names = archive.namelist()
            name_set = set(names)
            if len(name_set) != len(names):
                errors.append("The wheel contains duplicate archive paths")
            metadata_dirs = {
                PurePosixPath(name).parts[0] for name in names
                if PurePosixPath(name).parts
                and PurePosixPath(name).parts[0].startswith("weather_data_pipeline-")
                and PurePosixPath(name).parts[0].endswith(".dist-info")
            }
            if len(metadata_dirs) != 1:
                errors.append("Expected exactly one weather_data_pipeline .dist-info directory")
            for name in names:
                member = PurePosixPath(name)
                if member.is_absolute() or ".." in member.parts or "\\" in name:
                    errors.append(f"Unsafe archive path: {name}")
                    continue
                allowed_module = (
                    len(member.parts) == 2
                    and member.parts[0] == PACKAGE
                    and member.suffix == ".py"
                )
                allowed_metadata = (
                    len(member.parts) == 2
                    and member.parts[0] in metadata_dirs
                    and member.name in METADATA_FILES
                )
                if not (allowed_module or allowed_metadata or name == SAMPLE):
                    errors.append(f"Unexpected release file: {name}")
            for module in sorted(REQUIRED_MODULES):
                required = f"{PACKAGE}/{module}.py"
                if required not in name_set:
                    errors.append(f"Missing required module: {required}")
            if SAMPLE not in name_set:
                errors.append(f"Missing packaged demo data: {SAMPLE}")
            else:
                try:
                    sample = json.loads(archive.read(SAMPLE))
                    if not isinstance(sample, dict) or not sample:
                        errors.append("Packaged demo data must be a nonempty JSON object")
                except (ValueError, UnicodeError):
                    errors.append("Packaged demo data is not valid JSON")
            if len(metadata_dirs) == 1:
                metadata_dir = next(iter(metadata_dirs))
                for filename in ("METADATA", "WHEEL", "RECORD", "entry_points.txt"):
                    if f"{metadata_dir}/{filename}" not in name_set:
                        errors.append(f"Missing wheel metadata: {filename}")
                entry_path = f"{metadata_dir}/entry_points.txt"
                if entry_path in name_set:
                    try:
                        entry_points = configparser.ConfigParser(interpolation=None)
                        entry_points.read_string(archive.read(entry_path).decode("utf-8"))
                        target = entry_points.get("console_scripts", "weather-pipeline")
                        if target.strip() != "weather_pipeline.cli:main":
                            errors.append("weather-pipeline must point to weather_pipeline.cli:main")
                    except (configparser.Error, UnicodeError):
                        errors.append("Missing or invalid weather-pipeline console entry point")
            for name in name_set:
                if name.endswith((".py", ".json", "/METADATA")):
                    if re.search(rb"/(?:Users|home)/[^/\s]+/", archive.read(name)):
                        errors.append(f"Developer home path embedded in release file: {name}")
    except (OSError, zipfile.BadZipFile) as error:
        return {"status": "failed", "wheel": str(path), "errors": [str(error)]}
    return {
        "status": "failed" if errors else "passed",
        "wheel": str(path),
        "file_count": len(names),
        "errors": errors,
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("wheel", type=Path, help="Path to one built .whl archive")
    args = parser.parse_args()
    result = audit_wheel(args.wheel)
    print(json.dumps(result, indent=2))
    return 0 if result["status"] == "passed" else 1


if __name__ == "__main__":
    raise SystemExit(main())
