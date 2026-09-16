from datetime import datetime, timezone
import hashlib

import pytest

from weather_pipeline.pipeline import build_windows, parse_time, pipeline_lock, verify_raw_input


def test_incremental_windows_overlap_and_initial_stations():
    windows = build_windows(["KAUS", "KORD"], {"KAUS": "2026-09-13T10:00:00Z"},
                            "2026-09-13T12:00:00Z")
    assert windows[0]["start"] == "2026-09-13T08:00:00+00:00"
    assert windows[1]["start"] == "2026-09-12T12:00:00+00:00"


def test_explicit_backfill_ignores_live_checkpoint():
    windows = build_windows(["KAUS"], {"KAUS": "2026-09-13T10:00:00Z"},
                            "2026-09-12T12:00:00Z", start="2026-09-12T00:00:00Z")
    assert windows[0]["start"] == "2026-09-12T00:00:00+00:00"


def test_naive_and_reversed_times_fail():
    with pytest.raises(ValueError, match="timezone"):
        parse_time("2026-09-13T12:00:00")
    with pytest.raises(ValueError, match="precede"):
        build_windows(["KAUS"], {}, "2026-09-12T12:00:00Z", start="2026-09-13T00:00:00Z")
    assert parse_time("2026-09-13T07:00:00-05:00") == datetime(2026, 9, 13, 12, tzinfo=timezone.utc)


def test_single_writer_lock(tmp_path):
    with pipeline_lock(tmp_path):
        with pytest.raises(RuntimeError, match="Another pipeline"):
            with pipeline_lock(tmp_path):
                pass


def test_raw_digest_detects_truncation_before_processing(tmp_path):
    raw = tmp_path / "raw.jsonl"
    original = b'{"observation": 1}\n{"observation": 2}\n'
    raw.write_bytes(original)
    manifest = {"raw_path": str(raw), "raw_sha256": hashlib.sha256(original).hexdigest()}
    verify_raw_input(manifest)
    raw.write_bytes(original.splitlines(keepends=True)[0])
    with pytest.raises(ValueError, match="checksum mismatch"):
        verify_raw_input(manifest)
