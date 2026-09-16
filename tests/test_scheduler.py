import json
import threading
from datetime import datetime, timedelta, timezone

import pytest

from weather_pipeline.scheduler import run_schedule, worker_lock


class Clock:
    def __init__(self):
        self.seconds = 0.0
        self.waits = []

    def monotonic(self):
        return self.seconds

    def now(self):
        return datetime(2026, 9, 13, tzinfo=timezone.utc) + timedelta(seconds=self.seconds)

    def wait(self, duration):
        self.waits.append(duration)
        self.seconds += duration


def test_fixed_cadence_accounts_for_work_duration(tmp_path):
    clock = Clock()
    started = []

    def work():
        started.append(clock.seconds)
        clock.seconds += 10

    assert run_schedule(work, tmp_path, 1, 3, monotonic=clock.monotonic,
                        utc_now=clock.now, wait=clock.wait) == 0
    assert started == [0, 60, 120]
    assert clock.waits == [30, 20, 30, 20]
    state = json.loads((tmp_path / "scheduler.json").read_text())
    assert state["status"] == "stopped" and state["cycle"] == 3
    assert state["next_run_at"] is None


def test_overrunning_job_skips_slots_instead_of_catchup_storm(tmp_path):
    clock = Clock()
    started = []

    def work():
        started.append(clock.seconds)
        clock.seconds += 135

    run_schedule(work, tmp_path, 1, 2, monotonic=clock.monotonic,
                 utc_now=clock.now, wait=clock.wait)
    assert started == [0, 180]
    assert clock.waits == [30, 15]


def test_failure_recovers_next_slot_and_reports_results(tmp_path):
    clock = Clock()
    calls = []
    events = []

    def work():
        calls.append(clock.seconds)
        if len(calls) == 1:
            raise RuntimeError("API unavailable")
        return {"loaded": 2}

    assert run_schedule(work, tmp_path, 1, 2, monotonic=clock.monotonic,
                        utc_now=clock.now, wait=clock.wait, on_result=events.append) == 0
    assert [e["status"] for e in events] == ["failed", "succeeded"]
    assert json.loads((tmp_path / "scheduler.json").read_text())["last_error"] is None


def test_final_failure_has_nonzero_exit(tmp_path):
    def fail():
        raise RuntimeError("failed")
    assert run_schedule(fail, tmp_path, max_runs=1) == 1


def test_stop_during_wait_does_not_start_another_run(tmp_path):
    clock = Clock()
    stop = threading.Event()
    calls = []
    run_schedule(lambda: calls.append(1), tmp_path, 1, stop_event=stop,
                 monotonic=clock.monotonic, utc_now=clock.now, wait=lambda _: stop.set())
    assert calls == [1]


@pytest.mark.parametrize("interval", [0, -1, float("nan"), float("inf"), 1e308, True])
def test_bad_intervals_fail_without_creating_worker_state(tmp_path, interval):
    with pytest.raises(ValueError):
        run_schedule(lambda: None, tmp_path, interval)
    assert not (tmp_path / "scheduler.json").exists()


def test_duplicate_worker_cannot_replace_running_state(tmp_path):
    marker = tmp_path / "scheduler.json"
    marker.write_text('{"owner": "first"}')
    with worker_lock(tmp_path):
        with pytest.raises(RuntimeError, match="Another scheduler"):
            run_schedule(lambda: None, tmp_path, max_runs=1)
    assert json.loads(marker.read_text()) == {"owner": "first"}


def test_suspended_monotonic_clock_catches_up_once_then_resumes_cadence(tmp_path):
    clock = Clock()
    wall_offset = 0
    started = []
    waits = []

    def now():
        return clock.now() + timedelta(seconds=wall_offset)

    def wait(duration):
        nonlocal wall_offset
        waits.append(duration)
        if len(waits) == 1:
            wall_offset += 7200  # UTC advances while the VM's clock is paused.
        else:
            clock.seconds += duration

    def work():
        started.append(clock.seconds)
        clock.seconds += 10

    run_schedule(work, tmp_path, 1, 3, monotonic=clock.monotonic, utc_now=now, wait=wait)
    assert started == [0, 10, 70]
    assert max(waits) <= 30
    state = json.loads((tmp_path / "scheduler.json").read_text())
    assert state["clock_catchup_count"] == 1


def test_backwards_utc_correction_cannot_postpone_monotonic_slot(tmp_path):
    clock = Clock()
    wall_offset = 0
    started = []

    def wait(duration):
        nonlocal wall_offset
        wall_offset = -3600
        clock.wait(duration)

    run_schedule(lambda: started.append(clock.seconds), tmp_path, 1, 2,
                 monotonic=clock.monotonic,
                 utc_now=lambda: clock.now() + timedelta(seconds=wall_offset), wait=wait)
    assert started == [0, 60]
    assert json.loads((tmp_path / "scheduler.json").read_text())["clock_catchup_count"] == 0


def test_both_clocks_advancing_skips_missed_slots_without_a_burst(tmp_path):
    clock = Clock()
    started = []
    waits = []

    def wait(duration):
        waits.append(duration)
        clock.seconds += 7200 if len(waits) == 1 else duration

    run_schedule(lambda: started.append(clock.seconds), tmp_path, 1, 3,
                 monotonic=clock.monotonic, utc_now=clock.now, wait=wait)
    assert started == [0, 7200, 7260]
    assert max(waits) <= 30
