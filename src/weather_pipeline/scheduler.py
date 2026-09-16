"""A single-worker scheduler with observable state and interruptible shutdown."""

from __future__ import annotations

import fcntl
import math
import os
import threading
import time
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
from pathlib import Path

from weather_pipeline.pipeline import write_json


@contextmanager
def worker_lock(data_dir):
    """Hold a separate lifetime lock while allowing backup between data loads."""
    with (Path(data_dir) / ".scheduler.lock").open("a+") as handle:
        try:
            fcntl.flock(handle, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as exc:
            raise RuntimeError("Another scheduler already owns this data directory") from exc
        try:
            yield
        finally:
            fcntl.flock(handle, fcntl.LOCK_UN)


def run_schedule(run_once, data_dir, interval_minutes=60, max_runs=0, *,
                 stop_event=None, monotonic=time.monotonic, utc_now=None,
                 wait=None, on_result=None):
    """Run immediately, then on a fixed cadence; skip missed slots, never overlap.

    The injectable clock/wait functions let tests exercise multiple hours without
    sleeping. The last cycle determines the finite-run return code. Failures do
    not stop an unbounded worker; health checks report their operational impact.
    """
    if (isinstance(interval_minutes, bool) or not isinstance(interval_minutes, (int, float))
            or not math.isfinite(interval_minutes) or interval_minutes <= 0):
        raise ValueError("interval_minutes must be finite and positive")
    if isinstance(max_runs, bool) or not isinstance(max_runs, int) or max_runs < 0:
        raise ValueError("max_runs must be a nonnegative integer")
    period = interval_minutes * 60
    if not math.isfinite(period) or period > threading.TIMEOUT_MAX:
        raise ValueError("interval_minutes exceeds the platform's supported wait duration")
    data_dir = Path(data_dir).resolve()
    data_dir.mkdir(parents=True, exist_ok=True)
    stop_event = stop_event or threading.Event()
    utc_now = utc_now or (lambda: datetime.now(timezone.utc))
    wait = wait or stop_event.wait
    state = {"version": 1, "pid": os.getpid(), "status": "starting", "cycle": 0,
             "interval_seconds": period, "started_at": utc_now().isoformat(),
             "last_success_at": None, "last_error": None, "next_run_at": None,
             "clock_catchup_count": 0}
    state_path = data_dir / "scheduler.json"
    exit_code = 0
    with worker_lock(data_dir):
        deadline = monotonic()
        try:
            while not stop_event.is_set():
                state.update(status="running", cycle=state["cycle"] + 1,
                             cycle_started_at=utc_now().isoformat(), next_run_at=None)
                write_json(state_path, state)
                try:
                    result = run_once()
                    exit_code = 0
                    state.update(last_success_at=utc_now().isoformat(), last_error=None)
                    if on_result:
                        on_result({"status": "succeeded", "cycle": state["cycle"], "result": result})
                except Exception as exc:
                    exit_code = 1
                    state["last_error"] = str(exc)
                    if on_result:
                        on_result({"status": "failed", "cycle": state["cycle"], "error": str(exc)})
                state["last_completed_at"] = utc_now().isoformat()
                if stop_event.is_set() or (max_runs and state["cycle"] >= max_runs):
                    break
                deadline += period
                current = monotonic()
                skipped = 0
                if deadline < current:
                    skipped = math.floor((current - deadline) / period) + 1
                    deadline += skipped * period
                remaining = max(0.0, deadline - current)
                next_run = utc_now() + timedelta(seconds=remaining)
                state.update(status="waiting", skipped_slots=skipped,
                             next_run_at=next_run.isoformat())
                write_json(state_path, state)
                while not stop_event.is_set():
                    remaining = deadline - monotonic()
                    if remaining <= 0:
                        break
                    wall_remaining = (next_run - utc_now()).total_seconds()
                    if wall_remaining <= 0:
                        # Some host/VM clocks pause during suspend. Catch up once
                        # when UTC is overdue, then anchor a fresh normal cadence.
                        deadline = monotonic()
                        state["clock_catchup_count"] += 1
                        break
                    # A bounded wait notices resume or clock correction promptly;
                    # a backwards UTC correction cannot delay the monotonic slot.
                    wait(min(30.0, remaining, wall_remaining))
        finally:
            state.update(status="stopped", stopped_at=utc_now().isoformat(), next_run_at=None)
            write_json(state_path, state)
    return exit_code
