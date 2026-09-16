"""Command-line entry points for running and operating the pipeline."""

from __future__ import annotations

import argparse
import json
import signal
import sys
import threading
import tomllib
from pathlib import Path

from weather_pipeline.pipeline import pipeline_lock, process_manifest, refresh_dashboard, run_pipeline, write_json


def parser():
    result = argparse.ArgumentParser(description="Real NWS data → PySpark → analytical warehouse")
    result.add_argument("--data-dir", default="data")
    result.add_argument("--settings", default="settings.toml")
    commands = result.add_subparsers(dest="command", required=True)
    run = commands.add_parser("run", help="Load recent or incremental observations")
    run.add_argument("--hours", type=int, help="Initial history only; existing stations use checkpoints")
    run.add_argument("--start", help="Explicit UTC backfill start; does not advance live checkpoints")
    run.add_argument("--end", help="Explicit UTC backfill end; does not advance live checkpoints")
    run.add_argument("--with-dashboard", action="store_true", help="Rebuild and audit Tableau after loading")
    replay = commands.add_parser("replay", help="Replay an immutable manifest without calling the API")
    replay.add_argument("manifest", type=Path)
    replay.add_argument("--with-dashboard", action="store_true")
    commands.add_parser("status", help="Show warehouse counts and checkpoints")
    commands.add_parser("export", help="Refresh the CSV datasets for Tableau or Power BI")
    commands.add_parser("dashboard", help="Build a portable native Tableau workbook from the warehouse")
    audit = commands.add_parser("audit-dashboard", help="Reconcile a packaged dashboard against the current warehouse")
    audit.add_argument("workbook", type=Path, nargs="?", help="Defaults to data-dir/tableau/weather_observatory.twbx")
    health = commands.add_parser("health", help="Check live freshness, station coverage, and unresolved failures")
    health.add_argument("--output", type=Path, help="Also save the JSON health report")
    backup = commands.add_parser("backup", help="Create a verified, consistent local backup")
    backup.add_argument("--output-dir", type=Path, default=Path("backups"))
    restore = commands.add_parser("restore", help="Restore a verified backup into a new data directory")
    restore.add_argument("archive", type=Path)
    restore.add_argument("--destination", type=Path, required=True)
    demo = commands.add_parser("demo", help="Run an offline recorded-data demonstration in a new directory")
    demo.add_argument("--output-dir", type=Path, default=Path("artifacts/demo"))
    schedule = commands.add_parser("schedule", help="Run repeatedly in the foreground")
    schedule.add_argument("--interval-minutes", type=float, default=60)
    schedule.add_argument("--max-runs", type=int, default=0, help="0 means continue until stopped")
    schedule.add_argument("--with-dashboard", action="store_true", help="Rebuild and audit Tableau after each load")
    return result


def main(argv=None):
    args = parser().parse_args(argv)
    data_dir = Path(args.data_dir).resolve()
    try:
        if args.command in {"run", "schedule"}:
            settings = tomllib.loads(Path(args.settings).read_text())
            if args.command == "run":
                output = run_pipeline(data_dir, settings, args.start, args.end, args.hours,
                                      with_dashboard=args.with_dashboard)
            else:
                from weather_pipeline.scheduler import run_schedule
                stop = threading.Event()
                previous = signal.getsignal(signal.SIGTERM)
                signal.signal(signal.SIGTERM, lambda *_: stop.set())
                try:
                    return run_schedule(
                        lambda: run_pipeline(data_dir, settings, with_dashboard=args.with_dashboard),
                        data_dir, args.interval_minutes, args.max_runs, stop_event=stop,
                        on_result=lambda event: print(json.dumps(event, default=str), flush=True),
                    )
                finally:
                    signal.signal(signal.SIGTERM, previous)
        elif args.command == "replay":
            manifest = json.loads(args.manifest.read_text())
            with pipeline_lock(data_dir):
                output = process_manifest(manifest, data_dir, with_dashboard=args.with_dashboard)
        elif args.command == "demo":
            from weather_pipeline.demo import run_demo
            output = run_demo(args.output_dir)
            print(json.dumps(output, indent=2, default=str))
            return 0 if output["status"] == "passed" else 1
        elif args.command == "health":
            from weather_pipeline.monitoring import check_health
            settings = tomllib.loads(Path(args.settings).read_text())
            output = check_health(data_dir, settings)
            if args.output:
                write_json(args.output, output)
            print(json.dumps(output, indent=2, default=str))
            return 0 if output["status"] == "healthy" else 1
        elif args.command == "backup":
            from weather_pipeline.backup import backup_data
            output = backup_data(data_dir, args.output_dir)
        elif args.command == "restore":
            from weather_pipeline.backup import restore_backup
            output = restore_backup(args.archive, args.destination)
        elif args.command == "audit-dashboard":
            from weather_pipeline.dashboard_audit import audit_dashboard
            with pipeline_lock(data_dir):
                output = audit_dashboard(data_dir / "warehouse.duckdb",
                                         args.workbook or data_dir / "tableau/weather_observatory.twbx")
            print(json.dumps(output, indent=2, default=str))
            return 0 if output["status"] == "passed" else 1
        elif args.command == "status":
            from weather_pipeline.warehouse import warehouse_summary
            output = warehouse_summary(data_dir / "warehouse.duckdb")
        else:
            from weather_pipeline.warehouse import export_dashboard
            with pipeline_lock(data_dir):
                output = export_dashboard(data_dir / "warehouse.duckdb", data_dir / "exports")
                if args.command == "dashboard":
                    output = refresh_dashboard(data_dir, output)
        print(json.dumps(output, indent=2, default=str))
        return 0
    except KeyboardInterrupt:
        print("Stopped.", file=sys.stderr)
        return 130
    except Exception as exc:
        print(f"Pipeline failed: {exc}", file=sys.stderr)
        return 1
