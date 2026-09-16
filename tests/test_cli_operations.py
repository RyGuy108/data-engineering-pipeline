"""User-visible operational exit codes and preservation on invalid requests."""

import json
import signal
from pathlib import Path

import pytest

from weather_pipeline import cli


@pytest.fixture
def settings(tmp_path):
    path = tmp_path / "settings.toml"
    path.write_text('stations = ["KAUS"]\nuser_agent = "test"\n')
    return path


def args(tmp_path, settings):
    return ["--data-dir", str(tmp_path / "data"), "--settings", str(settings)]


@pytest.mark.parametrize("status, expected_code", [("healthy", 0), ("warning", 1), ("critical", 1)])
def test_health_status_controls_exit_code_and_saved_report(tmp_path, settings, monkeypatch, capsys,
                                                         status, expected_code):
    report = {"status": status, "checks": [{"name": "freshness", "status": status}]}
    monkeypatch.setattr("weather_pipeline.monitoring.check_health", lambda *unused: report)
    output = tmp_path / "reports" / "health.json"
    result = cli.main([*args(tmp_path, settings), "health", "--output", str(output)])
    assert result == expected_code
    assert json.loads(capsys.readouterr().out) == report
    assert json.loads(output.read_text()) == report


def test_missing_warehouse_health_reports_failure_without_bootstrapping(tmp_path, settings, capsys):
    assert cli.main([*args(tmp_path, settings), "health"]) == 1
    report = json.loads(capsys.readouterr().out)
    assert report["status"] == "critical"
    assert "bootstrap" in report["checks"][0]["message"]
    assert not (tmp_path / "data").exists()


@pytest.mark.parametrize("status, expected_code", [("passed", 0), ("failed", 1)])
def test_dashboard_audit_status_controls_exit_code(tmp_path, settings, monkeypatch, capsys,
                                                  status, expected_code):
    observed = {}

    def audit(database, workbook):
        observed.update(database=database, workbook=workbook)
        return {"status": status, "checks": []}

    monkeypatch.setattr("weather_pipeline.dashboard_audit.audit_dashboard", audit)
    assert cli.main([*args(tmp_path, settings), "audit-dashboard"]) == expected_code
    assert json.loads(capsys.readouterr().out)["status"] == status
    assert observed["database"] == tmp_path / "data" / "warehouse.duckdb"
    assert observed["workbook"] == tmp_path / "data" / "tableau" / "weather_observatory.twbx"


@pytest.mark.parametrize("option, value", [
    ("--interval-minutes", "nan"), ("--interval-minutes", "inf"),
    ("--interval-minutes", "0"), ("--interval-minutes", "-1"),
    ("--interval-minutes", "1e308"), ("--max-runs", "-1"),
])
def test_invalid_schedule_options_preserve_state_and_never_run(tmp_path, settings, monkeypatch,
                                                              capsys, option, value):
    data_dir = tmp_path / "data"
    data_dir.mkdir()
    marker = data_dir / "scheduler.json"
    previous = b'{"status": "stopped", "cycle": 8}\n'
    marker.write_bytes(previous)
    calls = []
    monkeypatch.setattr(cli, "run_pipeline", lambda *a, **kw: calls.append((a, kw)))
    handler = signal.getsignal(signal.SIGTERM)
    request = [*args(tmp_path, settings), "schedule", "--max-runs", "1", option, value]
    assert cli.main(request) == 1
    assert not calls
    assert marker.read_bytes() == previous
    assert signal.getsignal(signal.SIGTERM) == handler
    assert "Pipeline failed:" in capsys.readouterr().err


@pytest.mark.parametrize("failure, expected_code", [(RuntimeError("API unavailable"), 1),
                                                    (KeyboardInterrupt(), 130)])
def test_schedule_errors_restore_signal_handler(tmp_path, settings, monkeypatch, capsys,
                                               failure, expected_code):
    def work(*unused, **options):
        raise failure

    monkeypatch.setattr(cli, "run_pipeline", work)
    handler = signal.getsignal(signal.SIGTERM)
    assert cli.main([*args(tmp_path, settings), "schedule", "--max-runs", "1"]) == expected_code
    assert signal.getsignal(signal.SIGTERM) == handler
    assert json.loads((tmp_path / "data" / "scheduler.json").read_text())["status"] == "stopped"
    output = capsys.readouterr()
    if expected_code == 1:
        assert json.loads(output.out)["status"] == "failed"
    else:
        assert "Stopped." in output.err


def test_scheduler_sigterm_finishes_active_cycle_without_starting_another(tmp_path, settings,
                                                                       monkeypatch, capsys):
    calls = []

    def work(*unused, **options):
        calls.append(options)
        # The CLI's installed handler requests a graceful stop. It must not
        # terminate the current operation or leave the handler installed.
        signal.raise_signal(signal.SIGTERM)
        return {"completed": True}

    monkeypatch.setattr(cli, "run_pipeline", work)
    handler = signal.getsignal(signal.SIGTERM)
    assert cli.main([*args(tmp_path, settings), "schedule", "--with-dashboard"]) == 0
    assert calls == [{"with_dashboard": True}]
    assert signal.getsignal(signal.SIGTERM) == handler
    assert json.loads(capsys.readouterr().out)["result"]["completed"] is True
    state = json.loads((tmp_path / "data" / "scheduler.json").read_text())
    assert state["status"] == "stopped" and state["cycle"] == 1


def test_backup_dispatch_does_not_require_source_settings(tmp_path, monkeypatch, capsys):
    observed = {}

    def backup(data_dir, output_dir):
        observed.update(data_dir=data_dir, output_dir=output_dir)
        return {"status": "backed_up", "archive_path": "verified.zip"}

    monkeypatch.setattr("weather_pipeline.backup.backup_data", backup)
    backup_root = tmp_path / "snapshots"
    assert cli.main([*args(tmp_path, tmp_path / "absent.toml"), "backup",
                     "--output-dir", str(backup_root)]) == 0
    assert observed == {"data_dir": tmp_path / "data", "output_dir": backup_root}
    assert json.loads(capsys.readouterr().out)["status"] == "backed_up"


def test_restore_dispatch_uses_explicit_destination(tmp_path, monkeypatch, capsys):
    observed = {}

    def restore(archive, destination):
        observed.update(archive=archive, destination=destination)
        return {"status": "restored", "data_dir": str(destination)}

    monkeypatch.setattr("weather_pipeline.backup.restore_backup", restore)
    destination = tmp_path / "restored"
    archive = tmp_path / "verified.zip"
    assert cli.main([*args(tmp_path, tmp_path / "absent.toml"), "restore", str(archive),
                     "--destination", str(destination)]) == 0
    assert observed == {"archive": archive, "destination": destination}
    assert Path(json.loads(capsys.readouterr().out)["data_dir"]) == destination


def test_restore_refuses_nonempty_destination_without_changing_it(tmp_path, capsys):
    destination = tmp_path / "restored"
    destination.mkdir()
    marker = destination / "keep.txt"
    marker.write_text("existing records")
    assert cli.main(["restore", str(tmp_path / "absent.zip"),
                     "--destination", str(destination)]) == 1
    assert marker.read_text() == "existing records"
    assert list(destination.iterdir()) == [marker]
    assert "existing data is never overwritten" in capsys.readouterr().err
