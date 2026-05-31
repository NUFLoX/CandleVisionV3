import datetime
import subprocess

import tools.report_scheduler as scheduler


def dt(year, month, day, hour, minute, second=0):
    return datetime.datetime(year, month, day, hour, minute, second)


def test_parse_hhmm_schedule_correctly() -> None:
    assert scheduler.parse_hhmm("23:30") == (23, 30)
    assert scheduler.parse_hhmm("00:05") == (0, 5)


def test_parse_hhmm_rejects_invalid_schedule() -> None:
    for value in ("24:00", "23:60", "bad", "12"):
        try:
            scheduler.parse_hhmm(value)
        except ValueError:
            pass
        else:
            raise AssertionError(f"expected invalid schedule for {value!r}")


def test_learning_report_due_logic_uses_interval_and_avoids_same_minute() -> None:
    last = dt(2026, 5, 31, 10, 0, 15)

    assert scheduler.learning_report_due(dt(2026, 5, 31, 10, 0, 55), last, 60) is False
    assert scheduler.learning_report_due(dt(2026, 5, 31, 10, 59, 59), last, 60) is False
    assert scheduler.learning_report_due(dt(2026, 5, 31, 11, 0, 15), last, 60) is True
    assert scheduler.learning_report_due(dt(2026, 5, 31, 11, 1, 0), last, 60) is True


def test_learning_report_due_when_never_run_before() -> None:
    assert scheduler.learning_report_due(dt(2026, 5, 31, 10, 0), None, 60) is True


def test_watchlist_report_due_runs_once_per_day_for_schedule() -> None:
    scheduled = dt(2026, 5, 31, 23, 30, 10)
    run_key = scheduler.watchlist_run_key(scheduled, "23:30")

    assert scheduler.watchlist_report_due(dt(2026, 5, 31, 23, 29, 59), "23:30", None) is False
    assert scheduler.watchlist_report_due(scheduled, "23:30", None) is True
    assert scheduler.watchlist_report_due(dt(2026, 5, 31, 23, 30, 45), "23:30", run_key) is False
    assert scheduler.watchlist_report_due(dt(2026, 6, 1, 23, 30, 0), "23:30", run_key) is True


def test_failed_subprocess_does_not_crash_scheduler_helper(monkeypatch, capsys) -> None:
    def fail_run(command, check):
        raise subprocess.CalledProcessError(2, command)

    monkeypatch.setattr(scheduler.subprocess, "run", fail_run)

    assert scheduler.run_report("learning", ["python", "-m", "tools.learning_report"]) is False
    output = capsys.readouterr().out
    assert "learning report started" in output
    assert "report failed" in output
