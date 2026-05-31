import datetime
import os
import subprocess
import sys
import time

DEFAULT_POLL_SECONDS = 30
DEFAULT_LEARNING_EVERY_MINUTES = 60
DEFAULT_LEARNING_DB = "data\\signals.db"
DEFAULT_LEARNING_OUT_DIR = "reports_learning"
DEFAULT_LEARNING_SINCE_HOURS = 24
DEFAULT_LEARNING_MIN_SAMPLE = 5
DEFAULT_WATCHLIST_AT = "23:30"
DEFAULT_WATCHLIST_DB = "data\\signals.db"
DEFAULT_WATCHLIST_OUT_DIR = "reports_watchlist"


def env_bool(name, default):
    raw = os.environ.get(name)
    if raw is None or raw.strip() == "":
        return default
    return raw.strip().lower() in ("1", "true", "yes", "on")


def env_int(name, default, minimum=None):
    raw = os.environ.get(name)
    if raw is None or raw.strip() == "":
        return default
    try:
        value = int(raw)
    except ValueError:
        return default
    if minimum is not None and value < minimum:
        return default
    return value


def env_text(name, default):
    raw = os.environ.get(name)
    if raw is None or raw.strip() == "":
        return default
    return raw.strip()


def parse_hhmm(value):
    text = (value or "").strip()
    parts = text.split(":")
    if len(parts) != 2:
        raise ValueError("schedule time must be HH:MM")
    try:
        hour = int(parts[0])
        minute = int(parts[1])
    except ValueError as exc:
        raise ValueError("schedule time must be HH:MM") from exc
    if hour < 0 or hour > 23 or minute < 0 or minute > 59:
        raise ValueError("schedule time must be a valid local HH:MM")
    return hour, minute


def same_minute(left, right):
    if left is None or right is None:
        return False
    return left.replace(second=0, microsecond=0) == right.replace(second=0, microsecond=0)


def learning_report_due(now, last_run_at, every_minutes):
    if every_minutes <= 0:
        return False
    if last_run_at is None:
        return True
    if same_minute(now, last_run_at):
        return False
    elapsed = (now - last_run_at).total_seconds()
    return elapsed >= every_minutes * 60


def watchlist_run_key(now, at_text):
    return now.strftime("%Y-%m-%d") + " " + at_text


def watchlist_report_due(now, at_text, last_run_key):
    hour, minute = parse_hhmm(at_text)
    if now.hour != hour or now.minute != minute:
        return False
    key = watchlist_run_key(now, at_text)
    return key != last_run_key


def learning_report_command():
    return [
        sys.executable,
        "-m",
        "tools.learning_report",
        "--db",
        env_text("LEARNING_REPORT_DB", DEFAULT_LEARNING_DB),
        "--out-dir",
        env_text("LEARNING_REPORT_OUT_DIR", DEFAULT_LEARNING_OUT_DIR),
        "--since-hours",
        str(env_int("LEARNING_REPORT_SINCE_HOURS", DEFAULT_LEARNING_SINCE_HOURS, 1)),
        "--min-sample",
        str(env_int("LEARNING_REPORT_MIN_SAMPLE", DEFAULT_LEARNING_MIN_SAMPLE, 1)),
    ]


def watchlist_report_command():
    return [
        sys.executable,
        os.path.join(".", "tools", "watchlist_transition_study.py"),
        "--db",
        env_text("WATCHLIST_REPORT_DB", DEFAULT_WATCHLIST_DB),
        "--out-dir",
        env_text("WATCHLIST_REPORT_OUT_DIR", DEFAULT_WATCHLIST_OUT_DIR),
    ]


def run_report(label, command):
    print(label + " report started", flush=True)
    try:
        subprocess.run(command, check=True)
    except subprocess.CalledProcessError as exc:
        print("report failed: " + label + " exit_code=" + str(exc.returncode), flush=True)
        return False
    except OSError as exc:
        print("report failed: " + label + " " + str(exc), flush=True)
        return False
    print(label + " report finished", flush=True)
    return True


def scheduler_loop():
    poll_seconds = env_int("REPORT_SCHEDULER_POLL_SECONDS", DEFAULT_POLL_SECONDS, 1)
    schedule_learning = env_bool("SCHEDULE_LEARNING_REPORT", True)
    learning_every_minutes = env_int(
        "LEARNING_REPORT_EVERY_MINUTES", DEFAULT_LEARNING_EVERY_MINUTES, 1
    )
    schedule_watchlist = env_bool("SCHEDULE_WATCHLIST_REPORT", True)
    watchlist_at = env_text("WATCHLIST_REPORT_AT", DEFAULT_WATCHLIST_AT)

    try:
        parse_hhmm(watchlist_at)
    except ValueError:
        print(
            "report failed: watchlist invalid WATCHLIST_REPORT_AT="
            + watchlist_at
            + "; using "
            + DEFAULT_WATCHLIST_AT,
            flush=True,
        )
        watchlist_at = DEFAULT_WATCHLIST_AT

    print("scheduler started", flush=True)
    last_learning_run_at = None
    last_watchlist_run_key = None

    while True:
        now = datetime.datetime.now()
        if schedule_learning and learning_report_due(
            now, last_learning_run_at, learning_every_minutes
        ):
            last_learning_run_at = now
            run_report("learning", learning_report_command())

        if schedule_watchlist and watchlist_report_due(
            now, watchlist_at, last_watchlist_run_key
        ):
            last_watchlist_run_key = watchlist_run_key(now, watchlist_at)
            run_report("watchlist", watchlist_report_command())

        time.sleep(poll_seconds)


def main():
    scheduler_loop()


if __name__ == "__main__":
    main()
