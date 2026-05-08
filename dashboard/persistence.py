from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any

DEFAULT_DASHBOARD_STATE_PATH = "data/dashboard_state.json"


def dashboard_state_path() -> Path:
    return Path(os.getenv("DASHBOARD_STATE_PATH", DEFAULT_DASHBOARD_STATE_PATH))


def read_state(path: Path | None = None) -> dict[str, Any]:
    state_path = path or dashboard_state_path()
    if not state_path.exists():
        return {}
    try:
        return json.loads(state_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}


def write_state(payload: dict[str, Any], path: Path | None = None) -> None:
    state_path = path or dashboard_state_path()
    state_path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = state_path.with_suffix(f"{state_path.suffix}.tmp")
    tmp_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    tmp_path.replace(state_path)
