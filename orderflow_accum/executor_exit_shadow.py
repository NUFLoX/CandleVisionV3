from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime

POLICY_TRAILING_40PCT_GIVEBACK_AFTER_1R = "trailing_40pct_giveback_after_1r"
DEFAULT_EXIT_SHADOW_POLICY = POLICY_TRAILING_40PCT_GIVEBACK_AFTER_1R


@dataclass(frozen=True)
class ExitShadowEvaluation:
    policy: str
    peak_r: float
    floor_r: float | None
    current_r: float | None
    triggered: bool
    exit_r: float | None


def utc_now_iso() -> str:
    return datetime.now(UTC).isoformat()


def current_unrealized_r(*, side: str, current_price: float, entry_price: float, initial_sl: float) -> float | None:
    risk = abs(float(entry_price) - float(initial_sl))
    if risk <= 0:
        return None
    if str(side).strip().lower() in {"sell", "short"}:
        return (float(entry_price) - float(current_price)) / risk
    return (float(current_price) - float(entry_price)) / risk


def evaluate_exit_shadow_policy(
    *,
    policy: str = DEFAULT_EXIT_SHADOW_POLICY,
    previous_peak_r: float | None = None,
    observed_max_gain_r: float | None = None,
    current_r: float | None = None,
) -> ExitShadowEvaluation:
    """Evaluate diagnostic-only exit shadow policy.

    This pure function never returns executable actions and does not mutate trade
    state. It only describes what the selected policy would have observed.
    """
    policy_id = str(policy or DEFAULT_EXIT_SHADOW_POLICY).strip() or DEFAULT_EXIT_SHADOW_POLICY
    if policy_id != POLICY_TRAILING_40PCT_GIVEBACK_AFTER_1R:
        policy_id = DEFAULT_EXIT_SHADOW_POLICY

    peak_candidates = [0.0]
    for value in (previous_peak_r, observed_max_gain_r):
        try:
            if value is not None:
                peak_candidates.append(float(value))
        except (TypeError, ValueError):
            continue
    peak_r = max(peak_candidates)

    if peak_r < 1.0:
        return ExitShadowEvaluation(
            policy=policy_id,
            peak_r=peak_r,
            floor_r=None,
            current_r=current_r,
            triggered=False,
            exit_r=None,
        )

    floor_r = peak_r * 0.60
    current = None if current_r is None else float(current_r)
    triggered = current is not None and current <= floor_r
    return ExitShadowEvaluation(
        policy=policy_id,
        peak_r=peak_r,
        floor_r=floor_r,
        current_r=current,
        triggered=triggered,
        exit_r=floor_r if triggered else None,
    )
