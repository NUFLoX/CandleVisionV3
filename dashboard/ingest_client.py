from __future__ import annotations

import asyncio
import json
import logging
import os
from datetime import datetime, timezone
from typing import Any
from urllib.parse import urljoin
from urllib.request import Request, urlopen
from uuid import uuid4

logger = logging.getLogger("CandleVision.DashboardIngest")


class DashboardIngestClient:
    """Small non-blocking client for pushing real bot events into the dashboard API."""

    def __init__(self, base_url: str | None = None, timeout_seconds: float | None = None) -> None:
        self.base_url = (base_url if base_url is not None else os.getenv("DASHBOARD_API_URL", "")).strip().rstrip("/")
        self.timeout_seconds = float(timeout_seconds or os.getenv("DASHBOARD_INGEST_TIMEOUT", "2.5"))
        self.ingest_token = os.getenv("DASHBOARD_INGEST_TOKEN", "").strip()
        self.enabled = bool(self.base_url)

    async def post_signal(self, signal: Any, *, exchange: str = "Bybit", timeframe: str = "live") -> None:
        if not self.enabled:
            return
        payload = signal_to_dashboard_payload(signal, exchange=exchange, timeframe=timeframe)
        await self._post("/api/ingest/signal", payload)

    async def post_watchlist(
        self,
        symbol: str,
        *,
        exchange: str = "Bybit",
        timeframe: str = "1m",
        score: float = 0.0,
        reason: str = "scanner watchlist",
        expires_in_hours: int = 24,
    ) -> None:
        if not self.enabled:
            return
        await self._post(
            "/api/ingest/watchlist",
            {
                "symbol": symbol,
                "exchange": exchange,
                "timeframe": timeframe,
                "score": score,
                "reason": reason,
                "expires_in_hours": expires_in_hours,
            },
        )

    async def post_trade(self, trade: dict[str, Any]) -> None:
        if not self.enabled:
            return
        await self._post(
            "/api/ingest/trade",
            {
                "id": trade.get("id"),
                "symbol": trade.get("symbol", "UNKNOWN"),
                "timeframe": trade.get("timeframe", "1m"),
                "entry": float(trade.get("entry", 0.0) or 0.0),
                "stop_loss": float(trade.get("sl", trade.get("stop_loss", 0.0)) or 0.0),
                "take_profit": float(trade.get("tp", trade.get("take_profit", 0.0)) or 0.0),
                "status": trade.get("status", "open"),
                "pnl_pct": float(trade.get("pnl_pct", 0.0) or 0.0),
            },
        )

    async def post_log(self, message: str, *, source: str = "bot", severity: str = "info") -> None:
        if not self.enabled:
            return
        await self._post(
            "/api/ingest/log",
            {
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "message": message,
                "source": source,
                "severity": severity,
            },
        )

    async def post_heartbeat(self, component: str, *, status: str = "online", meta: dict[str, Any] | None = None) -> None:
        if not self.enabled:
            return
        await self._post(
            "/api/ingest/heartbeat",
            {
                "component": component,
                "status": status,
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "meta": meta or {},
            },
        )

    async def _post(self, path: str, payload: dict[str, Any]) -> None:
        try:
            await asyncio.to_thread(self._post_sync, path, payload)
        except Exception as exc:
            logger.debug("Dashboard ingest failed for %s: %r", path, exc)

    def _post_sync(self, path: str, payload: dict[str, Any]) -> None:
        url = urljoin(f"{self.base_url}/", path.lstrip("/"))
        headers = {"Content-Type": "application/json", "User-Agent": "CandleVisionDashboardIngest/1.0"}
        if self.ingest_token:
            headers["Authorization"] = f"Bearer {self.ingest_token}"
        request = Request(
            url,
            data=json.dumps(payload).encode("utf-8"),
            headers=headers,
            method="POST",
        )
        with urlopen(request, timeout=self.timeout_seconds) as response:
            response.read()


def signal_to_dashboard_payload(signal: Any, *, exchange: str = "Bybit", timeframe: str = "live") -> dict[str, Any]:
    score = float(getattr(signal, "score", 0.0) or 0.0)
    kind = str(getattr(signal, "kind", "SIGNAL"))
    source = str(getattr(signal, "source", "scanner"))
    side = str(getattr(signal, "side", ""))
    reasons = list(getattr(signal, "reasons", []) or [])
    meta = dict(getattr(signal, "meta", {}) or {})
    return {
        "id": f"{source}-{kind}-{getattr(signal, 'symbol', 'UNKNOWN')}-{uuid4().hex[:10]}",
        "symbol": str(getattr(signal, "symbol", "UNKNOWN")),
        "exchange": exchange,
        "timeframe": _timeframe_from_signal(source, kind, timeframe),
        "score": score,
        "strength": _strength_from_score(score),
        "signal_type": _signal_type_from_signal(source, kind),
        "entry": float(getattr(signal, "entry", 0.0) or 0.0),
        "stop_loss": float(getattr(signal, "stop_loss", 0.0) or 0.0),
        "take_profit_1": float(getattr(signal, "take_profit_1", 0.0) or 0.0),
        "take_profit_2": float(getattr(signal, "take_profit_2", 0.0) or 0.0),
        "reason": _reason_text(side, kind, source, reasons, meta),
        "status": "ACTIVE",
        "created_at": datetime.now(timezone.utc).isoformat(),
    }


def _strength_from_score(score: float) -> str:
    if score >= 7.5:
        return "Strong"
    if score >= 5.0:
        return "Medium"
    return "Weak"


def _signal_type_from_signal(source: str, kind: str) -> str:
    text = f"{source} {kind}".upper()
    if "EARLY" in text or "BASE" in text:
        return "Watchlist"
    if "BREAKOUT" in text or "READY" in text or "CONFIRMED" in text:
        return "Confirmed"
    return "Aggressive" if "ORDERFLOW" in text else "Watchlist"


def _timeframe_from_signal(source: str, kind: str, fallback: str) -> str:
    text = f"{source} {kind}".upper()
    if "MACRO" in text:
        return "4h"
    if fallback and fallback != "live":
        return fallback
    return "1m"


def _reason_text(side: str, kind: str, source: str, reasons: list[Any], meta: dict[str, Any]) -> str:
    chunks = [f"{kind} · {source} · {side}".strip(" ·")]
    chunks.extend(str(item) for item in reasons[:6])
    if meta:
        chunks.append(", ".join(f"{key}={value}" for key, value in list(meta.items())[:6]))
    return " | ".join(chunk for chunk in chunks if chunk)
