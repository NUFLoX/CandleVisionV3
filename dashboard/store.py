from __future__ import annotations

import asyncio
from collections import deque
from copy import deepcopy
from datetime import datetime, timezone
from typing import TypeVar

from .persistence import dashboard_state_path, read_state, write_state
from .schemas import (
    BotLog,
    BotStatus,
    CoinMetrics,
    DashboardSnapshot,
    Heartbeat,
    MarketState,
    PressureStrip,
    Signal,
    Trade,
    WatchlistItem,
)

T = TypeVar("T")


def _now() -> datetime:
    return datetime.now(timezone.utc)


class DashboardStore:
    """Small live data hub for the MVP dashboard.

    The bot can push events into this store through the ingest endpoints. The
    implementation persists a compact JSON snapshot so ingested events survive
    restarts while keeping the API contract stable for a future PostgreSQL/Redis
    backend.
    """

    def __init__(self) -> None:
        self._lock = asyncio.Lock()
        self._state_path = dashboard_state_path()
        self.status = BotStatus(
            scanner="standby",
            executor="standby",
            telegram="not-checked",
            x_delivery="standby",
            bybit_api="not-checked",
            binance_api="not-configured",
            database="not-checked",
            redis="not-checked",
            rate_limit="not-checked",
            last_scan_seconds=0,
            open_trades=0,
            closed_trades_today=0,
        )
        self.market_state = MarketState(
            btc_filter="UNKNOWN",
            altcoin_mode="WAITING",
            liquidity="UNKNOWN",
            market_regime="UNKNOWN",
            usdt_dominance_trend="unknown",
            total3_strength="unknown",
            can_emit_alt_signals=False,
        )
        self.pressure_strips: list[PressureStrip] = []
        self.signals: deque[Signal] = deque(maxlen=300)
        self.logs: deque[BotLog] = deque(maxlen=500)
        self.watchlist: deque[WatchlistItem] = deque(maxlen=200)
        self.trades: deque[Trade] = deque(maxlen=300)
        self.coin_metrics: dict[str, CoinMetrics] = {}
        self.heartbeats: dict[str, Heartbeat] = {}
        self._load_persisted_state()
        if not self.logs:
            self.logs.appendleft(
                BotLog(
                    timestamp=_now(),
                    message="Dashboard started without demo data; waiting for live feeds or bot ingest.",
                    source="dashboard",
                    severity="info",
                )
            )

    async def snapshot(self) -> DashboardSnapshot:
        async with self._lock:
            return DashboardSnapshot(
                status=deepcopy(self.status),
                market_state=deepcopy(self.market_state),
                pressure_strips=list(deepcopy(self.pressure_strips)),
                signals=list(deepcopy(self.signals)),
                logs=list(deepcopy(self.logs)),
                watchlist=list(deepcopy(self.watchlist)),
                trades=list(deepcopy(self.trades)),
                heartbeats=deepcopy(self.heartbeats),
                meta={"mode": "persistent-json", "state_path": str(self._state_path), "updated_at": _now().isoformat()},
            )

    async def list_signals(
        self,
        strength: str | None = None,
        signal_type: str | None = None,
        exchange: str | None = None,
        timeframe: str | None = None,
    ) -> list[Signal]:
        snapshot = await self.snapshot()
        signals = snapshot.signals
        if strength:
            signals = [s for s in signals if s.strength.value.lower() == strength.lower()]
        if signal_type:
            signals = [s for s in signals if s.signal_type.value.lower() == signal_type.lower()]
        if exchange:
            signals = [s for s in signals if s.exchange.lower() == exchange.lower()]
        if timeframe:
            signals = [s for s in signals if s.timeframe.lower() == timeframe.lower()]
        return signals

    async def coin(self, raw_symbol: str) -> CoinMetrics:
        symbol = normalize_symbol(raw_symbol)
        async with self._lock:
            cached = deepcopy(self.coin_metrics.get(symbol))
        if cached:
            return cached

        from .live_data import fetch_live_coin_metrics

        metric = await fetch_live_coin_metrics(symbol)
        async with self._lock:
            self.coin_metrics[symbol] = metric
            self._save_persisted_state_locked()
        return deepcopy(metric)

    async def add_log(self, log: BotLog) -> BotLog:
        async with self._lock:
            self.logs.appendleft(log)
            self._save_persisted_state_locked()
        return log

    async def add_signal(self, signal: Signal) -> Signal:
        async with self._lock:
            self.signals.appendleft(signal)
            self.status.last_scan_seconds = 0
            self._save_persisted_state_locked()
        return signal

    async def add_watchlist_item(self, item: WatchlistItem) -> WatchlistItem:
        async with self._lock:
            self.watchlist.appendleft(item)
            self._save_persisted_state_locked()
        return item

    async def add_trade(self, trade: Trade) -> Trade:
        async with self._lock:
            if trade.id is None:
                max_id = max((item.id or 0 for item in self.trades), default=0)
                trade.id = max_id + 1
            self.trades.appendleft(trade)
            self.status.open_trades = sum(1 for item in self.trades if item.status.lower() == "open")
            self._save_persisted_state_locked()
        return trade

    async def update_market_state(self, market_state: MarketState) -> MarketState:
        async with self._lock:
            market_state.updated_at = _now()
            self.market_state = market_state
            self._save_persisted_state_locked()
        return market_state

    async def add_heartbeat(self, heartbeat: Heartbeat) -> Heartbeat:
        component = heartbeat.component.strip().lower()
        heartbeat.component = component
        async with self._lock:
            self.heartbeats[component] = heartbeat
            if component == "scanner":
                self.status.scanner = heartbeat.status
            elif component == "executor":
                self.status.executor = heartbeat.status
            self._save_persisted_state_locked()
        return heartbeat

    async def refresh_live_data(self) -> None:
        from .health import build_health_status
        from .live_data import fetch_live_dashboard_data

        live_data = await fetch_live_dashboard_data()
        async with self._lock:
            heartbeats = deepcopy(self.heartbeats)
        health_status = await build_health_status(live_data.status, heartbeats)
        async with self._lock:
            self.market_state = live_data.market_state
            self.pressure_strips = live_data.pressure_strips
            self.coin_metrics.update(live_data.coin_metrics)
            self.status = health_status
            for log in reversed(live_data.logs):
                self.logs.appendleft(log)
            self._save_persisted_state_locked()

    def _load_persisted_state(self) -> None:
        payload = read_state(self._state_path)
        if not payload:
            return
        self.status = _parse_model(BotStatus, payload.get("status"), self.status)
        self.market_state = _parse_model(MarketState, payload.get("market_state"), self.market_state)
        self.pressure_strips = _parse_list(PressureStrip, payload.get("pressure_strips"))
        self.signals = deque(_parse_list(Signal, payload.get("signals")), maxlen=300)
        self.logs = deque(_parse_list(BotLog, payload.get("logs")), maxlen=500)
        self.watchlist = deque(_parse_list(WatchlistItem, payload.get("watchlist")), maxlen=200)
        self.trades = deque(_parse_list(Trade, payload.get("trades")), maxlen=300)
        self.coin_metrics = {
            symbol: metric
            for symbol, metric in (
                (symbol, _parse_model(CoinMetrics, value, None))
                for symbol, value in (payload.get("coin_metrics") or {}).items()
            )
            if metric is not None
        }
        self.heartbeats = {
            component: heartbeat
            for component, heartbeat in (
                (component, _parse_model(Heartbeat, value, None))
                for component, value in (payload.get("heartbeats") or {}).items()
            )
            if heartbeat is not None
        }

    def _save_persisted_state_locked(self) -> None:
        write_state(
            {
                "status": _dump_model(self.status),
                "market_state": _dump_model(self.market_state),
                "pressure_strips": [_dump_model(item) for item in self.pressure_strips],
                "signals": [_dump_model(item) for item in self.signals],
                "logs": [_dump_model(item) for item in self.logs],
                "watchlist": [_dump_model(item) for item in self.watchlist],
                "trades": [_dump_model(item) for item in self.trades],
                "coin_metrics": {symbol: _dump_model(metric) for symbol, metric in self.coin_metrics.items()},
                "heartbeats": {component: _dump_model(heartbeat) for component, heartbeat in self.heartbeats.items()},
                "updated_at": _now().isoformat(),
            },
            self._state_path,
        )


def normalize_symbol(symbol: str) -> str:
    value = symbol.strip().upper().replace("/", "")
    if not value.endswith("USDT") and value not in {"BTC", "ETH"}:
        value = f"{value}USDT"
    if value == "BTC":
        value = "BTCUSDT"
    if value == "ETH":
        value = "ETHUSDT"
    return value


def _parse_list(model: type[T], raw_items: object) -> list[T]:
    if not isinstance(raw_items, list):
        return []
    parsed = [_parse_model(model, item, None) for item in raw_items]
    return [item for item in parsed if item is not None]


def _parse_model(model: type[T], raw: object, default: T | None) -> T | None:
    if raw is None:
        return default
    try:
        if hasattr(model, "model_validate"):
            return model.model_validate(raw)
        return model.parse_obj(raw)
    except Exception:
        return default


def _dump_model(model: object) -> dict:
    if hasattr(model, "model_dump"):
        return model.model_dump(mode="json")
    return model.dict()
