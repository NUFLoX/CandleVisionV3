from __future__ import annotations

import asyncio
import hashlib
import hmac
import json
import logging
import os
import time
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal, ROUND_DOWN, InvalidOperation
from typing import Any, Protocol
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen

BYBIT_TESTNET_BASE_URL = "https://api-testnet.bybit.com"
ENTRY_BLOCKED_INSUFFICIENT_TESTNET_BALANCE = "entry_blocked_insufficient_testnet_balance"
ENTRY_BLOCKED_TESTNET_POSITION_LIMIT = "entry_blocked_testnet_position_limit"
ENTRY_BLOCKED_TESTNET_DAILY_ORDER_LIMIT = "entry_blocked_testnet_daily_order_limit"
ENTRY_BLOCKED_TESTNET_QTY_TOO_SMALL = "entry_blocked_testnet_qty_too_small"
ENTRY_BLOCKED_TESTNET_DISABLED = "entry_blocked_testnet_disabled"
ENTRY_BLOCKED_TESTNET_DUPLICATE = "entry_blocked_testnet_duplicate"
ENTRY_BLOCKED_TESTNET_MAINNET_GUARD = "entry_blocked_testnet_mainnet_guard"
ENTRY_FAILED_TESTNET_API = "entry_failed_testnet_api"

logger = logging.getLogger("OrderFlow.BybitTestnetExecutor")


class TestnetOrderStore(Protocol):
    def insert_testnet_order(self, order: dict[str, Any]) -> dict[str, Any]: ...
    def get_testnet_order_by_signal(self, signal_key: str) -> dict[str, Any] | None: ...
    def count_open_testnet_positions(self) -> int: ...
    def count_testnet_orders_since(self, iso_since: str) -> int: ...
    def get_latest_open_testnet_order(self, signal_key: str) -> dict[str, Any] | None: ...


@dataclass(frozen=True, slots=True)
class TestnetOrderConfig:
    bybit_testnet: bool
    api_key: str
    api_secret: str
    trading_enabled: bool
    signals_only: bool
    kill_switch: bool
    allow_mainnet: bool
    order_notional_usdt: Decimal
    min_free_balance_usdt: Decimal
    max_open_positions: int
    daily_max_orders: int
    order_type: str
    base_url: str = BYBIT_TESTNET_BASE_URL
    category: str = "linear"

    @classmethod
    def from_env(cls) -> "TestnetOrderConfig":
        return cls(
            bybit_testnet=_env_bool("BYBIT_TESTNET", False),
            api_key=os.getenv("BYBIT_API_KEY", "").strip(),
            api_secret=os.getenv("BYBIT_API_SECRET", "").strip(),
            trading_enabled=_env_bool("TRADING_ENABLED", False),
            signals_only=_env_bool("SIGNALS_ONLY", True),
            kill_switch=_env_bool("TESTNET_KILL_SWITCH", False),
            allow_mainnet=_env_bool("TESTNET_ALLOW_MAINNET", False),
            order_notional_usdt=_env_decimal("TESTNET_ORDER_NOTIONAL_USDT", "100"),
            min_free_balance_usdt=_env_decimal("TESTNET_MIN_FREE_BALANCE_USDT", "20"),
            max_open_positions=max(0, _env_int("TESTNET_MAX_OPEN_POSITIONS", 3)),
            daily_max_orders=max(0, _env_int("TESTNET_DAILY_MAX_ORDERS", 30)),
            order_type=os.getenv("TESTNET_ORDER_TYPE", "Market").strip() or "Market",
        )

    @property
    def can_trade(self) -> bool:
        return (
            self.bybit_testnet
            and self.base_url == BYBIT_TESTNET_BASE_URL
            and self.trading_enabled
            and not self.signals_only
            and not self.kill_switch
            and not self.allow_mainnet
            and bool(self.api_key)
            and bool(self.api_secret)
        )

    def disabled_reason(self) -> str | None:
        if not self.bybit_testnet or self.base_url != BYBIT_TESTNET_BASE_URL or self.allow_mainnet:
            return ENTRY_BLOCKED_TESTNET_MAINNET_GUARD
        if not self.trading_enabled or self.signals_only or self.kill_switch or not self.api_key or not self.api_secret:
            return ENTRY_BLOCKED_TESTNET_DISABLED
        return None


TestnetOrderConfig.__test__ = False


def _env_bool(name: str, default: bool) -> bool:
    value = os.getenv(name)
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "on"}


def _env_int(name: str, default: int) -> int:
    value = os.getenv(name)
    if value is None or not value.strip():
        return default
    try:
        return int(value)
    except ValueError:
        return default


def _env_decimal(name: str, default: str) -> Decimal:
    value = os.getenv(name, default).strip() or default
    try:
        return Decimal(value)
    except InvalidOperation:
        return Decimal(default)


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _decimal(value: Any, default: str = "0") -> Decimal:
    try:
        return Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError):
        return Decimal(default)


def round_down_to_step(value: Decimal, step: Decimal) -> Decimal:
    if step <= 0:
        return value
    return (value / step).to_integral_value(rounding=ROUND_DOWN) * step


def _decimal_string(value: Decimal) -> str:
    normalized = value.normalize()
    return format(normalized, "f")


class BybitV5TestnetClient:
    def __init__(self, config: TestnetOrderConfig, *, timeout_seconds: int = 10) -> None:
        self.config = config
        self.timeout_seconds = timeout_seconds

    def _signed_request(self, method: str, path: str, payload: dict[str, Any] | None = None) -> dict[str, Any]:
        if self.config.base_url != BYBIT_TESTNET_BASE_URL:
            raise RuntimeError("Refusing to use non-testnet Bybit endpoint")
        payload = payload or {}
        timestamp = str(int(time.time() * 1000))
        recv_window = "5000"
        if method.upper() == "GET":
            body = urlencode(payload)
            url = f"{self.config.base_url}{path}" + (f"?{body}" if body else "")
            sign_payload = f"{timestamp}{self.config.api_key}{recv_window}{body}"
            data = None
        else:
            url = f"{self.config.base_url}{path}"
            body = json.dumps(payload, separators=(",", ":"), ensure_ascii=False)
            sign_payload = f"{timestamp}{self.config.api_key}{recv_window}{body}"
            data = body.encode("utf-8")
        signature = hmac.new(
            self.config.api_secret.encode("utf-8"), sign_payload.encode("utf-8"), hashlib.sha256
        ).hexdigest()
        request = Request(
            url,
            data=data,
            method=method.upper(),
            headers={
                "X-BAPI-API-KEY": self.config.api_key,
                "X-BAPI-TIMESTAMP": timestamp,
                "X-BAPI-RECV-WINDOW": recv_window,
                "X-BAPI-SIGN": signature,
                "Content-Type": "application/json",
                "User-Agent": "CandleVision-TestnetExecutor/1.0",
            },
        )
        try:
            with urlopen(request, timeout=self.timeout_seconds) as response:  # noqa: S310 - guarded testnet URL only
                data = json.loads(response.read().decode("utf-8"))
        except (HTTPError, URLError, TimeoutError) as exc:
            raise RuntimeError(f"Bybit testnet request failed: {exc!r}") from exc
        if data.get("retCode") != 0:
            raise RuntimeError(f"Bybit testnet API error: {data}")
        return data

    def wallet_balance(self) -> dict[str, Any]:
        return self._signed_request("GET", "/v5/account/wallet-balance", {"accountType": "UNIFIED", "coin": "USDT"})

    def instruments_info(self, symbol: str, category: str = "linear") -> dict[str, Any]:
        return self._signed_request("GET", "/v5/market/instruments-info", {"category": category, "symbol": symbol})

    def create_order(self, payload: dict[str, Any]) -> dict[str, Any]:
        return self._signed_request("POST", "/v5/order/create", payload)


class BybitTestnetOrderExecutor:
    def __init__(
        self,
        store: TestnetOrderStore,
        *,
        config: TestnetOrderConfig | None = None,
        client: Any | None = None,
        notifier: Any | None = None,
        logger_: logging.Logger | None = None,
    ) -> None:
        self.store = store
        self.config = config or TestnetOrderConfig.from_env()
        self.client = client or BybitV5TestnetClient(self.config)
        self.notifier = notifier
        self.logger = logger_ or logger

    def is_enabled(self) -> bool:
        return self.config.can_trade

    def build_entry_plan(self, *, signal_key: str, symbol: str, price: float) -> dict[str, Any]:
        disabled_reason = self.config.disabled_reason()
        if disabled_reason:
            return self._blocked(disabled_reason, signal_key=signal_key, symbol=symbol, price=price)
        if self.store.get_testnet_order_by_signal(signal_key) is not None:
            return self._blocked(ENTRY_BLOCKED_TESTNET_DUPLICATE, signal_key=signal_key, symbol=symbol, price=price)
        if self.store.count_open_testnet_positions() >= self.config.max_open_positions:
            return self._blocked(ENTRY_BLOCKED_TESTNET_POSITION_LIMIT, signal_key=signal_key, symbol=symbol, price=price)
        today = datetime.now(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0).isoformat()
        if self.store.count_testnet_orders_since(today) >= self.config.daily_max_orders:
            return self._blocked(ENTRY_BLOCKED_TESTNET_DAILY_ORDER_LIMIT, signal_key=signal_key, symbol=symbol, price=price)

        balance_response = self.client.wallet_balance()
        free_balance = self._extract_free_usdt(balance_response)
        notional = min(self.config.order_notional_usdt, self.config.order_notional_usdt)
        if free_balance < self.config.min_free_balance_usdt or free_balance < notional:
            return self._blocked(
                ENTRY_BLOCKED_INSUFFICIENT_TESTNET_BALANCE,
                signal_key=signal_key,
                symbol=symbol,
                price=price,
                notional_usdt=notional,
            )

        instrument = self._extract_instrument(self.client.instruments_info(symbol, self.config.category))
        qty_step = _decimal(instrument.get("qtyStep"), "1")
        min_qty = _decimal(instrument.get("minOrderQty"), "0")
        max_qty = _decimal(instrument.get("maxOrderQty"), "0")
        price_decimal = _decimal(price)
        qty = round_down_to_step(notional / price_decimal, qty_step) if price_decimal > 0 else Decimal("0")
        if max_qty > 0:
            qty = min(qty, max_qty)
        if qty <= 0 or qty < min_qty:
            return self._blocked(
                ENTRY_BLOCKED_TESTNET_QTY_TOO_SMALL,
                signal_key=signal_key,
                symbol=symbol,
                price=price,
                notional_usdt=notional,
                qty=qty,
            )
        return {
            "ok": True,
            "status": "planned",
            "reason": "testnet_entry_plan_ready",
            "signal_key": signal_key,
            "symbol": symbol,
            "category": self.config.category,
            "side": "Buy",
            "order_type": self.config.order_type,
            "qty": qty,
            "notional_usdt": notional,
            "price": price_decimal,
            "instrument": instrument,
        }

    def place_entry_order(self, *, signal_key: str, trade_key: str, symbol: str, price: float) -> dict[str, Any]:
        try:
            plan = self.build_entry_plan(signal_key=signal_key, symbol=symbol, price=price)
            if not plan.get("ok"):
                self._store_order(signal_key=signal_key, trade_key=trade_key, symbol=symbol, side="Buy", plan=plan)
                self._notify_blocked(symbol, plan)
                return plan
            order_link_id = self._order_link_id("cvtn-ent", signal_key)
            request_json = {
                "category": self.config.category,
                "symbol": symbol,
                "side": "Buy",
                "orderType": self.config.order_type,
                "qty": _decimal_string(plan["qty"]),
                "orderLinkId": order_link_id,
            }
            response_json = self.client.create_order(request_json)
            result = dict(response_json.get("result") or {})
            order_id = result.get("orderId")
            status = "placed"
            stored = self._store_order(
                signal_key=signal_key,
                trade_key=trade_key,
                symbol=symbol,
                side="Buy",
                plan={**plan, "status": status, "order_id": order_id, "order_link_id": order_link_id},
                request_json=request_json,
                response_json=response_json,
            )
            self._notify(f"✅ Testnet order placed: {symbol} Buy qty={_decimal_string(plan['qty'])} orderId={order_id}")
            return {**plan, "status": status, "order_id": order_id, "order_link_id": order_link_id, "stored_order": stored}
        except Exception as exc:  # keep scanner alive on API/rate limit/temporary failures
            self.logger.warning("Bybit testnet entry failed for %s: %r", symbol, exc)
            result = self._blocked(ENTRY_FAILED_TESTNET_API, signal_key=signal_key, symbol=symbol, price=price)
            result["status"] = "failed"
            result["error"] = repr(exc)
            self._store_order(signal_key=signal_key, trade_key=trade_key, symbol=symbol, side="Buy", plan=result)
            self._notify(f"⚠️ Testnet order failed: {symbol} {exc!r}")
            return result

    def place_exit_order(self, *, signal_key: str, trade_key: str, symbol: str, price: float) -> dict[str, Any]:
        try:
            open_order = self.store.get_latest_open_testnet_order(signal_key)
            if open_order is None:
                return self._blocked("exit_blocked_no_testnet_position", signal_key=signal_key, symbol=symbol, price=price)
            qty = _decimal(open_order.get("qty"), "0")
            if qty <= 0:
                return self._blocked("exit_blocked_testnet_qty_missing", signal_key=signal_key, symbol=symbol, price=price)
            order_link_id = self._order_link_id("cvtn-exit", signal_key)
            request_json = {
                "category": self.config.category,
                "symbol": symbol,
                "side": "Sell",
                "orderType": self.config.order_type,
                "qty": _decimal_string(qty),
                "reduceOnly": True,
                "orderLinkId": order_link_id,
            }
            response_json = self.client.create_order(request_json)
            result = dict(response_json.get("result") or {})
            order_id = result.get("orderId")
            stored = self._store_order(
                signal_key=signal_key,
                trade_key=trade_key,
                symbol=symbol,
                side="Sell",
                plan={
                    "ok": True,
                    "status": "placed",
                    "reason": "testnet_exit_order_placed",
                    "category": self.config.category,
                    "order_type": self.config.order_type,
                    "qty": qty,
                    "notional_usdt": qty * _decimal(price),
                    "price": _decimal(price),
                    "order_id": order_id,
                    "order_link_id": order_link_id,
                },
                request_json=request_json,
                response_json=response_json,
            )
            self._notify(f"✅ Testnet position closed: {symbol} Sell reduceOnly qty={_decimal_string(qty)} orderId={order_id}")
            return {"ok": True, "status": "placed", "order_id": order_id, "order_link_id": order_link_id, "qty": qty, "stored_order": stored}
        except Exception as exc:
            self.logger.warning("Bybit testnet exit failed for %s: %r", symbol, exc)
            result = self._blocked("exit_failed_testnet_api", signal_key=signal_key, symbol=symbol, price=price)
            result["status"] = "failed"
            result["error"] = repr(exc)
            self._store_order(signal_key=signal_key, trade_key=trade_key, symbol=symbol, side="Sell", plan=result)
            self._notify(f"⚠️ Testnet exit failed: {symbol} {exc!r}")
            return result

    def _store_order(
        self,
        *,
        signal_key: str,
        trade_key: str,
        symbol: str,
        side: str,
        plan: dict[str, Any],
        request_json: dict[str, Any] | None = None,
        response_json: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        return self.store.insert_testnet_order(
            {
                "signal_key": signal_key,
                "trade_key": trade_key,
                "symbol": symbol,
                "category": plan.get("category") or self.config.category,
                "side": side,
                "order_type": plan.get("order_type") or self.config.order_type,
                "qty": str(plan.get("qty") or ""),
                "notional_usdt": str(plan.get("notional_usdt") or ""),
                "price": str(plan.get("price") or ""),
                "order_id": plan.get("order_id"),
                "order_link_id": plan.get("order_link_id"),
                "status": plan.get("status") or "blocked",
                "reason": plan.get("reason"),
                "request_json": request_json or {},
                "response_json": response_json or ({"error": plan.get("error")} if plan.get("error") else {}),
                "created_at": _utc_now(),
                "updated_at": _utc_now(),
            }
        )

    @staticmethod
    def _extract_free_usdt(response: dict[str, Any]) -> Decimal:
        result = response.get("result") if "result" in response else response
        accounts = result.get("list") or [] if isinstance(result, dict) else []
        for account in accounts:
            for coin in account.get("coin") or []:
                if coin.get("coin") == "USDT":
                    return _decimal(coin.get("availableToWithdraw") or coin.get("walletBalance") or coin.get("equity"), "0")
        return Decimal("0")

    @staticmethod
    def _extract_instrument(response: dict[str, Any]) -> dict[str, Any]:
        result = response.get("result") if "result" in response else response
        rows = result.get("list") or [] if isinstance(result, dict) else []
        row = dict(rows[0]) if rows else {}
        lot = dict(row.get("lotSizeFilter") or {})
        price = dict(row.get("priceFilter") or {})
        return {
            "qtyStep": lot.get("qtyStep") or row.get("qtyStep") or "1",
            "minOrderQty": lot.get("minOrderQty") or row.get("minOrderQty") or "0",
            "maxOrderQty": lot.get("maxOrderQty") or row.get("maxOrderQty") or "0",
            "tickSize": price.get("tickSize") or row.get("tickSize"),
        }

    @staticmethod
    def _blocked(reason: str, **kwargs: Any) -> dict[str, Any]:
        return {"ok": False, "status": "blocked", "reason": reason, "testnet_order_attempted": False, **kwargs}

    @staticmethod
    def _order_link_id(prefix: str, signal_key: str) -> str:
        digest = hashlib.sha1(signal_key.encode("utf-8")).hexdigest()[:18]  # noqa: S324 - deterministic id only
        suffix = uuid.uuid4().hex[:8]
        return f"{prefix}-{digest}-{suffix}"[:36]

    def _notify_blocked(self, symbol: str, plan: dict[str, Any]) -> None:
        if plan.get("reason") == ENTRY_BLOCKED_INSUFFICIENT_TESTNET_BALANCE:
            self._notify(f"⚠️ Testnet order blocked: {symbol} insufficient USDT balance")

    def _notify(self, text: str) -> None:
        if self.notifier is None or not hasattr(self.notifier, "send_message"):
            return
        try:
            result = self.notifier.send_message(text)
            if hasattr(result, "__await__"):
                try:
                    loop = asyncio.get_running_loop()
                except RuntimeError:
                    asyncio.run(result)
                else:
                    loop.create_task(result)
        except Exception:
            self.logger.debug("Testnet Telegram notification failed", exc_info=True)
