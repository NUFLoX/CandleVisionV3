from __future__ import annotations

import asyncio
import hashlib
import hmac
import json
import os
import socket
import time
from datetime import datetime, timezone
from typing import Any
from urllib.parse import urlencode, urlparse
from urllib.request import Request, urlopen

from config.settings import API_TIMEOUT, BYBIT_API_KEY, BYBIT_API_SECRET, BYBIT_REST_BASE_URL, CHAT_ID, TOKEN
from .persistence import dashboard_state_path, write_state
from .schemas import BotStatus, Heartbeat

HEARTBEAT_MAX_AGE_SECONDS = int(os.getenv("DASHBOARD_HEARTBEAT_MAX_AGE_SECONDS", "90"))


async def build_health_status(base: BotStatus, heartbeats: dict[str, Heartbeat]) -> BotStatus:
    checks = await asyncio.gather(
        _check_bybit_public(),
        _check_bybit_private(),
        _check_telegram(),
        _check_persistence(),
        _check_redis(),
        return_exceptions=True,
    )
    bybit_public, bybit_private, telegram, persistence, redis = [
        "ERROR" if isinstance(item, Exception) else item for item in checks
    ]
    base.bybit_api = _combine_bybit_status(str(bybit_public), str(bybit_private))
    base.telegram = str(telegram)
    base.database = str(persistence)
    base.redis = str(redis)
    base.scanner = _heartbeat_status(heartbeats, "scanner")
    base.executor = _heartbeat_status(heartbeats, "executor")
    base.rate_limit = "OK" if str(bybit_public) == "OK" else "unknown"
    return base


async def _check_bybit_public() -> str:
    return await asyncio.to_thread(_bybit_public_sync)


async def _check_bybit_private() -> str:
    if not BYBIT_API_KEY or not BYBIT_API_SECRET:
        return "not-configured"
    return await asyncio.to_thread(_bybit_private_sync)


async def _check_telegram() -> str:
    if not TOKEN or not CHAT_ID:
        return "not-configured"
    return await asyncio.to_thread(_telegram_sync)


async def _check_persistence() -> str:
    return await asyncio.to_thread(_persistence_sync)


async def _check_redis() -> str:
    redis_url = os.getenv("REDIS_URL", "").strip()
    if not redis_url:
        return "not-configured"
    return await asyncio.to_thread(_redis_sync, redis_url)


def _bybit_public_sync() -> str:
    data = _http_json(f"{BYBIT_REST_BASE_URL}/v5/market/time", {})
    return "OK" if data.get("retCode") == 0 else "ERROR"


def _bybit_private_sync() -> str:
    recv_window = "5000"
    timestamp = str(int(time.time() * 1000))
    params = {"accountType": "UNIFIED"}
    query_string = urlencode(params)
    payload = f"{timestamp}{BYBIT_API_KEY}{recv_window}{query_string}"
    signature = hmac.new(BYBIT_API_SECRET.encode("utf-8"), payload.encode("utf-8"), hashlib.sha256).hexdigest()
    request = Request(
        f"{BYBIT_REST_BASE_URL}/v5/account/wallet-balance?{query_string}",
        headers={
            "X-BAPI-API-KEY": BYBIT_API_KEY,
            "X-BAPI-TIMESTAMP": timestamp,
            "X-BAPI-RECV-WINDOW": recv_window,
            "X-BAPI-SIGN": signature,
            "User-Agent": "CandleVisionHealth/1.0",
        },
    )
    with urlopen(request, timeout=API_TIMEOUT) as response:
        data = json.loads(response.read().decode("utf-8"))
    return "OK" if data.get("retCode") == 0 else f"ERROR:{data.get('retCode')}"


def _telegram_sync() -> str:
    data = _http_json(f"https://api.telegram.org/bot{TOKEN}/getMe", {})
    return "OK" if data.get("ok") else "ERROR"


def _persistence_sync() -> str:
    path = dashboard_state_path()
    payload = {"healthcheck": datetime.now(timezone.utc).isoformat()}
    write_state(payload, path.with_suffix(".healthcheck.json"))
    path.with_suffix(".healthcheck.json").unlink(missing_ok=True)
    return "OK"


def _redis_sync(redis_url: str) -> str:
    parsed = urlparse(redis_url)
    host = parsed.hostname or "localhost"
    port = parsed.port or 6379
    password = parsed.password
    with socket.create_connection((host, port), timeout=API_TIMEOUT) as sock:
        if password:
            sock.sendall(f"*2\r\n$4\r\nAUTH\r\n${len(password)}\r\n{password}\r\n".encode("utf-8"))
            sock.recv(1024)
        sock.sendall(b"*1\r\n$4\r\nPING\r\n")
        response = sock.recv(1024)
    return "OK" if b"PONG" in response else "ERROR"


def _http_json(url: str, params: dict[str, str]) -> dict[str, Any]:
    query = urlencode(params)
    request = Request(f"{url}?{query}" if query else url, headers={"User-Agent": "CandleVisionHealth/1.0"})
    with urlopen(request, timeout=API_TIMEOUT) as response:
        return json.loads(response.read().decode("utf-8"))


def _heartbeat_status(heartbeats: dict[str, Heartbeat], component: str) -> str:
    heartbeat = heartbeats.get(component)
    if not heartbeat:
        return "no-heartbeat"
    age = (datetime.now(timezone.utc) - heartbeat.timestamp).total_seconds()
    if age > HEARTBEAT_MAX_AGE_SECONDS:
        return f"stale:{int(age)}s"
    return heartbeat.status


def _combine_bybit_status(public_status: str, private_status: str) -> str:
    if public_status != "OK":
        return f"public:{public_status}"
    if private_status in {"OK", "not-configured"}:
        return "OK" if private_status == "OK" else "public-OK/private-not-configured"
    return f"public-OK/private:{private_status}"
