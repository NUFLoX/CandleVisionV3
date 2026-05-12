# -*- coding: utf-8 -*-
import sqlite3
import logging

class Database:
    """Локальное хранилище состояния бота (SQLite)."""
    def __init__(self, db_name="candlevision.db"):
        self.logger = logging.getLogger("CandleVision.DB")
        self.conn = sqlite3.connect(db_name, check_same_thread=False)
        
        self.conn.execute("PRAGMA journal_mode=WAL;")
        self.conn.execute("PRAGMA synchronous=NORMAL;") 
        
        self.cursor = self.conn.cursor()
        self._create_tables()

    def _create_tables(self):
        """Создает таблицы со всеми новыми параметрами HFT-ядра."""
        self.cursor.execute('''
            CREATE TABLE IF NOT EXISTS trades (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                symbol TEXT,
                entry REAL,
                sl REAL,
                tp REAL,
                tp1_price REAL,
                tp1_qty REAL,
                size REAL,
                remaining_size REAL,
                status TEXT,
                pnl_pct REAL,
                timeframe TEXT,
                trailing_active INTEGER,
                highest_price REAL
            )
        ''')
        self.conn.commit()

        self._ensure_column("reasons", "TEXT")
        self._ensure_column("side", "TEXT")
        self._ensure_column("order_link_id", "TEXT")
        self._ensure_column("order_id", "TEXT")

    def _ensure_column(self, name: str, column_type: str) -> None:
        try:
            self.cursor.execute(f"ALTER TABLE trades ADD COLUMN {name} {column_type}")
            self.conn.commit()
            self.logger.info(f"🧠 БД обновлена: добавлена колонка {name}")
        except sqlite3.OperationalError:
            pass # Если колонка уже создана, просто идем дальше

    def add_trade(self, trade: dict) -> int:
        """Сохраняет новую сделку и возвращает её ID."""
        self.cursor.execute('''
            INSERT INTO trades (
                symbol, entry, sl, tp, tp1_price, tp1_qty, 
                size, remaining_size, status, pnl_pct, timeframe, 
                trailing_active, highest_price, reasons, side, order_link_id, order_id
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', (
            trade['symbol'], trade['entry'], trade['sl'], trade['tp'],
            trade.get('tp1_price', 0), trade.get('tp1_qty', 0),
            trade['size'], trade.get('remaining_size', trade['size']),
            trade['status'], trade['pnl_pct'], trade['timeframe'],
            1 if trade.get('trailing_active') else 0,
            trade.get('highest_price', trade['entry']),
            trade.get('reasons', ''), # <--- СОХРАНЯЕМ ПРИЧИНЫ
            trade.get('side', 'Buy'),
            trade.get('order_link_id', ''),
            trade.get('order_id', '')
        ))
        self.conn.commit()
        return self.cursor.lastrowid

    def update_trade_status(self, trade_id: int, status: str, pnl: float):
        """Обновляет статус (например, при закрытии по стопу)."""
        self.cursor.execute('''
            UPDATE trades SET status = ?, pnl_pct = ? WHERE id = ?
        ''', (status, pnl, trade_id))
        self.conn.commit()

    def load_open_trades(self) -> list:
        """Загружает все активные сделки/ордера при старте бота."""
        self.cursor.execute("SELECT * FROM trades WHERE status IN ('open', 'pending_order')")
        rows = self.cursor.fetchall()
        columns = [item[0] for item in self.cursor.description]

        trades = []
        for row in rows:
            raw = dict(zip(columns, row))
            trades.append({
                "id": raw.get("id"),
                "symbol": raw.get("symbol"),
                "entry": raw.get("entry"),
                "sl": raw.get("sl"),
                "tp": raw.get("tp"),
                "tp1_price": raw.get("tp1_price"),
                "tp1_qty": raw.get("tp1_qty"),
                "size": raw.get("size"),
                "remaining_size": raw.get("remaining_size"),
                "status": raw.get("status"),
                "pnl_pct": raw.get("pnl_pct"),
                "timeframe": raw.get("timeframe"),
                "trailing_active": bool(raw.get("trailing_active")),
                "highest_price": raw.get("highest_price"),
                "reasons": raw.get("reasons") or "",
                "side": raw.get("side") or "Buy",
                "order_link_id": raw.get("order_link_id") or "",
                "order_id": raw.get("order_id") or "",
            })
        return trades
