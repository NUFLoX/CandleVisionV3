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

        try:
            self.cursor.execute("ALTER TABLE trades ADD COLUMN reasons TEXT")
            self.conn.commit()
            self.logger.info("🧠 БД обновлена: добавлена колонка reasons")
        except sqlite3.OperationalError:
            pass # Если колонка уже создана, просто идем дальше

    def add_trade(self, trade: dict) -> int:
        """Сохраняет новую сделку и возвращает её ID."""
        self.cursor.execute('''
            INSERT INTO trades (
                symbol, entry, sl, tp, tp1_price, tp1_qty, 
                size, remaining_size, status, pnl_pct, timeframe, 
                trailing_active, highest_price, reasons 
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', (
            trade['symbol'], trade['entry'], trade['sl'], trade['tp'],
            trade.get('tp1_price', 0), trade.get('tp1_qty', 0),
            trade['size'], trade.get('remaining_size', trade['size']),
            trade['status'], trade['pnl_pct'], trade['timeframe'],
            1 if trade.get('trailing_active') else 0,
            trade.get('highest_price', trade['entry']),
            trade.get('reasons', '') # <--- СОХРАНЯЕМ ПРИЧИНЫ
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
        """Загружает все открытые сделки при старте бота."""
        self.cursor.execute("SELECT * FROM trades WHERE status='open'")
        rows = self.cursor.fetchall()
        
        trades = []
        for row in rows:
            trades.append({
                "id": row[0],
                "symbol": row[1],
                "entry": row[2],
                "sl": row[3],
                "tp": row[4],
                "tp1_price": row[5],
                "tp1_qty": row[6],
                "size": row[7],
                "remaining_size": row[8],
                "status": row[9],
                "pnl_pct": row[10],
                "timeframe": row[11],
                "trailing_active": bool(row[12]),
                "highest_price": row[13],
                "reasons": row[14] if len(row) > 14 else "" # <--- ЧИТАЕМ ПРИЧИНЫ
            })
        return trades