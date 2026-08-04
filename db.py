"""
Neon PostgreSQL persistence layer.
All dashboard state (signals, scan log, trades, portfolio snapshots) is written here.
"""
import logging
import os

import psycopg2
from psycopg2.extras import RealDictCursor
from dotenv import load_dotenv

load_dotenv()
logger = logging.getLogger(__name__)

_URL = os.getenv("DATABASE_URL", "")


def _conn():
    return psycopg2.connect(_URL)


# ── Schema ─────────────────────────────────────────────────────────────────

def init_tables() -> None:
    with _conn() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS signals (
                    id              SERIAL PRIMARY KEY,
                    fired_at        TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    scan_time_label VARCHAR(60),
                    ticker          VARCHAR(10)  NOT NULL,
                    composite_score NUMERIC(5,4),
                    technical_score NUMERIC(5,4),
                    sentiment_score NUMERIC(5,4),
                    historical_score NUMERIC(5,4),
                    sentiment_label VARCHAR(20),
                    top_headline    TEXT,
                    top_url         TEXT,
                    rsi             NUMERIC(5,1),
                    macd_cross      BOOLEAN,
                    ema_reclaim     BOOLEAN,
                    volume_ratio    NUMERIC(7,2),
                    current_price   NUMERIC(12,2),
                    entry_low       NUMERIC(12,2),
                    entry_high      NUMERIC(12,2),
                    stop_loss       NUMERIC(12,2),
                    take_profit     NUMERIC(12,2)
                )
            """)
            cur.execute("""
                CREATE TABLE IF NOT EXISTS scan_log (
                    id              SERIAL PRIMARY KEY,
                    scanned_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    scan_time_label VARCHAR(60),
                    event_type      VARCHAR(20) NOT NULL,
                    signal_count    INTEGER     DEFAULT 0,
                    tickers         TEXT[]
                )
            """)
            cur.execute("""
                CREATE TABLE IF NOT EXISTS trades (
                    id          SERIAL PRIMARY KEY,
                    timestamp   TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    ticker      VARCHAR(10) NOT NULL,
                    action      VARCHAR(10) NOT NULL,
                    qty         INTEGER     NOT NULL,
                    price       NUMERIC(12,4),
                    score       NUMERIC(5,4),
                    reason      TEXT,
                    scan_time   VARCHAR(60),
                    dry_run     BOOLEAN     DEFAULT FALSE,
                    pnl         NUMERIC(12,4) DEFAULT 0
                )
            """)
            cur.execute("""
                CREATE TABLE IF NOT EXISTS portfolio_snapshots (
                    id              SERIAL PRIMARY KEY,
                    snapshot_date   DATE        NOT NULL UNIQUE,
                    total_value     NUMERIC(14,2),
                    cash            NUMERIC(14,2),
                    positions_value NUMERIC(14,2)
                )
            """)
            cur.execute("""
                CREATE TABLE IF NOT EXISTS circuit_breaker_events (
                    id          SERIAL PRIMARY KEY,
                    timestamp   TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    reason      TEXT
                )
            """)
        conn.commit()
    logger.info("DB tables ready")


# ── Writes ─────────────────────────────────────────────────────────────────

def save_signals(signals: list[dict], scan_time: str) -> None:
    try:
        with _conn() as conn:
            with conn.cursor() as cur:
                for s in signals:
                    cur.execute("""
                        INSERT INTO signals
                            (scan_time_label, ticker, composite_score, technical_score,
                             sentiment_score, historical_score, sentiment_label,
                             top_headline, top_url, rsi, macd_cross, ema_reclaim,
                             volume_ratio, current_price, entry_low, entry_high,
                             stop_loss, take_profit)
                        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                    """, (
                        scan_time, s["ticker"],
                        s.get("composite_score"), s.get("technical_score"),
                        s.get("sentiment_score"), s.get("historical_score"),
                        s.get("sentiment_label"),
                        s.get("top_headline"), s.get("top_url", ""),
                        s.get("rsi"),
                        s.get("macd_cross", False), s.get("ema_reclaim", False),
                        s.get("volume_ratio", 0.0), s.get("current_price"),
                        s.get("entry_low"), s.get("entry_high"),
                        s.get("stop_loss"), s.get("take_profit"),
                    ))
            conn.commit()
    except Exception as exc:
        logger.error(f"DB save_signals failed: {exc}")


def save_scan_log(scan_time: str, event_type: str,
                  signal_count: int = 0, tickers: list[str] | None = None) -> None:
    try:
        with _conn() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO scan_log (scan_time_label, event_type, signal_count, tickers)
                    VALUES (%s, %s, %s, %s)
                """, (scan_time, event_type, signal_count, tickers or []))
            conn.commit()
    except Exception as exc:
        logger.error(f"DB save_scan_log failed: {exc}")


def save_trade(trade: dict) -> None:
    try:
        with _conn() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO trades
                        (ticker, action, qty, price, score, reason, scan_time, dry_run, pnl)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                """, (
                    trade["ticker"], trade["action"], trade["qty"],
                    trade.get("price"), trade.get("score"),
                    trade.get("reason", ""), trade.get("scan_time", ""),
                    trade.get("dry_run", False), trade.get("pnl", 0.0),
                ))
            conn.commit()
    except Exception as exc:
        logger.error(f"DB save_trade failed: {exc}")


def save_portfolio_snapshot(snapshot: dict) -> None:
    """Upsert today's portfolio snapshot (one row per day)."""
    try:
        with _conn() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO portfolio_snapshots
                        (snapshot_date, total_value, cash, positions_value)
                    VALUES (CURRENT_DATE, %s, %s, %s)
                    ON CONFLICT (snapshot_date) DO UPDATE SET
                        total_value     = EXCLUDED.total_value,
                        cash            = EXCLUDED.cash,
                        positions_value = EXCLUDED.positions_value
                """, (
                    snapshot.get("total_value"), snapshot.get("cash"),
                    snapshot.get("positions_value"),
                ))
            conn.commit()
    except Exception as exc:
        logger.error(f"DB save_portfolio_snapshot failed: {exc}")


def save_circuit_breaker_event(reason: str) -> None:
    try:
        with _conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "INSERT INTO circuit_breaker_events (reason) VALUES (%s)",
                    (reason,)
                )
            conn.commit()
    except Exception as exc:
        logger.error(f"DB save_circuit_breaker_event failed: {exc}")


# ── Reads ──────────────────────────────────────────────────────────────────

def load_signals(limit: int = 20) -> list[dict]:
    """Return the most recent signals, oldest-first (matches in-memory order)."""
    try:
        with _conn() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute("""
                    SELECT * FROM (
                        SELECT * FROM signals ORDER BY fired_at DESC LIMIT %s
                    ) sub ORDER BY fired_at ASC
                """, (limit,))
                rows = cur.fetchall()
        return [_row_to_signal(r) for r in rows]
    except Exception as exc:
        logger.error(f"DB load_signals failed: {exc}")
        return []


def load_scan_log(limit: int = 50) -> list[dict]:
    """Return the most recent scan log entries, oldest-first."""
    try:
        with _conn() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute("""
                    SELECT * FROM (
                        SELECT * FROM scan_log ORDER BY scanned_at DESC LIMIT %s
                    ) sub ORDER BY scanned_at ASC
                """, (limit,))
                rows = cur.fetchall()
        return [_row_to_log(r) for r in rows]
    except Exception as exc:
        logger.error(f"DB load_scan_log failed: {exc}")
        return []


def load_trades(limit: int = 100) -> list[dict]:
    """Return the most recent trades, oldest-first."""
    try:
        with _conn() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute("""
                    SELECT * FROM (
                        SELECT * FROM trades ORDER BY timestamp DESC LIMIT %s
                    ) sub ORDER BY timestamp ASC
                """, (limit,))
                rows = cur.fetchall()
        return [_row_to_trade(r) for r in rows]
    except Exception as exc:
        logger.error(f"DB load_trades failed: {exc}")
        return []


def load_portfolio_snapshots(days: int = 365) -> list[dict]:
    """Return portfolio snapshots for the last N days, oldest-first."""
    try:
        with _conn() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute("""
                    SELECT snapshot_date, total_value, cash, positions_value
                    FROM portfolio_snapshots
                    WHERE snapshot_date >= CURRENT_DATE - (%s || ' days')::INTERVAL
                    ORDER BY snapshot_date ASC
                """, (str(days),))
                rows = cur.fetchall()
        return [
            {
                "date": str(r["snapshot_date"]),
                "total_value": float(r["total_value"] or 0),
                "cash": float(r["cash"] or 0),
                "positions_value": float(r["positions_value"] or 0),
            }
            for r in rows
        ]
    except Exception as exc:
        logger.error(f"DB load_portfolio_snapshots failed: {exc}")
        return []


def load_latest_portfolio() -> dict | None:
    """Return the most recent portfolio snapshot as a partial portfolio dict."""
    try:
        with _conn() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute("""
                    SELECT total_value, cash, positions_value
                    FROM portfolio_snapshots
                    ORDER BY snapshot_date DESC LIMIT 1
                """)
                row = cur.fetchone()
        if row:
            return {
                "total_value":     float(row["total_value"] or 0),
                "cash":            float(row["cash"] or 0),
                "positions_value": float(row["positions_value"] or 0),
                "daily_pnl":       0.0,
                "daily_pnl_pct":   0.0,
            }
        return None
    except Exception as exc:
        logger.error(f"DB load_latest_portfolio failed: {exc}")
        return None


# ── Helpers ────────────────────────────────────────────────────────────────

def _row_to_signal(row) -> dict:
    return {
        "ticker":          row["ticker"],
        "composite_score": float(row["composite_score"] or 0),
        "technical_score": float(row["technical_score"] or 0),
        "sentiment_score": float(row["sentiment_score"] or 0),
        "historical_score":float(row["historical_score"] or 0),
        "sentiment_label": row["sentiment_label"] or "",
        "top_headline":    row["top_headline"] or "",
        "top_url":         row["top_url"] or "",
        "rsi":             float(row["rsi"]) if row["rsi"] is not None else None,
        "macd_cross":      bool(row["macd_cross"]),
        "ema_reclaim":     bool(row["ema_reclaim"]),
        "volume_ratio":    float(row["volume_ratio"] or 0),
        "current_price":   float(row["current_price"] or 0),
        "entry_low":       float(row["entry_low"] or 0),
        "entry_high":      float(row["entry_high"] or 0),
        "stop_loss":       float(row["stop_loss"] or 0),
        "take_profit":     float(row["take_profit"] or 0),
        "scan_time_label": row["scan_time_label"] or "",
    }


def _row_to_log(row) -> dict:
    entry: dict = {
        "time": row["scan_time_label"] or str(row["scanned_at"]),
        "type": row["event_type"],
    }
    if row["event_type"] == "signals":
        entry["count"]   = row["signal_count"] or 0
        entry["tickers"] = list(row["tickers"] or [])
    return entry


def _row_to_trade(row) -> dict:
    return {
        "ticker":    row["ticker"],
        "action":    row["action"],
        "qty":       int(row["qty"]),
        "price":     float(row["price"] or 0),
        "score":     float(row["score"] or 0),
        "reason":    row["reason"] or "",
        "scan_time": row["scan_time"] or "",
        "dry_run":   bool(row["dry_run"]),
        "pnl":       float(row["pnl"] or 0),
        "timestamp": str(row["timestamp"]),
    }
