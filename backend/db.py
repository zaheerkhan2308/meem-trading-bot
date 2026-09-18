"""
Neon PostgreSQL persistence layer.
All dashboard state (signals, scan log, trades, portfolio snapshots) is written here.
"""
import logging
import os
import time

import psycopg2
from psycopg2.extras import RealDictCursor
from .logging_config import instrument_module
from .config import DATABASE_URL, NEON_DATABASE_URL

logger = logging.getLogger(__name__)

# NEON_DATABASE_URL takes priority so Railway's own Postgres plugin
# (which auto-injects DATABASE_URL pointing to a local socket) can't override it.
_URL = NEON_DATABASE_URL or DATABASE_URL

if not _URL or _URL.startswith("/") or "/.s.PGSQL." in _URL:
    raise RuntimeError(
        "No valid Neon connection string found. "
        "Set NEON_DATABASE_URL in Railway → service → Variables "
        "(copy the postgresql://... URL from your Neon dashboard)."
    )


def _conn():
    # connect_timeout=30 gives Neon time to wake from cold start (can take 10-30 s).
    # Retry up to 3 times with backoff in case the first attempt races the wakeup.
    last_exc = None
    for delay in (0, 5, 15):
        try:
            if delay:
                time.sleep(delay)
            return psycopg2.connect(_URL, connect_timeout=30)
        except psycopg2.OperationalError as exc:
            last_exc = exc
            logger.warning(f"DB connect failed (retrying): {exc}")
    raise last_exc


# ── Schema ─────────────────────────────────────────────────────────────────

def init_tables() -> None:
    with _conn() as conn:
        with conn.cursor() as cur:
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
                    qty         NUMERIC(12,4) NOT NULL,
                    price       NUMERIC(12,4),
                    score       NUMERIC(5,4),
                    reason      TEXT,
                    scan_time   VARCHAR(60),
                    dry_run     BOOLEAN     DEFAULT FALSE,
                    pnl         NUMERIC(12,4) DEFAULT 0,
                    strategy    VARCHAR(20)  NOT NULL DEFAULT 'default'
                )
            """)
            cur.execute("""
                ALTER TABLE trades ALTER COLUMN qty TYPE NUMERIC(12,4)
            """)
            cur.execute("""
                ALTER TABLE trades ADD COLUMN IF NOT EXISTS strategy VARCHAR(20) NOT NULL DEFAULT 'default'
            """)
            cur.execute("CREATE TABLE IF NOT EXISTS default_trades (LIKE trades INCLUDING ALL)")
            cur.execute("CREATE TABLE IF NOT EXISTS selected_trades (LIKE trades INCLUDING ALL)")
            cur.execute("""
                INSERT INTO default_trades
                SELECT * FROM trades WHERE strategy = 'default'
                ON CONFLICT DO NOTHING
            """)
            cur.execute("""
                INSERT INTO selected_trades
                SELECT * FROM trades WHERE strategy = 'selected'
                ON CONFLICT DO NOTHING
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
            cur.execute("""
                CREATE TABLE IF NOT EXISTS watchlist_log (
                    id          SERIAL PRIMARY KEY,
                    scanned_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    scan_time   VARCHAR(60),
                    tickers     JSONB NOT NULL DEFAULT '[]'
                )
            """)
            cur.execute("""
                CREATE TABLE IF NOT EXISTS selected_watchlist_log (
                    id          SERIAL PRIMARY KEY,
                    scanned_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    scan_time   VARCHAR(60),
                    tickers     JSONB NOT NULL DEFAULT '[]'
                )
            """)
            cur.execute("""
                CREATE TABLE IF NOT EXISTS trading_settings (
                    id          INTEGER PRIMARY KEY CHECK (id = 1),
                    mode        VARCHAR(20) NOT NULL,
                    tickers     JSONB NOT NULL DEFAULT '[]',
                    updated_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
                )
            """)
            cur.execute("""
                CREATE TABLE IF NOT EXISTS strategy_settings (
                    strategy    VARCHAR(20) PRIMARY KEY,
                    tickers     JSONB NOT NULL DEFAULT '[]',
                    overrides   JSONB NOT NULL DEFAULT '{}',
                    updated_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
                )
            """)
            cur.execute("""
                INSERT INTO strategy_settings (strategy)
                VALUES ('default'), ('selected')
                ON CONFLICT (strategy) DO NOTHING
            """)
        conn.commit()
    logger.info("DB tables ready")


def load_trading_settings() -> dict:
    try:
        with _conn() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute("SELECT mode, tickers FROM trading_settings WHERE id = 1")
                row = cur.fetchone()
        if not row:
            return {"mode": "default", "tickers": []}
        tickers = row["tickers"]
        return {"mode": row["mode"], "tickers": list(tickers or [])}
    except Exception as exc:
        logger.error(f"DB load_trading_settings failed: {exc}")
        return {"mode": "default", "tickers": []}


def save_trading_settings(mode: str, tickers: list[str]) -> None:
    import json
    try:
        with _conn() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO trading_settings (id, mode, tickers)
                    VALUES (1, %s, %s::jsonb)
                    ON CONFLICT (id) DO UPDATE SET
                        mode = EXCLUDED.mode,
                        tickers = EXCLUDED.tickers,
                        updated_at = NOW()
                """, (mode, json.dumps(tickers)))
            conn.commit()
    except Exception as exc:
        logger.error(f"DB save_trading_settings failed: {exc}")


def load_strategy_settings(strategy: str) -> dict:
    try:
        with _conn() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute("SELECT tickers, overrides FROM strategy_settings WHERE strategy = %s", (strategy,))
                row = cur.fetchone()
        if not row:
            return {"strategy": strategy, "tickers": [], "overrides": {}}
        return {"strategy": strategy, "tickers": list(row["tickers"] or []), "overrides": dict(row["overrides"] or {})}
    except Exception as exc:
        logger.error(f"DB load_strategy_settings failed: {exc}")
        return {"strategy": strategy, "tickers": [], "overrides": {}}


def save_strategy_settings(strategy: str, tickers: list[str], overrides: dict) -> dict:
    import json
    with _conn() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO strategy_settings (strategy, tickers, overrides)
                VALUES (%s, %s::jsonb, %s::jsonb)
                ON CONFLICT (strategy) DO UPDATE SET
                    tickers = EXCLUDED.tickers,
                    overrides = EXCLUDED.overrides,
                    updated_at = NOW()
            """, (strategy, json.dumps(tickers), json.dumps(overrides)))
        conn.commit()
    return {"strategy": strategy, "tickers": tickers, "overrides": overrides}


def save_selected_watchlist(scan_time: str, tickers: list[dict]) -> None:
    import json
    try:
        with _conn() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO selected_watchlist_log (scan_time, tickers)
                    VALUES (%s, %s::jsonb)
                """, (scan_time, json.dumps(tickers)))
                cur.execute("""
                    DELETE FROM selected_watchlist_log
                    WHERE scanned_at < NOW() - INTERVAL '7 days'
                """)
            conn.commit()
    except Exception as exc:
        logger.error(f"DB save_selected_watchlist failed: {exc}")


def load_selected_watchlist() -> dict | None:
    import json
    try:
        with _conn() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute("""
                    SELECT scan_time, tickers FROM selected_watchlist_log
                    ORDER BY scanned_at DESC LIMIT 1
                """)
                row = cur.fetchone()
        if not row:
            return None
        tickers = row["tickers"]
        if isinstance(tickers, str):
            tickers = json.loads(tickers)
        return {"scan_time": row["scan_time"], "tickers": list(tickers or [])}
    except Exception as exc:
        logger.error(f"DB load_selected_watchlist failed: {exc}")
        return None


def clear_selected_watchlist() -> None:
    try:
        with _conn() as conn:
            with conn.cursor() as cur:
                cur.execute("DELETE FROM selected_watchlist_log")
            conn.commit()
    except Exception as exc:
        logger.error(f"DB clear_selected_watchlist failed: {exc}")


# ── Writes ─────────────────────────────────────────────────────────────────


def save_scan_log(scan_time: str, event_type: str,
                  signal_count: int = 0, tickers: list[str] | None = None) -> None:
    try:
        with _conn() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO scan_log (scan_time_label, event_type, signal_count, tickers)
                    VALUES (%s, %s, %s, %s)
                """, (scan_time, event_type, signal_count, tickers or []))
                cur.execute("DELETE FROM scan_log WHERE scanned_at < NOW() - INTERVAL '2 days'")
            conn.commit()
    except Exception as exc:
        logger.error(f"DB save_scan_log failed: {exc}")


def save_trade(trade: dict) -> None:
    try:
        strategy = trade.get("strategy", "default")
        if strategy not in {"default", "selected"}:
            strategy = "default"
        table = f"{strategy}_trades"
        with _conn() as conn:
            with conn.cursor() as cur:
                cur.execute(f"""
                    INSERT INTO {table}
                        (ticker, action, qty, price, score, reason, scan_time, dry_run, pnl, strategy)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """, (
                    trade["ticker"], trade["action"], trade["qty"],
                    trade.get("price"), trade.get("score"),
                    trade.get("reason", ""), trade.get("scan_time", ""),
                    trade.get("dry_run", False), trade.get("pnl", 0.0), strategy,
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


def load_trades(limit: int = 100, days: int = 2, strategy: str | None = None) -> list[dict]:
    """Return the most recent trades from the last N days, oldest-first."""
    try:
        table = f"{strategy}_trades" if strategy in {"default", "selected"} else "trades"
        with _conn() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute(f"""
                    SELECT * FROM (
                        SELECT * FROM {table}
                        WHERE timestamp >= NOW() - (%s || ' days')::INTERVAL
                        ORDER BY timestamp DESC LIMIT %s
                    ) sub ORDER BY timestamp ASC
                """, (str(days), limit))
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


def load_todays_circuit_breaker() -> str | None:
    """Return the most recent circuit breaker reason from today, or None."""
    try:
        with _conn() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT reason FROM circuit_breaker_events
                    WHERE timestamp AT TIME ZONE 'America/New_York' >= CURRENT_DATE
                    ORDER BY timestamp DESC LIMIT 1
                """)
                row = cur.fetchone()
        return row[0] if row else None
    except Exception as exc:
        logger.error(f"DB load_todays_circuit_breaker failed: {exc}")
        return None


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


def save_watchlist(scan_time: str, tickers: list[dict]) -> None:
    import json
    try:
        with _conn() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO watchlist_log (scan_time, tickers)
                    VALUES (%s, %s::jsonb)
                """, (scan_time, json.dumps(tickers)))
                cur.execute("DELETE FROM watchlist_log WHERE scanned_at < NOW() - INTERVAL '7 days'")
            conn.commit()
    except Exception as exc:
        logger.error(f"DB save_watchlist failed: {exc}")


def load_watchlist() -> dict | None:
    """Return the most recent watchlist snapshot (top 10 tickers)."""
    import json
    try:
        with _conn() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute("""
                    SELECT scan_time, tickers FROM watchlist_log
                    ORDER BY scanned_at DESC LIMIT 1
                """)
                row = cur.fetchone()
        if row:
            tickers = row["tickers"]
            if isinstance(tickers, str):
                tickers = json.loads(tickers)
            return {"scan_time": row["scan_time"], "tickers": list(tickers or [])}
        return None
    except Exception as exc:
        logger.error(f"DB load_watchlist failed: {exc}")
        return None


def _row_to_log(row) -> dict:
    entry: dict = {
        "time": row["scan_time_label"] or str(row["scanned_at"]),
        "type": row["event_type"],
    }
    if row["event_type"] == "scan":
        entry["count"]   = row["signal_count"] or 0
        entry["tickers"] = list(row["tickers"] or [])
    return entry


def _row_to_trade(row) -> dict:
    return {
        "ticker":    row["ticker"],
        "action":    row["action"],
        "qty":       float(row["qty"]),
        "price":     float(row["price"] or 0),
        "score":     float(row["score"] or 0),
        "reason":    row["reason"] or "",
        "scan_time": row["scan_time"] or "",
        "dry_run":   bool(row["dry_run"]),
        "pnl":       float(row["pnl"] or 0),
        "timestamp": str(row["timestamp"]),
        "strategy": row.get("strategy", "default"),
    }


instrument_module(globals())
