import logging
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone

import pandas_market_calendars as mcal
from apscheduler.schedulers.background import BackgroundScheduler
from zoneinfo import ZoneInfo

logging.basicConfig(
    level=logging.INFO,
    format="[%(asctime)s] %(levelname)s — %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[
        logging.FileHandler("trading_bot.log", encoding="utf-8"),
        logging.StreamHandler(),
    ],
)

from config import (
    PORT,
    BUY_THRESHOLD, SELL_THRESHOLD, STOP_LOSS_PCT,
    MAX_POSITION_SIZE, DAILY_LOSS_LIMIT, DAILY_PROFIT_TARGET,
)
from scanner import get_top_movers
from scorer import get_composite_score
from sentiment import initialize_finbert
from notifier import send_startup, send_shutdown
from broker import BrokerClient
from risk import RiskManager
from engine import TradingEngine
import dashboard
import db

logger = logging.getLogger(__name__)

_NYSE = mcal.get_calendar("NYSE")
_ET = ZoneInfo("America/New_York")

# Initialized after FinBERT loads
_broker: BrokerClient | None = None
_risk: RiskManager | None = None
_engine: TradingEngine | None = None


def _now_et() -> str:
    return datetime.now(_ET).strftime("%a %b %d %I:%M %p ET")


def is_market_open() -> bool:
    now = datetime.now(timezone.utc)
    today = now.strftime("%Y-%m-%d")
    schedule = _NYSE.schedule(start_date=today, end_date=today)
    if schedule.empty:
        return False
    open_t  = schedule.iloc[0]["market_open"]
    close_t = schedule.iloc[0]["market_close"]
    return open_t <= now <= close_t


def _update_portfolio() -> None:
    if _broker is None:
        return
    try:
        account   = _broker.get_account()
        positions = _broker.get_positions()
        pos_value   = sum(p["market_value"]   for p in positions)
        unrealized  = sum(p["unrealized_pl"]  for p in positions)
        total       = account["portfolio_value"]
        daily_pnl   = (_risk.realized_pnl + unrealized) if _risk else unrealized
        daily_pnl_pct = (daily_pnl / total * 100) if total > 0 else 0.0
        dashboard.push_portfolio({
            "total_value":    total,
            "cash":           account["cash"],
            "positions_value": pos_value,
            "daily_pnl":      daily_pnl,
            "daily_pnl_pct":  daily_pnl_pct,
        })
    except Exception as exc:
        logger.error(f"Portfolio update failed: {exc}")


def _take_portfolio_snapshot() -> None:
    """Called daily at ~16:30 ET after market close."""
    if _broker is None:
        return
    try:
        account   = _broker.get_account()
        positions = _broker.get_positions()
        pos_value = sum(p["market_value"] for p in positions)
        db.save_portfolio_snapshot({
            "total_value":    account["portfolio_value"],
            "cash":           account["cash"],
            "positions_value": pos_value,
        })
        if _risk:
            _risk.reset_daily()
        logger.info("Daily portfolio snapshot saved and P&L reset")
    except Exception as exc:
        logger.error(f"Portfolio snapshot failed: {exc}")


def _log_watchlist(candidates: list[dict]) -> None:
    logger.info("=" * 70)
    logger.info(f"WATCHLIST — top {len(candidates)} scored tickers this cycle")
    logger.info(f"{'#':>3}  {'TICKER':<6}  {'SCORE':>5}  {'TECH%':>5} {'SENT%':>5} {'HIST%':>5}  {'RSI':>5} {'MACD':>4} {'EMA':>3} {'VOL':>5}  PRICE")
    logger.info("-" * 70)
    for i, c in enumerate(candidates, 1):
        comp = c["composite_score"]
        tech = c["technical_score"]
        sent = c["sentiment_score"]
        hist = c["historical_score"]
        if comp > 0:
            tech_pct = round(tech * 0.50 / comp * 100)
            sent_pct = round(sent * 0.30 / comp * 100)
            hist_pct = round(hist * 0.20 / comp * 100)
        else:
            tech_pct = sent_pct = hist_pct = 0
        rsi  = f"{c['rsi']:.1f}" if c.get("rsi") is not None else "  --"
        macd = "Y" if c.get("macd_cross") else "N"
        ema  = "Y" if c.get("ema_reclaim") else "N"
        vol  = f"{c.get('volume_ratio', 0):.1f}x"
        flag = " <-- BUY" if comp >= BUY_THRESHOLD else ""
        logger.info(
            f"{i:>3}. {c['ticker']:<6}  {comp:.3f}  "
            f"{tech_pct:>4}%  {sent_pct:>4}%  {hist_pct:>4}%  "
            f"{rsi:>5} {macd:>4} {ema:>3} {vol:>5}  ${c['current_price']:.2f}{flag}"
        )
    logger.info("=" * 70)


def run_scan() -> None:
    start_ts = time.time()
    logger.info("=== Scan cycle starting ===")
    scan_time = _now_et()

    try:
        market_open = is_market_open()
        dashboard.push_status(market_open=market_open, scan_time=scan_time)

        if not market_open:
            logger.info("Market closed — skipping scan")
            return

        tickers = get_top_movers()
        if not tickers:
            logger.warning("No movers returned by scanner")
            dashboard.push_scan_complete(scan_time, [])
            _update_portfolio()
            return
        logger.info(f"Top movers: {tickers}")

        candidates = []
        with ThreadPoolExecutor(max_workers=5) as executor:
            futures = {executor.submit(get_composite_score, t): t for t in tickers}
            for future in as_completed(futures):
                ticker = futures[future]
                try:
                    result = future.result(timeout=60)
                    if result is not None:
                        candidates.append(result)
                        logger.info(f"{ticker}: composite={result['composite_score']:.3f}")
                    else:
                        logger.info(f"{ticker}: scoring returned None — skipped")
                except Exception as exc:
                    logger.error(f"{ticker}: scoring error — {exc}")

        candidates.sort(key=lambda x: x["composite_score"], reverse=True)

        if candidates:
            _log_watchlist(candidates)

        elapsed = time.time() - start_ts
        logger.info(f"=== Scan complete in {elapsed:.1f}s — {len(candidates)} candidate(s) ===")

        dashboard.push_scan_complete(scan_time, [c["ticker"] for c in candidates])

        if _engine is not None:
            _engine.process_signals(candidates, scan_time)

        _update_portfolio()

    except Exception as exc:
        logger.error(f"Scan cycle failed: {exc}", exc_info=True)


def main() -> None:
    global _broker, _risk, _engine

    logger.info("Trading Signal Bot starting up")

    dashboard.start_dashboard(host="0.0.0.0", port=PORT)

    initialize_finbert()

    # Initialise trading engine
    _broker = BrokerClient(dry_run=dashboard.get_dry_run())
    _risk   = RiskManager(
        daily_loss_limit=DAILY_LOSS_LIMIT,
        daily_profit_target=DAILY_PROFIT_TARGET,
    )
    _engine = TradingEngine(
        broker=_broker, risk=_risk,
        buy_threshold=BUY_THRESHOLD,
        sell_threshold=SELL_THRESHOLD,
        stop_loss_pct=STOP_LOSS_PCT,
        max_position_usd=MAX_POSITION_SIZE,
    )

    # Wire callbacks
    _risk.register_halt_callback(dashboard.push_circuit_breaker)
    _engine.register_trade_callback(dashboard.push_trade)
    dashboard.set_engine(_engine)

    # Initial portfolio read
    _update_portfolio()

    scheduler = BackgroundScheduler()
    scheduler.add_job(
        run_scan,
        trigger="interval",
        minutes=10,
        next_run_time=datetime.now(),
        id="scan_job",
        max_instances=1,
        coalesce=True,
        misfire_grace_time=300,  # skip run if it's >5 min late (e.g. after OOM restart)
    )
    # Daily portfolio snapshot at 16:35 ET (after market close)
    scheduler.add_job(
        _take_portfolio_snapshot,
        trigger="cron",
        hour=16, minute=35,
        timezone=_ET,
        id="snapshot_job",
    )

    scheduler.start()
    logger.info("Scheduler started — scanning every 10 minutes")
    send_startup()

    try:
        while True:
            time.sleep(60)
    except (KeyboardInterrupt, SystemExit):
        logger.info("Shutting down…")
        scheduler.shutdown()
        _take_portfolio_snapshot()
        dashboard.push_shutdown()
        send_shutdown()
        logger.info("Scheduler stopped. Goodbye.")


if __name__ == "__main__":
    main()
