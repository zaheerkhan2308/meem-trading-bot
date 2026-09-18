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

from .logging_config import configure_logging
from .logging_config import instrument_module

configure_logging()

from .config import (
    API_HOST, PORT,
    BUY_THRESHOLD, SELL_THRESHOLD, STOP_LOSS_PCT, TRAILING_STOP_PCT,
    MAX_POSITION_SIZE, MAX_POSITIONS, MAX_CAPITAL,
    DAILY_LOSS_LIMIT, DAILY_PROFIT_TARGET,
)
from .scanner import get_top_movers
from .scorer import get_composite_score
from .sentiment import initialize_finbert
from .notifier import send_startup, send_shutdown
from .broker import BrokerClient
from .risk import RiskManager
from .engine import TradingEngine
from . import api as dashboard
from . import db

logger = logging.getLogger(__name__)

_NYSE = mcal.get_calendar("NYSE")
_ET = ZoneInfo("America/New_York")

# Initialized after FinBERT loads
_broker: BrokerClient | None = None
_risk: RiskManager | None = None
_engine: TradingEngine | None = None
_selected_risk: RiskManager | None = None
_selected_engine: TradingEngine | None = None


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
        total       = account["portfolio_value"]
        last_equity = account["last_equity"]
        daily_pnl   = total - last_equity
        daily_pnl_pct = (daily_pnl / last_equity * 100) if last_equity > 0 else 0.0
        dashboard.push_portfolio({
            "total_value":    total,
            "cash":           account["cash"],
            "positions_value": pos_value,
            "daily_pnl":      daily_pnl,
            "daily_pnl_pct":  daily_pnl_pct,
        })
        dashboard.update_strategy_portfolios(positions)
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


def _score_tickers(tickers: list[str]) -> list[dict]:
    candidates = []
    with ThreadPoolExecutor(max_workers=5) as executor:
        futures = {executor.submit(get_composite_score, ticker): ticker for ticker in tickers}
        for future in as_completed(futures):
            ticker = futures[future]
            try:
                result = future.result(timeout=60)
                if result is not None:
                    candidates.append(result)
                    logger.info("%s: composite=%.3f", ticker, result["composite_score"])
            except Exception as exc:
                logger.error("%s: scoring error — %s", ticker, exc)
    candidates.sort(key=lambda item: item["composite_score"], reverse=True)
    return candidates
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
        selection = dashboard.get_trading_settings()

        if not market_open:
            if selection["mode"] == "manual" and selection["tickers"]:
                selected_candidates = _score_tickers(selection["tickers"])
                dashboard.push_selected_watchlist(selected_candidates, scan_time)
            logger.info("Market closed — skipping scan")
            return

        with ThreadPoolExecutor(max_workers=2) as scan_executor:
            default_tickers_future = scan_executor.submit(get_top_movers)
            selected_tickers = dashboard.get_strategy_tickers("selected")
            if not selected_tickers and selection["mode"] == "manual":
                selected_tickers = selection["tickers"]
            default_tickers = default_tickers_future.result()
            default_candidates_future = scan_executor.submit(_score_tickers, default_tickers)
            selected_candidates_future = (
                scan_executor.submit(_score_tickers, selected_tickers)
                if selected_tickers else None
            )
            default_candidates = default_candidates_future.result()
            selected_candidates = selected_candidates_future.result() if selected_candidates_future else []

        if not default_tickers and not selected_tickers:
            logger.warning("No movers returned by scanner")
            dashboard.push_scan_complete(scan_time, [])
            _update_portfolio()
            return
        _log_watchlist(default_candidates)
        dashboard.push_watchlist(default_candidates[:10], scan_time)
        dashboard.push_selected_watchlist(selected_candidates, scan_time)

        elapsed = time.time() - start_ts
        logger.info(
            "=== Scan complete in %.1fs — default=%d selected=%d candidate(s) ===",
            elapsed, len(default_candidates), len(selected_candidates),
        )

        dashboard.push_scan_complete(
            scan_time,
            [c["ticker"] for c in default_candidates + selected_candidates],
        )

        if _engine is not None:
            _engine.process_signals(default_candidates, scan_time)
        if _selected_engine is not None and selected_candidates:
            _selected_engine.process_signals(selected_candidates, scan_time)

        _update_portfolio()

    except Exception as exc:
        logger.error(f"Scan cycle failed: {exc}", exc_info=True)


def main() -> None:
    global _broker, _risk, _engine, _selected_risk, _selected_engine

    logger.info("Trading Signal Bot starting up")

    dashboard.start_api(host=API_HOST, port=PORT)

    initialize_finbert()

    # Initialise trading engine
    _broker = BrokerClient(dry_run=dashboard.get_dry_run())
    default_config = dashboard.get_strategy_config("default")
    selected_config = dashboard.get_strategy_config("selected")
    _risk   = RiskManager(
        daily_loss_limit=default_config["DAILY_LOSS_LIMIT"],
        daily_profit_target=default_config["DAILY_PROFIT_TARGET"],
    )
    _engine = TradingEngine(
        broker=_broker, risk=_risk,
        buy_threshold=default_config["BUY_THRESHOLD"],
        sell_threshold=default_config["SELL_THRESHOLD"],
        stop_loss_pct=default_config["STOP_LOSS_PCT"],
        max_position_usd=default_config["MAX_POSITION_SIZE"],
        max_positions=default_config["MAX_POSITIONS"],
        max_capital=default_config["MAX_CAPITAL"],
        trailing_stop_pct=default_config["TRAILING_STOP_PCT"],
        strategy="default",
    )
    _selected_risk = RiskManager(
        daily_loss_limit=selected_config["DAILY_LOSS_LIMIT"],
        daily_profit_target=selected_config["DAILY_PROFIT_TARGET"],
    )
    _selected_engine = TradingEngine(
        broker=_broker,
        risk=_selected_risk,
        buy_threshold=selected_config["BUY_THRESHOLD"],
        sell_threshold=selected_config["SELL_THRESHOLD"],
        stop_loss_pct=selected_config["STOP_LOSS_PCT"],
        max_position_usd=selected_config["MAX_POSITION_SIZE"],
        max_positions=selected_config["MAX_POSITIONS"],
        max_capital=selected_config["MAX_CAPITAL"],
        trailing_stop_pct=selected_config["TRAILING_STOP_PCT"],
        strategy="selected",
    )

    # Wire callbacks
    _risk.register_halt_callback(dashboard.push_circuit_breaker)
    _selected_risk.register_halt_callback(dashboard.push_circuit_breaker)
    _engine.register_trade_callback(dashboard.push_trade)
    _selected_engine.register_trade_callback(dashboard.push_trade)
    dashboard.set_engines([_engine, _selected_engine])

    # Restore today's circuit breaker state (survives Railway redeploys)
    dashboard.restore_circuit_breaker()

    # Initial portfolio read
    _update_portfolio()

    scheduler = BackgroundScheduler()
    scheduler.add_job(
        run_scan,
        trigger="interval",
        minutes=5,
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
    logger.info("Scheduler started — scanning every 5 minutes")
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


    instrument_module(globals())


if __name__ == "__main__":
    main()
