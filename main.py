import logging
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone

import pandas_market_calendars as mcal
from apscheduler.schedulers.background import BackgroundScheduler
from zoneinfo import ZoneInfo

# Configure logging before any module imports so all loggers inherit this setup
logging.basicConfig(
    level=logging.INFO,
    format="[%(asctime)s] %(levelname)s — %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[
        logging.FileHandler("trading_bot.log", encoding="utf-8"),
        logging.StreamHandler(),
    ],
)

from config import MAX_SIGNALS, PORT
from scanner import get_top_movers
from scorer import get_composite_score
from sentiment import initialize_finbert
from notifier import send_signal, send_no_signal, send_startup, send_shutdown
import dashboard

logger = logging.getLogger(__name__)

_NYSE = mcal.get_calendar("NYSE")
_ET = ZoneInfo("America/New_York")


def _now_et() -> str:
    return datetime.now(_ET).strftime("%a %b %d %I:%M %p ET")


def is_market_open() -> bool:
    now = datetime.now(timezone.utc)
    today = now.strftime("%Y-%m-%d")
    schedule = _NYSE.schedule(start_date=today, end_date=today)
    if schedule.empty:
        return False
    open_t = schedule.iloc[0]["market_open"]
    close_t = schedule.iloc[0]["market_close"]
    return open_t <= now <= close_t


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

        # Step 1: get top movers
        tickers = get_top_movers()
        if not tickers:
            logger.warning("No movers returned by scanner")
            send_no_signal()
            dashboard.push_no_signal(scan_time)
            return
        logger.info(f"Top movers: {tickers}")

        # Step 2: score each ticker in parallel
        signals = []
        with ThreadPoolExecutor(max_workers=5) as executor:
            futures = {executor.submit(get_composite_score, t): t for t in tickers}
            for future in as_completed(futures):
                ticker = futures[future]
                try:
                    result = future.result(timeout=60)
                    if result is not None:
                        signals.append(result)
                        logger.info(
                            f"{ticker}: composite={result['composite_score']:.3f} PASS"
                        )
                    else:
                        logger.info(f"{ticker}: below threshold — skipped")
                except Exception as exc:
                    logger.error(f"{ticker}: scoring error — {exc}")

        # Step 3: sort by composite score, cap at MAX_SIGNALS
        signals.sort(key=lambda x: x["composite_score"], reverse=True)
        signals = signals[:MAX_SIGNALS]

        elapsed = time.time() - start_ts
        logger.info(
            f"=== Scan complete in {elapsed:.1f}s — "
            f"{len(signals)} signal(s) firing ==="
        )

        if signals:
            send_signal(signals)
            dashboard.push_signals(signals, scan_time)
        else:
            send_no_signal()
            dashboard.push_no_signal(scan_time)

    except Exception as exc:
        logger.error(f"Scan cycle failed: {exc}", exc_info=True)


def main() -> None:
    logger.info("Trading Signal Bot starting up")

    dashboard.start_dashboard(host="0.0.0.0", port=PORT)

    logger.info("Loading FinBERT — this may take 1-2 minutes on first run...")
    initialize_finbert()

    scheduler = BackgroundScheduler()
    scheduler.add_job(
        run_scan,
        trigger="interval",
        minutes=10,
        next_run_time=datetime.now(),
        id="scan_job",
        max_instances=1,
        coalesce=True,
    )
    scheduler.start()
    logger.info("Scheduler started — scanning every 10 minutes")
    send_startup()

    try:
        while True:
            time.sleep(60)
    except (KeyboardInterrupt, SystemExit):
        logger.info("Shutting down...")
        scheduler.shutdown()
        dashboard.push_shutdown()
        send_shutdown()
        logger.info("Scheduler stopped. Goodbye.")


if __name__ == "__main__":
    main()
