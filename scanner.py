import logging
from datetime import datetime, timedelta, timezone

from alpaca.data.historical import StockHistoricalDataClient
from alpaca.data.requests import StockBarsRequest, StockSnapshotRequest
from alpaca.data.timeframe import TimeFrame
from alpaca.data.enums import DataFeed
from alpaca.trading.client import TradingClient
from alpaca.trading.requests import GetAssetsRequest
from alpaca.trading.enums import AssetClass, AssetStatus

import os
from config import (
    ALPACA_API_KEY,
    ALPACA_API_SECRET,
    retry_with_backoff,
)

logger = logging.getLogger(__name__)

_PAPER = not (
    os.getenv("TRADING_MODE", "paper").lower() == "live"
    and os.getenv("LIVE_TRADING_CONFIRMED", "").lower() == "yes_i_understand"
)

_data_client = StockHistoricalDataClient(ALPACA_API_KEY, ALPACA_API_SECRET)
_trading_client = TradingClient(ALPACA_API_KEY, ALPACA_API_SECRET, paper=_PAPER)

_SNAPSHOT_CHUNK = 500
_PHASE_A_CANDIDATES = 100


def _get_asset_universe() -> list[str]:
    req = GetAssetsRequest(
        asset_class=AssetClass.US_EQUITY,
        status=AssetStatus.ACTIVE,
    )
    assets = retry_with_backoff(lambda: _trading_client.get_all_assets(req))
    symbols = [
        a.symbol
        for a in assets
        if a.tradable
        and "/" not in a.symbol
        and "." not in a.symbol
        and len(a.symbol) <= 5
    ]
    logger.info(f"Asset universe: {len(symbols)} tradeable US equities")
    return symbols


def _fetch_snapshots(symbols: list[str]) -> dict:
    all_snaps: dict = {}
    for i in range(0, len(symbols), _SNAPSHOT_CHUNK):
        chunk = symbols[i : i + _SNAPSHOT_CHUNK]
        req = StockSnapshotRequest(symbol_or_symbols=chunk, feed=DataFeed.IEX)
        snaps = retry_with_backoff(lambda r=req: _data_client.get_stock_snapshot(r))
        all_snaps.update(snaps)
    logger.info(f"Fetched snapshots for {len(all_snaps)} symbols")
    return all_snaps


def _phase_a_filter(snapshots: dict) -> list[dict]:
    candidates = []
    n_no_bars = n_low_price = n_no_prev = 0
    for sym, snap in snapshots.items():
        try:
            latest_trade = snap.latest_trade
            daily_bar = snap.daily_bar
            prev_bar = snap.prev_daily_bar

            if not latest_trade or not daily_bar or not prev_bar:
                n_no_bars += 1
                continue

            price = float(latest_trade.price or 0)
            if price < 5.0:
                n_low_price += 1
                continue

            day_vol = float(daily_bar.volume or 0)
            day_close = float(daily_bar.close or price)
            prev_close = float(prev_bar.close or 0)
            if prev_close <= 0:
                n_no_prev += 1
                continue

            pct_change = (day_close - prev_close) / prev_close * 100

            # Spread filter: only apply when real bid/ask is available.
            # IEX rarely provides quotes, so the daily-range fallback is skipped.
            quote = snap.latest_quote
            if quote and quote.ask_price and quote.bid_price and float(quote.bid_price) > 0:
                ask = float(quote.ask_price)
                bid = float(quote.bid_price)
                spread = (ask - bid) / ((ask + bid) / 2)
                if spread > 0.02:  # relaxed from 0.5% to 2% for testing
                    continue

            candidates.append(
                {
                    "symbol": sym,
                    "price": price,
                    "day_volume": day_vol,
                    "pct_change": pct_change,
                    "raw_score": abs(pct_change) * day_vol,
                }
            )
        except Exception as exc:
            logger.debug(f"Phase A skip {sym}: {exc}")

    candidates.sort(key=lambda x: x["raw_score"], reverse=True)
    top = candidates[:_PHASE_A_CANDIDATES]
    logger.info(
        f"Phase A: {len(snapshots)} snapshots → missing bars={n_no_bars}, "
        f"price<$5={n_low_price}, no prev_close={n_no_prev}, "
        f"passed={len(candidates)}, keeping top {len(top)}"
    )
    return top


def _phase_b_filter(candidates: list[dict]) -> list[str]:
    symbols = [c["symbol"] for c in candidates]
    now = datetime.now(timezone.utc)
    start = now - timedelta(days=30)

    req = StockBarsRequest(
        symbol_or_symbols=symbols,
        timeframe=TimeFrame.Day,
        start=start,
        end=now,
        limit=20,
        feed=DataFeed.IEX,
    )
    daily_bars = retry_with_backoff(lambda: _data_client.get_stock_bars(req))

    qualified = []
    n_no_bars = n_low_vol_ratio = 0
    for c in candidates:
        sym = c["symbol"]
        try:
            bars_df = daily_bars[sym].df if hasattr(daily_bars[sym], "df") else None
            if bars_df is None or bars_df.empty:
                n_no_bars += 1
                logger.debug(f"Phase B {sym}: no historical bars")
                continue

            avg_vol = float(bars_df["volume"].mean())
            if avg_vol <= 0:
                n_no_bars += 1
                continue

            volume_ratio = c["day_volume"] / avg_vol
            logger.debug(
                f"Phase B {sym}: price=${c['price']:.2f} pct={c['pct_change']:.2f}% "
                f"vol_ratio={volume_ratio:.2f} (day={c['day_volume']:.0f} avg={avg_vol:.0f})"
            )
            # Lowered from 1.5x — intraday partial volume is well below a full-day
            # average, so 1.5x would never be reached mid-session.
            if volume_ratio < 0.3:
                n_low_vol_ratio += 1
                continue

            qualified.append(
                {
                    **c,
                    "avg_vol_20d": avg_vol,
                    "volume_ratio": volume_ratio,
                    "final_score": c["pct_change"] * volume_ratio,
                }
            )
        except Exception as exc:
            logger.debug(f"Phase B skip {sym}: {exc}")

    qualified.sort(key=lambda x: x["final_score"], reverse=True)
    result = [q["symbol"] for q in qualified[:50]]
    logger.info(
        f"Phase B: {len(candidates)} in → no_bars={n_no_bars}, "
        f"low_vol_ratio={n_low_vol_ratio}, qualified={len(qualified)}, top {len(result)}: {result}"
    )
    return result


def get_top_movers() -> list[str]:
    """Return up to 10 ticker symbols ranked by momentum x volume ratio."""
    logger.info("Scanner: fetching asset universe...")
    symbols = _get_asset_universe()

    logger.info("Scanner: fetching snapshots...")
    snapshots = _fetch_snapshots(symbols)

    logger.info("Scanner: phase A filtering...")
    phase_a = _phase_a_filter(snapshots)
    if not phase_a:
        logger.warning("Scanner: no candidates survived phase A")
        return []

    logger.info("Scanner: phase B (20-day avg volume) filtering...")
    return _phase_b_filter(phase_a)
