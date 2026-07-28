import logging
from datetime import datetime, timedelta, timezone

import pandas as pd

from alpaca.data.historical import StockHistoricalDataClient
from alpaca.data.requests import StockBarsRequest, StockSnapshotRequest
from alpaca.data.timeframe import TimeFrame
from alpaca.data.enums import DataFeed

from config import ALPACA_API_KEY, ALPACA_API_SECRET, SIGNAL_THRESHOLD, retry_with_backoff
from signals import get_technical_score
from sentiment import get_sentiment_score

logger = logging.getLogger(__name__)

_data_client = StockHistoricalDataClient(ALPACA_API_KEY, ALPACA_API_SECRET)


def _get_current_price(ticker: str) -> float:
    req = StockSnapshotRequest(symbol_or_symbols=[ticker], feed=DataFeed.IEX)
    snaps = retry_with_backoff(lambda: _data_client.get_stock_snapshot(req))
    snap = snaps.get(ticker)
    if snap and snap.latest_trade:
        return float(snap.latest_trade.price)
    return 0.0


def _get_52w_range(ticker: str) -> tuple[float, float]:
    now = datetime.now(timezone.utc)
    start = now - timedelta(days=365)
    req = StockBarsRequest(
        symbol_or_symbols=ticker,
        timeframe=TimeFrame.Day,
        start=start,
        end=now,
        limit=252,
        feed=DataFeed.IEX,
    )
    bars = retry_with_backoff(lambda: _data_client.get_stock_bars(req))
    df = bars.df
    if isinstance(df.index, pd.MultiIndex):
        df = df.xs(ticker, level="symbol")
    if df.empty:
        return (0.0, 0.0)
    return (float(df["high"].max()), float(df["low"].min()))


def get_composite_score(ticker: str) -> dict | None:
    """
    Score a single ticker. Returns None if below threshold or no fresh news.
    """
    logger.info(f"Scoring {ticker}...")

    # Technical score
    try:
        tech_result = get_technical_score(ticker)
        tech_score = tech_result["score"]
    except Exception as exc:
        logger.error(f"{ticker}: technical score failed: {exc}")
        return None

    # Sentiment score
    try:
        sentiment_score, sentiment_label, top_headline, top_url = get_sentiment_score(ticker)
    except Exception as exc:
        logger.error(f"{ticker}: sentiment score failed: {exc}")
        sentiment_score, sentiment_label, top_headline, top_url = 0.0, "NO_NEWS", "", ""

    # Gate: no news → skip
    if sentiment_label == "NO_NEWS":
        logger.debug(f"{ticker}: gated out (NO_NEWS)")
        return None

    # Current price + 52-week range
    try:
        current_price = _get_current_price(ticker)
    except Exception as exc:
        logger.error(f"{ticker}: price fetch failed: {exc}")
        return None

    if current_price <= 0:
        logger.warning(f"{ticker}: invalid price {current_price}")
        return None

    try:
        high_52w, low_52w = _get_52w_range(ticker)
    except Exception as exc:
        logger.warning(f"{ticker}: 52w range failed: {exc}, defaulting historical=0.5")
        high_52w, low_52w = 0.0, 0.0

    if high_52w == low_52w or high_52w <= 0:
        historical_score = 0.5
    else:
        raw = 1.0 - ((current_price - low_52w) / (high_52w - low_52w))
        historical_score = max(0.0, min(1.0, raw))

    composite = (
        tech_score * 0.45
        + sentiment_score * 0.35
        + historical_score * 0.20
    )
    composite = round(composite, 4)

    if composite < SIGNAL_THRESHOLD:
        logger.debug(f"{ticker}: composite {composite:.3f} below threshold {SIGNAL_THRESHOLD}")
        return None

    stop_loss = round(current_price * 0.975, 2)
    # 2:1 risk/reward: stop is 2.5% down, take-profit is 5% up
    take_profit = round(current_price * 1.05, 2)

    return {
        "ticker": ticker,
        "composite_score": composite,
        "technical_score": round(tech_score, 4),
        "sentiment_score": round(sentiment_score, 4),
        "historical_score": round(historical_score, 4),
        "sentiment_label": sentiment_label,
        "top_headline": top_headline,
        "top_url": top_url,
        "rsi": tech_result.get("rsi"),
        "macd_cross": tech_result.get("macd_cross", False),
        "ema_reclaim": tech_result.get("ema_reclaim", False),
        "volume_ratio": tech_result.get("volume_ratio", 0.0),
        "current_price": round(current_price, 2),
        "entry_low": round(current_price * 0.998, 2),
        "entry_high": round(current_price * 1.002, 2),
        "stop_loss": stop_loss,
        "take_profit": take_profit,
    }
