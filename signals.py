import logging
from datetime import datetime, timedelta, timezone

import pandas as pd
import pandas_ta as ta

from alpaca.data.historical import StockHistoricalDataClient
from alpaca.data.requests import StockBarsRequest
from alpaca.data.timeframe import TimeFrame
from alpaca.data.enums import DataFeed

from config import ALPACA_API_KEY, ALPACA_API_SECRET, retry_with_backoff

logger = logging.getLogger(__name__)

_data_client = StockHistoricalDataClient(ALPACA_API_KEY, ALPACA_API_SECRET)

_RSI_WEIGHT = 0.35
_MACD_WEIGHT = 0.35
_EMA_WEIGHT = 0.20
_VOL_WEIGHT = 0.10


def _safe_float(val) -> float:
    try:
        v = float(val)
        return v if pd.notna(v) else 0.0
    except Exception:
        return 0.0


def compute_technical_score(df: pd.DataFrame) -> dict:
    """
    Pure pandas-ta computation on an OHLCV DataFrame.
    Returns a dict with score (0-1) and individual indicator values.
    No network calls — safe to call from backtester.
    """
    df = df.copy()
    df.columns = [c.lower() for c in df.columns]

    result = {
        "score": 0.0,
        "rsi": None,
        "macd_cross": False,
        "ema_reclaim": False,
        "volume_ratio": 0.0,
    }

    if len(df) < 15:
        return result

    score = 0.0

    # RSI(14)
    try:
        df.ta.rsi(length=14, append=True)
        rsi_col = "RSI_14"
        rsi_val = _safe_float(df[rsi_col].iloc[-1]) if rsi_col in df.columns else 0.0
        result["rsi"] = round(rsi_val, 1)
        if 28 < rsi_val < 55:
            score += _RSI_WEIGHT
    except Exception as exc:
        logger.debug(f"RSI error: {exc}")

    # MACD
    try:
        df.ta.macd(fast=12, slow=26, signal=9, append=True)
        macd_col = "MACD_12_26_9"
        sig_col = "MACDs_12_26_9"
        if macd_col in df.columns and sig_col in df.columns:
            macd_cross = False
            for i in range(-3, 0):
                m_cur = _safe_float(df[macd_col].iloc[i])
                s_cur = _safe_float(df[sig_col].iloc[i])
                m_prev = _safe_float(df[macd_col].iloc[i - 1])
                s_prev = _safe_float(df[sig_col].iloc[i - 1])
                if m_cur > s_cur and m_prev <= s_prev:
                    macd_cross = True
                    break
            result["macd_cross"] = macd_cross
            if macd_cross:
                score += _MACD_WEIGHT
    except Exception as exc:
        logger.debug(f"MACD error: {exc}")

    # EMA20 reclaim
    try:
        df.ta.ema(length=20, append=True)
        ema_col = "EMA_20"
        if ema_col in df.columns:
            ema_reclaim = False
            for i in range(-3, 0):
                c_cur = _safe_float(df["close"].iloc[i])
                e_cur = _safe_float(df[ema_col].iloc[i])
                c_prev = _safe_float(df["close"].iloc[i - 1])
                e_prev = _safe_float(df[ema_col].iloc[i - 1])
                if e_cur > 0 and e_prev > 0:
                    if c_cur > e_cur and c_prev <= e_prev:
                        ema_reclaim = True
                        break
            result["ema_reclaim"] = ema_reclaim
            if ema_reclaim:
                score += _EMA_WEIGHT
    except Exception as exc:
        logger.debug(f"EMA error: {exc}")

    # Volume spike: current bar > 2x avg of last 20 bars
    try:
        if "volume" in df.columns and len(df) >= 20:
            current_vol = _safe_float(df["volume"].iloc[-1])
            avg_vol = _safe_float(df["volume"].iloc[-20:].mean())
            vol_ratio = current_vol / avg_vol if avg_vol > 0 else 0.0
            result["volume_ratio"] = round(vol_ratio, 2)
            if vol_ratio > 2.0:
                score += _VOL_WEIGHT
    except Exception as exc:
        logger.debug(f"Volume error: {exc}")

    result["score"] = round(min(score, 1.0), 4)
    return result


def get_technical_score(ticker: str) -> dict:
    """Fetch 60 x 1-min bars from Alpaca, then compute technical score."""
    now = datetime.now(timezone.utc)
    start = now - timedelta(hours=3)

    req = StockBarsRequest(
        symbol_or_symbols=ticker,
        timeframe=TimeFrame.Minute,
        start=start,
        end=now,
        limit=60,
        feed=DataFeed.IEX,
    )
    bars = retry_with_backoff(lambda: _data_client.get_stock_bars(req))

    df = bars.df
    if isinstance(df.index, pd.MultiIndex):
        df = df.xs(ticker, level="symbol")

    if df.empty or len(df) < 15:
        logger.warning(f"{ticker}: insufficient bars ({len(df)})")
        return {"score": 0.0, "rsi": None, "macd_cross": False, "ema_reclaim": False, "volume_ratio": 0.0}

    return compute_technical_score(df)
