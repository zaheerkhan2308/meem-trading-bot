import logging

import requests

from config import FINNHUB_API_KEY, retry_with_backoff

logger = logging.getLogger(__name__)

_FINNHUB_BASE = "https://finnhub.io/api/v1"


def initialize_finbert() -> None:
    """No-op — sentiment now uses Finnhub API instead of a local model."""
    logger.info("Sentiment: using Finnhub news-sentiment API (no local model)")


def get_sentiment_score(ticker: str) -> tuple[float, str, str, str]:
    """
    Returns (score, label, "", "") using Finnhub's pre-computed news-sentiment.
    score is bullishPercent (0–1). Falls back to NEUTRAL on any error.
    """
    try:
        resp = retry_with_backoff(lambda: requests.get(
            f"{_FINNHUB_BASE}/news-sentiment",
            params={"symbol": ticker},
            headers={"X-Finnhub-Token": FINNHUB_API_KEY},
            timeout=10,
        ))
        resp.raise_for_status()
        data = resp.json()

        bullish = data.get("sentiment", {}).get("bullishPercent")
        if bullish is None:
            logger.debug(f"{ticker}: no Finnhub sentiment data — NEUTRAL")
            return (0.5, "NEUTRAL", "", "")

        score = round(float(bullish), 4)
        if score >= 0.65:
            label = "BULLISH"
        elif score <= 0.35:
            label = "BEARISH"
        else:
            label = "NEUTRAL"

        logger.debug(f"{ticker}: sentiment={label} ({score:.3f})")
        return (score, label, "", "")

    except Exception as exc:
        logger.warning(f"{ticker}: Finnhub sentiment failed ({exc}) — NEUTRAL")
        return (0.5, "NEUTRAL", "", "")
