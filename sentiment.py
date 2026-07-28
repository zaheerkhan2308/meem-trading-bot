import logging
import threading
import time
from datetime import datetime, timezone

import requests

from config import FINNHUB_API_KEY, retry_with_backoff

logger = logging.getLogger(__name__)

_finbert_pipeline = None
_lock = threading.Lock()

_FINNHUB_BASE = "https://finnhub.io/api/v1"
_FRESH_SECS = 4 * 3600   # 4-hour freshness window
_LOOKBACK_SECS = 24 * 3600  # fetch last 24h from Finnhub


def initialize_finbert() -> None:
    """Load ProsusAI/finbert once at startup. Must be called before any scan."""
    global _finbert_pipeline
    from transformers import pipeline as hf_pipeline

    logger.info("Loading FinBERT model (ProsusAI/finbert)...")
    _finbert_pipeline = hf_pipeline(
        "text-classification",
        model="ProsusAI/finbert",
        truncation=True,
        max_length=512,
        top_k=None,
    )
    logger.info("FinBERT loaded successfully.")


def _fetch_headlines(ticker: str) -> list[dict]:
    now = int(time.time())
    from_ts = now - _LOOKBACK_SECS
    from_date = datetime.utcfromtimestamp(from_ts).strftime("%Y-%m-%d")
    to_date = datetime.utcfromtimestamp(now).strftime("%Y-%m-%d")

    url = f"{_FINNHUB_BASE}/company-news"
    params = {"symbol": ticker, "from": from_date, "to": to_date}
    headers = {"X-Finnhub-Token": FINNHUB_API_KEY}

    resp = retry_with_backoff(
        lambda: requests.get(url, params=params, headers=headers, timeout=10)
    )
    resp.raise_for_status()
    articles = resp.json()
    # Keep up to 5 most recent
    articles = sorted(articles, key=lambda a: a.get("datetime", 0), reverse=True)[:5]
    return articles


def _label_to_score(label: str) -> float:
    label = label.lower()
    if label == "positive":
        return 1.0
    if label == "negative":
        return 0.0
    return 0.5  # neutral


def _run_finbert(texts: list[str]) -> list[dict]:
    """Thread-safe FinBERT inference. Returns list of {label, score} for best label."""
    with _lock:
        raw = _finbert_pipeline(texts)
    # raw is list[list[dict]] when top_k=None; each inner list has pos/neu/neg
    results = []
    for item in raw:
        # Pick the label with highest confidence
        best = max(item, key=lambda x: x["score"])
        results.append(best)
    return results


def get_sentiment_score(ticker: str) -> tuple[float, str, str, str]:
    """
    Returns (score, label, top_headline, top_url).
    score=0.0 and label=NO_NEWS when no fresh headlines found.
    """
    if _finbert_pipeline is None:
        logger.warning(f"{ticker}: FinBERT not initialized, returning NO_NEWS")
        return (0.0, "NO_NEWS", "", "")

    try:
        articles = _fetch_headlines(ticker)
    except Exception as exc:
        logger.warning(f"{ticker}: Finnhub fetch failed ({exc}), treating as NO_NEWS")
        return (0.0, "NO_NEWS", "", "")

    now_ts = int(time.time())
    fresh = [a for a in articles if now_ts - a.get("datetime", 0) <= _FRESH_SECS]

    if not fresh:
        logger.debug(f"{ticker}: no headlines in last 4h")
        return (0.0, "NO_NEWS", "", "")

    headlines = [a.get("headline", "") for a in fresh if a.get("headline")]
    top_headline = headlines[0][:150] if headlines else ""
    top_url = fresh[0].get("url", "") if fresh else ""

    if not headlines:
        return (0.0, "NO_NEWS", "", "")

    weighted_scores = []
    try:
        finbert_results = _run_finbert(headlines)
        for res in finbert_results:
            label_score = _label_to_score(res["label"])
            confidence = float(res["score"])
            weighted_scores.append(label_score * confidence)
    except Exception as exc:
        logger.warning(f"{ticker}: FinBERT inference error: {exc}")
        if not weighted_scores:
            return (0.0, "NO_NEWS", "", "")

    if not weighted_scores:
        return (0.0, "NO_NEWS", "", "")

    avg = sum(weighted_scores) / len(weighted_scores)

    if avg >= 0.65:
        sentiment_label = "BULLISH"
    elif avg <= 0.35:
        sentiment_label = "BEARISH"
    else:
        sentiment_label = "NEUTRAL"

    return (round(avg, 4), sentiment_label, top_headline, top_url)
