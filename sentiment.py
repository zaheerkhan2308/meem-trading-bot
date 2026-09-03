import logging
from datetime import date, timedelta

import requests

from config import FINNHUB_API_KEY, retry_with_backoff

logger = logging.getLogger(__name__)

_FINNHUB_BASE = "https://finnhub.io/api/v1"

_BULLISH = {
    "upgrade", "upgrades", "upgraded", "beat", "beats", "record", "growth",
    "surge", "surges", "surged", "rally", "rallies", "strong", "strength",
    "buy", "outperform", "outperforms", "positive", "profit", "profits",
    "raised", "raise", "raises", "expansion", "partnership", "breakthrough",
    "momentum", "gain", "gains", "recovery", "exceed", "exceeds", "exceeded",
    "robust", "accelerate", "accelerating", "improve", "improves", "improved",
    "optimistic", "upbeat", "bullish", "soar", "soars", "soared", "jump",
    "jumps", "jumped", "climb", "climbs", "climbed", "top", "tops", "topped",
    "boost", "boosts", "boosted", "demand", "opportunity", "opportunities",
}

_BEARISH = {
    "downgrade", "downgrades", "downgraded", "miss", "misses", "missed",
    "decline", "declines", "declined", "drop", "drops", "dropped", "weak",
    "weakness", "sell", "underperform", "underperforms", "negative", "loss",
    "losses", "layoff", "layoffs", "cut", "cuts", "warning", "warns",
    "lawsuit", "investigation", "recall", "recalls", "debt", "bankruptcy",
    "default", "concern", "concerns", "risk", "risks", "disappointing",
    "disappoint", "disappoints", "below", "lowered", "lower", "reduce",
    "reduces", "reduced", "bearish", "fall", "falls", "fell", "slump",
    "slumps", "slumped", "slide", "slides", "slid", "plunge", "plunges",
    "plunged", "halt", "halted", "suspend", "suspended", "probe", "fraud",
    "penalty", "fine", "fined", "charges", "charged",
}


def initialize_finbert() -> None:
    logger.info("Sentiment: using Finnhub company-news with keyword scoring (free tier)")


def _score_text(text: str) -> float:
    """Score a single headline/summary. Returns 1.0 (bullish), 0.0 (bearish), or 0.5 (neutral)."""
    words = text.lower().split()
    bull = sum(1 for w in words if w.strip(".,!?;:\"'()") in _BULLISH)
    bear = sum(1 for w in words if w.strip(".,!?;:\"'()") in _BEARISH)
    if bull > bear:
        return 1.0
    if bear > bull:
        return 0.0
    return 0.5


def get_sentiment_score(ticker: str) -> tuple[float, str, str, str]:
    """
    Returns (score, label, "", "") using keyword scoring on Finnhub company-news.
    score is a bullish ratio (0–1). Falls back to NEUTRAL on any error.
    """
    try:
        to_date = date.today()
        from_date = to_date - timedelta(days=7)

        resp = retry_with_backoff(lambda: requests.get(
            f"{_FINNHUB_BASE}/company-news",
            params={
                "symbol": ticker,
                "from": from_date.isoformat(),
                "to": to_date.isoformat(),
            },
            headers={"X-Finnhub-Token": FINNHUB_API_KEY},
            timeout=10,
        ))
        resp.raise_for_status()
        articles = resp.json()

        if not articles:
            logger.debug(f"{ticker}: no recent news — NEUTRAL")
            return (0.5, "NEUTRAL", "", "")

        scores = []
        for a in articles[:20]:
            text = (a.get("headline") or "") + " " + (a.get("summary") or "")
            if text.strip():
                scores.append(_score_text(text))

        if not scores:
            return (0.5, "NEUTRAL", "", "")

        score = round(sum(scores) / len(scores), 4)

        if score >= 0.65:
            label = "BULLISH"
        elif score <= 0.35:
            label = "BEARISH"
        else:
            label = "NEUTRAL"

        logger.debug(f"{ticker}: sentiment={label} ({score:.3f}) from {len(scores)} articles")
        return (score, label, "", "")

    except Exception as exc:
        logger.warning(f"{ticker}: Finnhub news failed ({exc}) — NEUTRAL")
        return (0.5, "NEUTRAL", "", "")
