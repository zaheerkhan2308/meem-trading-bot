"""
Standalone backtester. Run: python backtester.py
Uses yfinance only — no Alpaca/Finnhub API keys required.
Simulates 10-min scan cycles on last 5 trading days of 1-min bars.
"""
import logging
import sys
import os

# Configure logging before importing signals (which uses getLogger)
logging.basicConfig(
    level=logging.WARNING,
    format="[%(asctime)s] %(levelname)s — %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[logging.StreamHandler()],
)

# Suppress noisy third-party logs during backtest
logging.getLogger("yfinance").setLevel(logging.ERROR)
logging.getLogger("urllib3").setLevel(logging.ERROR)
logging.getLogger("peewee").setLevel(logging.ERROR)

import pandas as pd
import yfinance as yf

# Add project dir to path so we can import signals
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from signals import compute_technical_score

TICKERS = ["AAPL", "MSFT", "NVDA", "TSLA", "META", "AMZN", "GOOGL", "NFLX", "AMD", "INTC"]
SIGNAL_THRESHOLD = 0.50   # Lower than live threshold to generate meaningful backtest stats
WINDOW_BARS = 60          # Bars fed to compute_technical_score
SCAN_STRIDE = 10          # Simulate a scan every 10 bars (10 minutes)
FORWARD_BARS = 30         # Check outcome 30 min after signal
WIN_PCT = 0.01            # +1% = win
LOSS_PCT = -0.01          # -1% = loss


def download_data() -> dict[str, pd.DataFrame]:
    print(f"Downloading 1-min bars for {len(TICKERS)} tickers (last 8 calendar days)...")
    raw = yf.download(
        TICKERS,
        period="8d",
        interval="1m",
        group_by="ticker",
        auto_adjust=True,
        progress=False,
    )
    result: dict[str, pd.DataFrame] = {}
    for ticker in TICKERS:
        try:
            if isinstance(raw.columns, pd.MultiIndex):
                df = raw[ticker].copy()
            else:
                df = raw.copy()
            df = df.dropna(how="all")
            df.columns = [c.lower() for c in df.columns]
            # Keep only market-hours rows (9:30–16:00 ET)
            df.index = pd.to_datetime(df.index)
            if df.index.tz is not None:
                df = df.tz_convert("America/New_York")
            else:
                df = df.tz_localize("UTC").tz_convert("America/New_York")
            mask = (
                (df.index.hour > 9) | (df.index.hour == 9) & (df.index.minute >= 30)
            ) & (df.index.hour < 16)
            df = df[mask].dropna(subset=["close", "volume"])
            result[ticker] = df
            print(f"  {ticker}: {len(df)} bars across last 5 trading days")
        except Exception as exc:
            print(f"  {ticker}: download failed — {exc}")
    return result


def simulate_ticker(ticker: str, df: pd.DataFrame) -> tuple[int, int, int, list[dict]]:
    """Returns (total_signals, wins, losses, signal_log)."""
    total = wins = losses = 0
    log = []

    for start in range(WINDOW_BARS, len(df) - FORWARD_BARS, SCAN_STRIDE):
        window = df.iloc[start - WINDOW_BARS : start].copy()

        if len(window) < 15:
            continue

        try:
            result = compute_technical_score(window)
            score = result["score"]
        except Exception:
            continue

        if score < SIGNAL_THRESHOLD:
            continue

        entry_price = float(df["close"].iloc[start - 1])
        future_idx = start + FORWARD_BARS - 1
        if future_idx >= len(df):
            continue

        exit_price = float(df["close"].iloc[future_idx])
        if entry_price <= 0:
            continue

        pct_move = (exit_price - entry_price) / entry_price
        total += 1

        outcome = "NEUTRAL"
        if pct_move > WIN_PCT:
            wins += 1
            outcome = "WIN"
        elif pct_move < LOSS_PCT:
            losses += 1
            outcome = "LOSS"

        log.append({
            "ticker": ticker,
            "timestamp": str(df.index[start - 1]),
            "score": round(score, 3),
            "pct_move": round(pct_move * 100, 2),
            "outcome": outcome,
        })

    return total, wins, losses, log


def main() -> None:
    print("\n" + "=" * 60)
    print("TRADING SIGNAL BOT — BACKTESTER")
    print("=" * 60)

    data = download_data()
    if not data:
        print("No data downloaded. Exiting.")
        sys.exit(1)

    all_signals: list[dict] = []
    grand_total = grand_wins = grand_losses = 0

    print("\nSimulating scan cycles...\n")
    for ticker, df in data.items():
        if df.empty or len(df) < WINDOW_BARS + FORWARD_BARS:
            print(f"  {ticker}: not enough bars, skipping")
            continue

        total, wins, losses, log = simulate_ticker(ticker, df)
        grand_total += total
        grand_wins += wins
        grand_losses += losses
        all_signals.extend(log)

        win_rate = (wins / total * 100) if total > 0 else 0.0
        print(f"  {ticker}: {total} signals | Wins: {wins} | Losses: {losses} | Win rate: {win_rate:.1f}%")

    # Sample fired signals
    if all_signals:
        print("\n--- Sample signals fired (up to 10) ---")
        for s in all_signals[:10]:
            print(
                f"  {s['timestamp']} | {s['ticker']} | score={s['score']} "
                f"| +{s['pct_move']}% in 30min | {s['outcome']}"
            )

    grand_win_rate = (grand_wins / grand_total * 100) if grand_total > 0 else 0.0
    grand_neutrals = grand_total - grand_wins - grand_losses

    print("\n" + "=" * 60)
    print("BACKTEST SUMMARY")
    print("=" * 60)
    print(f"Tickers:               {', '.join(TICKERS)}")
    print(f"Signal threshold:      {SIGNAL_THRESHOLD}")
    print(f"Outcome window:        30 minutes | >{WIN_PCT*100:.0f}%=WIN, <{LOSS_PCT*100:.0f}%=LOSS")
    print(f"Total signals fired:   {grand_total}")
    print(f"Wins  (>+1% in 30min): {grand_wins}")
    print(f"Losses(<-1% in 30min): {grand_losses}")
    print(f"Neutral:               {grand_neutrals}")
    print(f"Win rate:              {grand_win_rate:.1f}%")
    print("=" * 60 + "\n")


if __name__ == "__main__":
    main()
