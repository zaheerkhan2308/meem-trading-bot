import os
import time
import logging
from dotenv import load_dotenv

load_dotenv()

ALPACA_API_KEY: str = os.getenv("ALPACA_API_KEY", "")
ALPACA_API_SECRET: str = os.getenv("ALPACA_API_SECRET", "")
FINNHUB_API_KEY: str = os.getenv("FINNHUB_API_KEY", "")
TELEGRAM_BOT_TOKEN: str = os.getenv("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT_ID: str = os.getenv("TELEGRAM_CHAT_ID", "")
SIGNAL_THRESHOLD: float = float(os.getenv("SIGNAL_THRESHOLD", "0.72"))
MAX_SIGNALS: int = int(os.getenv("MAX_SIGNALS", "5"))
PORT: int = int(os.getenv("PORT", "8000"))

# Trading engine
TRADING_MODE: str = os.getenv("TRADING_MODE", "paper")
MAX_POSITION_SIZE: float = float(os.getenv("MAX_POSITION_SIZE", "100"))
BUY_THRESHOLD: float = float(os.getenv("BUY_THRESHOLD", "0.72"))
SELL_THRESHOLD: float = float(os.getenv("SELL_THRESHOLD", "0.40"))
STOP_LOSS_PCT: float = float(os.getenv("STOP_LOSS_PCT", "0.06"))
DAILY_LOSS_LIMIT: float = float(os.getenv("DAILY_LOSS_LIMIT", "50.0"))
DAILY_PROFIT_TARGET: float = float(os.getenv("DAILY_PROFIT_TARGET", "50.0"))


def retry_with_backoff(func, max_retries: int = 3):
    """Call func(), retrying up to max_retries times with exponential backoff."""
    delays = [1, 2, 4]
    last_exc = None
    for attempt, delay in enumerate(delays[:max_retries], 1):
        try:
            return func()
        except Exception as exc:
            last_exc = exc
            logging.warning(
                f"Attempt {attempt}/{max_retries} failed: {exc}. "
                f"Retrying in {delay}s..."
            )
            if attempt < max_retries:
                time.sleep(delay)
    raise last_exc
