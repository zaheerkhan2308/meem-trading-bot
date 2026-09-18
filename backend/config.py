import os
import time
import logging
from pathlib import Path

from dotenv import load_dotenv

_ENV_FILE = Path(os.getenv("MEEM_ENV_FILE", ".env.local"))
if not _ENV_FILE.is_absolute():
    _ENV_FILE = Path(__file__).resolve().parents[1] / _ENV_FILE
if not _ENV_FILE.is_file():
    raise RuntimeError(f"Environment file not found: {_ENV_FILE}")
load_dotenv(_ENV_FILE, override=True)


def _required(name: str) -> str:
    value = os.getenv(name)
    if value is None:
        raise RuntimeError(f"Missing required setting {name} in {_ENV_FILE}")
    return value


def _required_int(name: str) -> int:
    return int(_required(name))


def _required_float(name: str) -> float:
    return float(_required(name))

ALPACA_API_KEY: str = _required("ALPACA_API_KEY")
ALPACA_API_SECRET: str = _required("ALPACA_API_SECRET")
FINNHUB_API_KEY: str = _required("FINNHUB_API_KEY")
TELEGRAM_BOT_TOKEN: str = _required("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID: str = _required("TELEGRAM_CHAT_ID")
API_HOST: str = _required("API_HOST")
PORT: int = _required_int("PORT")
API_CORS_ORIGINS: list[str] = [
    origin.strip() for origin in _required("API_CORS_ORIGINS").split(",")
    if origin.strip()
]
NEON_DATABASE_URL: str = _required("NEON_DATABASE_URL")
DATABASE_URL: str = _required("DATABASE_URL")
LIVE_TRADING_CONFIRMED: str = _required("LIVE_TRADING_CONFIRMED")

# Trading engine
TRADING_MODE: str = _required("TRADING_MODE")
MAX_POSITION_SIZE: float = _required_float("MAX_POSITION_SIZE")
MAX_POSITIONS: int = _required_int("MAX_POSITIONS")
MAX_CAPITAL: float = _required_float("MAX_CAPITAL")
BUY_THRESHOLD: float = _required_float("BUY_THRESHOLD")
SELL_THRESHOLD: float = _required_float("SELL_THRESHOLD")
STOP_LOSS_PCT: float = _required_float("STOP_LOSS_PCT")
TRAILING_STOP_PCT: float = _required_float("TRAILING_STOP_PCT")
DAILY_LOSS_LIMIT: float = _required_float("DAILY_LOSS_LIMIT")
DAILY_PROFIT_TARGET: float = _required_float("DAILY_PROFIT_TARGET")
KILL_SWITCH_PASSWORD: str = _required("KILL_SWITCH_PASSWORD")

from .logging_config import instrument_module


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


instrument_module(globals())
