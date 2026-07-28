import asyncio
import html
import logging
import sys
from datetime import datetime
from zoneinfo import ZoneInfo

from telegram import Bot

from config import TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID

logger = logging.getLogger(__name__)

# Required on Windows: python-telegram-bot uses httpx which needs SelectorEventLoop
if sys.platform == "win32":
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

_ET = ZoneInfo("America/New_York")


def _now_et_str() -> str:
    return datetime.now(_ET).strftime("%a %b %d %I:%M %p ET")


def _h(text: str) -> str:
    """Escape a string for Telegram HTML parse mode."""
    return html.escape(str(text))


def _format_signals(signals: list[dict]) -> str:
    lines = [f"<b>SIGNAL ALERT</b> — {_h(_now_et_str())}", ""]
    for i, s in enumerate(signals, 1):
        headline = s.get("top_headline", "")
        if len(headline) > 100:
            headline = headline[:97] + "..."
        macd_str = "YES" if s.get("macd_cross") else "NO"
        rsi_val = s.get("rsi")
        rsi_str = str(int(rsi_val)) if rsi_val is not None else "N/A"
        vol_ratio = s.get("volume_ratio", 0.0)
        lines += [
            f"{i}. <b>${_h(s['ticker'])}</b>  BUY  |  Score: <b>{s['composite_score']:.2f}</b>",
            f"   RSI: {rsi_str}  |  MACD cross: {macd_str}  |  Vol: {vol_ratio:.1f}x avg",
            f"   Sentiment: {_h(s['sentiment_label'])} ({s['sentiment_score']:.2f})",
            f"   News: <i>{_h(headline)}</i>",
            f"   {s.get('top_url', '')}",
            f"   Entry: ${_h(s['entry_low'])} – ${_h(s['entry_high'])}",
            f"   Stop: ${_h(s['stop_loss'])}  |  Target: ${_h(s['take_profit'])}",
            "",
        ]
    return "\n".join(lines).strip()


def _format_no_signal() -> str:
    return (
        f"<b>SCAN COMPLETE</b> — {_h(_now_et_str())}\n"
        "No strong signals this cycle. Next scan in 10 min."
    )


async def _send_async(text: str) -> None:
    async with Bot(token=TELEGRAM_BOT_TOKEN) as bot:
        await bot.send_message(chat_id=TELEGRAM_CHAT_ID, text=text, parse_mode="HTML")


def _send(text: str) -> None:
    try:
        asyncio.run(_send_async(text))
    except Exception as exc:
        logger.error(f"Telegram send failed: {exc}")


def send_signal(signals: list[dict]) -> None:
    pass  # Telegram disabled — dashboard only


def send_no_signal() -> None:
    pass  # Telegram disabled — dashboard only


def send_startup() -> None:
    pass  # Telegram disabled — dashboard only


def send_shutdown() -> None:
    pass  # Telegram disabled — dashboard only
