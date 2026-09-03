"""
BrokerClient — thin Alpaca wrapper.
Paper trading by default. Live requires TRADING_MODE=live AND
LIVE_TRADING_CONFIRMED=yes_i_understand in .env.
"""
import logging
import os

from alpaca.trading.client import TradingClient
from alpaca.trading.enums import OrderSide, QueryOrderStatus, TimeInForce
from alpaca.trading.requests import GetOrdersRequest, MarketOrderRequest
from dotenv import load_dotenv

from config import ALPACA_API_KEY, ALPACA_API_SECRET

load_dotenv()
logger = logging.getLogger(__name__)

_MODE = os.getenv("TRADING_MODE", "paper").lower()
_CONFIRMED = os.getenv("LIVE_TRADING_CONFIRMED", "").lower()
_PAPER = not (_MODE == "live" and _CONFIRMED == "yes_i_understand")


class BrokerClient:
    def __init__(self, dry_run: bool = False):
        self.dry_run = dry_run
        self._client = TradingClient(ALPACA_API_KEY, ALPACA_API_SECRET, paper=_PAPER)
        mode = "DRY-RUN" if dry_run else ("PAPER" if _PAPER else "LIVE")
        logger.info(f"BrokerClient initialized [{mode}]")

    # ── Account ──────────────────────────────────────────────────────────

    def get_account(self) -> dict:
        acct = self._client.get_account()
        return {
            "cash":            float(acct.cash),
            "equity":          float(acct.equity),
            "portfolio_value": float(acct.portfolio_value),
            "buying_power":    float(acct.buying_power),
        }

    # ── Positions ─────────────────────────────────────────────────────────

    def get_positions(self) -> list[dict]:
        try:
            positions = self._client.get_all_positions()
            return [
                {
                    "ticker":         p.symbol,
                    "qty":            float(p.qty),
                    "avg_entry":      float(p.avg_entry_price),
                    "current_price":  float(p.current_price),
                    "market_value":   float(p.market_value),
                    "unrealized_pl":  float(p.unrealized_pl),
                    "unrealized_plpc": float(p.unrealized_plpc),
                }
                for p in positions
            ]
        except Exception as exc:
            logger.error(f"get_positions failed: {exc}")
            return []

    # ── Open orders ───────────────────────────────────────────────────────

    def get_open_orders(self) -> list[dict]:
        try:
            orders = self._client.get_orders(
                filter=GetOrdersRequest(status=QueryOrderStatus.OPEN)
            )
            return [
                {"id": str(o.id), "ticker": o.symbol,
                 "side": str(o.side), "qty": float(o.qty)}
                for o in orders
            ]
        except Exception as exc:
            logger.error(f"get_open_orders failed: {exc}")
            return []

    # ── Orders ────────────────────────────────────────────────────────────

    def place_market_buy(self, ticker: str, notional: float) -> dict | None:
        if self.dry_run:
            logger.info(f"[DRY-RUN] BUY ${notional:.2f} of {ticker}")
            return {"dry_run": True, "ticker": ticker, "notional": notional, "side": "buy"}
        try:
            req = MarketOrderRequest(
                symbol=ticker, notional=round(notional, 2),
                side=OrderSide.BUY, time_in_force=TimeInForce.DAY,
            )
            order = self._client.submit_order(req)
            logger.info(f"BUY order submitted: {ticker} ${notional:.2f} id={order.id}")
            return {"id": str(order.id), "ticker": ticker, "notional": notional, "side": "buy"}
        except Exception as exc:
            logger.error(f"BUY {ticker} failed: {exc}")
            return None

    def place_market_sell(self, ticker: str, qty: int) -> dict | None:
        if self.dry_run:
            logger.info(f"[DRY-RUN] SELL {qty}x {ticker}")
            return {"dry_run": True, "ticker": ticker, "qty": qty, "side": "sell"}
        try:
            req = MarketOrderRequest(
                symbol=ticker, qty=qty,
                side=OrderSide.SELL, time_in_force=TimeInForce.DAY,
            )
            order = self._client.submit_order(req)
            logger.info(f"SELL order submitted: {ticker} x{qty} id={order.id}")
            return {"id": str(order.id), "ticker": ticker, "qty": qty, "side": "sell"}
        except Exception as exc:
            logger.error(f"SELL {ticker} failed: {exc}")
            return None

    def close_position(self, ticker: str) -> dict | None:
        if self.dry_run:
            logger.info(f"[DRY-RUN] CLOSE position {ticker}")
            return {"dry_run": True, "ticker": ticker, "side": "sell"}
        try:
            self._client.close_position(ticker)
            logger.info(f"Position closed: {ticker}")
            return {"ticker": ticker, "side": "sell"}
        except Exception as exc:
            logger.error(f"CLOSE {ticker} failed: {exc}")
            return None
