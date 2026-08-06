"""
Trading engine — converts signals into BUY/SELL/HOLD decisions.
"""
import logging
import math
from typing import Callable

import db
from broker import BrokerClient
from risk import RiskManager

logger = logging.getLogger(__name__)


class TradingEngine:
    def __init__(
        self,
        broker: BrokerClient,
        risk: RiskManager,
        buy_threshold: float,
        sell_threshold: float,
        stop_loss_pct: float,
        max_position_usd: float,
    ):
        self.broker = broker
        self.risk = risk
        self.buy_threshold = buy_threshold
        self.sell_threshold = sell_threshold
        self.stop_loss_pct = stop_loss_pct
        self.max_position_usd = max_position_usd
        self._processed_keys: set[str] = set()
        self._trade_callbacks: list[Callable[[dict], None]] = []

    def register_trade_callback(self, cb: Callable[[dict], None]) -> None:
        self._trade_callbacks.append(cb)

    def _fire_trade(self, trade: dict) -> None:
        db.save_trade(trade)
        for cb in self._trade_callbacks:
            try:
                cb(trade)
            except Exception as exc:
                logger.error(f"Trade callback error: {exc}")

    def process_signals(self, signals: list[dict], scan_time: str) -> None:
        if self.risk.is_halted():
            logger.warning("Engine halted — skipping signal processing")
            return

        try:
            positions = {p["ticker"]: p for p in self.broker.get_positions()}
            open_orders = {o["ticker"] for o in self.broker.get_open_orders()}
            account = self.broker.get_account()
            cash = account["cash"]
        except Exception as exc:
            logger.error(f"Engine: broker state fetch failed: {exc}")
            return

        scored = {s["ticker"]: s for s in signals}

        # Exit: sell positions that hit stop-loss or dropped below sell threshold
        for ticker, pos in positions.items():
            if self.risk.is_halted():
                break
            sig = scored.get(ticker)
            score = sig["composite_score"] if sig else 0.0
            price = pos["current_price"]
            entry = pos["avg_entry"]
            pnl_pct = (price - entry) / entry if entry else 0.0

            reason = None
            if pnl_pct <= -self.stop_loss_pct:
                reason = (
                    f"Stop-loss hit — entry ${entry:.2f} → ${price:.2f} "
                    f"({pnl_pct * 100:.1f}%)"
                )
            elif sig is None:
                reason = (
                    f"Not in scan this cycle — exiting position "
                    f"(entry ${entry:.2f}, current ${price:.2f}, {pnl_pct * 100:+.1f}%)"
                )
            elif score < self.sell_threshold:
                reason = (
                    f"Score {score:.3f} below sell threshold {self.sell_threshold} — "
                    f"entry ${entry:.2f}, current ${price:.2f} ({pnl_pct * 100:+.1f}%)"
                )

            if reason and ticker not in open_orders:
                result = self.broker.close_position(ticker)
                if result is not None:
                    realized = pos["unrealized_pl"]
                    self.risk.record_trade_pnl(realized)
                    trade = {
                        "ticker": ticker, "action": "SELL",
                        "qty": int(pos["qty"]), "price": price,
                        "score": score, "reason": reason,
                        "scan_time": scan_time,
                        "dry_run": self.broker.dry_run,
                        "pnl": realized,
                    }
                    self._fire_trade(trade)
                    logger.info(f"SELL {ticker}: {reason} P&L=${realized:+.2f}")

        # Entry: buy new positions for high-scoring candidates
        for sig in signals:
            if self.risk.is_halted():
                break
            ticker = sig["ticker"]
            score = sig["composite_score"]
            price = sig["current_price"]

            if score < self.buy_threshold:
                continue
            if ticker in positions:
                continue
            if ticker in open_orders:
                continue

            key = f"{scan_time}:{ticker}"
            if key in self._processed_keys:
                continue

            qty = math.floor(self.max_position_usd / price) if price > 0 else 0
            if qty < 1:
                logger.info(f"SKIP {ticker}: price ${price:.2f} > max ${self.max_position_usd:.0f}")
                continue
            if qty * price > cash:
                logger.info(f"SKIP {ticker}: insufficient cash (need ${qty*price:.2f}, have ${cash:.2f})")
                continue

            result = self.broker.place_market_buy(ticker, qty)
            if result is not None:
                self._processed_keys.add(key)
                cash -= qty * price
                trade = {
                    "ticker": ticker, "action": "BUY",
                    "qty": qty, "price": price,
                    "score": score,
                    "reason": (
                        f"Score {score:.3f} >= {self.buy_threshold} | "
                        f"tech={sig.get('technical_score', 0):.3f} "
                        f"sent={sig.get('sentiment_score', 0):.3f} "
                        f"hist={sig.get('historical_score', 0):.3f}"
                    ),
                    "scan_time": scan_time,
                    "dry_run": self.broker.dry_run,
                    "pnl": 0.0,
                }
                self._fire_trade(trade)
                logger.info(f"BUY {ticker} x{qty} @ ${price:.2f} score={score:.3f}")

        # Trim idempotency set
        if len(self._processed_keys) > 500:
            self._processed_keys = set(list(self._processed_keys)[-500:])
