"""
Daily P&L tracker, kill switch, and circuit breakers.
"""
import logging
from typing import Callable

logger = logging.getLogger(__name__)


class RiskManager:
    def __init__(self, daily_loss_limit: float, daily_profit_target: float):
        self.daily_loss_limit = daily_loss_limit
        self.daily_profit_target = daily_profit_target
        self._realized_pnl: float = 0.0
        self._kill_switch: bool = False
        self._halt_reason: str | None = None
        self._halt_callbacks: list[Callable[[str], None]] = []

    def register_halt_callback(self, cb: Callable[[str], None]) -> None:
        self._halt_callbacks.append(cb)

    def _fire_halt(self, reason: str) -> None:
        self._halt_reason = reason
        logger.warning(f"CIRCUIT BREAKER TRIGGERED: {reason}")
        for cb in self._halt_callbacks:
            try:
                cb(reason)
            except Exception as exc:
                logger.error(f"Halt callback error: {exc}")

    def record_trade_pnl(self, pnl: float) -> None:
        self._realized_pnl += pnl
        logger.info(f"Realized P&L updated: ${self._realized_pnl:+.2f}")
        if self._kill_switch:
            return
        if self._realized_pnl <= -self.daily_loss_limit:
            self._kill_switch = True
            self._fire_halt(f"Daily loss limit hit: ${self._realized_pnl:.2f}")
        elif self._realized_pnl >= self.daily_profit_target:
            self._kill_switch = True
            self._fire_halt(f"Daily profit target hit: ${self._realized_pnl:.2f}")

    def is_halted(self) -> bool:
        return self._kill_switch

    @property
    def halt_reason(self) -> str | None:
        return self._halt_reason

    @property
    def realized_pnl(self) -> float:
        return self._realized_pnl

    def set_kill_switch(self, active: bool) -> None:
        self._kill_switch = active
        if active:
            self._halt_reason = "Manual kill switch"
            logger.warning("Kill switch manually activated")
        else:
            self._halt_reason = None
            logger.info("Kill switch manually disabled")

    def reset_daily(self) -> None:
        self._realized_pnl = 0.0
        if not self._kill_switch:
            self._halt_reason = None
        logger.info("Daily P&L reset")
