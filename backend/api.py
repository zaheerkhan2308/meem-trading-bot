"""JSON and WebSocket API for the Meem trading runtime.

This module deliberately has no server-rendered UI.  The trading process calls
the public push_* functions below; connected frontend clients receive the same
events over WebSocket.
"""
import asyncio
import logging
import threading
from datetime import datetime

import uvicorn
from fastapi import FastAPI, HTTPException, Request, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from starlette.responses import Response

from . import db
from .config import (
    API_CORS_ORIGINS, BUY_THRESHOLD, DAILY_LOSS_LIMIT, DAILY_PROFIT_TARGET,
    KILL_SWITCH_PASSWORD, MAX_CAPITAL, MAX_POSITION_SIZE, MAX_POSITIONS,
    SELL_THRESHOLD, STOP_LOSS_PCT, TRAILING_STOP_PCT,
)
from .scanner import search_tickers, validate_tickers
from .scorer import get_composite_score
from .logging_config import _safe_value

logger = logging.getLogger(__name__)

app = FastAPI(title="Meem API", version="1.0.0")
app.add_middleware(
    CORSMiddleware,
    allow_origins=API_CORS_ORIGINS,
    allow_credentials=True,
    allow_methods=["GET", "POST", "OPTIONS"],
    allow_headers=["Content-Type", "Authorization"],
)


@app.middleware("http")
async def log_http_call(request: Request, call_next):
    body = await request.body()
    params = dict(request.query_params)
    if body:
        params["body"] = body.decode("utf-8", errors="replace")
    logger.info(
        "HTTP CALL %s %s params=%s",
        request.method, request.url.path, _safe_value(params),
    )
    try:
        response = await call_next(request)
        chunks = [chunk async for chunk in response.body_iterator]
        response_body = b"".join(chunks)
        level = logger.warning if response.status_code >= 400 else logger.info
        level(
            "HTTP RETURN %s %s status=%d response=%s",
            request.method, request.url.path, response.status_code,
            _safe_value(response_body.decode("utf-8", errors="replace")),
        )
        return Response(
            content=response_body,
            status_code=response.status_code,
            headers=dict(response.headers),
            media_type=response.media_type,
        )
    except Exception:
        logger.error("HTTP ERROR %s %s", request.method, request.url.path, exc_info=True)
        raise

_connections: set[WebSocket] = set()
_trades: list[dict] = []
_status: dict = {"running": True, "market_open": False, "last_scan": None}
_portfolio: dict = {
    "total_value": 0.0, "cash": 0.0, "positions_value": 0.0,
    "daily_pnl": 0.0, "daily_pnl_pct": 0.0,
}
_strategy_portfolios: dict[str, dict] = {
    "default": dict(_portfolio),
    "selected": dict(_portfolio),
}
_kill_switch = False
_dry_run = False
_circuit_breaker: str | None = None
_watchlist: dict = {"scan_time": None, "tickers": []}
_selected_watchlist: dict = {"scan_time": None, "tickers": []}
_ticker_streaks: dict[str, int] = {}
_engine_refs: list = []
_loop: asyncio.AbstractEventLoop | None = None
_state_loaded = False
_trading_settings: dict = {"mode": "default", "tickers": []}
_strategy_settings: dict[str, dict] = {
    "default": {"strategy": "default", "tickers": [], "overrides": {}},
    "selected": {"strategy": "selected", "tickers": [], "overrides": {}},
}
_base_strategy_config = {
    "MAX_POSITION_SIZE": MAX_POSITION_SIZE,
    "MAX_POSITIONS": MAX_POSITIONS,
    "MAX_CAPITAL": MAX_CAPITAL,
    "BUY_THRESHOLD": BUY_THRESHOLD,
    "SELL_THRESHOLD": SELL_THRESHOLD,
    "STOP_LOSS_PCT": STOP_LOSS_PCT,
    "TRAILING_STOP_PCT": TRAILING_STOP_PCT,
    "DAILY_LOSS_LIMIT": DAILY_LOSS_LIMIT,
    "DAILY_PROFIT_TARGET": DAILY_PROFIT_TARGET,
}


def _load_persisted_state() -> None:
    """Hydrate dashboard state for both the full runtime and API-only mode."""
    global _state_loaded
    if _state_loaded:
        return
    _trades[:] = db.load_trades(100, days=2)
    for trade in _trades:
        strategy = trade.get("strategy", "default")
        if strategy not in _strategy_portfolios:
            continue
        if trade["action"] == "BUY":
            _strategy_portfolios[strategy]["positions_value"] += trade.get("qty", 0) * trade.get("price", 0)
        elif trade["action"] == "SELL":
            _strategy_portfolios[strategy]["positions_value"] -= trade.get("qty", 0) * trade.get("price", 0)
        _strategy_portfolios[strategy]["daily_pnl"] += trade.get("pnl", 0)
    if latest_portfolio := db.load_latest_portfolio():
        _portfolio.update(latest_portfolio)
    if latest_watchlist := db.load_watchlist():
        _watchlist.update(latest_watchlist)
        _ticker_streaks.update({
            ticker["ticker"]: ticker["streak"]
            for ticker in latest_watchlist["tickers"]
            if "streak" in ticker
        })
    if latest_selected_watchlist := db.load_selected_watchlist():
        _selected_watchlist.update(latest_selected_watchlist)
    _trading_settings.update(db.load_trading_settings())
    for strategy in ("default", "selected"):
        loaded = db.load_strategy_settings(strategy)
        loaded["overrides"] = {**_base_strategy_config, **loaded.get("overrides", {})}
        _strategy_settings[strategy].update(loaded)
    _state_loaded = True
    logger.info(
        "Loaded persisted dashboard state: %d trade(s), %d watchlist item(s)",
        len(_trades), len(_watchlist["tickers"]),
    )


@app.on_event("startup")
async def load_persisted_state_on_startup() -> None:
    """Make `python -m backend.run_api` useful with existing Neon data."""
    await asyncio.to_thread(_load_persisted_state)


@app.get("/")
@app.get("/health")
async def health() -> dict:
    """JSON health response; no server-rendered dashboard is served here."""
    return {"ok": True, "service": "meem-api", "running": _status["running"]}


@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket) -> None:
    logger.info("WS CALL path=%s client=%s", websocket.url.path, websocket.client)
    await websocket.accept()
    _connections.add(websocket)
    try:
        snapshot = _snapshot()
        await websocket.send_json(snapshot)
        logger.info("WS RETURN path=%s response=%s", websocket.url.path, _safe_value(snapshot))
        while True:
            await websocket.receive_text()
    except WebSocketDisconnect:
        logger.info("WS RETURN path=%s disconnected", websocket.url.path)
        pass
    except Exception:
        logger.error("WS ERROR path=%s", websocket.url.path, exc_info=True)
        raise
    finally:
        _connections.discard(websocket)


@app.get("/api/portfolio")
async def portfolio(strategy: str = "default") -> dict:
    if strategy == "shared":
        return _portfolio
    if strategy not in _strategy_portfolios:
        raise HTTPException(422, "strategy must be shared, default, or selected")
    return _strategy_portfolios[strategy]


@app.get("/api/status")
async def status() -> dict:
    return _status


@app.get("/api/settings")
async def settings() -> dict:
    return _trading_settings


@app.get("/api/strategy-settings")
async def strategy_settings(strategy: str = "default") -> dict:
    if strategy not in _strategy_settings:
        raise HTTPException(422, "strategy must be default or selected")
    return _strategy_settings[strategy]


@app.post("/api/strategy-settings")
async def update_strategy_settings(request: Request) -> dict:
    data = await request.json()
    strategy = str(data.get("strategy", "default")).lower()
    if strategy not in _strategy_settings:
        raise HTTPException(422, "strategy must be default or selected")
    tickers = sorted({str(ticker).strip().upper() for ticker in data.get("tickers", []) if str(ticker).strip()})
    invalid = await asyncio.to_thread(validate_tickers, tickers)
    if invalid:
        raise HTTPException(422, f"Unsupported ticker(s): {', '.join(invalid)}")
    overrides = data.get("overrides", {}) or {}
    cleaned = {}
    for key, value in overrides.items():
        if key not in _base_strategy_config:
            raise HTTPException(422, f"Unsupported strategy setting: {key}")
        cleaned[key] = int(value) if key == "MAX_POSITIONS" else float(value)
    previous_tickers = set(_strategy_settings[strategy].get("tickers", []))
    added = sorted(set(tickers) - previous_tickers)
    removed = sorted(previous_tickers - set(tickers))
    saved = db.save_strategy_settings(strategy, tickers, cleaned)
    db.save_stock_positions_history(strategy, added, "ADDED")
    db.save_stock_positions_history(strategy, removed, "REMOVED")
    saved["overrides"] = {**_base_strategy_config, **cleaned}
    _strategy_settings[strategy] = saved
    for engine in _engine_refs:
        if engine.strategy == strategy:
            config = saved["overrides"]
            engine.buy_threshold = config["BUY_THRESHOLD"]
            engine.sell_threshold = config["SELL_THRESHOLD"]
            engine.stop_loss_pct = config["STOP_LOSS_PCT"]
            engine.max_position_usd = config["MAX_POSITION_SIZE"]
            engine.max_positions = config["MAX_POSITIONS"]
            engine.max_capital = config["MAX_CAPITAL"]
            engine.trailing_stop_pct = config["TRAILING_STOP_PCT"]
            engine.risk.daily_loss_limit = config["DAILY_LOSS_LIMIT"]
            engine.risk.daily_profit_target = config["DAILY_PROFIT_TARGET"]
    _dispatch(_broadcast({"type": "strategy_settings_update", "strategy_settings": _strategy_settings}))
    if strategy == "selected":
        asyncio.create_task(_refresh_selected_watchlist(tickers))
    return saved


@app.get("/api/stock-positions-history")
async def stock_positions_history(strategy: str = "default") -> dict:
    if strategy not in {"default", "selected"}:
        raise HTTPException(422, "strategy must be default or selected")
    return {"entries": await asyncio.to_thread(db.load_stock_positions_history, strategy)}


async def _refresh_selected_watchlist(tickers: list[str]) -> None:
    if not tickers:
        push_selected_watchlist([], "")
        return
    results = await asyncio.gather(
        *(asyncio.to_thread(get_composite_score, ticker) for ticker in tickers),
        return_exceptions=True,
    )
    candidates = [result for result in results if isinstance(result, dict)]
    candidates.sort(key=lambda item: item.get("composite_score", 0), reverse=True)
    push_selected_watchlist(candidates, datetime.now().astimezone().strftime("%a %b %d %I:%M %p ET"))


def get_strategy_config(strategy: str) -> dict:
    return {**_base_strategy_config, **_strategy_settings[strategy].get("overrides", {})}


def get_strategy_tickers(strategy: str) -> list[str]:
    return list(_strategy_settings[strategy].get("tickers", []))


@app.get("/api/ticker-search")
async def ticker_search(q: str = "") -> dict:
    return {"results": await asyncio.to_thread(search_tickers, q)}


@app.post("/api/settings")
async def update_settings(request: Request) -> dict:
    global _trading_settings
    data = await request.json()
    mode = str(data.get("mode", "default")).lower()
    tickers = sorted({str(ticker).strip().upper() for ticker in data.get("tickers", []) if str(ticker).strip()})
    if mode not in {"default", "manual"}:
        raise HTTPException(422, "mode must be default or manual")
    if mode == "manual" and not tickers:
        raise HTTPException(422, "manual mode requires at least one ticker")
    invalid = await asyncio.to_thread(validate_tickers, tickers)
    if invalid:
        raise HTTPException(422, f"Unsupported ticker(s): {', '.join(invalid)}")
    db.save_trading_settings(mode, tickers)
    _trading_settings = {"mode": mode, "tickers": tickers}
    if mode == "default":
        global _selected_watchlist
        _selected_watchlist = {"scan_time": None, "tickers": []}
        db.clear_selected_watchlist()
        _dispatch(_broadcast({
            "type": "selected_watchlist_update",
            "selected_watchlist": _selected_watchlist,
        }))
    _dispatch(_broadcast({"type": "settings_update", "settings": _trading_settings}))
    return _trading_settings


@app.get("/api/trades")
async def trades(limit: int = 100, days: int = 2, strategy: str | None = None) -> dict:
    if not 1 <= limit <= 500 or not 1 <= days <= 365:
        raise HTTPException(422, "limit must be 1-500 and days must be 1-365")
    if strategy not in {None, "default", "selected"}:
        raise HTTPException(422, "strategy must be default or selected")
    return {"trades": await asyncio.to_thread(db.load_trades, limit, days, strategy)}


@app.get("/api/scan-log")
async def scan_log(limit: int = 50) -> dict:
    if not 1 <= limit <= 500:
        raise HTTPException(422, "limit must be 1-500")
    return {"entries": await asyncio.to_thread(db.load_scan_log, limit)}


@app.get("/api/watchlist")
async def watchlist() -> dict:
    return _watchlist


@app.get("/api/selected-watchlist")
async def selected_watchlist() -> dict:
    return _selected_watchlist


@app.get("/api/circuit-breaker")
async def circuit_breaker() -> dict:
    return {"active": _circuit_breaker is not None, "reason": _circuit_breaker}


@app.get("/api/chart")
async def chart(days: int = 365) -> dict:
    if not 1 <= days <= 3650:
        raise HTTPException(422, "days must be 1-3650")
    return {"snapshots": await asyncio.to_thread(db.load_portfolio_snapshots, days)}


@app.post("/api/kill-switch")
async def set_kill_switch(request: Request) -> dict:
    global _kill_switch, _circuit_breaker
    data = await request.json()
    if data.get("password") != KILL_SWITCH_PASSWORD:
        raise HTTPException(401, "Invalid password")
    _kill_switch = bool(data.get("active", False))
    if not _kill_switch:
        _circuit_breaker = None
    for engine in _engine_refs:
        engine.risk.set_kill_switch(_kill_switch)
    _dispatch(_broadcast(_control_message()))
    return {"ok": True, "kill_switch": _kill_switch}


@app.post("/api/dry-run")
async def set_dry_run(request: Request) -> dict:
    global _dry_run
    data = await request.json()
    _dry_run = bool(data.get("active", False))
    if _engine_ref is not None:
        _engine_ref.broker.dry_run = _dry_run
    _dispatch(_broadcast(_control_message()))
    return {"ok": True, "dry_run": _dry_run}


async def _broadcast(message: dict) -> None:
    dead: set[WebSocket] = set()
    for ws in list(_connections):
        try:
            await ws.send_json(message)
        except Exception:
            dead.add(ws)
    _connections.difference_update(dead)


def _dispatch(coro) -> None:
    if _loop and not _loop.is_closed():
        asyncio.run_coroutine_threadsafe(coro, _loop)


def _snapshot() -> dict:
    return {
        "type": "init", "status": _status, "trades": _trades,
        "portfolio": _portfolio, "kill_switch": _kill_switch,
        "dry_run": _dry_run, "circuit_breaker": _circuit_breaker,
        "watchlist": _watchlist,
        "selected_watchlist": _selected_watchlist,
    }


def _control_message() -> dict:
    return {"type": "control_state", "kill_switch": _kill_switch,
            "dry_run": _dry_run, "circuit_breaker": _circuit_breaker}


# Public callback API used by backend.main. Keep this stable while the trading
# modules remain unchanged.
def push_scan_complete(scan_time: str, tickers: list[str]) -> None:
    _status["last_scan"] = scan_time
    _dispatch(_broadcast({"type": "scan_complete", "status": _status, "tickers": tickers}))


def push_status(market_open: bool, scan_time: str) -> None:
    _status.update(market_open=market_open, last_scan=scan_time)
    _dispatch(_broadcast({"type": "status", "status": _status}))


def push_shutdown() -> None:
    _status["running"] = False
    _dispatch(_broadcast({"type": "shutdown", "status": _status}))


def push_portfolio(portfolio_state: dict) -> None:
    _portfolio.update(portfolio_state)
    for strategy_portfolio in _strategy_portfolios.values():
        strategy_portfolio.update({
            "total_value": _portfolio["total_value"],
            "cash": _portfolio["cash"],
        })
    _dispatch(_broadcast({
        "type": "portfolio_update",
        "portfolio": dict(_portfolio),
        "strategy_portfolios": _strategy_portfolios,
    }))


def update_strategy_portfolios(positions: list[dict]) -> None:
    """Calculate separate strategy equity from tagged trades and live positions."""
    trades_by_strategy = {
        strategy: db.load_trades(500, days=365, strategy=strategy)
        for strategy in ("default", "selected")
    }
    quantities: dict[str, dict[str, float]] = {}
    for strategy, trades in trades_by_strategy.items():
        quantities[strategy] = {}
        for trade in trades:
            sign = 1 if trade["action"] == "BUY" else -1
            quantities[strategy][trade["ticker"]] = (
                quantities[strategy].get(trade["ticker"], 0.0)
                + sign * float(trade["qty"])
            )

    position_values = {strategy: 0.0 for strategy in quantities}
    for position in positions:
        ticker = position["ticker"]
        total_qty = sum(max(0.0, quantities[strategy].get(ticker, 0.0)) for strategy in quantities)
        if total_qty <= 0:
            continue
        for strategy in quantities:
            strategy_qty = max(0.0, quantities[strategy].get(ticker, 0.0))
            position_values[strategy] += float(position["market_value"]) * strategy_qty / total_qty

    for strategy, trades in trades_by_strategy.items():
        buy_cost = sum(float(t["qty"]) * float(t.get("price") or 0) for t in trades if t["action"] == "BUY")
        sell_proceeds = sum(float(t["qty"]) * float(t.get("price") or 0) for t in trades if t["action"] == "SELL")
        realized_pnl = sum(float(t.get("pnl") or 0) for t in trades)
        cash = MAX_CAPITAL - buy_cost + sell_proceeds
        total_value = cash + position_values[strategy]
        daily_pnl = realized_pnl + position_values[strategy] - max(0.0, buy_cost - sell_proceeds)
        _strategy_portfolios[strategy].update({
            "total_value": total_value,
            "cash": cash,
            "positions_value": position_values[strategy],
            "daily_pnl": daily_pnl,
            "daily_pnl_pct": daily_pnl / MAX_CAPITAL * 100 if MAX_CAPITAL else 0.0,
        })
    _dispatch(_broadcast({"type": "strategy_portfolio_update", "strategy_portfolios": _strategy_portfolios}))


def push_trade(trade: dict) -> None:
    _trades.append(trade)
    del _trades[:-100]
    strategy = trade.get("strategy", "default")
    if strategy in _strategy_portfolios:
        if trade["action"] == "BUY":
            _strategy_portfolios[strategy]["positions_value"] += trade.get("qty", 0) * trade.get("price", 0)
        elif trade["action"] == "SELL":
            _strategy_portfolios[strategy]["positions_value"] -= trade.get("qty", 0) * trade.get("price", 0)
        _strategy_portfolios[strategy]["daily_pnl"] += trade.get("pnl", 0)
    _dispatch(_broadcast({"type": "trade_event", "trade": trade}))


def push_circuit_breaker(reason: str) -> None:
    global _circuit_breaker
    _circuit_breaker = reason
    db.save_circuit_breaker_event(reason)
    _dispatch(_broadcast({"type": "circuit_breaker_alert", "reason": reason}))


def push_watchlist(candidates: list[dict], scan_time: str, limit: int = 10) -> None:
    global _watchlist, _ticker_streaks
    previous = {ticker["ticker"] for ticker in _watchlist.get("tickers", [])}
    streaks: dict[str, int] = {}
    enriched: list[dict] = []
    for candidate in candidates:
        ticker = candidate["ticker"]
        streaks[ticker] = _ticker_streaks.get(ticker, 1) + 1 if ticker in previous else 1
        enriched.append({**candidate, "streak": streaks[ticker]})
    _ticker_streaks = streaks
    _watchlist = {"scan_time": scan_time, "tickers": enriched[:limit], "limit": limit}
    db.save_watchlist(scan_time, enriched)
    _dispatch(_broadcast({"type": "watchlist_update", "watchlist": _watchlist}))


def push_selected_watchlist(candidates: list[dict], scan_time: str) -> None:
    global _selected_watchlist
    _selected_watchlist = {"scan_time": scan_time, "tickers": candidates}
    db.save_selected_watchlist(scan_time, candidates)
    _dispatch(_broadcast({
        "type": "selected_watchlist_update",
        "selected_watchlist": _selected_watchlist,
    }))


def set_engine(engine) -> None:
    set_engines([engine])


def set_engines(engines: list) -> None:
    global _engine_refs
    _engine_refs = engines


def get_kill_switch() -> bool:
    return _kill_switch


def get_dry_run() -> bool:
    return _dry_run


def get_trading_settings() -> dict:
    return _trading_settings


def restore_circuit_breaker() -> None:
    global _circuit_breaker
    reason = db.load_todays_circuit_breaker()
    if reason:
        _circuit_breaker = reason
        for engine in _engine_refs:
            engine.risk.set_kill_switch(True)
        logger.warning("Restored today's circuit breaker: %s", reason)


def start_api(host: str = "0.0.0.0", port: int = 8000) -> None:
    """Start the API alongside the existing trading scheduler."""
    db.init_tables()
    _load_persisted_state()

    def run() -> None:
        global _loop
        _loop = asyncio.new_event_loop()
        asyncio.set_event_loop(_loop)
        server = uvicorn.Server(uvicorn.Config(app, host=host, port=port, loop="none", log_level="warning"))
        _loop.run_until_complete(server.serve())

    threading.Thread(target=run, daemon=True, name="meem-api").start()
    logger.info("Meem API running at http://%s:%s", host, port)


from .logging_config import instrument_module

instrument_module(globals())
