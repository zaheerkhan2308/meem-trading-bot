"""JSON and WebSocket API for the Meem trading runtime.

This module deliberately has no server-rendered UI.  The trading process calls
the public push_* functions below; connected frontend clients receive the same
events over WebSocket.
"""
import asyncio
import logging
import threading

import uvicorn
from fastapi import FastAPI, HTTPException, Request, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from starlette.responses import Response

from . import db
from .config import API_CORS_ORIGINS, KILL_SWITCH_PASSWORD
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
_kill_switch = False
_dry_run = False
_circuit_breaker: str | None = None
_watchlist: dict = {"scan_time": None, "tickers": []}
_ticker_streaks: dict[str, int] = {}
_engine_ref = None
_loop: asyncio.AbstractEventLoop | None = None
_state_loaded = False


def _load_persisted_state() -> None:
    """Hydrate dashboard state for both the full runtime and API-only mode."""
    global _state_loaded
    if _state_loaded:
        return
    _trades[:] = db.load_trades(100, days=2)
    if latest_portfolio := db.load_latest_portfolio():
        _portfolio.update(latest_portfolio)
    if latest_watchlist := db.load_watchlist():
        _watchlist.update(latest_watchlist)
        _ticker_streaks.update({
            ticker["ticker"]: ticker["streak"]
            for ticker in latest_watchlist["tickers"]
            if "streak" in ticker
        })
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
async def portfolio() -> dict:
    return _portfolio


@app.get("/api/status")
async def status() -> dict:
    return _status


@app.get("/api/trades")
async def trades(limit: int = 100, days: int = 2) -> dict:
    if not 1 <= limit <= 500 or not 1 <= days <= 365:
        raise HTTPException(422, "limit must be 1-500 and days must be 1-365")
    return {"trades": await asyncio.to_thread(db.load_trades, limit, days)}


@app.get("/api/scan-log")
async def scan_log(limit: int = 50) -> dict:
    if not 1 <= limit <= 500:
        raise HTTPException(422, "limit must be 1-500")
    return {"entries": await asyncio.to_thread(db.load_scan_log, limit)}


@app.get("/api/watchlist")
async def watchlist() -> dict:
    return _watchlist


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
    if _engine_ref is not None:
        _engine_ref.risk.set_kill_switch(_kill_switch)
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
    _dispatch(_broadcast({"type": "portfolio_update", "portfolio": dict(_portfolio)}))


def push_trade(trade: dict) -> None:
    _trades.append(trade)
    del _trades[:-100]
    _dispatch(_broadcast({"type": "trade_event", "trade": trade}))


def push_circuit_breaker(reason: str) -> None:
    global _circuit_breaker
    _circuit_breaker = reason
    db.save_circuit_breaker_event(reason)
    _dispatch(_broadcast({"type": "circuit_breaker_alert", "reason": reason}))


def push_watchlist(candidates: list[dict], scan_time: str) -> None:
    global _watchlist, _ticker_streaks
    previous = {ticker["ticker"] for ticker in _watchlist.get("tickers", [])}
    streaks: dict[str, int] = {}
    enriched: list[dict] = []
    for candidate in candidates:
        ticker = candidate["ticker"]
        streaks[ticker] = _ticker_streaks.get(ticker, 1) + 1 if ticker in previous else 1
        enriched.append({**candidate, "streak": streaks[ticker]})
    _ticker_streaks = streaks
    _watchlist = {"scan_time": scan_time, "tickers": enriched}
    db.save_watchlist(scan_time, enriched)
    _dispatch(_broadcast({"type": "watchlist_update", "watchlist": _watchlist}))


def set_engine(engine) -> None:
    global _engine_ref
    _engine_ref = engine


def get_kill_switch() -> bool:
    return _kill_switch


def get_dry_run() -> bool:
    return _dry_run


def restore_circuit_breaker() -> None:
    global _circuit_breaker
    reason = db.load_todays_circuit_breaker()
    if reason:
        _circuit_breaker = reason
        if _engine_ref is not None:
            _engine_ref.risk.set_kill_switch(True)
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
