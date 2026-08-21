"""
Live dashboard — FastAPI + WebSocket.
Runs in a daemon thread alongside APScheduler.
Public API called from main.py:
  push_scan_complete(), push_status(), push_shutdown()
  push_portfolio(), push_trade(), push_circuit_breaker()
  set_engine(), get_kill_switch(), get_dry_run()
"""
import asyncio
import logging
import threading
from datetime import datetime
from zoneinfo import ZoneInfo

import uvicorn
from fastapi import FastAPI, Request as _Request, WebSocket, WebSocketDisconnect
from fastapi.responses import HTMLResponse, JSONResponse
import db

_ET = ZoneInfo("America/New_York")

def _now_et() -> str:
    return datetime.now(_ET).strftime("%a %b %d %I:%M %p ET")

logger = logging.getLogger(__name__)

_app = FastAPI()
_connections: set[WebSocket] = set()

# ── In-memory state ────────────────────────────────────────────────────────
_scan_log: list[dict] = []
_trades: list[dict] = []
_status: dict = {"running": True, "market_open": False, "last_scan": None}
_portfolio: dict = {
    "total_value": 0.0, "cash": 0.0,
    "positions_value": 0.0,
    "daily_pnl": 0.0, "daily_pnl_pct": 0.0,
}
_kill_switch: bool = False
_dry_run: bool = False
_circuit_breaker: str | None = None
_watchlist: dict = {"scan_time": None, "tickers": []}
_engine_ref = None
_loop: asyncio.AbstractEventLoop | None = None


def _append_log(entry: dict) -> None:
    _scan_log.append(entry)
    if len(_scan_log) > 50:
        _scan_log[:] = _scan_log[-50:]


# ── WebSocket ──────────────────────────────────────────────────────────────

@_app.websocket("/ws")
async def ws_endpoint(websocket: WebSocket):
    await websocket.accept()
    _connections.add(websocket)
    try:
        await websocket.send_json({
            "type":            "init",
            "status":          _status,
            "scan_log":        _scan_log,
            "trades":          _trades[-50:],
            "portfolio":       _portfolio,
            "kill_switch":     _kill_switch,
            "dry_run":         _dry_run,
            "circuit_breaker": _circuit_breaker,
            "watchlist":       _watchlist,
        })
        while True:
            await websocket.receive_text()
    except WebSocketDisconnect:
        pass
    finally:
        _connections.discard(websocket)


# ── REST endpoints ─────────────────────────────────────────────────────────

@_app.get("/", response_class=HTMLResponse)
async def index():
    return HTMLResponse(_HTML)


@_app.post("/api/kill-switch")
async def api_kill_switch(request: _Request):
    global _kill_switch, _circuit_breaker
    data = await request.json()
    _kill_switch = bool(data.get("active", False))
    if not _kill_switch:
        # Re-enabling: clear the circuit breaker banner so a page refresh
        # doesn't re-show it and re-activate the kill switch in the UI.
        _circuit_breaker = None
    if _engine_ref is not None:
        _engine_ref.risk.set_kill_switch(_kill_switch)
    _dispatch(_broadcast({
        "type": "control_state",
        "kill_switch": _kill_switch,
        "dry_run": _dry_run,
        "circuit_breaker": _circuit_breaker,
    }))
    return {"ok": True, "kill_switch": _kill_switch}


@_app.post("/api/dry-run")
async def api_dry_run(request: _Request):
    global _dry_run
    data = await request.json()
    _dry_run = bool(data.get("active", False))
    if _engine_ref is not None:
        _engine_ref.broker.dry_run = _dry_run
    _dispatch(_broadcast({
        "type": "control_state",
        "kill_switch": _kill_switch,
        "dry_run": _dry_run,
    }))
    return {"ok": True, "dry_run": _dry_run}


@_app.get("/api/chart")
async def api_chart():
    snapshots = db.load_portfolio_snapshots(365)
    return JSONResponse({"snapshots": snapshots})


# ── Broadcast helpers ──────────────────────────────────────────────────────

async def _broadcast(message: dict) -> None:
    dead = set()
    for ws in list(_connections):
        try:
            await ws.send_json(message)
        except Exception:
            dead.add(ws)
    _connections.difference_update(dead)


def _dispatch(coro) -> None:
    if _loop and not _loop.is_closed():
        asyncio.run_coroutine_threadsafe(coro, _loop)


# ── Public API (called from main.py) ──────────────────────────────────────

def push_scan_complete(scan_time: str, tickers: list[str]) -> None:
    _status["last_scan"] = scan_time
    entry = {"time": scan_time, "type": "scan", "count": len(tickers), "tickers": tickers}
    _append_log(entry)
    db.save_scan_log(scan_time, "scan", len(tickers), tickers)
    _dispatch(_broadcast({"type": "scan_complete", "status": _status, "log_entry": entry}))


def push_status(market_open: bool, scan_time: str) -> None:
    _status["market_open"] = market_open
    _status["last_scan"] = scan_time
    _dispatch(_broadcast({"type": "status", "status": _status}))


def push_shutdown() -> None:
    _status["running"] = False
    _dispatch(_broadcast({"type": "shutdown", "status": _status}))


def push_portfolio(portfolio: dict) -> None:
    _portfolio.update(portfolio)
    _dispatch(_broadcast({"type": "portfolio_update", "portfolio": dict(_portfolio)}))


def push_trade(trade: dict) -> None:
    _trades.append(trade)
    if len(_trades) > 100:
        _trades[:] = _trades[-100:]
    _dispatch(_broadcast({"type": "trade_event", "trade": trade}))


def push_circuit_breaker(reason: str) -> None:
    global _circuit_breaker
    _circuit_breaker = reason
    db.save_circuit_breaker_event(reason)
    _dispatch(_broadcast({"type": "circuit_breaker_alert", "reason": reason}))


def push_watchlist(candidates: list[dict], scan_time: str) -> None:
    global _watchlist
    _watchlist = {"scan_time": scan_time, "tickers": candidates}
    db.save_watchlist(scan_time, candidates)
    _dispatch(_broadcast({"type": "watchlist_update", "watchlist": _watchlist}))


def restore_circuit_breaker() -> None:
    """
    Called from main.py after the engine is wired up.
    Loads today's circuit breaker event from DB and restores halt state so that
    a Railway redeploy mid-day doesn't silently re-enable trading after a halt.
    """
    global _circuit_breaker
    reason = db.load_todays_circuit_breaker()
    if reason:
        _circuit_breaker = reason
        if _engine_ref is not None:
            _engine_ref.risk.set_kill_switch(True)
        logger.warning(f"Startup: restoring today's circuit breaker from DB — {reason}")
    else:
        logger.info("Startup: no circuit breaker event for today — starting unhalted")


def set_engine(engine) -> None:
    global _engine_ref
    _engine_ref = engine


def get_kill_switch() -> bool:
    return _kill_switch


def get_dry_run() -> bool:
    return _dry_run


# ── Server startup ─────────────────────────────────────────────────────────

def start_dashboard(host: str = "0.0.0.0", port: int = 8000) -> None:
    db.init_tables()

    _scan_log[:] = db.load_scan_log(50)
    _trades[:] = db.load_trades(100)

    latest_pf = db.load_latest_portfolio()
    if latest_pf:
        _portfolio.update(latest_pf)

    latest_wl = db.load_watchlist()
    if latest_wl:
        _watchlist.update(latest_wl)

    logger.info(
        f"Loaded {len(_scan_log)} log entries, {len(_trades)} trade(s) from DB"
    )

    def _run():
        global _loop
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        _loop = loop
        config = uvicorn.Config(
            _app, host=host, port=port,
            loop="none", log_level="warning", access_log=False,
        )
        server = uvicorn.Server(config)
        loop.run_until_complete(server.serve())

    t = threading.Thread(target=_run, daemon=True, name="dashboard")
    t.start()
    logger.info(f"Dashboard running at http://localhost:{port}")


# ── Embedded HTML ──────────────────────────────────────────────────────────

_HTML = """<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Meem Trading Bot</title>
<script>
  (function(){
    var t = localStorage.getItem('meem-theme') || 'dark';
    document.documentElement.setAttribute('data-theme', t);
  })();
</script>
<style>
*, *::before, *::after { box-sizing: border-box; margin: 0; padding: 0; }

:root {
  --bg:           #000000;
  --bg2:          #1c1c1e;
  --bg3:          #2c2c2e;
  --bg4:          #3a3a3c;
  --sep:          #38383a;
  --label:        #ffffff;
  --label2:       rgba(235,235,245,0.60);
  --label3:       rgba(235,235,245,0.30);
  --green:        #30d158;
  --green-bg:     rgba(48,209,88,0.12);
  --red:          #ff453a;
  --red-bg:       rgba(255,69,58,0.12);
  --blue:         #0a84ff;
  --blue-bg:      rgba(10,132,255,0.12);
  --orange:       #ff9f0a;
  --orange-bg:    rgba(255,159,10,0.12);
  --purple:       #bf5af2;
  --purple-bg:    rgba(191,90,242,0.12);
  --yellow:       #ffd60a;
  --yellow-bg:    rgba(255,214,10,0.12);
}

body {
  background: var(--bg);
  color: var(--label);
  font-family: -apple-system, "SF Pro Text", "SF Pro Display",
               BlinkMacSystemFont, "Segoe UI", "Helvetica Neue", Arial, sans-serif;
  font-size: 15px;
  -webkit-font-smoothing: antialiased;
  -moz-osx-font-smoothing: grayscale;
  min-height: 100vh;
}

/* ── Header ── */
header {
  position: sticky; top: 0; z-index: 100;
  background: rgba(0,0,0,0.72);
  backdrop-filter: saturate(180%) blur(20px);
  -webkit-backdrop-filter: saturate(180%) blur(20px);
  border-bottom: 0.5px solid var(--sep);
  padding: 10px 28px; min-height: 60px;
  display: flex; align-items: center; justify-content: space-between;
  gap: 12px;
}
.hdr-left { display: flex; flex-direction: column; gap: 1px; flex: 1 1 0; min-width: 0; }
.hdr-title { display: block; font-size: 17px; font-weight: 600; letter-spacing: -0.3px; color: var(--label); white-space: nowrap; overflow: hidden; text-overflow: ellipsis; }
.hdr-sub   { display: block; font-size: 11px; color: var(--label3); font-weight: 400; white-space: nowrap; overflow: hidden; text-overflow: ellipsis; }
.hdr-right { display: flex; align-items: center; gap: 8px; flex-wrap: wrap; justify-content: flex-end; flex-shrink: 0; }

/* ── Badges ── */
.badge {
  display: inline-flex; align-items: center; gap: 5px;
  padding: 4px 10px; border-radius: 999px;
  font-size: 11px; font-weight: 600; letter-spacing: 0.2px;
}
.dot { width: 6px; height: 6px; border-radius: 50%; flex-shrink: 0; }
.badge-online    { background: var(--green-bg);              color: var(--green);  }
.badge-offline   { background: var(--red-bg);                color: var(--red);    }
.badge-mktopen   { background: var(--blue-bg);               color: var(--blue);   }
.badge-mktclosed { background: rgba(120,120,128,0.12);        color: var(--label3); }
.badge-online  .dot { background: var(--green); animation: pulse 2s ease-in-out infinite; }
.badge-mktopen .dot { background: var(--blue);  animation: pulse 2s ease-in-out infinite; }
.badge-offline .dot { background: var(--red);   }
.badge-mktclosed .dot { background: var(--label3); }
@keyframes pulse { 0%,100%{opacity:1} 50%{opacity:0.4} }

/* ── Control toggles ── */
.ctrl-btn {
  display: inline-flex; align-items: center; gap: 5px;
  padding: 7px 12px; border-radius: 9px; border: none;
  font-size: 11px; font-weight: 600; letter-spacing: 0.2px;
  cursor: pointer; font-family: inherit;
  transition: background 0.2s, color 0.2s;
  white-space: nowrap; min-height: 34px; touch-action: manipulation;
}
.ctrl-btn .dot { width: 5px; height: 5px; }
#kill-btn.ks-off { background: rgba(235,235,245,0.08); color: var(--label3); }
#kill-btn.ks-on  { background: var(--red-bg);          color: var(--red);    }
#kill-btn.ks-on .dot { background: var(--red); animation: pulse 1.5s infinite; }
#dry-btn.dr-off  { background: rgba(235,235,245,0.08); color: var(--label3); }
#dry-btn.dr-on   { background: var(--blue-bg);         color: var(--blue);   }
#dry-btn.dr-on .dot { background: var(--blue); }

/* ── Theme toggle ── */
#theme-btn {
  width: 34px; height: 34px; border-radius: 50%; border: none;
  background: rgba(235,235,245,0.08); color: var(--label2);
  cursor: pointer; display: flex; align-items: center; justify-content: center;
  transition: background 0.2s; flex-shrink: 0;
}
#theme-btn:hover { background: rgba(235,235,245,0.14); }
#theme-btn svg { width: 16px; height: 16px; }

/* ── Main layout ── */
main { max-width: 1240px; margin: 0 auto; padding: 28px 28px 60px; }

.section-header {
  display: flex; align-items: center; justify-content: space-between;
  margin-bottom: 14px;
}
.section-label {
  font-size: 11px; font-weight: 700; text-transform: uppercase;
  letter-spacing: 0.8px; color: var(--label3);
}

/* ── Circuit breaker banner ── */
#cb-banner {
  background: var(--red-bg);
  border: 0.5px solid rgba(255,69,58,0.35);
  border-radius: 14px; padding: 14px 18px; margin-bottom: 20px;
  display: flex; align-items: center; gap: 12px;
  animation: bannerIn 0.3s ease;
}
@keyframes bannerIn { from{opacity:0;transform:translateY(-6px)} to{opacity:1;transform:translateY(0)} }
.cb-icon { font-size: 18px; flex-shrink: 0; }
.cb-body { flex: 1; }
.cb-title { font-size: 13px; font-weight: 700; color: var(--red); margin-bottom: 2px; }
.cb-msg   { font-size: 12px; color: var(--label2); }
#cb-reenable {
  padding: 6px 14px; border-radius: 8px; border: none;
  background: var(--red); color: #fff;
  font-size: 12px; font-weight: 600; cursor: pointer; font-family: inherit;
  flex-shrink: 0;
}
#cb-reenable:hover { opacity: 0.85; }

/* ── Portfolio bar ── */
#portfolio-section { margin-bottom: 28px; }
#portfolio-bar {
  display: grid;
  grid-template-columns: 1.6fr 1fr 1fr 1fr;
  gap: 10px;
}
.pf-tile {
  background: var(--bg2); border-radius: 16px; padding: 16px 18px;
}
.pf-lbl {
  font-size: 10px; font-weight: 700; text-transform: uppercase;
  letter-spacing: 0.6px; color: var(--label3); margin-bottom: 8px;
}
.pf-val    { font-size: 22px; font-weight: 700; letter-spacing: -0.5px; font-variant-numeric: tabular-nums; }
.pf-val-sm { font-size: 18px; font-weight: 700; letter-spacing: -0.4px; font-variant-numeric: tabular-nums; }
.pf-sub    { font-size: 12px; font-weight: 600; margin-top: 3px; font-variant-numeric: tabular-nums; }
.color-green { color: var(--green); }
.color-red   { color: var(--red);   }
.color-dim   { color: var(--label3); }

/* ── Chart ── */
#chart-section { margin-bottom: 28px; }
.chart-ranges { display: flex; gap: 2px; }
.range-btn {
  padding: 4px 10px; border-radius: 7px; border: none;
  background: none; color: var(--label3);
  font-size: 11px; font-weight: 600; cursor: pointer; font-family: inherit;
  transition: all 0.15s;
}
.range-btn:hover { color: var(--label2); }
.range-btn.active { background: var(--bg3); color: var(--label); }
.chart-card {
  background: var(--bg2); border-radius: 18px; padding: 20px; overflow: hidden;
}
.chart-kpi { margin-bottom: 16px; }
#chart-total { font-size: 26px; font-weight: 700; letter-spacing: -0.5px; }
#chart-delta { font-size: 13px; font-weight: 600; margin-top: 3px; }
#chart-inner { min-height: 120px; }
.chart-svg { width: 100%; height: 140px; display: block; }
.chart-no-data { padding: 40px 0; text-align: center; font-size: 13px; color: var(--label3); }

/* ── Scan History ── */
#log-section { margin-top: 36px; }
.log-header { display: flex; align-items: center; justify-content: space-between; margin-bottom: 12px; }
#log-clear {
  font-size: 12px; font-weight: 500; color: var(--blue);
  background: none; border: none; cursor: pointer; padding: 0; font-family: inherit;
}
#log-clear:hover { opacity: 0.7; }
#log-box { background: var(--bg2); border-radius: 16px; overflow: hidden; }
#log-empty { padding: 28px 20px; text-align: center; font-size: 13px; color: var(--label3); }
.log-row {
  display: grid; grid-template-columns: 145px 110px 1fr;
  align-items: center; gap: 12px; padding: 11px 18px;
  border-bottom: 0.5px solid var(--sep);
  animation: logIn 0.25s ease;
}
.log-row:last-child { border-bottom: none; }
.log-row:hover { background: rgba(128,128,128,0.05); }
@keyframes logIn { from{opacity:0;transform:translateX(-4px)} to{opacity:1;transform:translateX(0)} }
.log-time {
  font-size: 12px; font-variant-numeric: tabular-nums;
  font-family: ui-monospace,"SF Mono",Menlo,monospace;
  color: var(--label3); letter-spacing: 0.1px;
}
.log-badge {
  display: inline-flex; align-items: center; gap: 5px;
  padding: 3px 9px; border-radius: 7px; font-size: 11px; font-weight: 600; letter-spacing: 0.2px;
}
.log-badge .dot { width: 5px; height: 5px; border-radius: 50%; flex-shrink: 0; }
.log-badge.lb-scan     { background: var(--green-bg);        color: var(--green);  }
.log-badge.lb-scan .dot { background: var(--green); }
.log-badge.lb-empty    { background: var(--orange-bg);       color: var(--orange); }
.log-badge.lb-empty .dot { background: var(--orange); }
.log-badge.lb-closed   { background: rgba(128,128,128,0.10); color: var(--label3); }
.log-badge.lb-closed .dot { background: var(--label3); }
.log-desc { font-size: 12px; color: var(--label2); white-space: nowrap; overflow: hidden; text-overflow: ellipsis; }
.log-tickers { display: inline-flex; gap: 5px; flex-wrap: wrap; }
.log-ticker-tag {
  background: var(--bg3); color: var(--label2);
  font-size: 11px; font-weight: 600; padding: 2px 7px; border-radius: 5px;
}

/* ── Trade history ── */
#trade-section { margin-top: 36px; }
#trade-box { background: var(--bg2); border-radius: 16px; overflow: hidden; }
#trade-empty { padding: 28px 20px; text-align: center; font-size: 13px; color: var(--label3); }
.trade-row {
  display: grid;
  grid-template-columns: 145px 64px 90px 90px 70px 1fr;
  align-items: center; gap: 10px; padding: 11px 18px;
  border-bottom: 0.5px solid var(--sep);
  animation: logIn 0.25s ease;
}
.trade-row:last-child { border-bottom: none; }
.trade-row:hover { background: rgba(128,128,128,0.05); }
.trade-time { font-size: 11px; font-family: ui-monospace,"SF Mono",Menlo,monospace; color: var(--label3); font-variant-numeric: tabular-nums; }
.trade-action {
  display: inline-flex; align-items: center; justify-content: center;
  padding: 3px 9px; border-radius: 7px; font-size: 11px; font-weight: 700; letter-spacing: 0.3px;
}
.trade-action.buy  { background: var(--green-bg); color: var(--green); }
.trade-action.sell { background: var(--red-bg);   color: var(--red);   }
.trade-ticker { font-size: 13px; font-weight: 700; color: var(--label); }
.trade-qty    { font-size: 12px; color: var(--label2); font-variant-numeric: tabular-nums; }
.trade-pnl    { font-size: 12px; font-weight: 600; font-variant-numeric: tabular-nums; }
.trade-pnl.pos { color: var(--green); }
.trade-pnl.neg { color: var(--red);   }
.trade-pnl.zer { color: var(--label3); }
.trade-reason { font-size: 11px; color: var(--label3); white-space: nowrap; overflow: hidden; text-overflow: ellipsis; }
.trade-dry { font-size: 10px; color: var(--orange); font-weight: 600; margin-left: 4px; }

/* ── Status bar ── */
#statusbar {
  background: var(--bg2); border-radius: 14px; padding: 12px 18px;
  display: flex; align-items: center; justify-content: space-between;
  font-size: 12px; color: var(--label3); font-weight: 500;
  margin-bottom: 28px;
}
#statusbar > span { display: flex; align-items: center; gap: 6px; }
#wsdot { width: 6px; height: 6px; border-radius: 50%; background: var(--red); transition: background 0.4s; flex-shrink: 0; }
#wsdot.on { background: var(--green); animation: pulse 2s infinite; }

/* ── Light mode ── */
[data-theme="light"] {
  --bg:     #f2f2f7;
  --bg2:    #ffffff;
  --bg3:    #e5e5ea;
  --bg4:    #d1d1d6;
  --sep:    rgba(60,60,67,0.12);
  --label:  #000000;
  --label2: rgba(60,60,67,0.60);
  --label3: rgba(60,60,67,0.45);
  --green:  #34c759; --green-bg:  rgba(52,199,89,0.16);
  --red:    #ff3b30; --red-bg:    rgba(255,59,48,0.13);
  --blue:   #007aff; --blue-bg:   rgba(0,122,255,0.13);
  --orange: #ff9500; --orange-bg: rgba(255,149,0,0.14);
  --purple: #af52de; --purple-bg: rgba(175,82,222,0.13);
  --yellow: #ffcc00; --yellow-bg: rgba(255,204,0,0.15);
}
[data-theme="light"] .pf-tile,
[data-theme="light"] .chart-card,
[data-theme="light"] #statusbar,
[data-theme="light"] #log-box,
[data-theme="light"] #trade-box {
  box-shadow: 0 1px 3px rgba(0,0,0,0.08), 0 0 0 0.5px rgba(0,0,0,0.06);
}
[data-theme="light"] header { background: rgba(242,242,247,0.92); }
[data-theme="light"] .badge-mktclosed { background: rgba(60,60,67,0.08); }
[data-theme="light"] #theme-btn        { background: rgba(60,60,67,0.07); }
[data-theme="light"] #theme-btn:hover  { background: rgba(60,60,67,0.12); }
[data-theme="light"] #kill-btn.ks-off  { background: rgba(60,60,67,0.07); color: rgba(60,60,67,0.50); }
[data-theme="light"] #dry-btn.dr-off   { background: rgba(60,60,67,0.07); color: rgba(60,60,67,0.50); }
[data-theme="light"] .log-row:hover,
[data-theme="light"] .trade-row:hover  { background: rgba(0,0,0,0.035); }

@media (max-width: 680px) {
  header { padding: 0 16px; }
  main { padding: 20px 16px 60px; }
  #portfolio-bar { grid-template-columns: 1fr 1fr; }
  #kill-btn span.ctrl-label, #dry-btn span.ctrl-label { display: none; }

  /* Scan log: tighten columns */
  .log-row { grid-template-columns: auto 100px 1fr; gap: 8px; padding: 10px 14px; }
  .log-time { font-size: 11px; }

  /* Trade rows: collapse to 2-line card layout */
  .trade-row {
    grid-template-columns: auto 1fr auto auto;
    grid-template-rows: auto auto;
    gap: 4px 10px;
    padding: 11px 14px;
  }
  .trade-time   { display: none; }
  .trade-action { grid-column: 1; grid-row: 1; }
  .trade-ticker { grid-column: 2; grid-row: 1; font-size: 14px; }
  .trade-qty    { grid-column: 3; grid-row: 1; }
  .trade-pnl    { grid-column: 4; grid-row: 1; }
  .trade-reason {
    grid-column: 1 / -1; grid-row: 2;
    white-space: normal; overflow: visible; text-overflow: unset;
    font-size: 11px; line-height: 1.4;
  }

  /* Status bar: stack vertically */
  #statusbar { flex-direction: column; gap: 6px; text-align: center; }
}

@media (max-width: 430px) {
  /* Header: allow badge + controls to wrap */
  .hdr-right { flex-wrap: wrap; justify-content: flex-end; gap: 6px; }
  .hdr-title { font-size: 15px; }
  .hdr-sub   { font-size: 10px; }

  /* Portfolio tiles: slightly smaller text */
  .pf-val    { font-size: 19px; }
  .pf-val-sm { font-size: 16px; }
  .pf-lbl    { font-size: 9px; }

  /* Chart delta smaller */
  #chart-total { font-size: 22px; }

  /* Scan log: hide time on very small screens, just show badge + desc */
  .log-row { grid-template-columns: 100px 1fr; }
  .log-time { display: none; }
}

/* ── Watchlist ── */
#watchlist-section { margin-bottom: 28px; }
.wl-scan-meta { font-size: 11px; color: var(--label3); font-weight: 500; }
#watchlist-box { background: var(--bg2); border-radius: 16px; overflow: hidden; }
#watchlist-empty { padding: 28px 20px; text-align: center; font-size: 13px; color: var(--label3); }

.wl-header-row,
.wl-row {
  display: grid;
  grid-template-columns: 28px 66px 86px 140px 48px 40px 40px 56px 80px 74px;
  gap: 8px; padding: 9px 18px; align-items: center;
}
.wl-header-row { border-bottom: 0.5px solid var(--sep); }
.wl-col-label { font-size: 10px; font-weight: 700; text-transform: uppercase; letter-spacing: 0.5px; color: var(--label3); }
.wl-col-label.right { text-align: right; }
.wl-row { border-bottom: 0.5px solid var(--sep); transition: background 0.15s; }
.wl-row:last-child { border-bottom: none; }
.wl-row:hover { background: rgba(128,128,128,0.06); }
.wl-row.buy-signal { background: rgba(48,209,88,0.04); }
.wl-row.buy-signal:hover { background: rgba(48,209,88,0.08); }

.wl-rank { font-size:12px; color:var(--label3); font-variant-numeric:tabular-nums; font-family:ui-monospace,"SF Mono",Menlo,monospace; text-align:right; }
.wl-ticker { font-size:13px; font-weight:700; }
.wl-score-wrap { display:flex; align-items:center; gap:6px; }
.wl-score-val { font-size:13px; font-weight:700; font-variant-numeric:tabular-nums; font-family:ui-monospace,"SF Mono",Menlo,monospace; min-width:38px; }
.wl-score-bar-track { flex:1; height:3px; background:var(--bg4); border-radius:2px; overflow:hidden; }
.wl-score-bar-fill { height:100%; border-radius:2px; }
.score-high { color:var(--green); } .score-mid { color:var(--orange); } .score-low { color:var(--label3); }
.fill-high { background:var(--green); } .fill-mid { background:var(--orange); } .fill-low { background:var(--label3); }
.contrib-wrap { display:flex; flex-direction:column; gap:3px; min-width:0; }
.wl-contrib { display:flex; height:5px; border-radius:3px; overflow:hidden; gap:1px; }
.contrib-tech { background:var(--blue); } .contrib-sent { background:var(--purple); } .contrib-hist { background:var(--orange); }
.contrib-labels { display:flex; justify-content:space-between; font-size:9px; font-weight:600; }
.cl-tech { color:var(--blue); } .cl-sent { color:var(--purple); } .cl-hist { color:var(--orange); }
.wl-rsi { font-size:12px; font-variant-numeric:tabular-nums; font-family:ui-monospace,"SF Mono",Menlo,monospace; text-align:right; color:var(--label2); }
.rsi-ok { color:var(--green); } .rsi-hot { color:var(--red); }
.sig-badge { display:inline-flex; align-items:center; justify-content:center; width:26px; height:18px; border-radius:5px; font-size:10px; font-weight:700; }
.sig-y { background:var(--green-bg); color:var(--green); } .sig-n { background:rgba(120,120,128,0.10); color:var(--label3); }
.wl-vol { font-size:12px; font-variant-numeric:tabular-nums; font-family:ui-monospace,"SF Mono",Menlo,monospace; text-align:right; color:var(--label2); }
.vol-high { color:var(--green); }
.wl-price { font-size:12px; font-variant-numeric:tabular-nums; font-family:ui-monospace,"SF Mono",Menlo,monospace; text-align:right; color:var(--label2); }
.wl-flag { display:flex; justify-content:flex-end; }
.buy-badge { display:inline-flex; align-items:center; gap:4px; padding:3px 8px; border-radius:6px; background:var(--green-bg); color:var(--green); font-size:10px; font-weight:700; letter-spacing:0.3px; }
.buy-badge .dot { width:4px; height:4px; border-radius:50%; background:var(--green); animation:pulse 1.5s infinite; }

[data-theme="light"] #watchlist-box { box-shadow: 0 1px 3px rgba(0,0,0,0.08), 0 0 0 0.5px rgba(0,0,0,0.06); }
[data-theme="light"] .wl-row.buy-signal { background: rgba(52,199,89,0.05); }
[data-theme="light"] .wl-row.buy-signal:hover { background: rgba(52,199,89,0.09); }
[data-theme="light"] .wl-row:hover { background: rgba(0,0,0,0.035); }

@media (max-width: 860px) {
  .wl-header-row, .wl-row { grid-template-columns: 24px 58px 78px 1fr 44px 36px 36px; }
  .wl-vol, .wl-price, .wl-flag { display: none; }
}
@media (max-width: 560px) {
  .wl-header-row, .wl-row { grid-template-columns: 20px 54px 70px 1fr 40px; }
  .wl-rsi, .wl-macd, .wl-ema { display: none; }
}
</style>
</head>
<body>

<header>
  <div class="hdr-left">
    <span class="hdr-title">Meem Trading Bot</span>
    <span class="hdr-sub" id="last-scan">Waiting for first scan…</span>
  </div>
  <div class="hdr-right">
    <span id="mkt-badge" class="badge badge-mktclosed">
      <span class="dot"></span><span id="mkt-text">Market Closed</span>
    </span>
    <span id="bot-badge" class="badge badge-online">
      <span class="dot"></span><span id="bot-text">Online</span>
    </span>
    <button id="kill-btn" class="ctrl-btn ks-off" onclick="toggleKillSwitch()" title="Halt all new trades">
      <span class="dot"></span><span class="ctrl-label">Kill Switch</span>
    </button>
    <button id="dry-btn" class="ctrl-btn dr-off" onclick="toggleDryRun()" title="Log decisions without placing orders">
      <span class="dot"></span><span class="ctrl-label">Dry Run</span>
    </button>
    <button id="theme-btn" onclick="toggleTheme()" aria-label="Toggle theme">
      <svg id="theme-icon" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.8" stroke-linecap="round" stroke-linejoin="round"></svg>
    </button>
  </div>
</header>

<main>

  <!-- Circuit breaker banner -->
  <div id="cb-banner" style="display:none">
    <span class="cb-icon">⚡</span>
    <div class="cb-body">
      <div class="cb-title">Trading Halted — Circuit Breaker</div>
      <div class="cb-msg" id="cb-msg">—</div>
    </div>
    <button id="cb-reenable" onclick="reenableTrading()">Re-enable</button>
  </div>

  <!-- Portfolio -->
  <div id="portfolio-section">
    <div class="section-header" style="margin-bottom:12px">
      <span class="section-label">Portfolio</span>
    </div>
    <div id="portfolio-bar">
      <div class="pf-tile">
        <div class="pf-lbl">Total Value</div>
        <div class="pf-val" id="pf-total">—</div>
        <div class="pf-sub color-dim" id="pf-pnl-sub">—</div>
      </div>
      <div class="pf-tile">
        <div class="pf-lbl">Daily P&amp;L</div>
        <div class="pf-val-sm" id="pf-pnl">—</div>
        <div class="pf-sub color-dim" id="pf-pnl-pct">—</div>
      </div>
      <div class="pf-tile">
        <div class="pf-lbl">Cash</div>
        <div class="pf-val-sm" id="pf-cash">—</div>
      </div>
      <div class="pf-tile">
        <div class="pf-lbl">Positions</div>
        <div class="pf-val-sm" id="pf-positions">—</div>
      </div>
    </div>
  </div>

  <!-- Status bar -->
  <div id="statusbar">
    <span><span id="wsdot"></span><span id="ws-lbl">Connecting…</span></span>
    <span>Scans every 10 min · Market hours only</span>
  </div>

  <!-- Watchlist -->
  <div id="watchlist-section">
    <div class="section-header">
      <span class="section-label">Watchlist</span>
      <span class="wl-scan-meta" id="wl-meta">Waiting for first scan…</span>
    </div>
    <div id="watchlist-box">
      <div id="watchlist-empty">No watchlist data yet</div>
    </div>
  </div>

  <!-- Performance Chart -->
  <div id="chart-section">
    <div class="section-header">
      <span class="section-label">Performance</span>
      <div class="chart-ranges">
        <button class="range-btn" onclick="setRange('1W')">1W</button>
        <button class="range-btn" onclick="setRange('1M')">1M</button>
        <button class="range-btn active" onclick="setRange('3M')">3M</button>
        <button class="range-btn" onclick="setRange('1Y')">1Y</button>
        <button class="range-btn" onclick="setRange('ALL')">ALL</button>
      </div>
    </div>
    <div class="chart-card">
      <div class="chart-kpi">
        <div id="chart-total">—</div>
        <div id="chart-delta" style="color:var(--label3)">—</div>
      </div>
      <div id="chart-inner">
        <div class="chart-no-data">Loading performance data…</div>
      </div>
    </div>
  </div>

  <!-- Trade History -->
  <div id="trade-section">
    <div class="log-header">
      <span class="section-label">Trade History</span>
    </div>
    <div id="trade-box">
      <div id="trade-empty">No trades yet</div>
    </div>
  </div>

  <!-- Scan History -->
  <div id="log-section">
    <div class="log-header">
      <span class="section-label">Scan History</span>
      <button id="log-clear" onclick="clearLog()">Clear</button>
    </div>
    <div id="log-box">
      <div id="log-empty">No scans yet this session</div>
    </div>
  </div>

</main>

<script>
/* ═══════════════════════ State ═══════════════════════ */
var ws, wsDelay = 2000;
var _killSwitch = false, _dryRun = false;
var _chartData = [], _chartRange = '3M';

/* ═══════════════════════ WebSocket ═══════════════════════ */
function connect() {
  var proto = location.protocol === 'https:' ? 'wss://' : 'ws://';
  ws = new WebSocket(proto + location.host + '/ws');

  ws.onopen = function() {
    document.getElementById('wsdot').classList.add('on');
    document.getElementById('ws-lbl').textContent = 'Live';
    wsDelay = 2000;
  };

  ws.onclose = function() {
    document.getElementById('wsdot').classList.remove('on');
    document.getElementById('ws-lbl').textContent = 'Reconnecting…';
    setTimeout(connect, wsDelay);
    wsDelay = Math.min(wsDelay * 1.5, 30000);
  };

  ws.onmessage = function(e) {
    var msg = JSON.parse(e.data);
    if (msg.type === 'init') {
      applyStatus(msg.status);
      applyControls(msg.kill_switch, msg.dry_run);
      applyPortfolio(msg.portfolio);
      if (msg.circuit_breaker) applyCircuitBreaker(msg.circuit_breaker);
      (msg.scan_log || []).forEach(function(e){ addLogEntry(e, false); });
      (msg.trades || []).forEach(function(t){ addTrade(t, false); });
      if (msg.watchlist && msg.watchlist.tickers && msg.watchlist.tickers.length) renderWatchlist(msg.watchlist);
    } else if (msg.type === 'scan_complete' || msg.type === 'status') {
      applyStatus(msg.status);
      if (msg.log_entry) addLogEntry(msg.log_entry, true);
    } else if (msg.type === 'shutdown') {
      document.getElementById('bot-badge').className = 'badge badge-offline';
      document.getElementById('bot-text').textContent = 'Offline';
    } else if (msg.type === 'portfolio_update') {
      applyPortfolio(msg.portfolio);
    } else if (msg.type === 'trade_event') {
      addTrade(msg.trade, true);
    } else if (msg.type === 'circuit_breaker_alert') {
      applyCircuitBreaker(msg.reason);
    } else if (msg.type === 'watchlist_update') {
      renderWatchlist(msg.watchlist);
    } else if (msg.type === 'control_state') {
      applyControls(msg.kill_switch, msg.dry_run);
      if (!msg.circuit_breaker) {
        document.getElementById('cb-banner').style.display = 'none';
      }
    }
  };
}

/* ═══════════════════════ Status ═══════════════════════ */
function applyStatus(s) {
  if (!s) return;
  var mb = document.getElementById('mkt-badge');
  if (s.market_open) {
    mb.className = 'badge badge-mktopen';
    document.getElementById('mkt-text').textContent = 'Market Open';
  } else {
    mb.className = 'badge badge-mktclosed';
    document.getElementById('mkt-text').textContent = 'Market Closed';
  }
  if (s.last_scan) document.getElementById('last-scan').textContent = 'Last scan  ' + s.last_scan;
}

/* ═══════════════════════ Controls ═══════════════════════ */
function applyControls(ks, dr) {
  _killSwitch = ks; _dryRun = dr;
  document.getElementById('kill-btn').className = 'ctrl-btn ' + (ks ? 'ks-on' : 'ks-off');
  document.getElementById('dry-btn').className  = 'ctrl-btn ' + (dr ? 'dr-on' : 'dr-off');
}

function toggleKillSwitch() {
  var next = !_killSwitch;
  if (next && !confirm('Activate kill switch? This will halt all new trades until manually disabled.')) return;
  fetch('/api/kill-switch', { method: 'POST', headers: {'Content-Type':'application/json'}, body: JSON.stringify({active: next}) })
    .then(function(r){ return r.json(); })
    .then(function(d){ applyControls(d.kill_switch, _dryRun); })
    .catch(function(err){ console.error('kill-switch toggle failed', err); });
}

function toggleDryRun() {
  fetch('/api/dry-run', { method: 'POST', headers: {'Content-Type':'application/json'}, body: JSON.stringify({active: !_dryRun}) })
    .then(function(r){ return r.json(); })
    .then(function(d){ applyControls(_killSwitch, d.dry_run); })
    .catch(function(err){ console.error('dry-run toggle failed', err); });
}

/* ═══════════════════════ Circuit breaker ═══════════════════════ */
function applyCircuitBreaker(reason) {
  document.getElementById('cb-msg').textContent = reason;
  document.getElementById('cb-banner').style.display = 'flex';
  applyControls(true, _dryRun);
}

function reenableTrading() {
  fetch('/api/kill-switch', { method: 'POST', headers: {'Content-Type':'application/json'}, body: JSON.stringify({active: false}) })
    .then(function(r){ return r.json(); })
    .then(function(d){
      applyControls(d.kill_switch, _dryRun);
      if (!d.kill_switch) document.getElementById('cb-banner').style.display = 'none';
    });
}

/* ═══════════════════════ Portfolio ═══════════════════════ */
function fmt$(v) { return '$' + Number(v || 0).toLocaleString('en-US', {minimumFractionDigits:2, maximumFractionDigits:2}); }

function applyPortfolio(p) {
  if (!p) return;
  document.getElementById('pf-total').textContent = fmt$(p.total_value);
  var pnl = p.daily_pnl || 0, pnlPct = p.daily_pnl_pct || 0;
  var pnlEl = document.getElementById('pf-pnl');
  pnlEl.textContent = (pnl >= 0 ? '+' : '') + fmt$(pnl);
  pnlEl.className = 'pf-val-sm ' + (pnl > 0 ? 'color-green' : pnl < 0 ? 'color-red' : 'color-dim');
  document.getElementById('pf-pnl-pct').textContent = (pnlPct >= 0 ? '+' : '') + pnlPct.toFixed(2) + '%';
  document.getElementById('pf-cash').textContent = fmt$(p.cash);
  document.getElementById('pf-positions').textContent = fmt$(p.positions_value);
  document.getElementById('pf-pnl-sub').textContent = 'Today';
}

/* ═══════════════════════ Chart ═══════════════════════ */
fetch('/api/chart').then(function(r){ return r.json(); }).then(function(data) {
  _chartData = data.snapshots || [];
  renderChart(_chartRange);
}).catch(function() {
  document.getElementById('chart-inner').innerHTML = '<div class="chart-no-data">Could not load chart data</div>';
});

function setRange(r) {
  _chartRange = r;
  document.querySelectorAll('.range-btn').forEach(function(b){
    b.classList.toggle('active', b.textContent === r);
  });
  renderChart(r);
}

function renderChart(range) {
  var cutoff = new Date();
  if (range === '1W')  cutoff.setDate(cutoff.getDate() - 7);
  else if (range === '1M')  cutoff.setMonth(cutoff.getMonth() - 1);
  else if (range === '3M')  cutoff.setMonth(cutoff.getMonth() - 3);
  else if (range === '1Y')  cutoff.setFullYear(cutoff.getFullYear() - 1);
  else cutoff = new Date(0);

  var filtered = _chartData.filter(function(s){ return new Date(s.date) >= cutoff; });
  var inner = document.getElementById('chart-inner');

  if (filtered.length < 2) {
    document.getElementById('chart-total').textContent = '—';
    document.getElementById('chart-delta').textContent = 'Portfolio snapshots save daily after market close';
    inner.innerHTML = '';
    return;
  }

  var first = filtered[0].total_value, last = filtered[filtered.length-1].total_value;
  var chg = last - first, chgPct = first ? (chg / first * 100) : 0;
  var lineColor = chg >= 0 ? '#30d158' : '#ff453a';

  document.getElementById('chart-total').textContent = fmt$(last);
  var deltaEl = document.getElementById('chart-delta');
  deltaEl.textContent = (chg >= 0 ? '+' : '') + chg.toFixed(2) + '  (' + (chgPct >= 0 ? '+' : '') + chgPct.toFixed(2) + '%)';
  deltaEl.style.color = lineColor;

  var vals = filtered.map(function(s){ return s.total_value; });
  var minV = Math.min.apply(null, vals), maxV = Math.max.apply(null, vals);
  var rangeV = maxV - minV || 1, W = 1000, H = 140, pY = 14, n = filtered.length;

  var pts = filtered.map(function(s, i) {
    return [(i / (n-1) * W).toFixed(1), (pY + (1 - (s.total_value - minV) / rangeV) * (H - pY*2)).toFixed(1)];
  });
  var pathD = pts.map(function(p,i){ return (i===0?'M':'L')+p[0]+','+p[1]; }).join(' ');
  var areaD = pathD + ' L'+pts[n-1][0]+','+H+' L'+pts[0][0]+','+H+' Z';
  var gId = 'g'+Date.now();

  inner.innerHTML = '<svg viewBox="0 0 '+W+' '+H+'" xmlns="http://www.w3.org/2000/svg" class="chart-svg" preserveAspectRatio="none">'
    + '<defs><linearGradient id="'+gId+'" x1="0" y1="0" x2="0" y2="1">'
    + '<stop offset="0%" stop-color="'+lineColor+'" stop-opacity="0.22"/>'
    + '<stop offset="100%" stop-color="'+lineColor+'" stop-opacity="0"/>'
    + '</linearGradient></defs>'
    + '<path d="'+areaD+'" fill="url(#'+gId+')"/>'
    + '<path d="'+pathD+'" fill="none" stroke="'+lineColor+'" stroke-width="2.5" stroke-linejoin="round" stroke-linecap="round"/>'
    + '</svg>';
}

/* ═══════════════════════ Scan log ═══════════════════════ */
function addLogEntry(entry, prepend) {
  var box = document.getElementById('log-box');
  var empty = document.getElementById('log-empty');
  if (empty) empty.remove();

  var badgeClass, badgeLabel, descHtml;
  if (entry.type === 'scan') {
    if (entry.count > 0) {
      badgeClass = 'lb-scan';
      badgeLabel = entry.count + ' Ticker' + (entry.count !== 1 ? 's' : '');
      var tags = (entry.tickers || []).map(function(t){ return '<span class="log-ticker-tag">$'+t+'</span>'; }).join('');
      descHtml = '<span class="log-tickers">'+tags+'</span>';
    } else {
      badgeClass = 'lb-empty'; badgeLabel = 'No Candidates';
      descHtml = '<span>No tickers passed scoring</span>';
    }
  } else {
    badgeClass = 'lb-closed'; badgeLabel = 'Mkt Closed';
    descHtml = '<span>Scan skipped</span>';
  }

  var row = document.createElement('div'); row.className = 'log-row';
  row.innerHTML =
    '<span class="log-time">'+(entry.time||'—')+'</span>'
    +'<span class="log-badge '+badgeClass+'"><span class="dot"></span>'+badgeLabel+'</span>'
    +'<span class="log-desc">'+descHtml+'</span>';

  if (prepend) {
    box.insertBefore(row, box.firstChild);
    var rows = box.querySelectorAll('.log-row');
    if (rows.length > 50) rows[rows.length-1].remove();
  } else {
    box.appendChild(row);
  }
}

function clearLog() {
  document.getElementById('log-box').innerHTML = '<div id="log-empty">No scans yet this session</div>';
}

/* ═══════════════════════ Trade history ═══════════════════════ */
function addTrade(t, prepend) {
  var box = document.getElementById('trade-box');
  var empty = document.getElementById('trade-empty');
  if (empty) empty.remove();

  var pnl = t.pnl || 0;
  var pnlClass = pnl > 0.005 ? 'pos' : pnl < -0.005 ? 'neg' : 'zer';
  var pnlStr = t.action === 'BUY' ? '—' : (pnl >= 0 ? '+' : '') + '$' + Math.abs(pnl).toFixed(2);

  var ts = '';
  try {
    ts = new Date(t.timestamp).toLocaleString('en-US', {month:'short', day:'numeric', hour:'2-digit', minute:'2-digit', timeZone:'America/New_York'});
  } catch(e) { ts = t.scan_time || '—'; }

  var dryTag = t.dry_run ? '<span class="trade-dry">DRY</span>' : '';

  var row = document.createElement('div'); row.className = 'trade-row';
  row.innerHTML =
    '<span class="trade-time">'+ts+'</span>'
    +'<span class="trade-action '+t.action.toLowerCase()+'">'+t.action+dryTag+'</span>'
    +'<span class="trade-ticker">$'+t.ticker+'</span>'
    +'<span class="trade-qty">'+t.qty+' @ $'+(t.price||0).toFixed(2)+'</span>'
    +'<span class="trade-pnl '+pnlClass+'">'+pnlStr+'</span>'
    +'<span class="trade-reason">'+(t.reason||'—')+'</span>';

  if (prepend) {
    box.insertBefore(row, box.firstChild);
    var rows = box.querySelectorAll('.trade-row');
    if (rows.length > 100) rows[rows.length-1].remove();
  } else {
    box.appendChild(row);
  }
}

/* ═══════════════════════ Watchlist ═══════════════════════ */
var _buyThreshold = 0.72;

function renderWatchlist(data) {
  if (!data || !data.tickers || !data.tickers.length) return;
  var box = document.getElementById('watchlist-box');
  var meta = document.getElementById('wl-meta');
  if (data.scan_time) {
    meta.textContent = 'Last scan ' + data.scan_time + ' · Top ' + data.tickers.length;
  }

  box.innerHTML =
    '<div class="wl-header-row">'
    + '<span class="wl-col-label right">#</span>'
    + '<span class="wl-col-label">Ticker</span>'
    + '<span class="wl-col-label">Score</span>'
    + '<span class="wl-col-label">Tech / Sent / Hist</span>'
    + '<span class="wl-col-label right">RSI</span>'
    + '<span class="wl-col-label right wl-macd">MACD</span>'
    + '<span class="wl-col-label right wl-ema">EMA</span>'
    + '<span class="wl-col-label right wl-vol">Vol</span>'
    + '<span class="wl-col-label right wl-price">Price</span>'
    + '<span class="wl-col-label wl-flag"></span>'
    + '</div>';

  data.tickers.forEach(function(d, i) {
    var comp = d.composite_score || 0;
    var tech = d.technical_score  || 0;
    var sent = d.sentiment_score  || 0;
    var hist = d.historical_score || 0;
    var total = tech * 0.50 + sent * 0.30 + hist * 0.20;
    var techPct = total > 0 ? Math.round(tech * 0.50 / total * 100) : 0;
    var sentPct = total > 0 ? Math.round(sent * 0.30 / total * 100) : 0;
    var histPct = 100 - techPct - sentPct;
    var isBuy = comp >= _buyThreshold;
    var rsi = d.rsi;
    var rsiCls = (rsi != null) ? (rsi < 30 ? 'rsi-ok' : rsi > 60 ? 'rsi-hot' : '') : '';
    var vol = d.volume_ratio || 0;
    var scoreCls = comp >= _buyThreshold ? 'score-high' : comp >= 0.55 ? 'score-mid' : 'score-low';
    var fillCls  = comp >= _buyThreshold ? 'fill-high'  : comp >= 0.55 ? 'fill-mid'  : 'fill-low';

    var row = document.createElement('div');
    row.className = 'wl-row' + (isBuy ? ' buy-signal' : '');
    row.innerHTML =
      '<span class="wl-rank">' + (i + 1) + '</span>'
      + '<span class="wl-ticker">$' + d.ticker + '</span>'
      + '<span class="wl-score-wrap">'
      +   '<span class="wl-score-val ' + scoreCls + '">' + comp.toFixed(3) + '</span>'
      +   '<span class="wl-score-bar-track"><span class="wl-score-bar-fill ' + fillCls + '" style="width:' + (comp * 100).toFixed(0) + '%"></span></span>'
      + '</span>'
      + '<span class="contrib-wrap">'
      +   '<span class="wl-contrib">'
      +     '<span class="contrib-tech" style="width:' + techPct + '%"></span>'
      +     '<span class="contrib-sent" style="width:' + sentPct + '%"></span>'
      +     '<span class="contrib-hist" style="width:' + histPct + '%"></span>'
      +   '</span>'
      +   '<span class="contrib-labels">'
      +     '<span class="cl-tech">T ' + techPct + '%</span>'
      +     '<span class="cl-sent">S ' + sentPct + '%</span>'
      +     '<span class="cl-hist">H ' + histPct + '%</span>'
      +   '</span>'
      + '</span>'
      + '<span class="wl-rsi ' + rsiCls + '">' + (rsi != null ? rsi.toFixed(1) : '—') + '</span>'
      + '<span class="wl-macd"><span class="sig-badge ' + (d.macd_cross  ? 'sig-y' : 'sig-n') + '">' + (d.macd_cross  ? 'Y' : 'N') + '</span></span>'
      + '<span class="wl-ema"><span class="sig-badge '  + (d.ema_reclaim ? 'sig-y' : 'sig-n') + '">' + (d.ema_reclaim ? 'Y' : 'N') + '</span></span>'
      + '<span class="wl-vol ' + (vol >= 2.0 ? 'vol-high' : '') + '">' + vol.toFixed(1) + 'x</span>'
      + '<span class="wl-price">$' + (d.current_price || 0).toFixed(2) + '</span>'
      + '<span class="wl-flag">' + (isBuy ? '<span class="buy-badge"><span class="dot"></span>BUY</span>' : '') + '</span>';

    box.appendChild(row);
  });
}

/* ═══════════════════════ Theme ═══════════════════════ */
var MOON_PATH = 'M21 12.79A9 9 0 1 1 11.21 3 7 7 0 0 0 21 12.79z';
var SUN_PATHS = ['M12 3v1','M12 20v1','M4.22 4.22l.71.71','M18.36 18.36l.71.71','M3 12h1','M20 12h1','M4.93 19.07l.71-.71','M18.36 5.64l.71-.71'];

function setThemeIcon(theme) {
  var svg = document.getElementById('theme-icon');
  svg.innerHTML = theme === 'light'
    ? '<path d="'+MOON_PATH+'"/>'
    : SUN_PATHS.map(function(d){ return '<path d="'+d+'"/>'; }).join('')+'<circle cx="12" cy="12" r="4"/>';
}

function toggleTheme() {
  var cur = document.documentElement.getAttribute('data-theme');
  var next = cur === 'light' ? 'dark' : 'light';
  document.documentElement.setAttribute('data-theme', next);
  localStorage.setItem('meem-theme', next);
  setThemeIcon(next);
}

setThemeIcon(document.documentElement.getAttribute('data-theme') || 'dark');
connect();
</script>
</body>
</html>"""
