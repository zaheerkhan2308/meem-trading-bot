"""
Live dashboard — FastAPI + WebSocket.
Runs in a daemon thread alongside APScheduler.
Call push_signals(), push_status(), push_no_signal() from main.py to update all browsers.
"""
import asyncio
import logging
import threading
from datetime import datetime
from zoneinfo import ZoneInfo

import uvicorn
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.responses import HTMLResponse

_ET = ZoneInfo("America/New_York")

def _now_et() -> str:
    return datetime.now(_ET).strftime("%a %b %d %I:%M %p ET")

logger = logging.getLogger(__name__)

_app = FastAPI()
_connections: set[WebSocket] = set()
_signals: list[dict] = []
_scan_log: list[dict] = []
_status: dict = {
    "running": True,
    "market_open": False,
    "last_scan": None,
}
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
        now = datetime.now(_ET).timestamp()
        fresh = [s for s in _signals if now - s.get("_ts", 0) < 3600]
        await websocket.send_json({"type": "init", "signals": fresh, "status": _status, "scan_log": _scan_log})
        while True:
            await websocket.receive_text()   # keep-alive
    except WebSocketDisconnect:
        pass
    finally:
        _connections.discard(websocket)


# ── Inject endpoint (testing only) ────────────────────────────────────────

from fastapi import Request as _Request

@_app.post("/inject")
async def inject(request: _Request):
    signals = await request.json()
    push_signals(signals, _now_et())
    return {"ok": True, "count": len(signals)}


# ── HTML page ──────────────────────────────────────────────────────────────

@_app.get("/", response_class=HTMLResponse)
async def index():
    return HTMLResponse(_HTML)


# ── Broadcast ──────────────────────────────────────────���───────────────────

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


# ── Public API (called from main.py) ──��───────────────────────────────────

def push_signals(signals: list[dict], scan_time: str) -> None:
    ts = datetime.now(_ET).timestamp()
    for s in signals:
        s["_ts"] = ts
    _signals[:] = signals
    _status["last_scan"] = scan_time
    entry = {"time": scan_time, "type": "signals",
             "count": len(signals), "tickers": [s["ticker"] for s in signals]}
    _append_log(entry)
    _dispatch(_broadcast({"type": "signals", "signals": signals, "status": _status, "log_entry": entry}))


def push_status(market_open: bool, scan_time: str) -> None:
    _status["market_open"] = market_open
    _status["last_scan"] = scan_time
    if not market_open:
        entry = {"time": scan_time, "type": "market_closed"}
        _append_log(entry)
        _dispatch(_broadcast({"type": "status", "status": _status, "log_entry": entry}))
    else:
        _dispatch(_broadcast({"type": "status", "status": _status}))


def push_no_signal(scan_time: str) -> None:
    _status["last_scan"] = scan_time
    entry = {"time": scan_time, "type": "no_signal"}
    _append_log(entry)
    _dispatch(_broadcast({"type": "no_signal", "status": _status, "log_entry": entry}))


def push_shutdown() -> None:
    _status["running"] = False
    _dispatch(_broadcast({"type": "shutdown", "status": _status}))


# ── Server startup ─────────────────────────────────────────────────────────

def start_dashboard(host: str = "0.0.0.0", port: int = 8000) -> None:
    """Start uvicorn in a daemon thread and capture its event loop."""

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


# ── Embedded HTML ───────────────────────────────────────────���──────────────

_HTML = """<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Meem Signal Agent</title>
<script>
  /* Runs before CSS renders — no flash of wrong theme */
  (function(){
    var t = localStorage.getItem('meem-theme') || 'dark';
    document.documentElement.setAttribute('data-theme', t);
  })();
</script>
<style>
*, *::before, *::after { box-sizing: border-box; margin: 0; padding: 0; }

:root {
  /* Apple dark mode system colors */
  --bg:           #000000;
  --bg2:          #1c1c1e;
  --bg3:          #2c2c2e;
  --bg4:          #3a3a3c;
  --sep:          #38383a;
  --label:        #ffffff;
  --label2:       rgba(235,235,245,0.60);
  --label3:       rgba(235,235,245,0.30);

  /* Apple system accent colors */
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
  font-family: -apple-system, BlinkMacSystemFont, "SF Pro Display",
               "SF Pro Text", "Helvetica Neue", Arial, sans-serif;
  font-size: 15px;
  -webkit-font-smoothing: antialiased;
  min-height: 100vh;
}

/* ── Header ───────────────────────────────────────────────────────────────── */
header {
  position: sticky;
  top: 0;
  z-index: 100;
  background: rgba(0,0,0,0.72);
  backdrop-filter: saturate(180%) blur(20px);
  -webkit-backdrop-filter: saturate(180%) blur(20px);
  border-bottom: 0.5px solid var(--sep);
  padding: 0 28px;
  height: 60px;
  display: flex;
  align-items: center;
  justify-content: space-between;
}

.hdr-left { display: flex; flex-direction: column; gap: 1px; }

.hdr-title {
  font-size: 17px;
  font-weight: 600;
  letter-spacing: -0.3px;
  color: var(--label);
}

.hdr-sub {
  font-size: 11px;
  color: var(--label3);
  font-weight: 400;
  letter-spacing: 0.1px;
}

.hdr-right { display: flex; align-items: center; gap: 10px; }

/* ── Badges ───────────────────────────────────────────────────────────────── */
.badge {
  display: inline-flex;
  align-items: center;
  gap: 5px;
  padding: 4px 10px;
  border-radius: 999px;
  font-size: 11px;
  font-weight: 600;
  letter-spacing: 0.2px;
}

.dot { width: 6px; height: 6px; border-radius: 50%; flex-shrink: 0; }

.badge-online  { background: var(--green-bg);  color: var(--green); }
.badge-offline { background: var(--red-bg);    color: var(--red);   }
.badge-mktopen { background: var(--blue-bg);   color: var(--blue);  }
.badge-mktclosed { background: rgba(235,235,245,0.06); color: var(--label3); }

.badge-online  .dot { background: var(--green); animation: pulse 2s ease-in-out infinite; }
.badge-mktopen .dot { background: var(--blue);  animation: pulse 2s ease-in-out infinite; }
.badge-offline .dot { background: var(--red); }
.badge-mktclosed .dot { background: var(--label3); }

@keyframes pulse {
  0%, 100% { opacity: 1; box-shadow: none; }
  50% { opacity: 0.4; }
}

/* ── Main layout ──────────────────────────────────────────────────────────── */
main { max-width: 1240px; margin: 0 auto; padding: 28px 28px 40px; }

.section-header {
  display: flex;
  align-items: center;
  justify-content: space-between;
  margin-bottom: 18px;
}

.section-label {
  font-size: 11px;
  font-weight: 700;
  text-transform: uppercase;
  letter-spacing: 0.8px;
  color: var(--label3);
}

#sig-count-label {
  font-size: 12px;
  color: var(--label3);
  font-weight: 500;
}

/* ── Signal grid ──────────────────────────────────────────────────────────── */
#grid {
  display: grid;
  grid-template-columns: repeat(auto-fill, minmax(360px, 1fr));
  gap: 14px;
  margin-bottom: 28px;
}

/* ── Empty state ──────────────────────────────────────────────────────────── */
#empty {
  grid-column: 1 / -1;
  display: flex;
  flex-direction: column;
  align-items: center;
  justify-content: center;
  padding: 80px 20px;
  color: var(--label3);
  text-align: center;
}

.empty-icon {
  width: 56px; height: 56px;
  border-radius: 16px;
  background: var(--bg2);
  display: flex; align-items: center; justify-content: center;
  font-size: 26px;
  margin-bottom: 18px;
}

#empty h3 { font-size: 16px; font-weight: 600; color: var(--label2); margin-bottom: 6px; }
#empty p  { font-size: 13px; line-height: 1.5; max-width: 300px; }

/* ── Signal card ──────────────────────────────────────────────────────────── */
.card {
  background: var(--bg2);
  border-radius: 18px;
  padding: 20px;
  animation: cardIn 0.4s cubic-bezier(0.34,1.56,0.64,1) both;
  position: relative;
  overflow: hidden;
}

.card::before {
  content: '';
  position: absolute;
  inset: 0;
  border-radius: 18px;
  padding: 1px;
  background: linear-gradient(135deg, rgba(255,255,255,0.08), rgba(255,255,255,0.02));
  -webkit-mask: linear-gradient(#fff 0 0) content-box, linear-gradient(#fff 0 0);
  -webkit-mask-composite: destination-out;
  mask-composite: exclude;
  pointer-events: none;
}

.card.fresh::before {
  background: linear-gradient(135deg, var(--green), rgba(48,209,88,0.2));
}

.card.fresh { box-shadow: 0 0 32px rgba(48,209,88,0.10); }

@keyframes cardIn {
  from { opacity: 0; transform: scale(0.95) translateY(-6px); }
  to   { opacity: 1; transform: scale(1)    translateY(0); }
}

/* card top */
.card-top {
  display: flex;
  justify-content: space-between;
  align-items: flex-start;
  margin-bottom: 18px;
}

.ticker-wrap {}

.ticker-row {
  display: flex;
  align-items: center;
  gap: 8px;
  margin-bottom: 3px;
}

.ticker {
  font-size: 24px;
  font-weight: 700;
  letter-spacing: -0.5px;
  color: var(--label);
}

.buy-badge {
  background: var(--green-bg);
  color: var(--green);
  font-size: 10px;
  font-weight: 700;
  letter-spacing: 0.8px;
  padding: 3px 8px;
  border-radius: 6px;
  align-self: center;
}

.cur-price {
  font-size: 13px;
  color: var(--label3);
  font-variant-numeric: tabular-nums;
}

/* Score badge */
.score-bubble {
  display: flex;
  flex-direction: column;
  align-items: center;
  justify-content: center;
  width: 60px; height: 60px;
  border-radius: 50%;
  flex-shrink: 0;
}

.score-bubble.s-hi { background: var(--green-bg);  }
.score-bubble.s-md { background: var(--orange-bg); }
.score-bubble.s-lo { background: var(--red-bg);    }

.score-num {
  font-size: 20px;
  font-weight: 700;
  line-height: 1;
  letter-spacing: -0.5px;
}

.score-bubble.s-hi .score-num { color: var(--green);  }
.score-bubble.s-md .score-num { color: var(--orange); }
.score-bubble.s-lo .score-num { color: var(--red);    }

.score-lbl {
  font-size: 9px;
  font-weight: 600;
  text-transform: uppercase;
  letter-spacing: 0.4px;
  margin-top: 1px;
  opacity: 0.6;
}

.score-bubble.s-hi .score-lbl { color: var(--green);  }
.score-bubble.s-md .score-lbl { color: var(--orange); }
.score-bubble.s-lo .score-lbl { color: var(--red);    }

/* Price levels */
.levels {
  display: grid;
  grid-template-columns: 1.1fr 1fr 1fr;
  gap: 8px;
  margin-bottom: 16px;
}

.lvl {
  background: var(--bg3);
  border-radius: 12px;
  padding: 10px 12px;
}

.lvl-lbl {
  font-size: 10px;
  font-weight: 600;
  text-transform: uppercase;
  letter-spacing: 0.5px;
  color: var(--label3);
  margin-bottom: 5px;
}

.lvl-price {
  font-size: 14px;
  font-weight: 600;
  font-variant-numeric: tabular-nums;
  letter-spacing: -0.3px;
}

.lvl-price2 {
  font-size: 11px;
  font-variant-numeric: tabular-nums;
  opacity: 0.45;
  margin-top: 1px;
}

.lvl-pct {
  font-size: 11px;
  font-weight: 600;
  margin-top: 3px;
  letter-spacing: 0.1px;
}

.lvl-entry .lvl-price  { color: var(--label); }
.lvl-stop  .lvl-price  { color: var(--red);   }
.lvl-stop  .lvl-pct    { color: var(--red);   opacity: 0.75; }
.lvl-tgt   .lvl-price  { color: var(--green); }
.lvl-tgt   .lvl-pct    { color: var(--green); opacity: 0.75; }

/* Indicator pills */
.pills {
  display: flex;
  gap: 6px;
  flex-wrap: wrap;
  margin-bottom: 14px;
}

.pill {
  display: inline-flex;
  align-items: center;
  gap: 3px;
  padding: 4px 9px;
  border-radius: 8px;
  font-size: 11px;
  font-weight: 600;
  background: var(--bg3);
  color: var(--label2);
  letter-spacing: 0.1px;
}

.pill.p-on   { background: var(--blue-bg);   color: var(--blue);   }
.pill.p-bull { background: var(--green-bg);  color: var(--green);  }
.pill.p-bear { background: var(--red-bg);    color: var(--red);    }
.pill.p-neu  { background: var(--orange-bg); color: var(--orange); }
.pill.p-dim  { opacity: 0.45; }

/* News section */
.news-block {
  border-top: 0.5px solid var(--sep);
  padding-top: 12px;
}

.news-headline {
  font-size: 12px;
  line-height: 1.55;
  color: var(--label2);
  margin-bottom: 8px;
}

.news-link {
  display: inline-flex;
  align-items: center;
  gap: 4px;
  font-size: 11px;
  font-weight: 600;
  color: var(--blue);
  text-decoration: none;
  letter-spacing: 0.1px;
}

.news-link:hover { text-decoration: underline; }

.news-link svg {
  opacity: 0.7;
  flex-shrink: 0;
}

/* Card footer */
.card-foot {
  margin-top: 12px;
  font-size: 11px;
  color: var(--label3);
  text-align: right;
  font-variant-numeric: tabular-nums;
}

/* ── Status bar ───────────────────────────────────────────────────────────── */
#statusbar {
  background: var(--bg2);
  border-radius: 14px;
  padding: 12px 18px;
  display: flex;
  align-items: center;
  justify-content: space-between;
  font-size: 12px;
  color: var(--label3);
  font-weight: 500;
}

#statusbar > span { display: flex; align-items: center; gap: 6px; }

#wsdot {
  width: 6px; height: 6px;
  border-radius: 50%;
  background: var(--red);
  transition: background 0.4s;
  flex-shrink: 0;
}

#wsdot.on { background: var(--green); animation: pulse 2s infinite; }

/* ── Theme toggle button ──────────────────────────────────────────────────── */
#theme-btn {
  width: 34px; height: 34px;
  border-radius: 50%;
  border: none;
  background: rgba(235,235,245,0.08);
  color: var(--label2);
  cursor: pointer;
  display: flex; align-items: center; justify-content: center;
  transition: background 0.2s;
  flex-shrink: 0;
}
#theme-btn:hover { background: rgba(235,235,245,0.14); }
#theme-btn svg { width: 16px; height: 16px; }

/* ── Light mode ───────────────────────────────────────────────────────────── */
[data-theme="light"] {
  --bg:           #f2f2f7;
  --bg2:          #ffffff;
  --bg3:          #f2f2f7;
  --bg4:          #e5e5ea;
  --sep:          rgba(60,60,67,0.18);
  --label:        #000000;
  --label2:       rgba(60,60,67,0.60);
  --label3:       rgba(60,60,67,0.30);

  --green:        #34c759;
  --green-bg:     rgba(52,199,89,0.12);
  --red:          #ff3b30;
  --red-bg:       rgba(255,59,48,0.10);
  --blue:         #007aff;
  --blue-bg:      rgba(0,122,255,0.10);
  --orange:       #ff9500;
  --orange-bg:    rgba(255,149,0,0.10);
  --purple:       #af52de;
  --purple-bg:    rgba(175,82,222,0.10);
  --yellow:       #ffcc00;
  --yellow-bg:    rgba(255,204,0,0.12);
}

[data-theme="light"] body {
  background: var(--bg);
}

[data-theme="light"] header {
  background: rgba(242,242,247,0.88);
}

[data-theme="light"] #theme-btn {
  background: rgba(60,60,67,0.06);
}
[data-theme="light"] #theme-btn:hover {
  background: rgba(60,60,67,0.10);
}

/* Cards in light mode: real shadow instead of gradient inner border */
[data-theme="light"] .card::before {
  display: none;
}
[data-theme="light"] .card {
  box-shadow: 0 1px 4px rgba(0,0,0,0.07), 0 0 0 0.5px rgba(0,0,0,0.05);
}
[data-theme="light"] .card.fresh {
  box-shadow: 0 0 0 1.5px var(--green), 0 4px 20px rgba(52,199,89,0.10);
}

/* ── Scan history log ─────────────────────────────────────────────────────── */
#log-section { margin-top: 36px; }

.log-header {
  display: flex;
  align-items: center;
  justify-content: space-between;
  margin-bottom: 12px;
}

#log-clear {
  font-size: 12px;
  font-weight: 500;
  color: var(--blue);
  background: none;
  border: none;
  cursor: pointer;
  padding: 0;
  font-family: inherit;
}
#log-clear:hover { opacity: 0.7; }

#log-box {
  background: var(--bg2);
  border-radius: 16px;
  overflow: hidden;
}

#log-empty {
  padding: 28px 20px;
  text-align: center;
  font-size: 13px;
  color: var(--label3);
}

.log-row {
  display: grid;
  grid-template-columns: 145px 130px 1fr;
  align-items: center;
  gap: 12px;
  padding: 11px 18px;
  border-bottom: 0.5px solid var(--sep);
  animation: logIn 0.25s ease;
}
.log-row:last-child { border-bottom: none; }
.log-row:hover { background: rgba(128,128,128,0.05); }

@keyframes logIn {
  from { opacity: 0; transform: translateX(-4px); }
  to   { opacity: 1; transform: translateX(0); }
}

.log-time {
  font-size: 12px;
  font-variant-numeric: tabular-nums;
  font-family: ui-monospace, "SF Mono", Menlo, monospace;
  color: var(--label3);
  letter-spacing: 0.1px;
}

.log-badge {
  display: inline-flex;
  align-items: center;
  gap: 5px;
  padding: 3px 9px;
  border-radius: 7px;
  font-size: 11px;
  font-weight: 600;
  letter-spacing: 0.2px;
}
.log-badge .dot { width: 5px; height: 5px; border-radius: 50%; flex-shrink: 0; }

.log-badge.lb-signals     { background: var(--green-bg);        color: var(--green);  }
.log-badge.lb-signals .dot { background: var(--green); }
.log-badge.lb-no-signal   { background: var(--orange-bg);       color: var(--orange); }
.log-badge.lb-no-signal .dot { background: var(--orange); }
.log-badge.lb-mkt-closed  { background: rgba(128,128,128,0.10); color: var(--label3); }
.log-badge.lb-mkt-closed .dot { background: var(--label3); }

.log-desc {
  font-size: 12px;
  color: var(--label2);
  white-space: nowrap;
  overflow: hidden;
  text-overflow: ellipsis;
}

.log-tickers {
  display: inline-flex;
  gap: 5px;
  flex-wrap: wrap;
}

.log-ticker-tag {
  background: var(--bg3);
  color: var(--label2);
  font-size: 11px;
  font-weight: 600;
  padding: 2px 7px;
  border-radius: 5px;
  font-variant-numeric: tabular-nums;
}

</style>
</head>
<body>

<header>
  <div class="hdr-left">
    <span class="hdr-title">Meem Signal Agent</span>
    <span class="hdr-sub" id="last-scan">Waiting for first scan…</span>
  </div>
  <div class="hdr-right">
    <span id="mkt-badge" class="badge badge-mktclosed">
      <span class="dot"></span><span id="mkt-text">Market Closed</span>
    </span>
    <span id="bot-badge" class="badge badge-online">
      <span class="dot"></span><span id="bot-text">Online</span>
    </span>
    <button id="theme-btn" onclick="toggleTheme()" aria-label="Toggle theme">
      <svg id="theme-icon" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.8" stroke-linecap="round" stroke-linejoin="round"></svg>
    </button>
  </div>
</header>

<main>
  <div class="section-header">
    <span class="section-label">Live Signals</span>
    <span id="sig-count-label"></span>
  </div>

  <div id="grid">
    <div id="empty">
      <div class="empty-icon">📡</div>
      <h3>Scanning for signals…</h3>
      <p>Strong setups appear here in real-time when the market is open</p>
    </div>
  </div>

  <div id="statusbar">
    <span><span id="wsdot"></span><span id="ws-lbl">Connecting…</span></span>
    <span id="sig-count">0 signals today</span>
    <span>Scans every 10 min · Market hours only</span>
  </div>

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
let total = 0, ws, delay = 2000;

function connect() {
  const wsProto = location.protocol === 'https:' ? 'wss://' : 'ws://';
  ws = new WebSocket(wsProto + location.host + '/ws');

  ws.onopen = () => {
    document.getElementById('wsdot').classList.add('on');
    document.getElementById('ws-lbl').textContent = 'Live';
    delay = 2000;
  };

  ws.onclose = () => {
    document.getElementById('wsdot').classList.remove('on');
    document.getElementById('ws-lbl').textContent = 'Reconnecting…';
    setTimeout(connect, delay);
    delay = Math.min(delay * 1.5, 30000);
  };

  ws.onmessage = e => {
    const msg = JSON.parse(e.data);
    if (msg.type === 'init') {
      applyStatus(msg.status);
      [...msg.signals].reverse().forEach(s => addCard(s, false));
      if (msg.scan_log && msg.scan_log.length) {
        msg.scan_log.forEach(entry => addLogEntry(entry, false));
      }
    } else if (msg.type === 'signals') {
      applyStatus(msg.status);
      clearGrid();
      msg.signals.forEach(s => addCard(s, true));
      if (msg.log_entry) addLogEntry(msg.log_entry, true);
    } else if (msg.type === 'status' || msg.type === 'no_signal') {
      applyStatus(msg.status);
      if (msg.log_entry) addLogEntry(msg.log_entry, true);
    } else if (msg.type === 'shutdown') {
      document.getElementById('bot-badge').className = 'badge badge-offline';
      document.getElementById('bot-text').textContent = 'Offline';
    }
  };
}

function applyStatus(s) {
  if (!s) return;
  const mb = document.getElementById('mkt-badge');
  if (s.market_open) {
    mb.className = 'badge badge-mktopen';
    document.getElementById('mkt-text').textContent = 'Market Open';
  } else {
    mb.className = 'badge badge-mktclosed';
    document.getElementById('mkt-text').textContent = 'Market Closed';
  }
  if (s.last_scan) {
    document.getElementById('last-scan').textContent = 'Last scan  ' + s.last_scan;
  }
}

function scoreBubbleClass(score) {
  if (score >= 0.80) return 's-hi';
  if (score >= 0.65) return 's-md';
  return 's-lo';
}

function sentPillClass(label) {
  if (label === 'BULLISH') return 'p-bull';
  if (label === 'BEARISH') return 'p-bear';
  return 'p-neu';
}

function pct(current, target) {
  return ((target - current) / current * 100).toFixed(1);
}

function clearGrid() {
  const grid = document.getElementById('grid');
  grid.querySelectorAll('.card').forEach(c => c.remove());
  total = 0;
  document.getElementById('sig-count').textContent = '';
  document.getElementById('sig-count-label').textContent = '';
  if (!grid.querySelector('#empty')) {
    const ph = document.createElement('div');
    ph.id = 'empty';
    grid.appendChild(ph);
  }
}

function addCard(s, isNew) {
  const grid = document.getElementById('grid');
  const empty = document.getElementById('empty');
  if (empty) empty.remove();

  total++;
  const cnt = total + ' signal' + (total !== 1 ? 's' : '') + ' today';
  document.getElementById('sig-count').textContent = cnt;
  document.getElementById('sig-count-label').textContent = cnt;

  const sc   = scoreBubbleClass(s.composite_score);
  const sent = sentPillClass(s.sentiment_label);
  const rsi  = s.rsi !== null && s.rsi !== undefined ? s.rsi : '—';
  const stopPct   = pct(s.current_price, s.stop_loss);
  const targetPct = pct(s.current_price, s.take_profit);

  const newsHtml = s.top_url
    ? '<a class="news-link" href="' + s.top_url + '" target="_blank" rel="noopener">' +
        'Read article' +
        '<svg width="10" height="10" viewBox="0 0 10 10" fill="none" xmlns="http://www.w3.org/2000/svg">' +
          '<path d="M1.5 8.5L8.5 1.5M8.5 1.5H3.5M8.5 1.5V6.5" stroke="currentColor" stroke-width="1.5" stroke-linecap="round" stroke-linejoin="round"/>' +
        '</svg>' +
      '</a>'
    : '';

  const card = document.createElement('div');
  card.className = 'card' + (isNew ? ' fresh' : '');
  card.innerHTML =
    '<div class="card-top">' +
      '<div class="ticker-wrap">' +
        '<div class="ticker-row">' +
          '<span class="ticker">$' + s.ticker + '</span>' +
          '<span class="buy-badge">BUY</span>' +
        '</div>' +
        '<div class="cur-price">Current  $' + s.current_price.toFixed(2) + '</div>' +
      '</div>' +
      '<div class="score-bubble ' + sc + '">' +
        '<span class="score-num">' + s.composite_score.toFixed(2) + '</span>' +
        '<span class="score-lbl">Score</span>' +
      '</div>' +
    '</div>' +

    '<div class="levels">' +
      '<div class="lvl lvl-entry">' +
        '<div class="lvl-lbl">Entry Zone</div>' +
        '<div class="lvl-price">$' + s.entry_low + '</div>' +
        '<div class="lvl-price2">$' + s.entry_high + '</div>' +
      '</div>' +
      '<div class="lvl lvl-stop">' +
        '<div class="lvl-lbl">Stop Loss</div>' +
        '<div class="lvl-price">$' + s.stop_loss + '</div>' +
        '<div class="lvl-pct">' + stopPct + '%</div>' +
      '</div>' +
      '<div class="lvl lvl-tgt">' +
        '<div class="lvl-lbl">Target</div>' +
        '<div class="lvl-price">$' + s.take_profit + '</div>' +
        '<div class="lvl-pct">+' + targetPct + '%</div>' +
      '</div>' +
    '</div>' +

    '<div class="pills">' +
      '<span class="pill">RSI ' + rsi + '</span>' +
      '<span class="pill ' + (s.macd_cross  ? 'p-on'  : 'p-dim') + '">MACD '  + (s.macd_cross  ? '✓' : '✗') + '</span>' +
      '<span class="pill ' + (s.ema_reclaim ? 'p-on'  : 'p-dim') + '">EMA '   + (s.ema_reclaim ? '✓' : '✗') + '</span>' +
      '<span class="pill">Vol ' + s.volume_ratio.toFixed(1) + 'x</span>' +
      '<span class="pill ' + sent + '">' + s.sentiment_label + '  ' + s.sentiment_score.toFixed(2) + '</span>' +
    '</div>' +

    '<div class="news-block">' +
      '<div class="news-headline">' + s.top_headline + '</div>' +
      newsHtml +
    '</div>' +

    '<div class="card-foot">' +
      new Date().toLocaleTimeString('en-US', {hour:'2-digit', minute:'2-digit', timeZone:'America/New_York'}) + ' ET' +
    '</div>';

  if (isNew) setTimeout(() => card.classList.remove('fresh'), 4000);
  grid.insertBefore(card, grid.firstChild);

  const cards = grid.querySelectorAll('.card');
  if (cards.length > 20) cards[cards.length - 1].remove();

  // Auto-remove after 1 hour from when the signal was generated
  const signalTs = s._ts ? s._ts * 1000 : Date.now();
  const expiresIn = Math.max(0, signalTs + 3600000 - Date.now());
  setTimeout(() => {
    card.remove();
    total = Math.max(0, total - 1);
    const cnt = total > 0 ? total + ' signal' + (total !== 1 ? 's' : '') + ' today' : '';
    document.getElementById('sig-count').textContent = cnt;
    document.getElementById('sig-count-label').textContent = cnt;
    if (!document.getElementById('grid').querySelector('.card')) {
      const ph = document.createElement('div');
      ph.id = 'empty';
      ph.innerHTML = '<div class="empty-icon">📡</div><h3>Scanning for signals…</h3><p>Strong setups appear here in real-time when the market is open</p>';
      document.getElementById('grid').appendChild(ph);
    }
  }, expiresIn);
}

/* ── Scan history log ── */
function addLogEntry(entry, prepend) {
  var box = document.getElementById('log-box');
  var empty = document.getElementById('log-empty');
  if (empty) empty.remove();

  var badgeClass, badgeLabel, descHtml;
  if (entry.type === 'signals') {
    badgeClass = 'lb-signals';
    badgeLabel = entry.count + ' Signal' + (entry.count !== 1 ? 's' : '');
    var tags = (entry.tickers || []).map(function(t) {
      return '<span class="log-ticker-tag">$' + t + '</span>';
    }).join('');
    descHtml = '<span class="log-tickers">' + tags + '</span>';
  } else if (entry.type === 'no_signal') {
    badgeClass = 'lb-no-signal';
    badgeLabel = 'No Signals';
    descHtml = '<span>No setups passed threshold</span>';
  } else {
    badgeClass = 'lb-mkt-closed';
    badgeLabel = 'Mkt Closed';
    descHtml = '<span>Scan skipped</span>';
  }

  var row = document.createElement('div');
  row.className = 'log-row';
  row.innerHTML =
    '<span class="log-time">' + (entry.time || '—') + '</span>' +
    '<span class="log-badge ' + badgeClass + '"><span class="dot"></span>' + badgeLabel + '</span>' +
    '<span class="log-desc">' + descHtml + '</span>';

  if (prepend) {
    box.insertBefore(row, box.firstChild);
    var rows = box.querySelectorAll('.log-row');
    if (rows.length > 50) rows[rows.length - 1].remove();
  } else {
    box.appendChild(row);
  }
}

function clearLog() {
  document.getElementById('log-box').innerHTML = '<div id="log-empty">No scans yet this session</div>';
}

/* ── Theme toggle ── */
var MOON_PATH = 'M21 12.79A9 9 0 1 1 11.21 3 7 7 0 0 0 21 12.79z';
var SUN_PATHS = [
  'M12 3v1','M12 20v1','M4.22 4.22l.71.71','M18.36 18.36l.71.71',
  'M3 12h1','M20 12h1','M4.93 19.07l.71-.71','M18.36 5.64l.71-.71'
];

function setThemeIcon(theme) {
  var svg = document.getElementById('theme-icon');
  if (theme === 'light') {
    svg.innerHTML = '<path d="' + MOON_PATH + '"/>';
  } else {
    svg.innerHTML = SUN_PATHS.map(function(d){ return '<path d="' + d + '"/>'; }).join('') +
      '<circle cx="12" cy="12" r="4"/>';
  }
}

function toggleTheme() {
  var current = document.documentElement.getAttribute('data-theme');
  var next = current === 'light' ? 'dark' : 'light';
  document.documentElement.setAttribute('data-theme', next);
  localStorage.setItem('meem-theme', next);
  setThemeIcon(next);
}

/* Set correct icon on load */
setThemeIcon(document.documentElement.getAttribute('data-theme') || 'dark');

connect();
</script>
</body>
</html>"""
