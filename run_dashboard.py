"""
Standalone dashboard server — reads from Neon DB, no bot/scanner running.
Run from the trading-signal-bot directory:
    python run_dashboard.py
"""
import logging
import time
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="[%(asctime)s] %(levelname)s — %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)

import dashboard

dashboard.start_dashboard(host="127.0.0.1", port=8000)

print("\n  Dashboard -> http://localhost:8000\n  Press Ctrl+C to stop.\n")

try:
    while True:
        time.sleep(60)
except KeyboardInterrupt:
    print("Stopped.")
