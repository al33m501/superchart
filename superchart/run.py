"""Run Streamlit and the screener updater in the SAME Heroku dyno/filesystem."""

import logging
import os
from pathlib import Path
import signal
import subprocess
import sys
import threading

from dotenv import load_dotenv

from superchart.screener_data import refresh_interval, run_updater


def main():
    load_dotenv()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    interval = refresh_interval()
    stop = threading.Event()
    for signum in (signal.SIGTERM, signal.SIGINT):
        signal.signal(signum, lambda *_: stop.set())

    root = Path(__file__).resolve().parents[1]
    environment = os.environ.copy()
    environment["PYTHONPATH"] = str(root) + os.pathsep + environment.get("PYTHONPATH", "")
    web = subprocess.Popen([
        sys.executable, "-m", "streamlit", "run", str(root / "superchart" / "moex_stocks.py"),
        "--server.port", os.getenv("PORT", "8501"), "--server.address", "0.0.0.0",
        "--server.headless", "true",
    ], env=environment)
    updater = threading.Thread(target=run_updater, args=(stop, interval), name="screener-updater", daemon=True)
    updater.start()
    try:
        while web.poll() is None and not stop.wait(0.5):
            if not updater.is_alive():
                logging.error("Screener updater stopped unexpectedly; restarting the dyno process")
                return 1
        return web.returncode if web.returncode is not None else 0
    finally:
        stop.set()
        if web.poll() is None:
            web.terminate()
            try:
                web.wait(timeout=10)
            except subprocess.TimeoutExpired:
                web.kill()
                web.wait()
        updater.join(timeout=1)


if __name__ == "__main__":
    sys.exit(main())
