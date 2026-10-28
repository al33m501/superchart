"""Screener snapshots shared by the web process and its background updater."""

import json
import logging
import os
from pathlib import Path
import tempfile

import pandas as pd
import requests


LOGGER = logging.getLogger(__name__)
ROOT = Path(__file__).resolve().parents[1]
RETURN = "Return 1d, %"
TURNOVER = "Turnover, M RUB"
MEDIAN = "Median(90d) Turnover, M RUB"
COLUMNS = [RETURN, TURNOVER, MEDIAN]


def data_directory():
    return Path(os.getenv("PATH_TO_DATA_FOLDER") or ROOT / "data")


def cache_path():
    return Path(os.getenv("SCREENER_CACHE_PATH") or ROOT / ".cache" / "screener.json")


def refresh_interval():
    return max(10, int(os.getenv("SCREENER_REFRESH_SECONDS", "60")))


def make_snapshot(table, market_time):
    table = table[COLUMNS].apply(pd.to_numeric, errors="coerce")
    table = table.replace([float("inf"), -float("inf")], float("nan")).dropna()
    table = table.loc[(table[TURNOVER] >= 0) & (table[MEDIAN] >= 0)]
    table.index.name = "SECID"
    snapshot = {
        "version": 1,
        "market_time": pd.Timestamp(market_time).isoformat(),
        "saved_at": pd.Timestamp.now(tz="UTC").isoformat(),
        # Store the calculated result, so old quotes are never joined to new history.
        "rows": json.loads(table.reset_index().to_json(orient="records", double_precision=15)),
    }
    snapshot_table(snapshot)
    return snapshot


def snapshot_table(snapshot):
    if snapshot["version"] != 1:
        raise ValueError("Unsupported screener snapshot")
    for field in ("market_time", "saved_at"):
        if pd.isna(pd.Timestamp(snapshot[field])):
            raise ValueError("Missing snapshot timestamp")
    if pd.Timestamp(snapshot["saved_at"]).tzinfo is None:
        raise ValueError("Snapshot save time must include a timezone")
    table = pd.DataFrame(snapshot["rows"]).set_index("SECID")[COLUMNS]
    values = table.apply(pd.to_numeric, errors="coerce")
    values = values.replace([float("inf"), -float("inf")], float("nan"))
    if (values.empty or values.isna().any().any() or not values.index.is_unique
            or values.index.isna().any() or (values[[TURNOVER, MEDIAN]] < 0).any().any()):
        raise ValueError("Empty or invalid screener snapshot")
    return values


def read_snapshot(path=None):
    try:
        with (Path(path) if path is not None else cache_path()).open(encoding="utf-8") as handle:
            snapshot = json.load(handle)
        snapshot_table(snapshot)
        return snapshot
    except (OSError, ValueError, KeyError, TypeError):
        return None


def write_snapshot(snapshot, path=None):
    """Atomic replacement: readers see either the old complete file or the new one."""
    snapshot_table(snapshot)
    path = Path(path) if path is not None else cache_path()
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = None
    try:
        with tempfile.NamedTemporaryFile(mode="w", encoding="utf-8", dir=path.parent,
                                         prefix=path.name, suffix=".tmp", delete=False) as handle:
            temporary = Path(handle.name)
            json.dump(snapshot, handle, ensure_ascii=False, allow_nan=False)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, path)
    finally:
        if temporary is not None:
            temporary.unlink(missing_ok=True)


def fetch_snapshot(data_dir=None):
    data_dir = Path(data_dir) if data_dir is not None else data_directory()
    token = os.getenv("APIMOEX_TOKEN")
    if not token:
        raise ValueError("APIMOEX_TOKEN is not configured")
    response = requests.get(
        "https://apim.moex.com/iss/engines/stock/markets/shares/boards/TQBR/securities.json",
        headers={"Authorization": "Bearer " + token},
        params={"iss.meta": "off", "iss.only": "marketdata",
                "marketdata.columns": "SECID,LAST,VALTODAY,SYSTIME"},
        timeout=(5, 15),
    )
    response.raise_for_status()
    block = response.json()["marketdata"]
    quotes = pd.DataFrame(block["data"], columns=block["columns"]).set_index("SECID")
    quotes = quotes[["LAST", "VALTODAY", "SYSTIME"]].copy()
    quotes[["LAST", "VALTODAY"]] = quotes[["LAST", "VALTODAY"]].apply(pd.to_numeric, errors="coerce")
    quotes["SYSTIME"] = pd.to_datetime(quotes["SYSTIME"], errors="coerce")
    quotes = quotes.dropna()
    quotes = quotes.loc[(quotes["LAST"] > 0) & (quotes["VALTODAY"] > 0)]
    if quotes.empty:
        raise ValueError("MOEX returned no traded stocks")
    market_time = quotes["SYSTIME"].max()
    quotes = quotes.loc[quotes["SYSTIME"].dt.date == market_time.date()]
    reference = pd.read_pickle(data_dir / "last_prices.p").rename(columns={"PX_LAST": "previous"})
    table = quotes.join(reference, how="inner")
    table = table.loc[table["previous"] > 0].copy()
    dividends = pd.read_pickle(data_dir / "div_table.p")
    dates = pd.to_datetime(dividends["ex_date"]).dt.date
    dividends = dividends.loc[dates == market_time.date()].groupby("ticker")["dividend_amount"].sum()
    amounts = table.index.to_series().map(dividends).fillna(0)
    table[RETURN] = (table["LAST"] + amounts) / table["previous"] - 1
    table = table.rename(columns={"VALTODAY": TURNOVER, "MEDIAN_TURNOVER": MEDIAN})
    return make_snapshot(table, market_time)


def refresh_once():
    """A failed request or calculation never changes the last successful result."""
    try:
        candidate = fetch_snapshot()
        previous = read_snapshot()
        if previous and pd.Timestamp(candidate["market_time"]) < pd.Timestamp(previous["market_time"]):
            LOGGER.warning("Screener: ignoring an older MOEX snapshot")
            return False
        write_snapshot(candidate)
        LOGGER.info("Screener updated: %s, %d stocks", candidate["market_time"], len(candidate["rows"]))
        return True
    except Exception as exc:
        # Do not log exception messages: network errors can contain credentials/URLs.
        LOGGER.warning("Screener refresh failed (%s); keeping previous snapshot", type(exc).__name__)
        return False


def run_updater(stop_event, interval=None):
    interval = refresh_interval() if interval is None else interval
    while not stop_event.is_set():
        refresh_once()
        stop_event.wait(interval)
