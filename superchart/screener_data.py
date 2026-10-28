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


class ScreenerLoadError(ValueError):
    """An application-owned message that is safe to show without redaction."""


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
    write_json(snapshot, path)


def write_json(payload, path):
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = None
    try:
        with tempfile.NamedTemporaryFile(mode="w", encoding="utf-8", dir=path.parent,
                                         prefix=path.name, suffix=".tmp", delete=False) as handle:
            temporary = Path(handle.name)
            json.dump(payload, handle, ensure_ascii=False, allow_nan=False)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, path)
    finally:
        if temporary is not None:
            temporary.unlink(missing_ok=True)


def status_path():
    return cache_path().with_suffix(".status.json")


def read_refresh_status():
    try:
        with status_path().open(encoding="utf-8") as handle:
            status = json.load(handle)
        if status["state"] not in ("running", "success", "error"):
            return None
        for field in ("attempted_at", "updated_at"):
            timestamp = pd.Timestamp(status[field])
            if pd.isna(timestamp) or timestamp.tzinfo is None:
                return None
        if not isinstance(status["message"], str):
            return None
        return status
    except (OSError, ValueError, KeyError, TypeError):
        return None


def report_refresh(state, attempted_at, message):
    try:
        write_json({"state": state, "attempted_at": attempted_at,
                    "updated_at": pd.Timestamp.now(tz="UTC").isoformat(),
                    "message": message}, status_path())
    except OSError as exc:
        LOGGER.warning("Screener status could not be saved (%s)", type(exc).__name__)


def failure_reason(exc):
    # Only application-owned messages and status codes; never response bodies,
    # request headers, raw exception messages or credential-bearing URLs.
    if isinstance(exc, ScreenerLoadError):
        return str(exc)
    if isinstance(exc, requests.exceptions.SSLError):
        return "Ошибка TLS-соединения с MOEX (SSLError)."
    if isinstance(exc, requests.Timeout):
        return "MOEX не ответил вовремя (Timeout)."
    if isinstance(exc, requests.HTTPError):
        code = getattr(exc.response, "status_code", None)
        hints = {401: "Проверьте APIMOEX_TOKEN.", 403: "Доступ отклонён; проверьте права токена.",
                 429: "Превышен лимит запросов."}
        return "MOEX вернул HTTP {}. {}".format(code, hints.get(code, "Сервер отклонил запрос."))
    if isinstance(exc, requests.ConnectionError):
        return "Не удалось подключиться к MOEX (ConnectionError)."
    if isinstance(exc, FileNotFoundError):
        return "Не найден файл данных: проверьте PATH_TO_DATA_FOLDER, last_prices.p и div_table.p."
    if isinstance(exc, PermissionError):
        return "Недостаточно прав для чтения данных или записи кэша (PermissionError)."
    if isinstance(exc, (ValueError, KeyError, TypeError)):
        return "Некорректный ответ MOEX или формат исходных данных ({}).".format(type(exc).__name__)
    return "Ошибка загрузки или сохранения данных ({}).".format(type(exc).__name__)


def fetch_snapshot(data_dir=None):
    data_dir = Path(data_dir) if data_dir is not None else data_directory()
    token = os.getenv("APIMOEX_TOKEN")
    if not token:
        raise ScreenerLoadError("Не задан APIMOEX_TOKEN в окружении процесса обновления.")
    response = requests.get(
        "https://apim.moex.com/iss/engines/stock/markets/shares/boards/TQBR/securities.json",
        headers={"Authorization": "Bearer " + token},
        params={"iss.meta": "off", "iss.only": "marketdata",
                "marketdata.columns": "SECID,LAST,VALTODAY,SYSTIME"},
        verify=False,
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
        raise ScreenerLoadError("MOEX не вернул котировки с ценой и ненулевым оборотом.")
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
    attempted_at = pd.Timestamp.now(tz="UTC").isoformat()
    report_refresh("running", attempted_at, "Запрос данных MOEX выполняется.")
    LOGGER.info("Screener refresh started")
    try:
        candidate = fetch_snapshot()
        previous = read_snapshot()
        if previous and pd.Timestamp(candidate["market_time"]) < pd.Timestamp(previous["market_time"]):
            LOGGER.warning("Screener: ignoring an older MOEX snapshot")
            report_refresh("error", attempted_at, "MOEX вернул данные старше сохранённого результата.")
            return False
        write_snapshot(candidate)
        report_refresh("success", attempted_at, "Обновление успешно: {} бумаг.".format(len(candidate["rows"])))
        LOGGER.info("Screener updated: %s, %d stocks", candidate["market_time"], len(candidate["rows"]))
        return True
    except Exception as exc:
        reason = failure_reason(exc)
        report_refresh("error", attempted_at, reason)
        LOGGER.warning("Screener refresh failed (%s): %s; keeping previous snapshot", type(exc).__name__, reason)
        return False


def run_updater(stop_event, interval=None):
    interval = refresh_interval() if interval is None else interval
    LOGGER.info("Screener updater started; refresh interval=%ss", interval)
    while not stop_event.is_set():
        refresh_once()
        stop_event.wait(interval)


def main():
    """Read-only check, also safe to run in a separate Heroku one-off dyno."""
    import argparse
    from dotenv import load_dotenv

    parser = argparse.ArgumentParser(description="Check the MOEX request and screener calculation without writing a cache")
    parser.add_argument("--check", action="store_true", required=True)
    parser.parse_args()
    load_dotenv()
    try:
        snapshot = fetch_snapshot()
    except Exception as exc:
        print("Screener check FAILED: " + failure_reason(exc))
        return 1
    print("Screener check OK: {} stocks; MOEX time {}".format(len(snapshot["rows"]), snapshot["market_time"]))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
