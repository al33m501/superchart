"""MOEX futures candles from local daily pickles and the MOEX ISS API."""

import json
import os
from pathlib import Path

import pandas as pd
import requests
import streamlit as st
from dotenv import load_dotenv
from streamlit_lightweight_charts import renderLightweightCharts


load_dotenv()

FUTURES = ("CRZ6", "MXZ6", "SiZ6")
PRICE_PRECISION = {"CRZ6": 3, "MXZ6": 0, "SiZ6": 0}
OHLC = ["open", "high", "low", "close"]
TIMEFRAMES = {
    "Daily": (252, None),
    "Weekly": (252 * 5, "W-FRI"),
    "Monthly": (252 * 15, pd.offsets.MonthEnd()),
}


def load_history(path):
    data = pd.read_pickle(path).rename(columns={
        "OPEN": "open", "HIGH": "high", "LOW": "low",
        "CLOSE": "close", "VALUE": "value",
    })[OHLC + ["value"]].copy()
    data.index = pd.to_datetime(data.index).normalize()
    data = data.apply(pd.to_numeric, errors="coerce")
    data = data.replace([float("inf"), -float("inf")], float("nan")).dropna()
    data = data.loc[data.index.notna()]
    data = data.loc[~data.index.duplicated(keep="last")].sort_index()
    if data.empty:
        raise ValueError("No futures candles are available.")
    return data


def get_current_candle(ticker):
    token = os.getenv("APIMOEX_TOKEN")
    host = "https://apim.moex.com" if token else "https://iss.moex.com"
    headers = {"Authorization": "Bearer " + token} if token else {}
    response = requests.get(
        host + "/iss/engines/futures/markets/forts/boards/RFUD/securities/{}.json".format(ticker),
        headers=headers,
        params={"iss.meta": "off", "iss.only": "marketdata"},
        timeout=10,
    )
    response.raise_for_status()
    block = response.json()["marketdata"]
    if not block["data"]:
        return None, None
    quote = dict(zip(block["columns"], block["data"][0]))
    trade_date = pd.to_datetime(quote["TRADEDATE"], errors="coerce")
    if pd.isna(trade_date):
        return None, None
    candle = pd.DataFrame([{
        "open": quote["OPEN"], "high": quote["HIGH"], "low": quote["LOW"],
        "close": quote["LAST"], "value": quote["VALTODAY"],
    }], index=[trade_date.normalize()]).apply(pd.to_numeric, errors="coerce")
    candle = candle.replace([float("inf"), -float("inf")], float("nan")).dropna()
    if candle.empty:
        return None, None
    row = candle.iloc[0]
    if not (0 < row["low"] <= min(row["open"], row["close"])
            <= max(row["open"], row["close"]) <= row["high"] and row["value"] >= 0):
        return None, None
    return candle, quote["UPDATETIME"]


def resample_candlestick(data, frequency):
    resampled = data.resample(frequency).agg({
        "open": "first", "high": "max", "low": "min",
        "close": "last", "value": "sum",
    }).dropna()
    # Label the unfinished week/month with the last available trading day.
    return resampled.rename(index={resampled.index[-1]: data.index[-1]})


def render_candlestick_chart(data, ticker, timeframe):
    data = data.copy()
    data.index.name = "time"
    data = data.reset_index()
    data["time"] = data["time"].dt.strftime("%Y-%m-%d")
    bull, bear = "rgba(38,166,154,0.9)", "rgba(239,83,80,0.9)"
    data["color"] = [bull if close > opening else bear
                     for opening, close in zip(data["open"], data["close"])]
    candles = json.loads(data[["time"] + OHLC].to_json(orient="records"))
    turnover = json.loads(data[["time", "value", "color"]].to_json(orient="records"))
    precision = PRICE_PRECISION[ticker]
    chart_options = {
        "handleScroll": False,
        "handleScale": False,
        "layout": {"background": {"type": "solid", "color": "white"},
                   "textColor": "black"},
        "grid": {"vertLines": {"color": "rgba(197,203,206,0.5)"},
                 "horzLines": {"color": "rgba(197,203,206,0.5)"}},
        "crosshair": {"mode": 0},
        "timeScale": {"borderColor": "rgba(197,203,206,0.8)", "barSpacing": 15},
    }
    renderLightweightCharts([
        {
            "chart": dict(chart_options, height=550),
            "series": [{
                "type": "Candlestick", "data": candles,
                "options": {
                    "upColor": bull, "downColor": bear, "borderVisible": False,
                    "wickUpColor": bull, "wickDownColor": bear,
                    "priceFormat": {"type": "price", "precision": precision,
                                    "minMove": 10 ** -precision},
                },
            }],
        },
        {
            "chart": dict(chart_options, height=100),
            "series": [{
                "type": "Histogram", "data": turnover,
                "options": {"priceFormat": {"type": "volume"}},
            }],
        },
    ], key="moex_futures_{}_{}".format(ticker, timeframe))


def main():
    st.set_page_config(page_title="Superchart", page_icon="📈", layout="wide")
    st.markdown("<style>#MainMenu {visibility: hidden;}</style>", unsafe_allow_html=True)
    st.sidebar.subheader("📈 Superchart")
    ticker = st.sidebar.selectbox("Select asset:", FUTURES, key="moex_futures_ticker")
    data_dir = Path(os.getenv("PATH_TO_DATA_FOLDER") or Path(__file__).resolve().parents[2] / "data")
    try:
        data = load_history(data_dir / (ticker.lower() + ".p"))
    except (OSError, ValueError, KeyError) as exc:
        st.error("Unable to load history for {}: {}".format(ticker, exc))
        return

    updated_at = "{:%d.%m.%Y}".format(data.index[-1])
    try:
        candle, quote_time = get_current_candle(ticker)
        today = pd.Timestamp.now(tz="Europe/Moscow").tz_localize(None).normalize()
        # Keep completed historical days; replace today's candle on refresh.
        if candle is not None and (candle.index[0] > data.index[-1]
                                  or candle.index[0] == data.index[-1] == today):
            data = pd.concat([data, candle])
            data = data.loc[~data.index.duplicated(keep="last")].sort_index()
            updated_at = "{:%d.%m.%Y} {}".format(candle.index[0], quote_time)
            if not os.getenv("APIMOEX_TOKEN"):
                st.caption("Public MOEX ISS quotes may be delayed.")
    except (requests.RequestException, KeyError, ValueError, TypeError):
        st.caption("Current quote is unavailable. Showing daily history.")

    change_label = ""
    if len(data) > 1 and data["close"].iloc[-2] != 0:
        change = (data["close"].iloc[-1] / data["close"].iloc[-2] - 1) * 100
        color = "green" if change >= 0 else "red"
        change_label = " :{}[{:+.2f}%]".format(color, change)
    st.subheader(ticker + change_label)
    st.markdown("Price updated at: **{}**".format(updated_at))
    st.markdown("[Trading View](https://ru.tradingview.com/chart/?symbol=MOEX:{})".format(ticker))
    timeframe = st.selectbox("Select timeframe:", list(TIMEFRAMES), key="moex_futures_timeframe")
    history_rows, frequency = TIMEFRAMES[timeframe]
    chart_data = data.iloc[-history_rows:]
    if frequency is not None:
        chart_data = resample_candlestick(chart_data, frequency)
    render_candlestick_chart(chart_data, ticker, timeframe)


if __name__ == "__main__":
    main()
