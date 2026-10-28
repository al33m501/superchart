"""Render the last successful screener calculation without waiting for MOEX."""

import pandas as pd
import streamlit as st
from dotenv import load_dotenv

from superchart.screener_data import (
    MEDIAN, RETURN, TURNOVER, read_refresh_status, read_snapshot, refresh_interval, snapshot_table,
)


load_dotenv()


def display_table(table):
    table = table.copy()
    table[RETURN] = table[RETURN].mul(100).round(2).astype(str) + "%"
    for column in (TURNOVER, MEDIAN):
        table[column] = table[column].div(1e6).round(2).astype(str)
    return table


@st.fragment(run_every="30s")
def render_all(min_turnover):
    snapshot = read_snapshot()
    with st.expander("Состояние загрузки", expanded=snapshot is None):
        status = read_refresh_status()
        if status is None:
            st.write("Фоновая загрузка ещё не зарегистрировала попытку. "
                     "Проверьте, что приложение запущено командой python -m superchart.run.")
        else:
            attempted_at = pd.Timestamp(status["attempted_at"]).tz_convert("Europe/Moscow")
            st.write("Последняя попытка: {:%d.%m.%Y %H:%M:%S} МСК".format(attempted_at))
            st.write(status["message"])
            status_age = (pd.Timestamp.now(tz="UTC") - pd.Timestamp(status["updated_at"])).total_seconds()
            if status_age > max(180, 3 * refresh_interval()):
                st.write("Новых попыток давно нет: проверьте, что процесс обновления работает.")
        if snapshot:
            saved_at = pd.Timestamp(snapshot["saved_at"]).tz_convert("Europe/Moscow")
            st.write("Последний успешный расчёт сохранён: {:%d.%m.%Y %H:%M:%S} МСК".format(saved_at))
    if snapshot is None:
        st.error("Не удалось получить данные скринера: сохранённого результата пока нет. "
                 "Подробности — в состоянии загрузки выше.")
        return

    market_time = pd.Timestamp(snapshot["market_time"])
    age = (pd.Timestamp.now(tz="UTC") - pd.Timestamp(snapshot["saved_at"])).total_seconds()
    st.caption("Данные MOEX на {:%d.%m.%Y %H:%M:%S} МСК. ".format(market_time)
               + "Показан последний успешный расчёт; обновление выполняется в фоне.")
    if age > max(180, 3 * refresh_interval()):
        st.warning("Данные давно не обновлялись. Показан сохранённый результат на указанную дату.")

    all_stocks = snapshot_table(snapshot)
    stocks = all_stocks.loc[all_stocks[MEDIAN] >= min_turnover]
    if stocks.empty:
        st.info("Нет бумаг, соответствующих выбранному минимальному обороту.")
    else:
        col1, col2 = st.columns(2)
        with col1:
            st.write("Top 10:")
            st.table(display_table(stocks.sort_values(RETURN, ascending=False).head(10)))
        with col2:
            st.write("Bottom 10:")
            st.table(display_table(stocks.sort_values(RETURN).head(10)))
        with st.expander("Full table ⬇️"):
            st.table(display_table(stocks.sort_values(RETURN)))

    st.write("Top 20 stocks by turnover ({:%d.%m.%Y}):".format(market_time))
    st.table(display_table(all_stocks.sort_values(TURNOVER, ascending=False).head(20)))


def main():
    st.set_page_config(page_title="Superchart", page_icon="📈", layout="wide")
    st.markdown("<style>#MainMenu {visibility: hidden;}</style>", unsafe_allow_html=True)
    st.sidebar.subheader("📈 Superchart")
    st.subheader("Screener")
    choices = {"100M₽": 100e6, "50M₽": 50e6, "0M₽": 0}
    selected = st.radio("Minimum median turnover:", list(choices))
    render_all(choices[selected])


if __name__ == "__main__":
    main()
