import streamlit as st
import pickle
import pandas as pd
import os
import requests
from dotenv import load_dotenv
from sqlalchemy import (
    create_engine, text
)
import asyncio
from sqlalchemy.ext.asyncio import create_async_engine

load_dotenv()
CACHE_TTL_SECONDS = 300


class APIMOEXError(Exception):
    pass

url = os.getenv("NEON_URL")
token = os.getenv("APIMOEX_TOKEN")


EXCHANGE_MAP = {"MOEX": {"market": "shares", "engine": "stock", "board": "tqbr"},
                "MOEX CETS": {"market": "selt", "engine": "currency", "board": "cets"},
                "MOEX SPBFUT": {"market": "forts", "engine": "futures", "board": "spbfut"},
                "SNDX": {"market": "index", "engine": "stock", "board": "SNDX"}}


def get_current_stock_table(exchange):
    arguments = {}
    headers = {
        'Authorization': f'Bearer {token}',
    }
    response = requests.get(f"https://apim.moex.com/iss/engines/{EXCHANGE_MAP[exchange]['engine']}/"
                            f"markets/{EXCHANGE_MAP[exchange]['market']}/boards/{EXCHANGE_MAP[exchange]['board']}/securities.json",
                            headers=headers,
                            params=arguments,
                            verify=False)
    data = response.json()
    data = pd.DataFrame(data['marketdata']['data'], columns=data['marketdata']['columns'])
    return data

async def load_data_neon(query):
    engine = create_async_engine(url)
    try:
        async with engine.begin() as conn:
            result = await conn.execute(text(query))
            return pd.DataFrame(result)
    except Exception as e:
        print(f"Error loading data: {e}")
        return []
    finally:
        await engine.dispose()


@st.cache_data(show_spinner=False, ttl=CACHE_TTL_SECONDS)
def load_data_neon_sync(table):
    df = asyncio.run(load_data_neon(f"select * from {table}"))
    return df


def get_stock_table(min_turnover=0):
    last_prices = load_data_neon_sync("last_prices2").set_index("index")
    print(f"LSSSR {last_prices}")
    div_table = load_data_neon_sync("div_table")
    stock_table = get_current_stock_table("MOEX").set_index("SECID")[['LAST', 'VALTODAY', 'SYSTIME']].dropna()
    stock_table = pd.merge(left=stock_table, left_index=True,
                           right=last_prices.rename(columns={"PX_LAST": "yesterday_price"}), right_index=True)
    today_dt = pd.to_datetime(get_current_stock_table("MOEX")['SYSTIME'].iloc[0]).date()
    stock_table = stock_table.drop(['SYSTIME'], axis=1)
    today_divs = div_table[div_table['ex_date'] == today_dt.strftime("%Y-%m-%d")]
    stock_table = pd.merge(left=stock_table, left_index=True, right=today_divs.set_index("ticker")['dividend_amount'],
                           right_index=True, how='left')
    stock_table['dividend_amount'] = stock_table['dividend_amount'].fillna(0)
    stock_table['Return 1d, %'] = (stock_table['LAST'] + stock_table['dividend_amount'] - stock_table[
        'yesterday_price']) / stock_table[
                                      'yesterday_price']
    stock_table = stock_table[['Return 1d, %', 'VALTODAY', 'MEDIAN_TURNOVER']].rename(
        columns={"VALTODAY": "Turnover today, M RUB",
                 "MEDIAN_TURNOVER": "Median(90d) Turnover, M RUB"}).sort_values('Return 1d, %')

    stock_table_non_liquid = stock_table.copy()
    stock_table['Turnover today, M RUB'] = stock_table['Turnover today, M RUB'].div(1e6).round(2).astype(str)

    stock_table = stock_table[stock_table['Median(90d) Turnover, M RUB'] >= min_turnover]
    stock_table['Median(90d) Turnover, M RUB'] = stock_table['Median(90d) Turnover, M RUB'].div(1e6).round(2).astype(
        str)
    return stock_table, stock_table_non_liquid

def render_all(min_turnover):
    stock_table, stock_table_non_liquid = get_stock_table(min_turnover=min_turnover)

    col1, col2 = st.columns(2)
    with col1:
        st.write("Top 10:")
        top_10 = stock_table.sort_values("Return 1d, %", ascending=False).iloc[:10].copy()
        top_10['Return 1d, %'] = top_10['Return 1d, %'].mul(100).round(2).astype(str) + "%"
        st.table(top_10)

    with col2:
        st.write("Bottom 10:")
        bottom_10 = stock_table.sort_values("Return 1d, %", ascending=True).iloc[:10].copy()
        bottom_10['Return 1d, %'] = bottom_10['Return 1d, %'].mul(100).round(2).astype(str) + "%"
        st.table(bottom_10)

    with st.expander("Full table ⬇️"):
        stock_table = stock_table.sort_values("Return 1d, %", ascending=True)
        stock_table['Return 1d, %'] = stock_table['Return 1d, %'].mul(100).round(2).astype(str) + "%"
        st.table(stock_table)

    st.write("Top 20 stocks by turnover, today:")
    stock_table_non_liquid = stock_table_non_liquid.sort_values("Turnover today, M RUB", ascending=False).iloc[:20]
    stock_table_non_liquid['Median(90d) Turnover, M RUB'] = stock_table_non_liquid['Median(90d) Turnover, M RUB'].div(
        1e6).round(2).astype(
        str)
    stock_table_non_liquid['Turnover today, M RUB'] = stock_table_non_liquid['Turnover today, M RUB'].div(
        1e6).round(2).astype(
        str)
    stock_table_non_liquid['Return 1d, %'] = stock_table_non_liquid['Return 1d, %'].mul(100).round(2).astype(str) + "%"
    st.table(stock_table_non_liquid)


def main():
    st.set_page_config(
        page_title="Superchart",
        page_icon="📈",
        layout='wide'
    )
    hide_menu_style = """
                    <style>
                    #MainMenu {visibility: hidden;}
                    </style>
                    """
    st.markdown(hide_menu_style, unsafe_allow_html=True)
    st.sidebar.subheader("""📈 Superchart""")
    st.subheader(f"""Screener""")

    data_select = st.radio('Minumum median turnover:', ('100M₽', '50M₽', '0M₽'))
    if data_select == '0M₽':
        render_all(0)
    elif data_select == '50M₽':
        render_all(50e6)
    elif data_select == '100M₽':
        render_all(100e6)
    print()
    # date = st.date_input("Select start date", value=dt(2022, 7, 25))
    # st.write("Selected date:", date)


main()
