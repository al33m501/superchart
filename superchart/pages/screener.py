import streamlit as st
import traceback
import json
from streamlit_lightweight_charts import renderLightweightCharts
import streamlit_lightweight_charts.dataSamples as data
import pickle
import pandas as pd
import numpy as np
import os
import apimoex
from requests.auth import HTTPBasicAuth
import requests
from dotenv import load_dotenv
from moexalgo.session import authorize
from moexalgo import Ticker, Market

load_dotenv()
# authorize(os.getenv("MOEX_LOGIN"), os.getenv("MOEX_PASSWORD"))
# url = 'https://passport.moex.com/authenticate'
# username = os.getenv("MOEX_LOGIN")
# password = os.getenv("MOEX_PASSWORD")
# response = requests.get(url, auth=HTTPBasicAuth(username, password))
# cert = response.cookies['MicexPassportCert']


class APIMOEXError(Exception):
    pass


with open(os.path.join(os.getenv("PATH_TO_DATA_FOLDER"), 'cert.p'), 'rb') as f:
    cert = pickle.load(f)
EXCHANGE_MAP = {"MOEX": {"market": "shares", "engine": "stock", "board": "tqbr"},
                "MOEX CETS": {"market": "selt", "engine": "currency", "board": "cets"},
                "MOEX SPBFUT": {"market": "forts", "engine": "futures", "board": "spbfut"},
                "SNDX": {"market": "index", "engine": "stock", "board": "SNDX"}}


def get_current_stock_table(exchange):
    arguments = {}
    response = requests.get(f"https://iss.moex.com/iss/engines/{EXCHANGE_MAP[exchange]['engine']}/"
                            f"markets/{EXCHANGE_MAP[exchange]['market']}/boards/{EXCHANGE_MAP[exchange]['board']}/securities.json",
                            cookies={'MicexPassportCert': cert},
                            params=arguments)
    data = response.json()
    data = pd.DataFrame(data['marketdata']['data'], columns=data['marketdata']['columns'])
    return data


def get_stock_table(min_turnover=0):
    with open(os.path.join(os.getenv("PATH_TO_DATA_FOLDER"), 'last_prices.p'), 'rb') as f:
        last_prices = pickle.load(f)
    stock_table = get_current_stock_table("MOEX").set_index("SECID")[['LAST', 'VALTODAY']].dropna()
    stock_table = pd.merge(left=stock_table, left_index=True,
                           right=last_prices.rename(columns={"PX_LAST": "yesterday_price"}), right_index=True)
    stock_table['Return 1d, %'] = (stock_table['LAST'] - stock_table['yesterday_price']) / stock_table[
        'yesterday_price']
    stock_table = stock_table[['Return 1d, %', 'VALTODAY', 'MEDIAN_TURNOVER']].rename(
        columns={"VALTODAY": "Turnover today, M RUB",
                 "MEDIAN_TURNOVER": "Median(90d) Turnover, M RUB"}).sort_values('Return 1d, %')
    stock_table = stock_table[stock_table['Median(90d) Turnover, M RUB'] > min_turnover]
    stock_table['Turnover today, M RUB'] = stock_table['Turnover today, M RUB'].div(1e6).round(2).astype(str)
    stock_table['Median(90d) Turnover, M RUB'] = stock_table['Median(90d) Turnover, M RUB'].div(1e6).round(2).astype(str)
    stock_table['Return 1d, %'] = stock_table['Return 1d, %'].mul(100).round(2).astype(str) + "%"
    return stock_table


def main():
    st.set_page_config(
        page_title="Superchart",
        page_icon="📈",
        layout='wide'
    )
    st.sidebar.subheader("""📈 Superchart""")
    st.subheader(f"""Screener""")

    stock_table = get_stock_table(min_turnover=50e6)
    st.table(stock_table)


main()
