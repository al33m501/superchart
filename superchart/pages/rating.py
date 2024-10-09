import streamlit as st
import pickle
import pandas as pd
from datetime import date as dt
import os
import requests
from dotenv import load_dotenv

load_dotenv()


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


def render_eod_to_eod(base_dict, ticker_list, date_start, date_end):
    date_start = pd.Timestamp(date_start)
    date_end = pd.Timestamp(date_end)
    st.write("Only stocks that NOW have >50M RUB median turnover (or stocks from special list)")
    rating = pd.DataFrame()
    missing_tickers = []
    for ticker, ticker_data in base_dict.items():
        try:
            price_old = ticker_data.loc[date_start, 'PX_LAST']
            price_new = ticker_data.loc[date_end, 'PX_LAST']

            ret = (price_new - price_old) / price_old
            rating.loc[ticker, f'Return'] = ret
            rating.loc[ticker, 'Median turnover, M₽'] = round(ticker_list.loc[ticker] / 1e6)
        except Exception as e:
            missing_tickers.append(ticker)
            continue
    st.write(f"Missing tickers: {missing_tickers}")
    rating = rating.sort_values(f'Return')
    rating['Return'] = rating['Return'].mul(100)  # .round(2).astype(str) + "%"
    rating['Median turnover, M₽'] = rating['Median turnover, M₽'].astype(int).astype(str)
    # st.table(rating)

    st.write(rating.style.background_gradient(cmap='RdYlGn', axis=None).to_html(),
             unsafe_allow_html=True)


def render_eod_to_rt(base_dict, ticker_list, date_start):
    date_start = pd.Timestamp(date_start)
    st.write("Only stocks that NOW have >50M RUB median turnover (or stocks from special list)")
    with open(os.path.join(os.getenv("PATH_TO_DATA_FOLDER"), 'div_table.p'), 'rb') as f:
        div_table = pickle.load(f)
    stock_table = get_current_stock_table("MOEX").set_index("SECID")[['LAST', 'SYSTIME']].dropna()
    today_dt = pd.to_datetime(stock_table['SYSTIME'].iloc[0]).date()
    stock_table = stock_table.drop(['SYSTIME'], axis=1)
    today_divs = div_table[div_table['ex_date'] == today_dt.strftime("%Y-%m-%d")]
    stock_table = pd.merge(left=stock_table, left_index=True, right=today_divs.set_index("ticker")['dividend_amount'],
                           right_index=True, how='left')
    stock_table['LAST'] = stock_table['LAST'] + stock_table['dividend_amount'].fillna(0)
    stock_table = stock_table['LAST']

    rating = pd.DataFrame()
    missing_tickers = []
    for ticker, ticker_data in base_dict.items():
        try:
            price_old = ticker_data.loc[date_start, 'PX_LAST']
            price_new = stock_table.loc[ticker]

            ret = (price_new - price_old) / price_old
            rating.loc[ticker, f'Return'] = ret
            rating.loc[ticker, 'Median turnover, M₽'] = round(ticker_list.loc[ticker] / 1e6)
        except Exception as e:
            missing_tickers.append(ticker)
            continue
    st.write(f"Missing tickers: {missing_tickers}")
    rating = rating.sort_values(f'Return')
    rating['Return'] = rating['Return'].mul(100)  # .astype(str) + "%"
    rating['Median turnover, M₽'] = rating['Median turnover, M₽'].astype(int).astype(str)
    # st.table(rating)
    st.write(rating.style.background_gradient(cmap='RdYlGn', axis=None).to_html(),
             unsafe_allow_html=True)


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
    st.subheader(f"""Rating""")

    data_select = st.radio('Rating type:', ('EOD to EOD', 'EOD to realtime'))
    with open(os.path.join(os.getenv("PATH_TO_DATA_FOLDER"), 'base_dict.p'), 'rb') as f:
        base_dict = pickle.load(f)
    with open(os.path.join(os.getenv("PATH_TO_DATA_FOLDER"), 'ticker_list.p'), 'rb') as f:
        ticker_list = pickle.load(f)
    if data_select == 'EOD to EOD':
        last_dt_in_db = base_dict['SBER'].index[-1].to_pydatetime().date()
        date_start = st.date_input("Select start date", value=dt(2024, 5, 17))
        date_end = st.date_input("Select end date", value=last_dt_in_db)
        render_eod_to_eod(base_dict, ticker_list, date_start, date_end)
    elif data_select == 'EOD to realtime':
        date_start = st.date_input("Select start date", value=dt(2024, 5, 17))
        render_eod_to_rt(base_dict, ticker_list, date_start)


main()
