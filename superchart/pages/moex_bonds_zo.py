import streamlit as st
import json
import re
from streamlit_lightweight_charts import renderLightweightCharts
import pickle
import os
from sqlalchemy import create_engine
from dotenv import load_dotenv
import pandas as pd

load_dotenv()


class APIMOEXError(Exception):
    pass


EXCHANGE_MAP = {"MOEX": {"market": "shares", "engine": "stock", "board": "tqbr"},
                "MOEX CETS": {"market": "selt", "engine": "currency", "board": "cets"},
                "MOEX SPBFUT": {"market": "forts", "engine": "futures", "board": "spbfut"},
                "SNDX": {"market": "index", "engine": "stock", "board": "SNDX"}}
# token = os.getenv("APIMOEX_TOKEN")

def get_rt(instrument):
    engine = create_engine(
        f"mysql+mysqlconnector://algouser:algouser@192.168.206.34:3306/bonds_moex_db")
    query = f'''
        select SYSTIME, open_YTM, high_YTM, low_YTM, last_YTM, value from bars_daily_live_zo bdl 
        left join symbol s on s.secid = bdl.SECID 
        where instrument='{instrument}'
        limit 1
        '''
    with engine.connect() as conn:
        data = conn.execute(query)
    rt = pd.DataFrame(data.fetchall(), columns=data.keys())
    return rt.rename(columns={"SYSTIME": "TRADEDATE"})


def get_instrument_isin(instrument):
    engine = create_engine(
        f"mysql+mysqlconnector://algouser:algouser@192.168.206.34:3306/bonds_moex_db")
    query = f'''
        select isin from bonds_moex_db.symbol
        where instrument='{instrument}'
        limit 1
        '''
    with engine.connect() as conn:
        data = conn.execute(query)
    rt = pd.DataFrame(data.fetchall(), columns=data.keys())
    return rt['isin'].iloc[0]


def render_candlestick_chart(data):
    data.index.name = 'time'
    data = data.reset_index()
    data['time'] = data['time'].astype(str)
    COLOR_BULL = 'rgba(38,166,154,0.9)'  # #26a69a
    COLOR_BEAR = 'rgba(239,83,80,0.9)'  # #ef5350
    data['chg'] = (data['close'] - data['open']) / data['open']
    data.loc[data[data['chg'] <= 0].index, 'color'] = 'red'

    candles = json.loads(data.to_json(orient="records"))
    volume = json.loads(data.to_json(orient="records"))

    chartMultipaneOptions = [
        {
            "height": 550,
            "handleScroll": False,
            "handleScale": False,
            # "mouseWheel": False,
            "layout": {
                "background": {
                    "type": "solid",
                    "color": 'white'
                },
                "textColor": "black"
            },
            "grid": {
                "vertLines": {
                    "color": "rgba(197, 203, 206, 0.5)"
                },
                "horzLines": {
                    "color": "rgba(197, 203, 206, 0.5)"
                }
            },
            "crosshair": {
                "mode": 0
            },
            "priceScale": {
                "borderColor": "rgba(197, 203, 206, 0.8)"
            },
            "timeScale": {
                "borderColor": "rgba(197, 203, 206, 0.8)",
                "barSpacing": 15
            },
            # "watermark": {
            #     "visible": True,
            #     "fontSize": 48,
            #     "horzAlign": 'center',
            #     "vertAlign": 'center',
            #     "color": 'rgba(171, 71, 188, 0.3)',
            #     "text": 'AAPL - D1',
            # }
        },
        {
            "height": 100,
            "handleScroll": False,
            "handleScale": False,
            "layout": {
                "background": {
                    "type": 'solid',
                    "color": 'transparent'
                },
                "textColor": 'black',
            },
            "grid": {
                "vertLines": {
                    "color": "rgba(197, 203, 206, 0.5)"
                },
                "horzLines": {
                    "color": "rgba(197, 203, 206, 0.5)"
                }
            },
            "timeScale": {
                "borderColor": "rgba(197, 203, 206, 0.8)",
                "barSpacing": 15
            },
            "priceScale": {
                "borderColor": "rgba(197, 203, 206, 0.8)"
            },
            # "watermark": {
            #     "visible": True,
            #     "fontSize": 18,
            #     "horzAlign": 'left',
            #     "vertAlign": 'top',
            #     "color": 'rgba(171, 71, 188, 0.7)',
            #     "text": 'Volume',
            # }
        },
    ]

    seriesCandlestickChart = [
        {
            "type": 'Candlestick',
            "data": candles,
            "options": {
                "upColor": COLOR_BULL,
                "downColor": COLOR_BEAR,
                "borderVisible": False,
                "wickUpColor": COLOR_BULL,
                "wickDownColor": COLOR_BEAR
            }
        }
    ]

    seriesVolumeChart = [
        {
            "type": 'Histogram',
            "data": volume,
            "options": {
                "priceFormat": {
                    "type": 'volume',
                },
            }
        }
    ]

    renderLightweightCharts([
        {
            "chart": chartMultipaneOptions[0],
            "series": seriesCandlestickChart
        },
        {
            "chart": chartMultipaneOptions[1],
            "series": seriesVolumeChart
        },
    ], 'multipane')


def resample_candlestick(stock_data, timeframe):
    apply_map = {'open': 'first',
                 'high': 'max',
                 'low': 'min',
                 'close': 'last',
                 'value': "sum"}
    resampled_stock_data = stock_data.copy().resample(timeframe).apply(apply_map)
    return resampled_stock_data.rename(index={resampled_stock_data.index[-1]: stock_data.index[-1]}).dropna()


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
    with open(os.path.join(os.getenv("PATH_TO_DATA_FOLDER"), 'ticker_list_zo_bonds.p'), 'rb') as f:
        ticker_turnovers = pickle.load(f)
    with open(os.path.join(os.getenv("PATH_TO_DATA_FOLDER"), 'base_dict_bonds.p'), 'rb') as f:
        base_dict = pickle.load(f)
    selected_stock = st.sidebar.selectbox("Select asset:", ticker_turnovers.to_list())
    selected_chart_type = st.sidebar.selectbox("Select YTM or RUB:", ['YTM', 'RUB'])
    short_stock_name = re.sub(r'\([^)]*\)', '', selected_stock)
    stock_data = base_dict[selected_stock][['open_YTM', 'last_YTM', 'low_YTM', 'high_YTM', 'px_RUB_open', 'px_RUB_high', 'px_RUB_low', 'px_RUB_last','value']]
    # try:
    #     rt = get_rt(selected_stock)
    #     stock_data = pd.concat([stock_data, rt.set_index("TRADEDATE")])
    # except:
    #     pass

    st.subheader(f"""{short_stock_name}""")
    st.markdown(f"{get_instrument_isin(selected_stock)}")
    st.markdown(f"Price updated at: **{stock_data.index[-1]}**")
    stock_data.index = pd.to_datetime(stock_data.index).normalize()
    selected_timeframe = st.selectbox("Select timeframe:", ['Daily', 'Weekly', 'Monthly'])
    if selected_timeframe == 'Daily':
        if selected_chart_type == 'YTM':
            render_candlestick_chart(
                stock_data[['open_YTM', 'last_YTM', 'low_YTM', 'high_YTM', 'value']].rename(
            columns={"last_YTM": 'close', 'low_YTM': 'low', 'high_YTM': 'high', 'open_YTM': 'open'}).astype(float).dropna().iloc[-252:])
        elif selected_chart_type == 'RUB':
            render_candlestick_chart(
                stock_data[['px_RUB_open', 'px_RUB_high', 'px_RUB_low', 'px_RUB_last', 'value']].rename(
                    columns={"px_RUB_last": 'close', 'px_RUB_low': 'low', 'px_RUB_high': 'high', 'px_RUB_open': 'open'}).astype(
                    float).dropna().iloc[-252:])
    elif selected_timeframe == 'Weekly':
        if selected_chart_type == 'YTM':
            render_candlestick_chart(
                resample_candlestick(
                    stock_data[['open_YTM', 'last_YTM', 'low_YTM', 'high_YTM', 'value']].rename(
            columns={"last_YTM": 'close', 'low_YTM': 'low', 'high_YTM': 'high', 'open_YTM': 'open'}).astype(float).dropna().iloc[-252 * 5:],
                    'W-FRI'))
        elif selected_chart_type == 'RUB':
            render_candlestick_chart(
                resample_candlestick(
                    stock_data[['px_RUB_open', 'px_RUB_high', 'px_RUB_low', 'px_RUB_last', 'value']].rename(
                        columns={"px_RUB_last": 'close', 'px_RUB_low': 'low', 'px_RUB_high': 'high',
                                 'px_RUB_open': 'open'}).astype(
                        float).dropna().iloc[-252 * 5:],
                    'W-FRI'))
    elif selected_timeframe == 'Monthly':
        if selected_chart_type == 'YTM':
            render_candlestick_chart(
                resample_candlestick(
                    stock_data[['open_YTM', 'last_YTM', 'low_YTM', 'high_YTM', 'value']].rename(
            columns={"last_YTM": 'close', 'low_YTM': 'low', 'high_YTM': 'high', 'open_YTM': 'open'}).astype(float).dropna().iloc[-252 * 15:],
                    'M'))
        elif selected_chart_type == 'RUB':
            render_candlestick_chart(
                resample_candlestick(
                    stock_data[['px_RUB_open', 'px_RUB_high', 'px_RUB_low', 'px_RUB_last', 'value']].rename(
                        columns={"px_RUB_last": 'close', 'px_RUB_low': 'low', 'px_RUB_high': 'high',
                                 'px_RUB_open': 'open'}).astype(
                        float).dropna().iloc[-252 * 15:],
                    'M'))


main()
