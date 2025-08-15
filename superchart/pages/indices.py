import streamlit as st
import traceback
import json
from streamlit_lightweight_charts import renderLightweightCharts
import pickle
import pandas as pd
import numpy as np
import os
import requests
from dotenv import load_dotenv
from sqlalchemy import (
    create_engine, text
)
import asyncio
from sqlalchemy.ext.asyncio import create_async_engine
load_dotenv()

token = os.getenv("APIMOEX_TOKEN")


class APIMOEXError(Exception):
    pass


url = os.getenv("NEON_URL")
EXCHANGE_MAP = {"MOEX": {"market": "shares", "engine": "stock", "board": "tqbr"},
                "MOEX CETS": {"market": "selt", "engine": "currency", "board": "cets"},
                "MOEX SPBFUT": {"market": "forts", "engine": "futures", "board": "spbfut"},
                "SNDX": {"market": "index", "engine": "stock", "board": "SNDX"}}


def get_current_date(t='SBER', exchange='MOEX'):
    headers = {
        'Authorization': f'Bearer {token}',
    }
    arguments = {"interval": 1, 'from': pd.Timestamp.today().strftime(
        "%Y-%m-%d") + " 09:59:00"}  # (pd.Timestamp.today()-pd.Timedelta(days=5)).strftime("%Y-%m-%d") + " 09:59:00"}
    response = requests.get(f"https://apim.moex.com/iss/engines/{EXCHANGE_MAP[exchange]['engine']}/"
                            f"markets/{EXCHANGE_MAP[exchange]['market']}/boards/{EXCHANGE_MAP[exchange]['board']}/securities/{t}/candles.json",
                            headers=headers,
                            params=arguments,
                            verify=False)
    data = response.json()
    if len(data['candles']['data']) == 0:
        return None
    else:
        return pd.Timestamp(data['candles']['data'][-1][-1]).replace(hour=0, minute=0, second=0)


def get_current_candle(exchange, ticker):
    if ticker == 'MCFTR':
        return None, None
    traded_date = get_current_date()
    if traded_date is None:
        return None, None
    headers = {
        'Authorization': f'Bearer {token}',
    }

    response = requests.get(f"https://apim.moex.com/iss/engines/{EXCHANGE_MAP[exchange]['engine']}/"
                            f"markets/{EXCHANGE_MAP[exchange]['market']}/boards/{EXCHANGE_MAP[exchange]['board']}/securities/{ticker}.json",
                            headers=headers,
                            params={},
                            verify=False)
    data = response.json()
    if len(data['marketdata']['data']) == 1:
        data = dict(zip(data['marketdata']['columns'], data['marketdata']['data'][0]))
    else:
        raise APIMOEXError(f"Error when loading realtime price for {ticker}")
    ticker_prices_df = pd.DataFrame(
        [{'price_date': traded_date, 'PX_OPEN': data['OPENVALUE'], 'PX_HIGH': data['HIGH'], 'PX_LOW': data['LOW'],
          'PX_LAST': data['CURRENTVALUE'], 'PX_VOLUME': 0, 'PX_TURNOVER': data['VALTODAY']}]).set_index(
        'price_date')
    ticker_prices_df.index = pd.to_datetime(ticker_prices_df.index)
    return ticker_prices_df, data['TIME']


def render_diff_chart(ser, key):
    data = ser.rename("value")
    data.index.name = 'time'
    data = data.reset_index()
    data['time'] = data['time'].astype(str)
    data = data.to_dict('records')
    chartOptions = {
        "handleScroll": False,
        "handleScale": False,
        "layout": {
            "textColor": 'black',
            "background": {
                "type": 'solid',
                "color": 'white'
            }
        },
        "rightPriceScale": {
            "mode": 0,
        },
    }
    seriesBaselineChart = [{
        "type": 'Baseline',
        "data": data,
        "options": {
            "baseValue": {"type": "price", "price": 0},
            "topLineColor": 'rgba( 38, 166, 154, 1)',
            "topFillColor1": 'rgba( 38, 166, 154, 0.28)',
            "topFillColor2": 'rgba( 38, 166, 154, 0.05)',
            "bottomLineColor": 'rgba( 239, 83, 80, 1)',
            "bottomFillColor1": 'rgba( 239, 83, 80, 0.05)',
            "bottomFillColor2": 'rgba( 239, 83, 80, 0.28)'
        }
    }]
    renderLightweightCharts([
        {
            "chart": chartOptions,
            "series": seriesBaselineChart,
        }
    ], key)


def render_candlestick_chart(data):
    data = data.rename(
        columns={"PX_LAST": 'close', 'PX_LOW': 'low', 'PX_HIGH': 'high', 'PX_OPEN': 'open', "PX_TURNOVER": "value"})
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
                #     "priceScaleId": ""  # set as an overlay setting,
                # },
                # "priceScale": {
                #     "scaleMargins": {
                #         "top": 0,
                #         "bottom": 0,
                #     },
                #     # "alignLabels": False
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

def render_line_chart(data, data_vol):
    data = data.rename("value")
    data.index.name = 'time'
    data = data.reset_index()
    data['time'] = data['time'].astype(str)
    data = data.to_dict('records')

    data_vol = data_vol.rename(
        columns={"PX_LAST": 'close', 'PX_LOW': 'low', 'PX_HIGH': 'high', 'PX_OPEN': 'open', "PX_TURNOVER": "value"})
    data_vol.index.name = 'time'
    data_vol = data_vol.reset_index()
    data_vol['time'] = data_vol['time'].astype(str)

    COLOR_BULL = 'rgba(38,166,154,0.9)'  # #26a69a
    COLOR_BEAR = 'rgba(239,83,80,0.9)'  # #ef5350

    # data['chg'] = (data['close'] - data['open']) / data['open']
    # data.loc[data[data['chg'] <= 0].index, 'color'] = 'red'

    volume = json.loads(data_vol.to_json(orient="records"))

    chartMultipaneOptions = [
        {
            "height": 550,
            "handleScroll": False,
            "handleScale": False,
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

    seriesBaselineChart = [{
        "type": 'Baseline',
        "data": data,
        "options": {
            "baseValue": {"type": "price", "price": 0},
            "topLineColor": 'rgba( 38, 166, 154, 1)',
            "topFillColor1": 'rgba( 38, 166, 154, 0.28)',
            "topFillColor2": 'rgba( 38, 166, 154, 0.05)',
            "bottomLineColor": 'rgba( 239, 83, 80, 1)',
            "bottomFillColor1": 'rgba( 239, 83, 80, 0.05)',
            "bottomFillColor2": 'rgba( 239, 83, 80, 0.28)'
        }
    }]

    seriesVolumeChart = [
        {
            "type": 'Histogram',
            "data": volume,
            "options": {
                "priceFormat": {
                    "type": 'volume',
                },
                #     "priceScaleId": ""  # set as an overlay setting,
                # },
                # "priceScale": {
                #     "scaleMargins": {
                #         "top": 0,
                #         "bottom": 0,
                #     },
                #     # "alignLabels": False
            }
        }
    ]

    renderLightweightCharts([
        {
            "chart": chartMultipaneOptions[0],
            "series": seriesBaselineChart
        },
        {
            "chart": chartMultipaneOptions[1],
            "series": seriesVolumeChart
        },
    ], 'multipane')


def compute_logdiff(series_1, series_2):
    logdata = np.log(pd.concat([series_1, series_2], axis=1).dropna())
    logdata = logdata - logdata.iloc[0]
    logdata.columns = [0, 1]
    return logdata[0] - logdata[1]


def resample_candlestick(stock_data, timeframe):
    apply_map = {'PX_OPEN': 'first',
                 'PX_HIGH': 'max',
                 'PX_LOW': 'min',
                 'PX_LAST': 'last',
                 'PX_TURNOVER': "sum"}
    resampled_stock_data = stock_data.copy().resample(timeframe).apply(apply_map)
    return resampled_stock_data.rename(index={resampled_stock_data.index[-1]: stock_data.index[-1]}).dropna()

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


def load_data_neon_sync(table):
    df = asyncio.run(load_data_neon(f"select * from {table}"))
    return df

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
    imoex2 = load_data_neon_sync("imoex2").set_index("TRADEDATE")[['CLOSE']]
    mcftr = load_data_neon_sync("mcftr").set_index("TRADEDATE")[['CLOSE']]
    selected_stock = st.sidebar.selectbox("Select index:", ['IMOEX2', 'IMOEX', 'MCFTR'])
    # st.subheader(f"""IMOEX""")
    # if selected_stock == 'IMOEX':
    #     stock_data = imoex.rename(columns={"OPEN": "PX_OPEN",
    #                                        "CLOSE": "PX_LAST",
    #                                        "LOW": "PX_LOW",
    #                                        "HIGH": "PX_HIGH",
    #                                        "VALTODAY_RUR": "PX_TURNOVER"})[
    #         ['PX_OPEN', 'PX_LAST', 'PX_LOW', 'PX_HIGH', 'PX_TURNOVER']]
    if selected_stock == 'IMOEX2':
        stock_data = imoex2.rename(columns={"OPEN": "PX_OPEN",
                                            "CLOSE": "PX_LAST",
                                            "LOW": "PX_LOW",
                                            "HIGH": "PX_HIGH",
                                            "VALTODAY_RUR": "PX_TURNOVER"})[
            ['PX_OPEN', 'PX_LAST', 'PX_LOW', 'PX_HIGH', 'PX_TURNOVER']]
    elif selected_stock == 'MCFTR':
        stock_data = mcftr.to_frame("PX_LAST")
        stock_data['PX_OPEN'] = stock_data['PX_LAST']
        stock_data['PX_LOW'] = stock_data['PX_LAST']
        stock_data['PX_HIGH'] = stock_data['PX_LAST']
        stock_data['PX_TURNOVER'] = imoex2['VALTODAY_RUR']
    try:
        rt_candle, time_updated = get_current_candle("SNDX", selected_stock)
        if rt_candle is None:
            st.subheader(f"""{selected_stock}""")
            st.markdown(f"Price updated at: **{stock_data.index[-1]:%d.%m.%Y}**")
        elif len(rt_candle.dropna()) == 1:
            if not rt_candle.index[0] in stock_data.index:
                return_1d = (rt_candle['PX_LAST'].iloc[-1] - stock_data['PX_LAST'].iloc[-1]) / \
                            stock_data['PX_LAST'].iloc[-1]
                stock_data = pd.concat(
                    [stock_data, rt_candle[['PX_OPEN', 'PX_LAST', 'PX_LOW', 'PX_HIGH', 'PX_TURNOVER']]])
                if return_1d > 0:
                    st.subheader(f"""{selected_stock} :green[+{round(return_1d * 100, 2)}%]""")
                    # st.markdown(f"")
                else:
                    st.subheader(f"""{selected_stock} :red[{round(return_1d * 100, 2)}%]""")
                    # st.markdown(f"")
                st.markdown(f"Price updated at: **{rt_candle.index[0]:%d.%m.%Y}** **{time_updated}**")
            else:
                st.subheader(f"""{selected_stock}""")
                st.markdown(f"Price updated_at: **{stock_data.index[-1]:%d.%m.%Y}**")
        else:
            st.subheader(f"""{selected_stock}""")
            st.markdown(f"Price updated at: **{stock_data.index[-1]:%d.%m.%Y}**")
    except Exception:
        print(traceback.format_exc())
        st.subheader(f"""{selected_stock}""")
        st.markdown(f"Price updated at: **{stock_data.index[-1]:%d.%m.%Y}**")

    selected_timeframe = st.selectbox("Select timeframe:", ['Daily', 'Weekly', 'Monthly'])

    if selected_timeframe == 'Daily' and selected_stock != 'MCFTR':
        render_candlestick_chart(
            stock_data[['PX_OPEN', 'PX_LAST', 'PX_LOW', 'PX_HIGH', 'PX_TURNOVER']].dropna().iloc[-252:])
    elif selected_timeframe == 'Daily' and selected_stock == 'MCFTR':
        render_line_chart(
            stock_data['PX_OPEN'].dropna().iloc[-252:],
        stock_data[['PX_OPEN', 'PX_LAST', 'PX_LOW', 'PX_HIGH', 'PX_TURNOVER']].dropna().iloc[-252:])
    elif selected_timeframe == 'Weekly':
        render_candlestick_chart(
            resample_candlestick(
                stock_data[['PX_OPEN', 'PX_LAST', 'PX_LOW', 'PX_HIGH', 'PX_TURNOVER']].dropna().iloc[-252 * 5:],
                'W-FRI'))
    elif selected_timeframe == 'Monthly':
        render_candlestick_chart(
            resample_candlestick(
                stock_data[['PX_OPEN', 'PX_LAST', 'PX_LOW', 'PX_HIGH', 'PX_TURNOVER']].dropna().iloc[-252 * 15:],
                'M'))


main()
