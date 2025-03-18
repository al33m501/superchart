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
from io import BytesIO

load_dotenv()


class APIMOEXError(Exception):
    pass


with open(os.path.join(os.getenv("PATH_TO_DATA_FOLDER"), 'cert.p'), 'rb') as f:
    cert = pickle.load(f)

EXCHANGE_MAP = {"MOEX": {"market": "shares", "engine": "stock", "board": "tqbr"},
                "MOEX CETS": {"market": "selt", "engine": "currency", "board": "cets"},
                "MOEX SPBFUT": {"market": "forts", "engine": "futures", "board": "spbfut"},
                "SNDX": {"market": "index", "engine": "stock", "board": "SNDX"}}
token = os.getenv("APIMOEX_TOKEN")


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


# def to_excel(df):
#     output = BytesIO()
#     with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
#         df.to_excel(writer, index=False, sheet_name='SUPERCHART')
#     processed_data = output.getvalue()
#     return processed_data


def get_current_candle_idx(exchange, ticker):
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
    # {'SECID': 'IMOEX', 'BOARDID': 'SNDX', 'LASTVALUE': 2545.07, 'OPENVALUE': 2552.32, 'CURRENTVALUE': 2577.89, 'LASTCHANGE': 32.82, 'LASTCHANGETOOPENPRC': 1, 'LASTCHANGETOOPEN': 25.57, 'UPDATETIME': '12:52:36', 'LASTCHANGEPRC': 1.29, 'VALT
    # ODAY': 60844276983.0, 'MONTHCHANGEPRC': -2.73, 'YEARCHANGEPRC': -16.82, 'SEQNUM': 20240903125236, 'SYSTIME': '2024-09-03 12:52:36', 'TIME': '12:52:36', 'VALTODAY_USD': 676037757.04, 'LASTCHANGEBP': 3282, 'MONTHCHANGEBP': -7243.00000000
    # 0001, 'YEARCHANGEBP': -52122, 'CAPITALIZATION': 4745813177844, 'CAPITALIZATION_USD': 52730495868.8819, 'HIGH': 2593.44, 'LOW': 2519.01, 'TRADEDATE': '2024-09-03', 'TRADINGSESSION': '1', 'VOLTODAY': None}
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


def get_current_candle(exchange, ticker, div_table):
    traded_date = get_current_date()
    if traded_date is None:
        return None, None
    ticker_divs_on_date = div_table[(div_table['ex_date'] == traded_date) & (div_table['ticker'] == ticker)]

    arguments = {'marketdata.columns': ('SECID,'
                                        'TIME,'
                                        'OPEN,'
                                        'LOW,'
                                        'HIGH,'
                                        'LAST,'
                                        'VOLTODAY,'
                                        'VALTODAY_RUR')}
    headers = {
        'Authorization': f'Bearer {token}',
    }
    response = requests.get(f"https://apim.moex.com/iss/engines/{EXCHANGE_MAP[exchange]['engine']}/"
                            f"markets/{EXCHANGE_MAP[exchange]['market']}/boards/{EXCHANGE_MAP[exchange]['board']}/securities/{ticker}.json",
                            headers=headers,
                            params=arguments,
                            verify=False)
    data = response.json()
    if len(data['marketdata']['data']) == 1:
        data = dict(zip(data['marketdata']['columns'], data['marketdata']['data'][0]))
    else:
        raise APIMOEXError(f"Error when loading realtime price for {ticker}")
    ticker_prices_df = pd.DataFrame(
        [{'price_date': traded_date, 'PX_OPEN': data['OPEN'], 'PX_HIGH': data['HIGH'], 'PX_LOW': data['LOW'],
          'PX_LAST': data['LAST'], 'PX_VOLUME': data['VOLTODAY'], 'PX_TURNOVER': data['VALTODAY_RUR']}]).set_index(
        'price_date')
    ticker_prices_df.index = pd.to_datetime(ticker_prices_df.index)
    if len(ticker_divs_on_date) > 0:
        ticker_prices_df[['PX_OPEN', 'PX_HIGH', 'PX_LOW', 'PX_LAST']] += float(
            ticker_divs_on_date['dividend_amount'].sum())
        st.markdown(
            f"Today dividend: **{float(ticker_divs_on_date['dividend_amount'].sum())} RUB**")
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
        },
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
    rt_candle = None
    st.sidebar.subheader("""📈 Superchart""")
    with open(os.path.join(os.getenv("PATH_TO_DATA_FOLDER"), 'ticker_list.p'), 'rb') as f:
        ticker_turnovers = pickle.load(f)
    with open(os.path.join(os.getenv("PATH_TO_DATA_FOLDER"), 'mcftr.p'), 'rb') as f:
        benchmark_raw = pickle.load(f)
    with open(os.path.join(os.getenv("PATH_TO_DATA_FOLDER"), 'base_dict.p'), 'rb') as f:
        base_dict = pickle.load(f)
    with open(os.path.join(os.getenv("PATH_TO_DATA_FOLDER"), 'div_table.p'), 'rb') as f:
        div_table = pickle.load(f)
    with open(os.path.join(os.getenv("PATH_TO_DATA_FOLDER"), 'imoex2.p'), 'rb') as f:
        imoex2 = pickle.load(f)
    selected_stock = st.sidebar.selectbox("Select asset:", ticker_turnovers.index.to_list())
    stock_data = base_dict[selected_stock][['PX_OPEN', 'PX_LAST', 'PX_LOW', 'PX_HIGH', 'PX_TURNOVER']]
    try:
        rt_candle, time_updated = get_current_candle("MOEX", selected_stock, div_table)
        if rt_candle is None:
            st.subheader(f"""{selected_stock}""")
            st.markdown(f"Price updated at: **{stock_data.index[-1]:%d.%m.%Y}**")
            st.markdown(f"[Trading View](https://ru.tradingview.com/chart/?symbol={selected_stock})")
        elif len(rt_candle.dropna()) == 1:
            if not rt_candle.index[0] in stock_data.index:
                return_1d = (rt_candle['PX_LAST'].iloc[-1] - stock_data['PX_LAST'].iloc[-1]) / \
                            stock_data['PX_LAST'].iloc[-1]
                stock_data = pd.concat(
                    [stock_data, rt_candle[['PX_OPEN', 'PX_LAST', 'PX_LOW', 'PX_HIGH', 'PX_TURNOVER']]])
                if return_1d > 0:
                    st.subheader(f"""{selected_stock} :green[+{round(return_1d * 100, 2)}%]""")
                else:
                    st.subheader(f"""{selected_stock} :red[{round(return_1d * 100, 2)}%]""")
                st.markdown(f"Price updated at: **{rt_candle.index[0]:%d.%m.%Y}** **{time_updated}**")
                st.markdown(f"[Trading View](https://ru.tradingview.com/chart/?symbol={selected_stock})")
            else:
                st.subheader(f"""{selected_stock}""")
                st.markdown(f"Price updated_at: **{stock_data.index[-1]:%d.%m.%Y}**")
                st.markdown(f"[Trading View](https://ru.tradingview.com/chart/?symbol={selected_stock})")
        else:
            st.subheader(f"""{selected_stock}""")
            st.markdown(f"Price updated at: **{stock_data.index[-1]:%d.%m.%Y}**")
            st.markdown(f"[Trading View](https://ru.tradingview.com/chart/?symbol={selected_stock})")
    except Exception:
        print(traceback.format_exc())
        st.subheader(f"""{selected_stock}""")
        st.markdown(f"Price updated at: **{stock_data.index[-1]:%d.%m.%Y}**")
        st.markdown(f"[Trading View](https://ru.tradingview.com/chart/?symbol={selected_stock})")
    st.markdown(
        f"Median turnover over last 90 calendar days: **{int(ticker_turnovers.loc[selected_stock] / 1e6)} M RUB**")
    selected_timeframe = st.selectbox("Select timeframe:", ['Daily', 'Weekly', 'Monthly'])

    if selected_timeframe == 'Daily':
        render_candlestick_chart(
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
    st.subheader(f"""Log diff. ({selected_stock} vs MCFTR)""")
    today_logdiff = None
    if not rt_candle is None:
        # st.markdown(f"Realtime price: {rt_candle_no_div['PX_LAST'].iloc[-1]}")

        stock_data_idx = imoex2['CLOSE']
        rt_candle_idx, time_updated_idx = get_current_candle_idx("SNDX", "IMOEX2")

        stock_data = base_dict[selected_stock][['PX_OPEN', 'PX_LAST', 'PX_LOW', 'PX_HIGH', 'PX_TURNOVER']]

        alpha_1d = ((rt_candle['PX_LAST'].iloc[-1] - stock_data['PX_LAST'].iloc[-1]) / stock_data['PX_LAST'].iloc[-1]) - \
                   ((rt_candle_idx['PX_LAST'].iloc[-1] - stock_data_idx.iloc[-1]) / stock_data_idx.iloc[-1])

        # stock_data = pd.concat(
        #     [stock_data, rt_candle[['PX_OPEN', 'PX_LAST', 'PX_LOW', 'PX_HIGH', 'PX_TURNOVER']]])
        #
        # imoex2_to_mcftr_adjusted = ((rt_candle_idx['PX_LAST'].iloc[-1] - stock_data_idx) / stock_data_idx) + 1 * \
        #                            benchmark_raw.iloc[-1]
        #
        # benchmark_raw.loc[rt_candle_idx.index[-1]] = imoex2_to_mcftr_adjusted

        stock_datafordiff = pd.concat(
            [stock_data[stock_data.index == stock_data.index[-1]]['PX_LAST'], rt_candle['PX_LAST']])
        idx_datafordiff = pd.concat(
            [stock_data_idx[stock_data_idx.index == stock_data_idx.index[-1]], rt_candle_idx['PX_LAST']])
        today_logdiff = compute_logdiff(stock_datafordiff, idx_datafordiff).iloc[-1]

        st.markdown(f"Diff. updated at: **{benchmark_raw.index[-1]:%d.%m.%Y} {time_updated_idx}**")

        if alpha_1d > 0:
            st.markdown(
                f"""Today Alpha vs IMOEX2: **:green[+{round(alpha_1d * 100, 2)}%]**, today logdiff: {round(today_logdiff * 100, 2)}%, today diff: {round((stock_datafordiff.pct_change() - idx_datafordiff.pct_change()).iloc[-1] * 100, 2)}%""")
        else:
            st.markdown(
                f"""Today Alpha vs IMOEX2: **:red[{round(alpha_1d * 100, 2)}%]**, today logdiff: {round(today_logdiff * 100, 2)}%, today diff: {round((stock_datafordiff.pct_change() - idx_datafordiff.pct_change()).iloc[-1] * 100, 2)}%""")
    else:
        st.markdown(f"Diff. updated at: **{benchmark_raw.index[-1]:%d.%m.%Y}**")
    for lookback_period, timeframe in zip([365, 1095, 1825, 5475], ['1d', '1d', 'W-FRI', 'M']):
        benchmark = benchmark_raw.resample('1d').last().ffill().dropna().iloc[
                    -lookback_period:]
        last_prices = stock_data['PX_LAST'].resample('1d').last().ffill().dropna().iloc[-lookback_period:]
        if timeframe != '1d':
            benchmark = benchmark.resample(timeframe).last().ffill()
            last_prices = last_prices.resample(timeframe).last().ffill()
        logdiff = compute_logdiff(last_prices, benchmark).reindex(
            benchmark.index).bfill()
        if timeframe == '1d' and today_logdiff is not None:
            logdiff.loc[rt_candle_idx.index[0]] = logdiff.iloc[-1] + today_logdiff
            # st.markdown(f"""last logdiffs: {logdiff.iloc[-2]}, {logdiff.iloc[-1]}""")
        # excel_file = to_excel(logdiff.reset_index())
        if today_logdiff is None:
            st.markdown(f"""today logdiff is not loaded!""")
        st.text(
            f'last {int(lookback_period / 365)} years, timeframe {timeframe.replace("W-FRI", "weekly").replace("M", "monthly").replace("1d", "daily")}')
        render_diff_chart(logdiff, f"{lookback_period}_{timeframe}")
        # st.download_button(
        #     label="💾",
        #     data=excel_file,
        #     file_name=f"{selected_stock}_logdiff.xlsx",
        #     mime="application/vnd.ms-excel",
        #     key=f"download_{selected_stock}_{lookback_period}_{timeframe}"
        # )


main()
