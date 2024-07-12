import streamlit as st
from streamlit_lightweight_charts import renderLightweightCharts
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

from marketdb.apimoex_connector import EXCHANGE_MAP, get_current_date


class APIMOEXError(Exception):
    pass


with open(os.path.join(os.getenv("PATH_TO_DATA_FOLDER"), 'cert.p'), 'rb') as f:
    cert = pickle.load(f)


# def get_current_candle(exchange, ticker, div_table):
#     traded_date = get_current_date()
#     ticker_divs_on_date = div_table[(div_table['ex_date'] == traded_date) & (div_table['ticker'] == ticker)]
#     arguments = {'marketdata.columns': ('SECID,'
#                                         'TIME,'
#                                         'OPEN,'
#                                         'LOW,'
#                                         'HIGH,'
#                                         'LAST,'
#                                         'VOLTODAY')}
#     with requests.Session() as session:
#         request_url = (f"https://iss.moex.com/iss/engines/{EXCHANGE_MAP[exchange]['engine']}/"
#                        f"markets/{EXCHANGE_MAP[exchange]['market']}/boards/{EXCHANGE_MAP[exchange]['board']}/securities/{ticker}.json")
#         iss = apimoex.ISSClient(session, request_url, arguments)
#         data = iss.get()
#     if len(data['marketdata']) == 1:
#         data = data['marketdata'][0]
#     else:
#         raise APIMOEXError(f"Error when loading realtime price for {ticker}")
#     ticker_prices_df = pd.DataFrame(
#         [{'price_date': traded_date, 'PX_OPEN': data['OPEN'], 'PX_HIGH': data['HIGH'], 'PX_LOW': data['LOW'],
#           'PX_LAST': data['LAST'], 'PX_VOLUME': data['VOLTODAY']}]).set_index('price_date')
#     ticker_prices_df.index = pd.to_datetime(ticker_prices_df.index)
#     if len(ticker_divs_on_date) > 0:
#         ticker_prices_df[['PX_OPEN', 'PX_HIGH', 'PX_LOW', 'PX_LAST']] += float(
#             ticker_divs_on_date['dividend_amount'].sum())
#         st.markdown(
#             f"Today dividend: **{float(ticker_divs_on_date['dividend_amount'].sum())} RUB**")
#     return ticker_prices_df, data['TIME']


def get_current_candle(exchange, ticker, div_table):
    traded_date = get_current_date()
    ticker_divs_on_date = div_table[(div_table['ex_date'] == traded_date) & (div_table['ticker'] == ticker)]

    arguments = {'marketdata.columns': ('SECID,'
                                        'TIME,'
                                        'OPEN,'
                                        'LOW,'
                                        'HIGH,'
                                        'LAST,'
                                        'VOLTODAY')}
    response = requests.get(f"https://iss.moex.com/iss/engines/{EXCHANGE_MAP[exchange]['engine']}/"
                            f"markets/{EXCHANGE_MAP[exchange]['market']}/boards/{EXCHANGE_MAP[exchange]['board']}/securities/{ticker}.json",
                            cookies={'MicexPassportCert': cert},
                            params=arguments)
    data = response.json()
    if len(data['marketdata']['data']) == 1:
        data = dict(zip(data['marketdata']['columns'], data['marketdata']['data'][0]))
    else:
        raise APIMOEXError(f"Error when loading realtime price for {ticker}")
    ticker_prices_df = pd.DataFrame(
        [{'price_date': traded_date, 'PX_OPEN': data['OPEN'], 'PX_HIGH': data['HIGH'], 'PX_LOW': data['LOW'],
          'PX_LAST': data['LAST'], 'PX_VOLUME': data['VOLTODAY']}]).set_index('price_date')
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
        }
    }]
    renderLightweightCharts([
        {
            "chart": chartOptions,
            "series": seriesBaselineChart,
        }
    ], key)


def render_candlestick_chart(data):
    data = data.rename(columns={"PX_LAST": 'close', 'PX_LOW': 'low', 'PX_HIGH': 'high', 'PX_OPEN': 'open'})
    data.index.name = 'time'
    data = data.reset_index()
    data['time'] = data['time'].astype(str)
    data = data.to_dict('records')
    chartOptions = {
        "height": 550,
        "handleScroll": False,
        "handleScale": False,
        "layout": {
            "textColor": 'black',
            "background": {
                "type": 'solid',
                "color": 'white'
            },
        }
    }
    seriesCandlestickChart = [{
        "type": 'Candlestick',
        "data": data,
        "options": {
            "upColor": '#26a69a',
            "downColor": '#ef5350',
            "borderVisible": False,
            "wickUpColor": '#26a69a',
            "wickDownColor": '#ef5350'
        }
    }]
    renderLightweightCharts([
        {
            "chart": chartOptions,
            "series": seriesCandlestickChart
        }
    ], 'candlestick')


def compute_logdiff(series_1, series_2):
    logdata = np.log(pd.concat([series_1, series_2], axis=1).dropna())
    logdata = logdata - logdata.iloc[0]
    logdata.columns = [0, 1]
    return logdata[0] - logdata[1]


def resample_candlestick(stock_data, timeframe):
    apply_map = {'PX_OPEN': 'first',
                 'PX_HIGH': 'max',
                 'PX_LOW': 'min',
                 'PX_LAST': 'last'}
    resampled_stock_data = stock_data.copy().resample(timeframe).apply(apply_map)
    return resampled_stock_data.rename(index={resampled_stock_data.index[-1]: stock_data.index[-1]}).dropna()


def main():
    st.set_page_config(
        page_title="Superchart",
        page_icon="📈",
        layout='wide'
    )
    st.sidebar.subheader("""📈 Superchart""")
    with open(os.path.join(os.getenv("PATH_TO_DATA_FOLDER"), 'ticker_list.p'), 'rb') as f:
        ticker_turnovers = pickle.load(f)
    with open(os.path.join(os.getenv("PATH_TO_DATA_FOLDER"), 'mcftr.p'), 'rb') as f:
        benchmark_raw = pickle.load(f)
    with open(os.path.join(os.getenv("PATH_TO_DATA_FOLDER"), 'base_dict.p'), 'rb') as f:
        base_dict = pickle.load(f)
    with open(os.path.join(os.getenv("PATH_TO_DATA_FOLDER"), 'div_table.p'), 'rb') as f:
        div_table = pickle.load(f)
    selected_stock = st.sidebar.selectbox("Select asset:", ticker_turnovers.index.to_list())
    st.subheader(f"""{selected_stock} dividend adjusted""")
    stock_data = base_dict[selected_stock][['PX_OPEN', 'PX_LAST', 'PX_LOW', 'PX_HIGH']]
    try:
        rt_candle, time_updated = get_current_candle("MOEX", selected_stock, div_table)
        if len(rt_candle.dropna()) == 1:
            stock_data = pd.concat([stock_data, rt_candle[['PX_OPEN', 'PX_LAST', 'PX_LOW', 'PX_HIGH']]])
            st.markdown(f"Price updated at: **{rt_candle.index[0]:%d.%m.%Y}** **{time_updated}**")
        else:
            st.markdown(f"Price updated at: **{stock_data.index[-1]:%d.%m.%Y}**")
    except:
        st.markdown(f"Price updated at: **{stock_data.index[-1]:%d.%m.%Y}**")
    st.markdown(
        f"Median turnover over last 90 calendar days: **{int(ticker_turnovers.loc[selected_stock] / 1e6)} M RUB**")
    selected_timeframe = st.selectbox("Select timeframe:", ['Daily', 'Weekly', 'Monthly'])

    if selected_timeframe == 'Daily':
        render_candlestick_chart(stock_data[['PX_OPEN', 'PX_LAST', 'PX_LOW', 'PX_HIGH']].dropna().iloc[-252:])
    elif selected_timeframe == 'Weekly':
        render_candlestick_chart(
            resample_candlestick(stock_data[['PX_OPEN', 'PX_LAST', 'PX_LOW', 'PX_HIGH']].dropna().iloc[-252 * 5:],
                                 'W-FRI'))
    elif selected_timeframe == 'Monthly':
        render_candlestick_chart(
            resample_candlestick(stock_data[['PX_OPEN', 'PX_LAST', 'PX_LOW', 'PX_HIGH']].dropna().iloc[-252 * 15:],
                                 'M'))
    st.subheader(f"""Log diff. ({selected_stock} vs MCFTR)""")
    st.markdown(f"Price updated at: **{benchmark_raw.index[-1]:%d.%m.%Y}**")
    for lookback_period, timeframe in zip([365, 1095, 1825, 5475], ['1d', '1d', 'W-FRI', 'M']):
        benchmark = benchmark_raw.resample('1d').last().ffill().dropna().iloc[
                    -lookback_period:]
        last_prices = stock_data['PX_LAST'].resample('1d').last().ffill().dropna().iloc[-lookback_period:]
        if timeframe != '1d':
            benchmark = benchmark.resample(timeframe).last().ffill()
            last_prices = last_prices.resample(timeframe).last().ffill()
        logdiff = compute_logdiff(last_prices, benchmark).reindex(
            benchmark.index).bfill()
        st.text(
            f'last {int(lookback_period / 365)} years, timeframe {timeframe.replace("W-FRI", "weekly").replace("M", "monthly").replace("1d", "daily")}')
        render_diff_chart(logdiff, f"{lookback_period}_{timeframe}")


main()
