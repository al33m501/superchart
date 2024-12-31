import pickle
import pandas as pd
import os
import streamlit as st
import requests
from dotenv import load_dotenv
import warnings
import plotly.express as px

warnings.simplefilter(action='ignore', category=FutureWarning)
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


def get_nearest_date_before(date_list, dt):
    return date_list[date_list <= dt][-1]


def calc_return(prices, old_date, new_date):
    new_date = pd.Timestamp(new_date)
    assert isinstance(old_date, pd.Timestamp) and isinstance(new_date, pd.Timestamp) and isinstance(prices,
                                                                                                    pd.DataFrame)
    assert old_date < new_date
    assert old_date in prices.index and new_date in prices.index
    returns = (prices.loc[new_date] - prices.loc[old_date]) / prices.loc[old_date]
    return returns


def get_return_report():
    with open(os.path.join(os.getenv("PATH_TO_DATA_FOLDER"), 'base_dict.p'), 'rb') as f:
        base_dict = pickle.load(f)
    with open(os.path.join(os.getenv("PATH_TO_DATA_FOLDER"), 'div_table.p'), 'rb') as f:
        div_table = pickle.load(f)
    today_dt = pd.to_datetime(get_current_stock_table("MOEX")['SYSTIME'].iloc[0]).date()
    stock_table = get_current_stock_table("MOEX").set_index("SECID")[['LAST', 'VALTODAY', 'SYSTIME']].dropna()
    today_divs = div_table[div_table['ex_date'] == today_dt.strftime("%Y-%m-%d")]
    stock_table = pd.merge(left=stock_table, left_index=True, right=today_divs.set_index("ticker")['dividend_amount'],
                           right_index=True, how='left')
    stock_table['dividend_amount'] = stock_table['dividend_amount'].fillna(0)
    if not stock_table.empty:
        all_prices = pd.DataFrame(index=base_dict['GAZP'].index.append(pd.DatetimeIndex([today_dt])))
    else:
        all_prices = pd.DataFrame(index=base_dict['GAZP'].index)
    missing_tickers = []
    for t, prices in base_dict.items():
        if t in stock_table.index and not stock_table.empty:
            temp = base_dict[t]['PX_LAST']
            temp.loc[today_dt] = stock_table.loc[t, 'LAST'] + stock_table.loc[t, 'dividend_amount']
            all_prices[t] = temp.copy()
        elif stock_table.empty:
            temp = base_dict[t]['PX_LAST']
            all_prices[t] = temp.copy()
        elif t not in stock_table.index and not stock_table.empty:
            missing_tickers.append(t)
    all_prices = all_prices.ffill()
    all_prices = all_prices[~all_prices.index.duplicated(keep='first')]
    report = pd.DataFrame()
    for old_date, friendly_name in zip([all_prices.index[-1] - pd.tseries.offsets.DateOffset(years=1),
                                        all_prices.index[-1] - pd.tseries.offsets.DateOffset(months=1), ],
                                       ['1 year return',
                                        '1 month return', ]):
        nearest_dt = get_nearest_date_before(all_prices.index, old_date)
        report[friendly_name] = calc_return(
            all_prices,
            nearest_dt,
            all_prices.index[-1])
    missing_tickers += report[report['1 year return'].isna() | report['1 month return'].isna()].index.tolist()
    return report, missing_tickers


def render_all():
    report, missing_tickers = get_return_report()
    report.index.name = 'ticker'
    report = report.reset_index()

    fig = px.scatter(report,
                     x='1 year return',
                     y='1 month return',
                     # size='dot_size',
                     text='ticker',
                     # log_x=True,
                     labels={
                         "x": "1 year return",
                         "y": "1 month return",
                         # 'dot_size': 'Portability',
                         'ticker': 'ticker '
                     },
                     title='Stocks 1-year vs 1-month dividend adjusted return',
                     )

    def improve_text_position(x):
        """ it is more efficient if the x values are sorted """
        positions = ['top center', 'bottom center']
        return [positions[i % len(positions)] for i in range(len(x))]

    fig.update_traces(textposition=improve_text_position(report['1 year return'].sort_values()))
    fig.update_layout(
        xaxis=dict(tickformat=".0%"),
        yaxis=dict(tickformat=".0%")
    )
    fig.update_layout(
        title_font_size=24,
        xaxis=dict(title_font_size=18, tickfont_size=14),
        yaxis=dict(title_font_size=18, tickfont_size=14),
    )
    fig.update_layout(height=800)
    fig.update_layout(
        shapes=[
            dict(
                type="line",
                x0=0,
                y0=min(report['1 month return']),
                x1=0,
                y1=max(report['1 month return']),
                line=dict(color="#e3e3e3", dash="dash"),
            ),
            dict(
                type="line",
                x0=min(report['1 year return']),
                y0=0,
                x1=max(report['1 year return']),
                y1=0,
                line=dict(color="#e3e3e3", dash="dash"),
            ),
        ]
    )
    st.plotly_chart(fig, use_container_width=True)
    st.text(f"Missing tickers: {missing_tickers}\nPossible reason: not enough data")


def main():
    st.set_page_config(
        page_title="Superchart",
        page_icon="📈",
        layout='wide',

    )
    hide_menu_style = """
                <style>
                #MainMenu {visibility: hidden;}
                </style>
                """
    st.markdown(hide_menu_style, unsafe_allow_html=True)
    st.sidebar.subheader("""📈 Superchart""")
    render_all()


main()
