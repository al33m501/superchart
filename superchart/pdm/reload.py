from marketdb.get_base_dict import get_base_dict, get_dividend, get_bd_file
import pickle
import pandas as pd
import os
from dotenv import load_dotenv
from moexalgo.session import authorize
import pandas_datareader as pdr
from requests.auth import HTTPBasicAuth
import requests

load_dotenv()

STOCKS_ALWAYS_TO_USE = ['YNDX', 'YDEX', 'RTKM', 'RTKMP', 'MTSS', 'AFKS', 'WUSH', 'SOFL', 'OZON', 'HEAD', 'CIAN', 'ASTR',
                        'VKCO', 'POSI', 'DIAS', 'MBNK', 'MDMG', 'IVAT', 'VSEH', 'DELI', 'PRMD', 'DATA']


def reload_base_dict():
    base_dict = get_base_dict()
    with open(os.path.join(os.getenv("PATH_TO_DATA_FOLDER"), 'base_dict_full.p'), 'wb') as f:
        pickle.dump(base_dict, f)
    new_base_dict = dict()
    last_prices = pd.DataFrame()
    for t, val in base_dict.items():
        if len(val) > 0:
            last_prices.loc[t, 'PX_LAST'] = val.iloc[-1].loc['PX_LAST']
            last_prices.loc[t, 'MEDIAN_TURNOVER'] = val['PX_TURNOVER'].resample("1d").last().rolling(90, min_periods=1).median().iloc[-1]
    for ticker, data in base_dict.items():
        if data.empty:
            continue
        data['MEDIAN_TURNOVER'] = data['PX_TURNOVER'].resample("1d").last().rolling(90, min_periods=1).median()
        data['MEDIAN_TURNOVER'] = data['MEDIAN_TURNOVER'].fillna(0)
        if data.iloc[-1].loc['MEDIAN_TURNOVER'] > int(os.getenv("MINIMUM_TURNOVER")) and (
                pd.Timestamp.today() - data.index[-1]) < pd.Timedelta(days=30):
            new_base_dict[ticker] = data[data.index > (data.index[-1] - pd.tseries.offsets.DateOffset(years=15))].copy()
        elif (ticker in STOCKS_ALWAYS_TO_USE) and (
                pd.Timestamp.today() - data.index[-1]) < pd.Timedelta(days=30):
            new_base_dict[ticker] = data[data.index > (data.index[-1] - pd.tseries.offsets.DateOffset(years=15))].copy()
    with open(os.path.join(os.getenv("PATH_TO_DATA_FOLDER"), 'base_dict.p'), 'wb') as f:
        pickle.dump(new_base_dict, f)
    with open(os.path.join(os.getenv("PATH_TO_DATA_FOLDER"), 'last_prices.p'), 'wb') as f:
        pickle.dump(last_prices, f)


def reload_div_table():
    div_table = get_dividend("2000-01-01")
    with open(os.path.join(os.getenv("PATH_TO_DATA_FOLDER"), 'div_table.p'), 'wb') as f:
        pickle.dump(div_table, f)


def reload_cert():
    authorize(os.getenv("MOEX_LOGIN"), os.getenv("MOEX_PASSWORD"))
    url = 'https://passport.moex.com/authenticate'
    username = os.getenv("MOEX_LOGIN")
    password = os.getenv("MOEX_PASSWORD")
    response = requests.get(url, auth=HTTPBasicAuth(username, password))
    cert = response.cookies['MicexPassportCert']
    if cert is not None:
        with open(os.path.join(os.getenv("PATH_TO_DATA_FOLDER"), 'cert.p'), 'wb') as f:
            pickle.dump(cert, f)
        print(cert)


def reload_mcftr():
    mcftr = pdr.get_data_moex("MCFTR", "2010-01-01")['CLOSE']
    with open(os.path.join(os.getenv("PATH_TO_DATA_FOLDER"), 'mcftr.p'), 'wb') as f:
        pickle.dump(mcftr, f)
    imoex = pdr.get_data_moex("IMOEX", "2010-01-01")[['CLOSE', 'OPEN', 'HIGH', 'LOW', 'VALUE']].rename(columns={"VALUE": "VALTODAY_RUR"})
    with open(os.path.join(os.getenv("PATH_TO_DATA_FOLDER"), 'imoex.p'), 'wb') as f:
        pickle.dump(imoex, f)
    imoex2 = pdr.get_data_moex("IMOEX2", "2010-01-01")[['CLOSE', 'OPEN', 'HIGH', 'LOW', 'VALUE']].rename(
        columns={"VALUE": "VALTODAY_RUR"})
    with open(os.path.join(os.getenv("PATH_TO_DATA_FOLDER"), 'imoex2.p'), 'wb') as f:
        pickle.dump(imoex2, f)


def reload_ticker_list():
    with open(os.path.join(os.getenv("PATH_TO_DATA_FOLDER"), 'base_dict.p'), 'rb') as f:
        base_dict = pickle.load(f)
    ticker_list = pd.Series(dtype='float64')
    for t, data in base_dict.items():
        ticker_list.loc[t] = data['MEDIAN_TURNOVER'].iloc[-1]
    ticker_list = ticker_list.sort_values(ascending=False)
    with open(os.path.join(os.getenv("PATH_TO_DATA_FOLDER"), 'ticker_list.p'), 'wb') as f:
        pickle.dump(ticker_list, f)
    with open("J:/Shared Folder/Quant_data/ticker_turnover_list/ticker_list.p", 'wb') as f:
        pickle.dump(ticker_list, f)

if __name__ == '__main__':
    # with open('../../data/base_dict_full.p', 'rb') as f:
    #     bd = pickle.load(f)
    # with open(os.path.join(os.getenv("PATH_TO_DATA_FOLDER"), 'div_table.p'), 'rb') as f:
    #     div_table = pickle.load(f)
    # print()
    # bd['PLZL'].to_excel("PLZL.xlsx")
    # bd['SBER'].to_excel("SBER.xlsx")
    # # with open('../../data/div_table.p', 'rb') as f:
    # #     div_table = pickle.load(f)
    # reload_div_table()
    # reload_base_dict()
    # reload_ticker_list()
    reload_mcftr()
    # reload_cert()
    # with open('../../data/mcftr.p', 'rb') as f:
    #     mcftr = pickle.load(f)
    # print(mcftr)
    # with open('../../data/base_dict_full.p', 'rb') as f:
    #     base_dict = pickle.load(f)
    # prices_upto_17=pd.DataFrame()
    # for k,v in bd.items():
    #     t = v.copy()
    #     t['ticker'] = k
    #     prices_upto_17 = pd.concat([prices_upto_17, t.copy()])
    #
    # prices = pd.DataFrame()
    # for k, v in base_dict.items():
    #     t = v.copy()
    #     t['ticker'] = k
    #     prices = pd.concat([prices, t.copy()])
    # t = ['ROSN', 'NVTK', 'LKOH', 'FESH', 'FLOT', 'SIBN', 'MAGN', 'GAZP', 'MTLR', 'GMKN', 'NLMK', 'MGNT', 'SNGS', 'PIKK', 'IRAO', 'ALRS', 'YNDX', 'MTSS', 'RTKM', 'CBOM', 'CHMF', 'ENPG', 'PHOR', 'HYDR', 'TATN', 'BSPB', 'WUSH', 'MRKP', 'MRKC', 'TRMK', 'MSNG', 'RUAL', 'FEES', 'GLTR', 'MOEX', 'TRNFP', 'VTBR', 'AFLT', 'POSI', 'SBER', 'SBERP', 'SNGSP', 'FIVE', 'AFKS', 'PLZL', 'TCSG', 'CIAN', 'HHRU', 'OZON', 'BANEP', 'POLY', 'SVCB', 'MDMG', 'SGZH', 'VKCO', 'BANE', 'DIAS', 'LSRG']
    # for t1 in t:
    #     try:
    #         base_dict[t1].loc['2024-05-23']
    #     except:
    #         print(t1)
    # print(len(base_dict))
    # print(base_dict['SBER'])
