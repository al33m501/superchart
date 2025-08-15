import asyncio
import pandas as pd
from sqlalchemy.ext.asyncio import create_async_engine
from sqlalchemy import (
    create_engine, text
)
from time import sleep
import numpy as np
from dotenv import load_dotenv
import os
import pickle

load_dotenv()
url = "postgresql+asyncpg://al33m501:npg_6AMRyhplGdj4@ep-muddy-mouse-640259-pooler.eu-central-1.aws.neon.tech/superchart"


def convert_decimals_to_float(df):
    return df.select_dtypes(include=[np.number]).astype(float).combine_first(df)


async def translator_for_ser_df(table, table_name):
    engine = create_async_engine(url)
    # table = convert_decimals_to_float(table)
    async with engine.begin() as conn:
        await conn.run_sync(
            lambda sync_conn: table.to_sql(table_name, sync_conn, index=False, if_exists="replace",
                                           chunksize=10000)
        )
    await engine.dispose()


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
with open(os.path.join(os.getenv("PATH_TO_DATA_FOLDER"), 'mcftr.p'), 'rb') as f:
    mcftr = pickle.load(f)
with open(os.path.join(os.getenv("PATH_TO_DATA_FOLDER"), 'base_dict_bonds.p'), 'rb') as f:
    base_dict_bonds = pickle.load(f)
with open(os.path.join(os.getenv("PATH_TO_DATA_FOLDER"), 'ticker_list_bonds.p'), 'rb') as f:
    ticker_list_bonds = pickle.load(f)


# base_dict_table = pd.DataFrame()
# base_dict_bonds_table = pd.DataFrame()
# for ticker, ticker_df in base_dict.items():
#     ticker_df['ticker'] = ticker
#     base_dict_table = pd.concat([base_dict_table, ticker_df.reset_index().copy()])
# for ticker, ticker_df in base_dict_bonds.items():
#     ticker_df['ticker'] = ticker
#     base_dict_bonds_table = pd.concat([base_dict_bonds_table, ticker_df.reset_index().copy()])
# print()
# asyncio.run(translator_for_ser_df(ticker_turnovers.reset_index(), 'ticker_list'))
# asyncio.run(translator_for_ser_df(benchmark_raw.reset_index(), 'mcftr'))
# asyncio.run(translator_for_ser_df(base_dict_table, 'base_dict'))
# asyncio.run(translator_for_ser_df(imoex2.reset_index(), 'imoex2'))
# asyncio.run(translator_for_ser_df(div_table, 'div_table'))
# base_dict_bonds_table[['open_YTM', 'high_YTM', 'low_YTM', 'last_YTM', 'value']] = base_dict_bonds_table[['open_YTM', 'high_YTM', 'low_YTM', 'last_YTM', 'value']].astype(float)
# asyncio.run(translator_for_ser_df(base_dict_bonds_table, 'base_dict_bonds'))

# asyncio.run(translator_for_ser_df(ticker_list_bonds, 'ticker_list_bonds'))

async def load_data_neon(table):
    engine = create_async_engine(url)
    try:
        async with engine.begin() as conn:
            query = f"""
            select * from {table}
            """
            result = await conn.execute(text(query))
            return pd.DataFrame(result)
    except Exception as e:
        print(f"Error loading data: {e}")
        return []
    finally:
        await engine.dispose()


def load_data_neon_sync(table):
    df = asyncio.run(load_data_neon(table))
    return df

print()
