import asyncio
import pandas as pd
from sqlalchemy.ext.asyncio import create_async_engine
from sqlalchemy import (
    create_engine,
)
from time import sleep
from dotenv import load_dotenv
import os


load_dotenv()


def get_bars_daily_long_mysql():
    engine = create_engine("mysql+mysqlconnector://algouser:algouser@192.168.206.34:3306/bonds_moex_db")
    query = f'''
        select * from bars_daily_live
        '''
    with engine.connect() as conn:
        data = conn.execute(query)
    data = pd.DataFrame(data.fetchall(), columns=data.keys())
    return data


async def translator():
    url = os.getenv("NEON_URL")
    engine = create_async_engine(url)
    table = get_bars_daily_long_mysql()
    async with engine.begin() as conn:
        await conn.run_sync(
            lambda sync_conn: table.to_sql("bars_daily_live", sync_conn, index=False, if_exists="replace",
                                           chunksize=10000)
        )
    await engine.dispose()


while True:
    asyncio.run(translator())
    sleep(60)

# loader from postgre
# async def load_data_raw_sql_async(instrument):
#     engine = create_async_engine(url)
#     try:
#         async with engine.begin() as conn:
#             query = f"""
#             select * from bars_daily_live bdl
#             """
#             result = await conn.execute(text(query))
#             return pd.DataFrame(result)
#     except Exception as e:
#         print(f"Error loading data: {e}")
#         return []
#     finally:
#         await engine.dispose()
#
#
# def get_rt(instrument):
#     df = asyncio.run(load_data_raw_sql_async(instrument))
#     df = df[df['SECID'] == instrument]
#     return df
#
#
# print(get_rt("SU26243RMFS4"))
