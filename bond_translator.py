import asyncio
import pandas as pd
from sqlalchemy.ext.asyncio import create_async_engine
from sqlalchemy import (
    create_engine,
)
from sqlalchemy import text

url = "postgresql+asyncpg://al33m501:R5U0LcgeZVaM@ep-muddy-mouse-640259-pooler.eu-central-1.aws.neon.tech/superchart"


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
    engine = create_async_engine(url)
    table = get_bars_daily_long_mysql()
    async with engine.begin() as conn:
        await conn.run_sync(
            lambda sync_conn: table.to_sql("bars_daily_live", sync_conn, index=False, if_exists="replace",
                                           chunksize=10000)
        )
    await engine.dispose()


# asyncio.run(translator())

# loader from postgre
async def load_data_raw_sql_async(instrument):
    engine = create_async_engine(url)
    try:
        async with engine.begin() as conn:
            query = f"""
            select * from bars_daily_live bdl 
"""
            result = await conn.execute(text(query))
            # rows = result.fetchall()
            # columns = result.keys()
            # data = [dict(zip(columns, row)) for row in rows]
            return pd.DataFrame(result)
    except Exception as e:
        print(f"Error loading data: {e}")
        return []
    finally:
        await engine.dispose()


def get_rt(instrument):
    return asyncio.run(load_data_raw_sql_async(instrument))


print(get_rt("SU26243RMFS4"))
