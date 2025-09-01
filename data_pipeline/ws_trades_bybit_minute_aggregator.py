import asyncio
import logging
import os
import numpy as np
import datetime
import polars as pl
import aiosqlite
from logging.handlers import RotatingFileHandler

BYBIT_URL = "wss://stream.bybit.com/v5/public/linear"
SYMBOLS = ["BTCUSDT", "ETHUSDT"]
queue = asyncio.Queue()

# Configure logger
log_file = '/root/logs/ws_trades_bybit_minute_aggregator.log'
if os.path.exists(log_file) :
    os.remove(log_file)

handler = RotatingFileHandler(log_file, maxBytes=5000000, backupCount=1)  # 10MB limit, 1 backup file
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)

logger = logging.getLogger()
logger.setLevel(logging.INFO)
logger.addHandler(handler)


RAW_DB = "/root/codes/py/db/bybit_trades.db"
AGG_DB = "/root/codes/py/db/bybit_agg_trades.db"

async def create_db_pool(path):
    conn = await aiosqlite.connect(path)
    await conn.execute("PRAGMA journal_mode=WAL;")
    await conn.execute("PRAGMA synchronous=NORMAL;")
    return conn

async def process_trade(trades, agg_db):
    """Process trades and aggregate them by 1-minute intervals."""
    trades_df = pl.DataFrame(np.array(trades)).rename({'column_0' : 'timestamp',
                                                        'column_1' : 'symbol',
                                                        'column_2' : 'side',
                                                        'column_3' : 'volume',
                                                        'column_4' : 'price',
                                                        'column_5' : 'transaction_id'}).with_columns(pl.col('timestamp').cast(pl.Int64), 
                                                                                                    pl.col('volume', 'price').cast(pl.Float64),
                                                                                                    (pl.col('volume').cast(pl.Float64) * pl.col('price').cast(pl.Float64)).alias('quotevolume'),
                                                                                                    ((pl.col('timestamp').cast(pl.Int64)/1000).cast(pl.Int64) - ((pl.col('timestamp').cast(pl.Int64)/1000).cast(pl.Int64) % 60)).alias('rounded_timestamp'))
    
    bm_df = trades_df.group_by(['symbol', 'side']).agg(pl.col('quotevolume').sum()).filter(pl.col('side') == 'Sell')
    bt_df = trades_df.group_by(['symbol', 'side']).agg(pl.col('quotevolume').sum()).filter(pl.col('side') == 'Buy')

    agg_df = trades_df.group_by(['symbol']).agg(pl.col('rounded_timestamp').first(),
                                                pl.col('price').first().alias('openprice'),
                                                pl.col('price').max().alias('highprice'),
                                                pl.col('price').min().alias('lowprice'),
                                                pl.col('price').last().alias('closeprice'),
                                                pl.col('quotevolume').sum(),
                                                pl.col('transaction_id').count().alias('trades')
                                                    ).join(bm_df.rename({'quotevolume' : 'buyer_maker_quotevolume'}).select(pl.col('symbol', 'buyer_maker_quotevolume')), 
                                                            on = 'symbol', 
                                                            how = 'left').join(
                                                                bt_df.rename({'quotevolume' : 'buyer_taker_quotevolume'}).select(pl.col('symbol', 'buyer_taker_quotevolume')), 
                                                                on = 'symbol', 
                                                                how = 'left')
    agg_df = agg_df.with_columns((pl.col('buyer_maker_quotevolume', 'buyer_taker_quotevolume').fill_null(0)))
    agg_df = agg_df.select(pl.col('rounded_timestamp',
                                    'symbol',
                                    'openprice',
                                    'highprice',
                                    'lowprice',
                                    'closeprice',
                                    'quotevolume',
                                    'buyer_maker_quotevolume',
                                    'buyer_taker_quotevolume',
                                    'trades'))
    
    query = """ 
            INSERT INTO bybit_minutely_agg_trades (current_timestamp, timestamp, symbol, openprice, highprice, lowprice, closeprice, quotevolume, buyer_maker_quotevolume, buyer_taker_quotevolume, trades)
            VALUES(?,?,?,?,?,?,?,?,?,?,?)
            """
    await agg_db.executemany(query, [(int(datetime.datetime.now(datetime.timezone.utc).timestamp()),) + tuple(agg) for agg in agg_df.to_numpy()])
    await agg_db.commit()

    logger.info(f"{datetime.datetime.now(datetime.timezone.utc)} Successfully insert into db for coins : {sorted(agg_df['symbol'].unique())}")


async def produce_from_db(queue, raw_db):
    """Fetch trades from the database for the current 1-minute window."""
    try :
        while True:
            now = datetime.datetime.now(datetime.timezone.utc).timestamp()
            standardized_now = ((now * 1000) - (now * 1000)%60000)

            query = """
                SELECT timestamp, symbol, side, volume, price, transaction_id
                FROM bybit_trades
                WHERE timestamp >= ? AND timestamp < ?
            """

            async with raw_db.execute(query, ((standardized_now) - 60000, standardized_now)) as cursor:
                rows = await cursor.fetchall()
                if rows :
                    logger.info(f'fetched data {np.array(rows).shape}')
                    await queue.put(np.array(rows))
                    
                else :
                    await asyncio.sleep(0.1)

            now = datetime.datetime.now(datetime.timezone.utc).timestamp()
            time_sleep = int((now - (now % 60)) + 60) - datetime.datetime.now(datetime.timezone.utc).timestamp()
            logger.info(f"Fetching data from db at {datetime.datetime.now(datetime.timezone.utc).timestamp()} and sleeping for {time_sleep} sec")
            await asyncio.sleep(time_sleep)

    except Exception as e:
        logger.error(f"Error in produce_from_db: {e}")


async def consume(queue, agg_db):
    """Consume trades from the queue and process them."""
    while True:
        trades = await queue.get()
        await process_trade(trades, agg_db)
        queue.task_done()


async def main():
    raw_db = await create_db_pool(RAW_DB)  # read raw trades
    agg_db = await create_db_pool(AGG_DB) 

    tasks = [
        asyncio.create_task(produce_from_db(queue, raw_db)),
        asyncio.create_task(consume(queue, agg_db))
        
        ]
    
    try:
        await asyncio.gather(*tasks)
    finally:
        await raw_db.close()
        await agg_db.close()


if __name__ == "__main__":
    logger.info(f'Starting Bybit\'s trades minute aggregator')
    try :
        asyncio.run(main())
    except Exception as e:
        logger.info(f"Unexpected error at minute aggregator: {e}")

