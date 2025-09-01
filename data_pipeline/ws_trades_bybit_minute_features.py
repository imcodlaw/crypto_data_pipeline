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
log_file = '/root/logs/ws_trades_bybit_minute_features.log'
if os.path.exists(log_file) :
    os.remove(log_file)

handler = RotatingFileHandler(log_file, maxBytes=5000000, backupCount=1)  # 10MB limit, 1 backup file
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)

logger = logging.getLogger()
logger.setLevel(logging.INFO)
logger.addHandler(handler)

async def create_db_pool():
    conn = await aiosqlite.connect("/root/codes/py/db/bybit_agg_trades.db")
    await conn.execute("PRAGMA journal_mode=WAL;")
    await conn.execute("PRAGMA synchronous=NORMAL;")
    return conn


async def process_trade(trades, db_pool):
    """Processing trades, extracting valuable features by 1-minute intervals, and pushing it to db."""
    trades_df = pl.DataFrame(np.array(trades)).rename({'column_0' : 'timestamp',
                                                        'column_1' : 'symbol',
                                                        'column_2' : 'quotevolume',
                                                        'column_3' : 'buyer_maker_quotevolume',
                                                        'column_4' : 'buyer_taker_quotevolume'}).with_columns(pl.col('timestamp').cast(pl.Int64), 
                                                                                                                pl.col('quotevolume', 'buyer_maker_quotevolume', 'buyer_taker_quotevolume').cast(pl.Float64))
    

    trades_df = trades_df.with_columns(
        ((pl.col('quotevolume') - (pl.col('quotevolume').rolling_mean(1440).over('symbol').shift(1)))/pl.col('quotevolume').rolling_std(1440).over('symbol').shift(1)).alias('normalized_24h_quotevolume'),
        ((pl.col('quotevolume') - (pl.col('quotevolume').rolling_mean(60).over('symbol').shift(1)))/pl.col('quotevolume').rolling_std(60).over('symbol').shift(1)).alias('normalized_1h_quotevolume'),
        ((pl.col('buyer_maker_quotevolume') - (pl.col('buyer_maker_quotevolume').rolling_mean(1440).over('symbol').shift(1)))/pl.col('buyer_maker_quotevolume').rolling_std(1440).over('symbol').shift(1)).alias('normalized_24h_buyer_maker_quotevolume'),
        ((pl.col('buyer_maker_quotevolume') - (pl.col('buyer_maker_quotevolume').rolling_mean(60).over('symbol').shift(1)))/pl.col('buyer_maker_quotevolume').rolling_std(60).over('symbol').shift(1)).alias('normalized_1h_buyer_maker_quotevolume'),
        ((pl.col('buyer_taker_quotevolume') - (pl.col('buyer_taker_quotevolume').rolling_mean(1440).over('symbol').shift(1)))/pl.col('buyer_taker_quotevolume').rolling_std(1440).over('symbol').shift(1)).alias('normalized_24h_buyer_taker_quotevolume'),
        ((pl.col('buyer_taker_quotevolume') - (pl.col('buyer_taker_quotevolume').rolling_mean(60).over('symbol').shift(1)))/pl.col('buyer_taker_quotevolume').rolling_std(60).over('symbol').shift(1)).alias('normalized_1h_buyer_taker_quotevolume')
        ).with_columns(
            (pl.col('normalized_24h_buyer_maker_quotevolume') - pl.col('normalized_24h_buyer_taker_quotevolume')).alias('normalized_spread_24h_maker_taker'),
            (pl.col('normalized_1h_buyer_maker_quotevolume') - pl.col('normalized_1h_buyer_taker_quotevolume')).alias('normalized_spread_1h_maker_taker'),
            (abs(pl.col('normalized_24h_buyer_maker_quotevolume') - pl.col('normalized_24h_buyer_taker_quotevolume'))).alias('abs_normalized_spread_24h_maker_taker'),
            (abs(pl.col('normalized_1h_buyer_maker_quotevolume') - pl.col('normalized_1h_buyer_taker_quotevolume'))).alias('abs_normalized_spread_1h_maker_taker')
            
            )
    
    trades_df = trades_df.filter(pl.col('timestamp') == pl.col('timestamp').max()).select(pl.col('timestamp',
                                                                                                    'symbol',
                                                                                                    'normalized_24h_quotevolume',
                                                                                                    'normalized_1h_quotevolume',
                                                                                                    'normalized_24h_buyer_maker_quotevolume',
                                                                                                    'normalized_1h_buyer_maker_quotevolume',
                                                                                                    'normalized_24h_buyer_taker_quotevolume',
                                                                                                    'normalized_1h_buyer_taker_quotevolume',
                                                                                                    'normalized_spread_24h_maker_taker',
                                                                                                    'normalized_spread_1h_maker_taker',
                                                                                                    'abs_normalized_spread_24h_maker_taker',
                                                                                                    'abs_normalized_spread_1h_maker_taker'
                                    ))
    
    query = """ 
            INSERT INTO bybit_minutely_features (
            current_timestamp, 
            timestamp, 
            'symbol',
            normalized_24h_quotevolume, 
            normalized_1h_quotevolume, 
            normalized_24h_buyer_maker_quotevolume, 
            normalized_1h_buyer_maker_quotevolume, 
            normalized_24h_buyer_taker_quotevolume, 
            normalized_1h_buyer_taker_quotevolume, 
            normalized_spread_24h_maker_taker, 
            normalized_spread_1h_maker_taker, 
            abs_normalized_spread_24h_maker_taker,
            abs_normalized_spread_1h_maker_taker)
            VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)
            """
    await db_pool.executemany(query, [(int(datetime.datetime.now(datetime.timezone.utc).timestamp()),) + tuple(agg) for agg in trades_df.to_numpy()])
    await db_pool.commit()
    logger.info(f"{datetime.datetime.now(datetime.timezone.utc)} Successfully insert into db for coins : {sorted(trades_df['symbol'].unique())}")
    trades_df = trades_df.clear()
    del trades_df


async def produce_from_db(queue, db_pool):
    """Fetch trades from the database for the current 1-minute window."""
    try :
        while True:
            now = datetime.datetime.now(datetime.timezone.utc).timestamp()
            standardized_now = int(now - (now%60))

            no_data = True
            while no_data :
                query_time = """
                SELECT max(timestamp)
                FROM
                `bybit_minutely_agg_trades`
                """

                async with db_pool.execute(query_time) as cursor1 :
                    max_time = await cursor1.fetchone()
                    max_time = int(max_time[0])
                    logger.info(f"max_time : {max_time} : timestamp : {(int(datetime.datetime.now(datetime.timezone.utc).timestamp()) - (int(datetime.datetime.now(datetime.timezone.utc).timestamp()) % 60))}")
                
                if (standardized_now - max_time) <= 60 :
                    query = """
                        SELECT 
                        timestamp, symbol, quotevolume, buyer_maker_quotevolume, buyer_taker_quotevolume
                        FROM bybit_minutely_agg_trades
                        WHERE timestamp >= ? AND timestamp < ?
                    """
                    async with db_pool.execute(query, (((standardized_now) - 60 * 60 * 24)-60, standardized_now,)) as cursor2:
                        logger.info(f'fetching data from {((standardized_now) - 60 * 60 * 24)-60} to {standardized_now}')
                        rows = await cursor2.fetchall()
                        if rows :
                            no_data = False
                            if (len(rows)/4) >= 1440 :
                                await queue.put(np.array(rows))
                                logger.info(f"Starting process at {now} and sleeping for {((int(now) - (int(now)%60)) + 60) - now} sec; len data per symbol : {len(rows)/4}")
                            else :
                                logger.info(f'current rows of data per symbol {len(rows)/4}')
                                logger.info(f"Still not enough data, sleep for {((int(now) - (int(now)%60)) + 60) - now} sec")

                else : 
                    no_data = True
                    logger.info('current minute data still not available, waiting...')
                    await asyncio.sleep(1)

            now = datetime.datetime.now(datetime.timezone.utc).timestamp()
            time_sleep = int((now - (now % 60)) + 60) - datetime.datetime.now(datetime.timezone.utc).timestamp()
            logger.info(f"Fetching data from db at {datetime.datetime.now(datetime.timezone.utc).timestamp()} and sleeping for {time_sleep} sec")
            await asyncio.sleep(time_sleep)

    except Exception as e:
        logger.error(f"Error in produce_from_db: {e}")


async def consume(queue, db_pool):
    """Consume trades from the queue and process them."""
    while True:
        trades = await queue.get()
        await process_trade(trades, db_pool)
        queue.task_done()


async def main():
    db_pool = await create_db_pool()
    queue = asyncio.Queue()

    tasks = [
        asyncio.create_task(produce_from_db(queue, db_pool)),
        asyncio.create_task(consume(queue, db_pool))
        
        ]
    
    try:
        await asyncio.gather(*tasks)
    finally:
        await db_pool.close()


if __name__ == "__main__":
    try :
        asyncio.run(main())
    except Exception as e:
        logger.info(f"Unexpected error at minute features: {e}")

