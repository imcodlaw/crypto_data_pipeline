import asyncio
import logging
import os
import numpy as np
import datetime
import polars as pl
import aiosqlite
import time
from logging.handlers import RotatingFileHandler

BYBIT_URL = "wss://stream.bybit.com/v5/public/linear"
SYMBOLS = ["BTCUSDT", "ETHUSDT", "SOLUSDT", "SUIUSDT"]  
queue = asyncio.Queue()

# Configure logger
log_file = '/root/logs/ws_klines_bybit_aggregator.log'
if os.path.exists(log_file) :
    os.remove(log_file)

handler = RotatingFileHandler(log_file, maxBytes=5000000, backupCount=1)  # 10MB limit, 1 backup file
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)

logger = logging.getLogger()
logger.setLevel(logging.INFO)
logger.addHandler(handler)


SYMBOL1 = 'BTCUSDT'
SYMBOL2 = 'SUIUSDT'

class Aggregator:
    def __init__(self, conn):
        self.conn = conn

    async def process_trade(self, klines):
        # process klines and extracting relevant features every minute
        df = pl.DataFrame(np.array(klines)).rename({
            'column_0' : 'symbol',
            'column_1' : 'opentime',
            'column_2' : 'openprice'}).with_columns(
                ((pl.col('openprice').cast(pl.Float64) - (pl.col('openprice').cast(pl.Float64)).rolling_mean(1440).shift(1).over(['symbol']))/(pl.col('openprice').cast(pl.Float64).rolling_std(1440).shift(1).over(['symbol']))).alias('normalized_24h_openprice'),
                ((pl.col('openprice').cast(pl.Float64) - (pl.col('openprice').cast(pl.Float64)).rolling_mean(60).shift(1).over(['symbol']))/(pl.col('openprice').cast(pl.Float64).rolling_std(60).shift(1).over(['symbol']))).alias('normalized_1h_openprice'))
        
        agg_df = df.filter((pl.col('symbol') == SYMBOL1)).select(pl.exclude('symbol')).rename({
            'openprice' : f'openprice_{SYMBOL1}',
            'normalized_24h_openprice' : f'normalized_24h_openprice_{SYMBOL1}',
            'normalized_1h_openprice' : f'normalized_1h_openprice_{SYMBOL1}'}).join(
                df.filter((pl.col('symbol') == SYMBOL2)).select(pl.exclude('symbol')).rename({
                    'openprice' : f'openprice_{SYMBOL2}',
                    'normalized_24h_openprice' : f'normalized_24h_openprice_{SYMBOL2}',
                    'normalized_1h_openprice' : f'normalized_1h_openprice_{SYMBOL2}'}),
                    on = 'opentime',
                    how = 'left').sort(by = 'opentime', descending = False).with_columns(
                        (pl.col(f'normalized_24h_openprice_{SYMBOL1}') - pl.col(f'normalized_24h_openprice_{SYMBOL2}')).alias('normalized_spread_24h'),
                        (pl.col(f'normalized_1h_openprice_{SYMBOL1}') - pl.col(f'normalized_1h_openprice_{SYMBOL2}')).alias('normalized_spread_1h'),
                        abs(pl.col(f'normalized_24h_openprice_{SYMBOL1}') - pl.col(f'normalized_24h_openprice_{SYMBOL2}')).alias('abs_normalized_spread_24h'),
                        abs(pl.col(f'normalized_1h_openprice_{SYMBOL1}') - pl.col(f'normalized_1h_openprice_{SYMBOL2}')).alias('abs_normalized_spread_1h')
                        ).filter(pl.col('opentime') == pl.col('opentime').max())
        
        query = """ 
                INSERT INTO bybit_agg_klines (
                current_timestamp, 
                opentime, 
                openprice_BTCUSDT, 
                normalized_24h_openprice_BTCUSDT, 
                normalized_1h_openprice_BTCUSDT, 
                openprice_SUIUSDT,
                normalized_24h_openprice_SUIUSDT, 
                normalized_1h_openprice_SUIUSDT, 
                normalized_spread_24h, 
                normalized_spread_1h, 
                abs_normalized_spread_24h, 
                abs_normalized_spread_1h)
                VALUES(?,?,?,?,?,?,?,?,?,?,?,?)
                """
        await self.conn.executemany(query, [(int(datetime.datetime.now(datetime.timezone.utc).timestamp()),) + tuple(agg) for agg in agg_df.to_numpy()])
        await self.conn.commit()
        logger.info(f"{datetime.datetime.now(datetime.timezone.utc)} Successfully insert into db for coins : {SYMBOL1} and {SYMBOL2}")
        await asyncio.sleep(0.001)


async def produce_from_db(queue, conn):
    # basically fetching the required data for the calculations for every start of a minute
    minutes = 1450  # 1440 minutes or 24 hours + 10 more minutes just in case
    """Fetching klines every minute from db"""
    try :
        while True:
            timestamp = datetime.datetime.now(datetime.timezone.utc).timestamp()
            logger.info(f"timestamp : {int(timestamp) - (int(timestamp) % 60)}")
            time_threshold = int(time.mktime((datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(minutes=minutes)).timetuple()))

            no_data = True
            while no_data :
                query_time = """
                SELECT max(opentime)
                FROM
                `bybit_klines`
                """
                async with conn.execute(query_time) as cursor :
                    max_time = await cursor.fetchone()
                    max_time = int(max_time[0]/1000)
                    logger.info(f"max_time : {max_time} : timestamp : {(int(datetime.datetime.now(datetime.timezone.utc).timestamp()) - (int(datetime.datetime.now(datetime.timezone.utc).timestamp()) % 60))}")


                if max_time == (int(datetime.datetime.now(datetime.timezone.utc).timestamp()) - (int(datetime.datetime.now(datetime.timezone.utc).timestamp()) % 60)):
                    no_data = False
                    query = """
                    SELECT `symbol`, `opentime`, `openprice`
                    FROM
                    `bybit_klines`
                    WHERE
                    `symbol` IN ('BTCUSDT', 'SUIUSDT')
                    AND 
                    `timestamp` >= ?
                    ORDER BY `opentime` ASC;
                    """
                    # fetching only the required data
                    async with conn.execute(query, (time_threshold,)) as cursor:
                        rows = await cursor.fetchall()
                        if rows :
                            no_data = False
                            # checking if all symbols are already obtained
                            if (len(rows)/2) >= minutes :
                                await queue.put(np.array(rows))
                                logger.info(f"Starting process at {timestamp} and sleeping for {((int(timestamp) - (int(timestamp)%60)) + 60) - timestamp} sec")
                            else :
                                logger.info(f"Still not enough data, sleep for {((int(timestamp) - (int(timestamp)%60)) + 60) - timestamp} sec")
                                
                elif max_time != (int(datetime.datetime.now(datetime.timezone.utc).timestamp()) - (int(datetime.datetime.now(datetime.timezone.utc).timestamp()) % 60)):
                    await asyncio.sleep(0.25)

            time_sleep = ((int(timestamp) - (int(timestamp)%60)) + 60) - (datetime.datetime.now(datetime.timezone.utc).timestamp())

            await asyncio.sleep(time_sleep)

    except Exception as e:
        logger.error(f"Error in produce_from_db: {e}")


async def consume(queue, aggregator):
    # consume trades from the queue and process them
    while True:
        klines = await queue.get()
        await aggregator.process_trade(klines)
        queue.task_done()


async def main():
    async with aiosqlite.connect("/root/codes/py/db/bybit_klines.db") as conn:
        queue = asyncio.Queue()
        aggregator = Aggregator(conn)

        producer = asyncio.create_task(produce_from_db(queue, conn))
        consumer = asyncio.create_task(consume(queue, aggregator))

        await asyncio.gather(producer, consumer)


if __name__ == "__main__":
    logger.info(f'Starting aggregator for {SYMBOL1} and {SYMBOL2}')
    try :
        asyncio.run(main())
    except Exception as e:
        logger.info(f"Unexpected error: {e}")

