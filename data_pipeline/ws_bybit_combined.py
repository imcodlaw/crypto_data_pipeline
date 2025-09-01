import asyncio
import websockets
import polars as pl
import datetime
import logging
import logging
import signal
import os
import json
import aiosqlite
import numpy as np
from logging.handlers import RotatingFileHandler
from websockets.asyncio.client import connect



# Configure a RotatingFileHandler
# need to manage the log by limiting the stored logs
# once it gets 1 mb of size, making a new one
# once the new one has 1 mb size of data, then the older one gets replaced so on 
log_file = '/root/logs/ws_bybit_combined_new.log'
if os.path.exists(log_file) :
    os.remove(log_file)
handler = RotatingFileHandler(log_file, maxBytes=10000000, backupCount=2)  # 10MB limit, 1 backup file
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)

logger = logging.getLogger()
logger.setLevel(logging.INFO)
logger.addHandler(handler)

BYBIT_URL = "wss://stream.bybit.com/v5/public/linear"
KLINES_SYMBOLS = ["BTCUSDT", "ETHUSDT", "SOLUSDT", "SUIUSDT"]
TRADES_SYMBOLS = ["BTCUSDT", "ETHUSDT", "SOLUSDT", "SUIUSDT"]
DEPTHS_SYMBOLS = ["BTCUSDT", "ETHUSDT", "SOLUSDT", "SUIUSDT"]
PING_INTERVAL = 15
PING_TIMEOUT = 10

klines_queue = asyncio.Queue()
trades_queue = asyncio.Queue()
trade_list_lock = asyncio.Lock()
trades_list = []


async def create_db_pool():
    """Creating a db pool to prevent making new connections for every process that requires connecting to db"""
    conn = await aiosqlite.connect("/root/codes/py/db/bybit_trades.db")
    await conn.execute("PRAGMA journal_mode=WAL;")
    await conn.execute("PRAGMA synchronous=NORMAL;")
    return conn

async def heartbeat_logger(ws, interval=PING_INTERVAL):
    """Manual heartbeat connection to minimize the possibility of losing connection due to inactivity + logger"""
    while True:
        try:
            pong = await ws.ping()
            await pong
            logger.info("💓 Heartbeat: ping → pong received")
        except Exception as e:
            logger.error(f"Heartbeat error: {e}")
            return  # Exit heartbeat on failure
        await asyncio.sleep(interval)


async def consumer_worker(message_queue):
    """Putting messages into queue"""
    first_timestamp = {} # basically a dict to store coins that already have first candlestick data
    klines_list_first = [] # containing the first candlestick data for all coins, basically to get the first openprice
    klines_list_update = [] # containing the last candlestick data for all coins that will be used to update the remaining of the data
    trade_list = [] # containing a batch of trades data to be pushed
    last_push_time = 0 # the last timestamp where trade data were pushed

    while True:
        message = await message_queue.get()
        try:
            first_timestamp, klines_list_first, klines_list_update, trade_list, last_push_time = await route_message(
                message,
                first_timestamp,
                klines_list_first,
                klines_list_update,
                trade_list,
                last_push_time
            )
        except Exception as e:
            logger.error(f"Error processing message: {e}")
        finally:
            message_queue.task_done()

async def route_message(message, first_timestamp, klines_list_first, klines_list_update, trade_list, last_push_time):
    """Route incoming messages to the right processing queue."""
    try:
        data = json.loads(message)

        # Ignore pings or subscription acks
        if "op" in data or "success" in data:
            logger.debug(f"Server ack: {data}")
            return first_timestamp, klines_list_first, klines_list_update, trade_list, last_push_time

        topic = data.get("topic", "")

        # Handle trades
        if topic.startswith("publicTrade"):
            trades = data.get("data", [])
            trades = [
                (datetime.datetime.now(datetime.timezone.utc).timestamp() * 1000,) + # generating the current timestamp which will be used later to calculate the delay between current time and the fetched data
                tuple(trade.values())[:7]
                for trade in trades
            ]
            trade_list.extend(trades)
            current_timestamp_trade = int(datetime.datetime.now(datetime.timezone.utc).timestamp())
            
            # trades data are pushed every 5 seconds 
            if (current_timestamp_trade % 5 == 0) and current_timestamp_trade > last_push_time:
                # Put a copy so clearing won't affect the queue
                await trades_queue.put(list(trade_list))
                logger.info(f"Count of trades added to queue: {len(trade_list)}")
                trade_list.clear()
                last_push_time = current_timestamp_trade

        # Handle klines
        elif topic.startswith("kline"):
            symbol = data.get('topic').split('.')[-1]
            kline_data = data.get('data')[0]

            # open kline (not confirmed)
            if not kline_data.get('confirm'):
                if symbol not in first_timestamp:
                    klines_list_first.append((symbol,) + tuple(kline_data.values()))
                    first_timestamp[symbol] = kline_data.get('start')

            # closed kline (confirmed)
            else:
                klines_list_update.append((symbol,) + tuple(kline_data.values()))
                first_timestamp.pop(symbol, None)

            # waiting for open klines for all symbol and then put it into a queue
            if klines_list_first and len(pl.DataFrame(np.array(klines_list_first))['column_0'].unique()) >= len(KLINES_SYMBOLS):
                await klines_queue.put(list(klines_list_first))  # push a copy
                logger.info(f"open klines added to queue: {len(klines_list_first)}, final_klines : {klines_list_first}")
                klines_list_first.clear()

            # waiting for closed klines for all symbol and then put it into a queue
            if klines_list_update and len(pl.DataFrame(np.array(klines_list_update))['column_0'].unique()) >= len(KLINES_SYMBOLS):
                await klines_queue.put(list(klines_list_update))  # push a copy
                logger.info(f"closed klines added to queue: {len(klines_list_update)}, final_klines : {klines_list_update}")
                klines_list_update.clear()

        else:
            logger.debug(f"Unhandled topic: {topic}")

    except json.JSONDecodeError:
        logger.error(f"Failed to parse JSON: {message}")
    except Exception as e:
        logger.error(f"Error routing message: {e}")

    return first_timestamp, klines_list_first, klines_list_update, trade_list, last_push_time
    
async def websocket_handler():
    """Webscoket handler logic"""
    message_queue = asyncio.Queue()

    # start consumer worker
    asyncio.create_task(consumer_worker(message_queue))

    while True:
        try:
            async with connect(
                BYBIT_URL,
                ping_interval=PING_INTERVAL,
                ping_timeout=PING_TIMEOUT,
                max_queue=None,
                close_timeout=5
            ) as ws:
                logger.info("Connected to WebSocket.")

                # start heartbeat
                asyncio.create_task(heartbeat_logger(ws))

                # subscribe to channels
                subscribe_payload = {
                    "req_id": "ws_bybit_combined",
                    "op": "subscribe",
                    "args": [f"publicTrade.{symbol}" for symbol in TRADES_SYMBOLS]
                           + [f"kline.1.{symbol}" for symbol in KLINES_SYMBOLS]
                }
                await ws.send(json.dumps(subscribe_payload))
                logger.info(f"📨 Sent subscription: {subscribe_payload}")

                # producer loop, no heavy processing here
                async for message in ws:
                    await message_queue.put(message)

        except (websockets.ConnectionClosed, websockets.WebSocketException) as e:
            logger.error(f"⚠ WebSocket error: {e}. Retrying soon...")
            await asyncio.sleep(0.01)


async def process_klines():
    """Connecting and putting data into db"""
    logger.info("process_klines started")
    async with aiosqlite.connect("/root/codes/py/db/bybit_klines.db") as conn:    
        try :    
            while True :
                klines = await klines_queue.get()
                logger.info(f'klines from process_klines : {klines}')
                
                # inserting data if data is not confirmed (open candle data) 
                if not klines[0][-2] :
                    timestamp = datetime.datetime.now(datetime.timezone.utc).timestamp()
                    query = """ 
                            INSERT INTO bybit_klines (symbol, opentime, closetime, interval, openprice, closeprice, highprice, lowprice, volume, quotevolume, confirm, timestamp)
                            VALUES(?,?,?,?,?,?,?,?,?,?,?,?)
                            """
                    await conn.executemany(query, klines)
                    await conn.commit()
                    logger.info(f'successfully saved open klines : {klines}')

                # updating data if data is confirmed (close candle data) 
                else :
                    query = """
                            UPDATE bybit_klines
                            SET closeprice = (case 
                            when `symbol` = 'BTCUSDT' then ?
                            when `symbol` = 'ETHUSDT' then ?
                            when `symbol` = 'SOLUSDT' then ? 
                            when `symbol` = 'SUIUSDT' then ? end), 

                            highprice = (case 
                            when `symbol` = 'BTCUSDT' then ?
                            when `symbol` = 'ETHUSDT' then ? 
                            when `symbol` = 'SOLUSDT' then ? 
                            when `symbol` = 'SUIUSDT' then ? end), 

                            lowprice = (case 
                            when `symbol` = 'BTCUSDT' then ?
                            when `symbol` = 'ETHUSDT' then ? 
                            when `symbol` = 'SOLUSDT' then ? 
                            when `symbol` = 'SUIUSDT' then ? end), 

                            volume = (case 
                            when `symbol` = 'BTCUSDT' then ?
                            when `symbol` = 'ETHUSDT' then ? 
                            when `symbol` = 'SOLUSDT' then ? 
                            when `symbol` = 'SUIUSDT' then ? end),

                            quotevolume = (case 
                            when `symbol` = 'BTCUSDT' then ?
                            when `symbol` = 'ETHUSDT' then ? 
                            when `symbol` = 'SOLUSDT' then ? 
                            when `symbol` = 'SUIUSDT' then ? end),

                            confirm = (case 
                            when `symbol` = 'BTCUSDT' then ?
                            when `symbol` = 'ETHUSDT' then ? 
                            when `symbol` = 'SOLUSDT' then ? 
                            when `symbol` = 'SUIUSDT' then ? end),

                            timestamp = (case 
                            when `symbol` = 'BTCUSDT' then ?
                            when `symbol` = 'ETHUSDT' then ? 
                            when `symbol` = 'SOLUSDT' then ? 
                            when `symbol` = 'SUIUSDT' then ? end)

                            WHERE `opentime` = ?;
                            """
                    timestamp = datetime.datetime.now(datetime.timezone.utc).timestamp()
                    df = pl.DataFrame(np.array(klines)).sort(by = 'column_0',descending = False)
                    opentime = min(df['column_1'].to_list())
                    closeprice = df['column_5'].to_list()
                    highprice = df['column_6'].to_list()
                    lowprice = df['column_7'].to_list()
                    volume = df['column_8'].to_list()
                    quotevolume = df['column_9'].to_list()
                    confirm = df['column_10'].to_list()
                    timestamp_col = df['column_11'].to_list()

                    klines = [
                        (
                            closeprice[0], closeprice[1], closeprice[2], closeprice[3],  # closeprice for BTCUSDT, ETHUSDT, SOLUSDT, SUIUSDT
                            highprice[0], highprice[1], highprice[2], highprice[3],      # highprice for BTCUSDT, ETHUSDT, SOLUSDT, SUIUSDT
                            lowprice[0], lowprice[1], lowprice[2], lowprice[3],          # lowprice for BTCUSDT, ETHUSDT, SOLUSDT, SUIUSDT
                            volume[0], volume[1], volume[2], volume[3],                 # volume for BTCUSDT, ETHUSDT, SOLUSDT, SUIUSDT
                            quotevolume[0], quotevolume[1], quotevolume[2], quotevolume[3],  # quotevolume for BTCUSDT, ETHUSDT, SOLUSDT, SUIUSDT
                            confirm[0], confirm[1], confirm[2], confirm[3],             # confirm for BTCUSDT, ETHUSDT, SOLUSDT, SUIUSDT
                            timestamp_col[0], timestamp_col[1], timestamp_col[2], timestamp_col[3],  # timestamp for BTCUSDT, ETHUSDT, SOLUSDT, SUIUSDT
                            opentime
                            )
                    ]
                    await conn.executemany(query, klines)
                    await conn.commit()
                    logger.info(f'successfully saved close klines : {klines}')
                logger.info(f"Total klines saved: {len(klines)}, time elapsed for saving data {(datetime.datetime.now(datetime.timezone.utc).timestamp() - timestamp)}, sleep for {(int(timestamp) + 60) -  datetime.datetime.now(datetime.timezone.utc).timestamp()} seconds")
                await asyncio.sleep(0.001)
        except Exception as e:
            logger.error(f"Error saving klines: {e}")

        finally:
            await conn.close()


async def consume_trades():
    """Consume messages from the queue and add them to the trade list."""
    logger.info(f'consume_trades started')
    while True:
        try:
            # Wait for a trade with a timeout of 1 millisecond
            trade = await asyncio.wait_for(trades_queue.get(), timeout=0.001)
            async with trade_list_lock:  # acquire lock
                trades_list.extend(trade)  # add new trades to the list
                logger.info(f"Fetched trades: {len(trade)}")
                
            trades_queue.task_done()  # mark the task as done
        except asyncio.TimeoutError:
            # if no trade arrives within the timeout, continue to the next iteration
            pass

        # sleep for a short time to avoid busy-waiting
        await asyncio.sleep(0.001)  #          



async def process_trades(db_pool):
    """Process trades using shared connection pool by pushing it into db"""
    logger.info('process_trades started')
    try:
        while True:
            async with trade_list_lock:
                if trades_list:
                    try:
                        query = """INSERT INTO bybit_trades 
                                  (current_timestamp, timestamp, symbol, side, volume, price, direction, transaction_id)
                                  VALUES(?,?,?,?,?,?,?,?)"""
                        await db_pool.executemany(query, trades_list)
                        await db_pool.commit()
                        logger.info(f"Saved {len(trades_list)} trades")
                        trades_list.clear()
                    except Exception as e:
                        logger.error(f"Error saving trades: {e}")
                        # Optional: Add a small delay after error to prevent tight error loops
                        await asyncio.sleep(1)

            await asyncio.sleep(0.1)  # Reduced frequency
            
    except asyncio.CancelledError:
        logger.info("process_trades task cancelled")
        raise  # Re-raise to allow proper task cancellation
        
    except Exception as e:
        logger.error(f"Unexpected error in process_trades: {e}")
        # Consider whether to re-raise or continue based on your needs
        
    finally:
        # Clean up resources if needed
        logger.info("process_trades shutting down")


async def delete_trades(db_pool):
    """
    Delete old trades in batches. Trades data are granular and will build up lots of memory usage in the future
    So need to delete the first 2.5 hours data
    """
    logger.info('delete_trades started')
    while True:
        try:
            async with db_pool.execute("SELECT max(timestamp), min(timestamp) FROM bybit_trades") as cursor:
                row = await cursor.fetchone()
                max_timestamp, min_timestamp = row if row else (None, None)

            if max_timestamp and min_timestamp and (max_timestamp - min_timestamp) >= 9000000:
                async with db_pool.execute(
                    "DELETE FROM bybit_trades WHERE timestamp < ?",
                    (max_timestamp - 9000000,)
                ):
                    await db_pool.commit()
                    logger.info('Cleared old trades from db')

        except Exception as e:
            logger.error(f"Error clearing trades: {e}")
            await asyncio.sleep(10)  # Wait before retrying

        await asyncio.sleep(1800)  # Run every 30 minutes

def handle_exit_signal(signum, frame):
    """Handle termination signals for graceful shutdown."""
    logger.info("Exiting...")
    # asyncio.create_task(flush_remaining_trades())
    asyncio.get_event_loop().stop()

async def main():
    """Main entry point with connection pool"""
    db_pool = await create_db_pool()
    
    tasks = [
        asyncio.create_task(websocket_handler()),
        asyncio.create_task(consume_trades()),
        asyncio.create_task(process_trades(db_pool)),
        asyncio.create_task(process_klines()),
        asyncio.create_task(delete_trades(db_pool))
    ]
    
    try:
        await asyncio.gather(*tasks)
    finally:
        await db_pool.close()

if __name__ == "__main__":
    logger.info(f'Starting new Bybit\'s ws stream for trades : {TRADES_SYMBOLS} and klines for : {KLINES_SYMBOLS}')
    signal.signal(signal.SIGINT, handle_exit_signal)  # Handle Ctrl+C
    signal.signal(signal.SIGTERM, handle_exit_signal)  # Handle termination signal
    try :
        asyncio.run(main())
    
    except Exception as e:
        logger.info(f"Unexpected error: {e}")
        handle_exit_signal(None, None)  # Ensure graceful cleanup 

        