import asyncio
from sqlalchemy import create_engine, MetaData, Table, Column, Integer, String, select
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, create_async_engine
from sqlalchemy.sql import text
from sqlalchemy.orm import sessionmaker
from sqlalchemy.types import Boolean

# Set up the database connection
DATABASE_URL = "sqlite+aiosqlite:////root/codes/py/db/bybit_klines.db"

# engine = create_engine(DATABASE_URL, echo=True)
engine = create_async_engine(DATABASE_URL, echo=True)
SessionLocal = sessionmaker(bind=engine, expire_on_commit=False, class_=AsyncSession)

# Initialize metadata
metadata = MetaData()

bybit_klines = Table('bybit_klines', metadata,
               Column('id', Integer, primary_key=True, autoincrement=True, nullable=False),
               Column('symbol', String, nullable=False),
               Column('opentime', Integer, nullable=False),
               Column('closetime', Integer, nullable=False),
               Column('interval', String, nullable=False),
               Column('openprice', String, nullable=False),
               Column('closeprice', String, nullable=False),
               Column('highprice', String, nullable=False),
               Column('lowprice', String, nullable=False),
               Column('volume', String, nullable=False),
               Column('quotevolume', String, nullable=False),
               Column('confirm', String, nullable=False),
               Column('timestamp', Integer, nullable=False)
              )

"""
columns :
['id', 'symbol', 'opentime', 'closetime', 'interval', 'openprice',
'closeprice', 'highprice', 'lowprice', 'volume',
'quotevolume', 'confirm', 'timestamp']
"""

bybit_agg_klines = Table('bybit_agg_klines', metadata,
                   Column('id', Integer, primary_key=True, autoincrement=True, nullable=False),
                   Column('current_timestamp', Integer, nullable=False),
                   Column('opentime', Integer, nullable=False),
                   Column('openprice_BTCUSDT', String, nullable=False),
                   Column('normalized_24h_openprice_BTCUSDT', String, nullable=False),
                   Column('normalized_1h_openprice_BTCUSDT', String, nullable=False),
                   Column('openprice_SUIUSDT', String, nullable=False),
                   Column('normalized_24h_openprice_SUIUSDT', String, nullable=False),
                   Column('normalized_1h_openprice_SUIUSDT', String, nullable=False),
                   Column('normalized_spread_24h', String, nullable=False),
                   Column('normalized_spread_1h', String, nullable=False),
                   Column('abs_normalized_spread_24h', String, nullable=False),
                   Column('abs_normalized_spread_1h', String, nullable=False)
                  )
"""
columns :
['id', 'current_timestamp', 'opentime', 'openprice_BTCUSDT', 'normalized_24h_openprice_BTCUSDT', 'normalized_1h_openprice_BTCUSDT',
'openprice_SUIUSDT', 'normalized_24h_openprice_SUIUSDT', 'normalized_1h_openprice_SUIUSDT', 'normalized_spread_24h',
'normalized_spread_1h', 'abs_normalized_spread_24h', 'abs_normalized_spread_1h']
"""

bybit_positions = Table('bybit_positions', metadata,
                        Column('id', Integer, primary_key=True, autoincrement=True, nullable=False),
                        Column('order_id', String, nullable=True),
                        Column('close_order_id', String, nullable=True),
                        Column('symbol', String, nullable=False),
                        Column('side', String, nullable=False),
                        Column('opentime', Integer, nullable=False),
                        Column('openprice', String, nullable=False),
                        Column('order_qty', String, nullable=False),
                        Column('actual_close_time', String, nullable=True),
                        Column('actual_close_price', String, nullable=True),
                        Column('actual_pnl', String, nullable=True),
                        Column('filled', String, nullable=True),
                        Column('normalized_1h_openprice_BTCUSDT', String, nullable=False),
                        Column('normalized_1h_openprice_SUIUSDT', String, nullable=False),
                        Column('normalized_24h_openprice_BTCUSDT', String, nullable=False),
                        Column('normalized_24h_openprice_SUIUSDT', String, nullable=False)
                        )



async def create_tables():
    async with engine.begin() as conn:
        # Use run_sync to execute metadata.create_all in a sync context
        await conn.run_sync(metadata.create_all)


if __name__ == "__main__":
    asyncio.run(create_tables())
