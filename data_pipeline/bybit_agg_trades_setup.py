import asyncio
from sqlalchemy import create_engine, MetaData, Table, Column, Integer, String, select
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, create_async_engine
from sqlalchemy.sql import text
from sqlalchemy.orm import sessionmaker
from sqlalchemy.types import Boolean

# Set up the database connection
DATABASE_URL = "sqlite+aiosqlite:////root/codes/py/db/bybit_agg_trades.db"

# engine = create_engine(DATABASE_URL, echo=True)
engine = create_async_engine(DATABASE_URL, echo=True)
SessionLocal = sessionmaker(bind=engine, expire_on_commit=False, class_=AsyncSession)

# Initialize metadata
metadata = MetaData()

bybit_minutely_agg_trades = Table('bybit_minutely_agg_trades', metadata,
                                  Column('id', Integer, primary_key=True, autoincrement=True, nullable=False),
                                  Column('current_timestamp', Integer, nullable=False),
                                  Column('timestamp', Integer, nullable=False),
                                  Column('symbol', String, nullable=False),
                                  Column('openprice', String, nullable=False),
                                  Column('highprice', String, nullable=False),
                                  Column('lowprice', String, nullable=False),
                                  Column('closeprice', String, nullable=False),
                                  Column('quotevolume', String, nullable=False),
                                  Column('buyer_maker_quotevolume', String, nullable=True, default= '0'),
                                  Column('buyer_taker_quotevolume', String, nullable=True, default= '0'),
                                  Column('trades', Integer, nullable=False)
                                  )

bybit_minutely_features = Table('bybit_minutely_features', metadata,
                                Column('id', Integer, primary_key=True, autoincrement=True, nullable=False),
                                Column('current_timestamp', String, nullable=False),
                                Column('timestamp', String, nullable=False),
                                Column('symbol', String, nullable=False),
                                Column('normalized_24h_quotevolume', String, nullable=True),
                                Column('normalized_1h_quotevolume', String, nullable=True),
                                Column('normalized_24h_buyer_maker_quotevolume', String, nullable=True),
                                Column('normalized_1h_buyer_maker_quotevolume', String, nullable=True),
                                Column('normalized_24h_buyer_taker_quotevolume', String, nullable=True),
                                Column('normalized_1h_buyer_taker_quotevolume', String, nullable=True),
                                Column('normalized_spread_24h_maker_taker', String, nullable=True),
                                Column('normalized_spread_1h_maker_taker', String, nullable=True),
                                Column('abs_normalized_spread_24h_maker_taker', String, nullable=True),
                                Column('abs_normalized_spread_1h_maker_taker', String, nullable=True)
                                )


async def create_tables():
    async with engine.begin() as conn:
        # Use run_sync to execute metadata.create_all in a sync context
        await conn.run_sync(metadata.create_all)


if __name__ == "__main__":
    asyncio.run(create_tables())