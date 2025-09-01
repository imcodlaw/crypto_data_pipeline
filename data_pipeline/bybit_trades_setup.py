import asyncio
from sqlalchemy import create_engine, MetaData, Table, Column, Integer, String, select
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, create_async_engine
from sqlalchemy.sql import text
from sqlalchemy.orm import sessionmaker
from sqlalchemy.types import Boolean

# Set up the database connection
DATABASE_URL = "sqlite+aiosqlite:////root/codes/py/db/bybit_trades.db"

# engine = create_engine(DATABASE_URL, echo=True)
engine = create_async_engine(DATABASE_URL, echo=True)
SessionLocal = sessionmaker(bind=engine, expire_on_commit=False, class_=AsyncSession)

# Initialize metadata
metadata = MetaData()

bybit_trades = Table('bybit_trades', metadata,
                     Column('id', Integer, primary_key=True, autoincrement=True, nullable=False),
                     Column('current_timestamp', Integer, nullable=False),
                     Column('timestamp', Integer, nullable=False),
                     Column('symbol', String, nullable=False),
                     Column('side', String, nullable=False),
                     Column('volume', String, nullable=False),
                     Column('price', String, nullable=False),
                     Column('direction', String, nullable=False),
                     Column('transaction_id', String, nullable=False)
                     )



async def create_tables():
    async with engine.begin() as conn:
        # Use run_sync to execute metadata.create_all in a sync context
        await conn.run_sync(metadata.create_all)


if __name__ == "__main__":
    asyncio.run(create_tables())

