import asyncio
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.orm import sessionmaker
import sys
import os

sys.path.append(os.path.join(os.getcwd(), "backend"))
import core.config
from services.domain_enrichment import run_enrichment_batch

async def run():
    engine = create_async_engine(core.config.DATABASE_URL)
    async_session = sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
    
    async with async_session() as session:
        print("🚀 Starting Domain Enrichment (Official API)...")
        res = await run_enrichment_batch(session)
        print(f"✅ Result: {res}")
    
    await engine.dispose()

if __name__ == "__main__":
    asyncio.run(run())
