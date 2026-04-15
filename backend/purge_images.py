import asyncio
from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine
import sys
import os

sys.path.append(os.path.join(os.getcwd(), "backend"))
import core.config

async def main():
    engine = create_async_engine(core.config.DATABASE_URL)
    async with engine.begin() as conn:
        await conn.execute(text("UPDATE leads SET main_image = NULL WHERE main_image LIKE '/streetview_images/%'"))
        print("✅ Database purged of streetview paths.")
    await engine.dispose()

if __name__ == "__main__":
    asyncio.run(main())
