import asyncio
from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine
import sys
import os

# Add backend to sys.path to load config
sys.path.append(os.path.join(os.getcwd(), "backend"))
import core.config

async def main():
    engine = create_async_engine(core.config.DATABASE_URL)
    async with engine.connect() as conn:
        total = (await conn.execute(text("SELECT COUNT(*) FROM leads"))).scalar()
        done_res = await conn.execute(text("SELECT id, address, owner_name, main_image FROM leads WHERE main_image LIKE '/streetview_images/%' LIMIT 1"))
        example = done_res.mappings().first()
        done_count = (await conn.execute(text("SELECT COUNT(*) FROM leads WHERE main_image LIKE '/streetview_images/%'"))).scalar()
        
        print(f"TOTAL_LEADS:{total}")
        print(f"DONE_LEADS:{done_count}")
        if example:
            print(f"EXAMPLE_ID:{example['id']}")
            print(f"EXAMPLE_ADDRESS:{example['address']}")
            print(f"EXAMPLE_IMAGE_PATH:{example['main_image']}")
    await engine.dispose()

if __name__ == "__main__":
    asyncio.run(main())
