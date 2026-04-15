
import asyncio
from sqlalchemy import text
from core.database import async_engine, init_sqlite_migrations

async def reproduce():
    # Try to initialize DB
    await init_sqlite_migrations()
    
    async with async_engine.connect() as conn:
        try:
            await conn.execute(text("SELECT * FROM notes LIMIT 1"))
            print("Table 'notes' exists")
        except Exception as e:
            print(f"Table 'notes' MISSING: {e}")
            
        try:
            await conn.execute(text("SELECT * FROM \"intelligence.event\" LIMIT 1"))
            print("Table 'intelligence.event' exists")
        except Exception as e:
            # Maybe it's just 'event'?
            try:
                await conn.execute(text("SELECT * FROM event LIMIT 1"))
                print("Table 'event' exists (without schema prefix)")
            except:
                print(f"Table 'intelligence.event' MISSING: {e}")

if __name__ == "__main__":
    asyncio.run(reproduce())
