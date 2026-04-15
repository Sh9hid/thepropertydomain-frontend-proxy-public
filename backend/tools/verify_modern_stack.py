import asyncio
import sys
from pathlib import Path

# Add backend to path
backend_dir = Path(__file__).resolve().parents[1]
sys.path.append(str(backend_dir))

from core.database import async_engine, get_redis
from sqlmodel import select, text

async def verify():
    print("--- Verifying Modern Stack ---")
    
    # 1. Verify Postgres
    print("Testing PostgreSQL connection...")
    try:
        async with async_engine.connect() as conn:
            result = await conn.execute(text("SELECT version();"))
            version = result.scalar()
            print(f"  [OK] PostgreSQL connected: {version}")
            
            # Check PostGIS
            try:
                result = await conn.execute(text("SELECT PostGIS_full_version();"))
                pg_version = result.scalar()
                print(f"  [OK] PostGIS is active: {pg_version}")
            except Exception as e:
                print(f"  [WARN] PostGIS not detected: {e}")
    except Exception as e:
        print(f"  [FAIL] PostgreSQL connection failed: {e}")

    # 2. Verify Redis
    print("Testing Redis connection...")
    try:
        r = await get_redis()
        await r.set("test_connection", "alive", ex=10)
        val = await r.get("test_connection")
        if val == "alive":
            print("  [OK] Redis connected and functional.")
        else:
            print(f"  [FAIL] Redis returned unexpected value: {val}")
    except Exception as e:
        print(f"  [FAIL] Redis connection failed: {e}")

    print("--- Verification Complete ---")

if __name__ == "__main__":
    asyncio.run(verify())
