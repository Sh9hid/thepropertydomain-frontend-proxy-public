import asyncio
from scraper import scrape_nsw_probate_market

async def run_once():
    await scrape_nsw_probate_market()

if __name__ == "__main__":
    asyncio.run(run_once())
