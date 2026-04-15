import asyncio

from sold_scraper import scrape_configured_feeds


if __name__ == "__main__":
    asyncio.run(scrape_configured_feeds())
