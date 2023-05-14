import asyncio

import ultima_scraper_api

from ultima_scraper_collection.managers.datascraper_manager.datascraper_manager import (
    DataScraperManager,
)


async def main():
    api = ultima_scraper_api.select_api("OnlyFans")
    _datascraper = DataScraperManager().select_datascraper(api)
    pass


asyncio.run(main())
