import asyncio

import ultima_scraper_api

from ultima_scraper_collection.config import UltimaScraperCollectionConfig
from ultima_scraper_collection.managers.datascraper_manager.datascraper_manager import (
    DataScraperManager,
)
from ultima_scraper_collection.managers.server_manager import ServerManager


async def main():
    config = UltimaScraperCollectionConfig()
    server_manager = ServerManager(config.settings.databases[0].connection_info.dict())
    api = ultima_scraper_api.select_api("OnlyFans")
    _datascraper = DataScraperManager(server_manager, config).select_datascraper(api)
    pass


asyncio.run(main())
