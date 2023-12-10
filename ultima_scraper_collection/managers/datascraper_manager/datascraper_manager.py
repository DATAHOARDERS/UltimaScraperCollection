import ultima_scraper_api
from ultima_scraper_api import SUPPORTED_SITES
from ultima_scraper_api.apis.onlyfans import onlyfans
from ultima_scraper_collection import datascraper_types
from ultima_scraper_collection.config import UltimaScraperCollectionConfig
from ultima_scraper_collection.managers.datascraper_manager.datascrapers.fansly import (
    FanslyDataScraper,
)
from ultima_scraper_collection.managers.datascraper_manager.datascrapers.onlyfans import (
    OnlyFansDataScraper,
)
from ultima_scraper_collection.managers.option_manager import OptionManager
from ultima_scraper_collection.managers.server_manager import ServerManager


class DataScraperManager:
    def __init__(
        self, server_manager: ServerManager, config: UltimaScraperCollectionConfig
    ) -> None:
        self.datascrapers: dict[str, datascraper_types] = {}
        self.server_manager: ServerManager = server_manager
        self.config = config
        for site_name in SUPPORTED_SITES:
            datascraper = self.add_datascraper(
                ultima_scraper_api.select_api(site_name, config),
                OptionManager(),
                self.server_manager,
            )
            datascraper.filesystem_manager.activate_directory_manager(
                self.get_site_config(site_name)
            )

    def get_site_config(self, name: str):
        return getattr(self.config.site_apis, name.lower())

    def find_datascraper(
        self,
        site_name: str,
    ):
        return self.datascrapers.get(site_name.lower())

    def select_datascraper(
        self,
        site_name: str,
    ):
        return self.datascrapers.get(site_name.lower())

    def add_datascraper(
        self,
        api: ultima_scraper_api.api_types,
        option_manager: OptionManager,
        server_manager: ServerManager,
    ):
        site_settings = self.get_site_config(api.site_name)
        if isinstance(api, onlyfans.OnlyFansAPI):
            datascraper = OnlyFansDataScraper(
                api, option_manager, server_manager, site_settings
            )
        else:
            datascraper = FanslyDataScraper(
                api, option_manager, server_manager, site_settings
            )
        self.datascrapers[api.site_name.lower()] = datascraper
        return datascraper
