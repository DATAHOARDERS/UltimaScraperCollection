import ultima_scraper_api
from ultima_scraper_api.apis.onlyfans import onlyfans

from ultima_scraper_collection import datascraper_types
from ultima_scraper_collection.managers.datascraper_manager.datascrapers.fansly import (
    FanslyDataScraper,
)
from ultima_scraper_collection.managers.datascraper_manager.datascrapers.onlyfans import (
    OnlyFansDataScraper,
)
from ultima_scraper_collection.managers.option_manager import OptionManager


class DataScraperManager:
    def __init__(self) -> None:
        self.active_datascraper = None
        self.datascrapers: list[datascraper_types] = []

    def select_datascraper(
        self, api: ultima_scraper_api.api_types, option_manager: OptionManager=OptionManager()
    ):
        self.active_datascraper = self.get_datascraper(api)
        if not self.active_datascraper:
            self.active_datascraper = self.add_datascraper(api, option_manager)
        return self.active_datascraper

    def add_datascraper(
        self, api: ultima_scraper_api.api_types, option_manager: OptionManager
    ):
        if isinstance(api, onlyfans.OnlyFansAPI):
            datascraper = OnlyFansDataScraper(api, option_manager)
        else:
            datascraper = FanslyDataScraper(api, option_manager)
        self.datascrapers.append(datascraper)
        return datascraper

    def get_datascraper(self, api: ultima_scraper_api.api_types):
        for datascraper in self.datascrapers:
            if datascraper.api.site_name == api.site_name:
                return datascraper
        return None
