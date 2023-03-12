from ultima_scraper_collection.managers.datascraper_manager.datascrapers.fansly import (
    FanslyDataScraper,
)
from ultima_scraper_collection.managers.datascraper_manager.datascrapers.onlyfans import (
    OnlyFansDataScraper,
)

datascraper_types = OnlyFansDataScraper | FanslyDataScraper
