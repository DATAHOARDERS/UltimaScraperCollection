from ultima_scraper_collection.config import UltimaScraperCollectionConfig
from ultima_scraper_collection.projects.project_manager import Project
from ultima_scraper_db.databases.ultima_archive import merged_metadata
from ultima_scraper_db.databases.ultima_archive.api.client import UAClient
from ultima_scraper_db.databases.ultima_archive.database_api import ArchiveAPI
from ultima_scraper_db.managers.database_manager import Alembica


class UltimaArchiveProject(Project):
    async def init(self, config: UltimaScraperCollectionConfig):
        # We could pass a database manager instead of config
        db_info = config.settings.databases[0].connection_info.dict()
        ultima_archive_db = await super()._init_db(db_info, Alembica(), merged_metadata)
        self.ultima_archive_db_api = await ArchiveAPI(ultima_archive_db).init()
        self.fast_api = UAClient(self.ultima_archive_db_api)
        UAClient.database_api = self.ultima_archive_db_api
        UAClient.config = config
        return self
