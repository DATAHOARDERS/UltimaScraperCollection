from pathlib import Path

from ultima_scraper_collection.managers.database_manager.connections.sqlite.sqlite_database import (
    SqliteDatabase,
)


class DatabaseManager:
    def __init__(self) -> None:
        self.active_db: SqliteDatabase | None = None

    def get_sqlite_db(self, path: Path, legacy: bool = False):
        sqlite_db = SqliteDatabase().init_db(path, legacy)
        self.active_db = sqlite_db
        return sqlite_db
