import shutil
from pathlib import Path
from typing import Any

import ultima_scraper_api
from sqlalchemy import inspect
from ultima_scraper_api.classes.prepare_metadata import legacy_metadata_fixer
from ultima_scraper_renamer.reformat import prepare_reformat

from ultima_scraper_collection.managers.database_manager.connections.sqlite.sqlite_database import (
    DBCollection,
    SqliteDatabase,
)
from ultima_scraper_collection.managers.database_manager.database_manager import (
    DatabaseManager,
)
from ultima_scraper_collection.managers.filesystem_manager import (
    DirectoryManager,
    FilesystemManager,
)

api_types = ultima_scraper_api.api_types
user_types = ultima_scraper_api.user_types


class MetadataManager:
    def __init__(
        self,
        subscription: user_types,
        filesystem_manager: FilesystemManager,
        db_manager: SqliteDatabase | None = None,
    ) -> None:
        self.subscription = subscription
        self.filesystem_manager = filesystem_manager
        self.db_manager = db_manager
        self.metadatas = self.find_metadatas()
        self.redundant_metadatas: list[Path] = []

    def find_metadatas(self):
        found_metadatas: list[Path] = []
        for file_ in self.filesystem_manager.get_file_manager(
            self.subscription.id
        ).files:
            if file_.suffix in [".db", ".json"]:
                found_metadatas.append(file_)
        return found_metadatas

    async def process_metadata(
        self,
        legacy_metadata_path: Path,
        new_metadata_object: list[dict[str, Any]],
        api_type: str,
        subscription: user_types,
        delete_metadatas: list[Path],
    ):
        if not self.db_manager:
            raise Exception("DatabaseManager has not been assigned")

        api = subscription.get_api()
        site_settings = api.get_site_settings()
        config = api.config
        if not (config and site_settings):
            return
        # We could put this in process_legacy_metadata
        legacy_db_manager = DatabaseManager().get_sqlite_db(
            legacy_metadata_path, legacy=True
        )

        final_result, delete_metadatas = legacy_db_manager.legacy_sqlite_updater(
            api_type, subscription, delete_metadatas
        )
        new_metadata_object.extend(final_result)
        return new_metadata_object

    def delete_metadatas(self):
        for legacy_metadata in self.redundant_metadatas:
            if self.subscription.get_api().get_site_settings().delete_legacy_metadata:
                legacy_metadata.unlink()
            else:
                if legacy_metadata.exists():
                    new_filepath = Path(
                        legacy_metadata.parent,
                        "__legacy_metadata__",
                        legacy_metadata.name,
                    )
                    new_filepath.parent.mkdir(exist_ok=True)
                    legacy_metadata.rename(new_filepath)

    async def fix_archived_db(
        self,
    ):
        api = self.subscription.get_api()
        directory_manager = self.filesystem_manager.get_directory_manager(
            self.subscription.id
        )
        for final_metadata in directory_manager.user.legacy_metadata_directories:
            archived_database_path = final_metadata.joinpath("Archived.db")
            if archived_database_path.exists():
                archived_db_manager = DatabaseManager().get_sqlite_db(
                    archived_database_path, legacy=True
                )
                if not archived_db_manager.session.bind:
                    continue
                for api_type, _value in api.ContentTypes():
                    database_path = final_metadata.joinpath(f"{api_type}.db")
                    legacy_db_manager = DatabaseManager().get_sqlite_db(
                        database_path, legacy=True
                    )
                    database_name = api_type.lower()
                    result: bool = inspect(
                        archived_db_manager.session.bind.engine
                    ).has_table(database_name)
                    if result:
                        archived_db_manager.alembic_directory = (
                            archived_db_manager.alembic_directory.parent.with_name(
                                database_name
                            ).joinpath("alembic")
                        )
                        archived_db_manager.run_migrations(legacy=True)
                        legacy_db_manager.run_migrations(legacy=True)

                        db_manager = DatabaseManager().get_sqlite_db(database_path)
                        modern_database_session = db_manager.session
                        db_collection = DBCollection()
                        database = db_collection.database_picker("user_data")
                        if not database:
                            return
                        table_name = database.table_picker(api_type, True)
                        if not table_name:
                            return
                        archived_result = archived_db_manager.session.query(
                            table_name
                        ).all()
                        for item in archived_result:
                            result2 = (
                                modern_database_session.query(table_name)
                                .filter(table_name.post_id == item.post_id)
                                .first()
                            )
                            if not result2:
                                item2 = item.__dict__
                                item2.pop("id")
                                item2.pop("_sa_instance_state")
                                item = table_name(**item2)
                                item.archived = True
                                modern_database_session.add(item)
                            else:
                                result2.archived = True
                        modern_database_session.commit()
                        modern_database_session.close()
                archived_db_manager.session.commit()
                archived_db_manager.session.close()
                new_filepath = Path(
                    archived_database_path.parent,
                    "__legacy_metadata__",
                    archived_database_path.name,
                )
                new_filepath.parent.mkdir(exist_ok=True)
                archived_database_path.rename(new_filepath)

    async def process_legacy_metadata(
        self,
        user: user_types,
        api_type: str,
        metadata_filepath: Path,
        directory_manager: DirectoryManager,
    ):
        p_r = prepare_reformat()
        file_manager = self.filesystem_manager.get_file_manager(user.id)
        old_metadata_filepaths = await p_r.find_metadata_files(
            file_manager.files, legacy_files=False
        )
        for old_metadata_filepath in old_metadata_filepaths:
            new_m_f = directory_manager.user.metadata_directory.joinpath(
                old_metadata_filepath.name
            )
            if old_metadata_filepath.exists() and not new_m_f.exists():
                shutil.move(old_metadata_filepath, new_m_f)
        old_metadata_filepaths = await p_r.find_metadata_files(
            file_manager.files, legacy_files=False
        )
        legacy_metadata_filepaths = [
            x for x in old_metadata_filepaths if x.stem.find(api_type) == 0
        ]
        legacy_metadata_object, delete_legacy_metadatas = await legacy_metadata_fixer(
            metadata_filepath, legacy_metadata_filepaths
        )
        delete_metadatas: list[Path] = []
        if delete_legacy_metadatas:
            print("Merging new metadata with legacy metadata.")
            delete_metadatas.extend(delete_legacy_metadatas)
        final_set: list[dict[str, Any]] = []
        for _media_type, value in legacy_metadata_object.content:
            for _status, value2 in value:
                for value3 in value2:
                    item = value3.convert(keep_empty_items=True)
                    item["archived"] = False
                    item["api_type"] = api_type
                    final_set.append(item)
        print("Finished processing metadata.")
        return final_set, delete_metadatas

    def export(self, api_type: str, datas: list[dict[str, Any]]):
        if api_type == "Posts":
            self.db_manager
        pass
