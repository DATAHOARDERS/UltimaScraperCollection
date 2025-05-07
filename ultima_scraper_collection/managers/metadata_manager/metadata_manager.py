import mimetypes
import re
from datetime import datetime
from pathlib import Path
from typing import Any, TypedDict
from urllib.parse import ParseResult, urlparse

import ultima_scraper_api
from sqlalchemy import inspect
from ultima_scraper_api.apis.onlyfans import preview_url_picker, url_picker
from ultima_scraper_api.apis.onlyfans.classes.mass_message_model import MassMessageModel
from ultima_scraper_api.helpers import main_helper
from ultima_scraper_collection.managers.content_manager import ContentManager
from ultima_scraper_collection.managers.database_manager.connections.sqlite.sqlite_database import (
    DBCollection,
    SqliteDatabase,
)
from ultima_scraper_collection.managers.database_manager.database_manager import (
    DatabaseManager,
)
from ultima_scraper_collection.managers.filesystem_manager import FilesystemManager
from ultima_scraper_db.databases.ultima_archive.schemas.templates.site import (
    MediaModel as DBMediaModel,
)
from ultima_scraper_db.databases.ultima_archive.schemas.templates.site import (
    MessageModel as DBMessageModel,
)
from ultima_scraper_db.databases.ultima_archive.schemas.templates.site import (
    PostModel as DBPostModel,
)
from ultima_scraper_db.databases.ultima_archive.schemas.templates.site import (
    StoryModel as DBStoryModel,
)
from ultima_scraper_db.databases.ultima_archive.site_api import content_model_types

api_types = ultima_scraper_api.api_types
user_types = ultima_scraper_api.user_types
content_types = ultima_scraper_api.content_types
invalid_subdomains = ["us", "uk", "ca", "ca2", "de", "sg"]


class DBContentExtractor:
    def __init__(
        self, content_item: DBMessageModel | DBPostModel | DBStoryModel
    ) -> None:
        self.item = content_item
        self.__api__: api_types | None = None

    def get_id(self):
        return self.item.id

    def get_user_id(self):
        return self.item.user_id

    def get_queue_id(self):
        return self.item.queue_id

    def get_text(self):
        if isinstance(self.item, DBStoryModel):
            return None
        return self.item.text

    def get_preview_ids(self):
        value: list[int] = []
        return value

    def resolve_archived(self):
        return False

    def get_date(self):
        return self.item.created_at

    def get_medias(self, content_metadata: "ContentMetadata"):
        final_assets: list[MediaMetadata] = []
        for media_item in self.item.media:
            if not media_item.category:
                from urllib.parse import urlparse

                if not media_item.url:
                    continue
                parsed_url = urlparse(media_item.url)
                path_ = parsed_url.path
                type_, _encoding = mimetypes.guess_type(path_)
                media_type = None
                if type_:
                    media_type, _subtype = type_.split("/")
                assert media_type and self.__api__
                true_media_type = self.__api__.MediaTypes().find_by_value(media_type)
                pass
            else:
                true_media_type = media_item.category

            new_asset = MediaMetadata(
                media_item.id,
                true_media_type,
                content_manager=content_metadata.content_manager,
                content_metadata=content_metadata,
                created_at=content_metadata.created_at,
            )
            new_asset.urls = [media_item.url]
            filepath = media_item.find_filepath(
                content_metadata.content_id, content_metadata.api_type
            )
            if filepath:
                pathed_filepath = Path(filepath.filepath)
                new_asset.directory = pathed_filepath.parent
                new_asset.filename = pathed_filepath.name
            else:
                pass
            new_asset.size = media_item.size
            final_assets.append(new_asset)
        return final_assets

    def resolve_paid(self):
        if not hasattr(self.item, "paid"):
            return None
        if isinstance(self.item, DBPostModel | DBMessageModel):
            return self.item.paid

    def get_receiver_id(self):
        if isinstance(self.item, DBMessageModel):
            return self.item.receiver_id


class ApiExtractor:
    def __init__(self, item: content_types) -> None:
        self.item = item

    def get_id(self):
        return self.item.id

    def get_user_id(self):
        return self.item.get_author().id

    def get_queue_id(self):
        if isinstance(self.item, ultima_scraper_api.message_types):
            return self.item.queue_id

    def get_is_from_queue(self):
        if isinstance(self.item, ultima_scraper_api.message_types):
            return self.item.is_from_queue

    def get_text(self):
        if isinstance(self.item, ultima_scraper_api.post_types):
            final_text = self.item.rawText or self.item.text
        elif isinstance(self.item, ultima_scraper_api.message_types):
            final_text = self.item.text
        elif isinstance(self.item, MassMessageModel):
            final_text = self.item.raw_text or self.item.text
        else:
            final_text = ""
        return final_text

    def get_preview_ids(self):
        if isinstance(self.item, ultima_scraper_api.post_types):
            final_preview_ids = self.item.preview or self.item.preview_ids
        elif isinstance(self.item, ultima_scraper_api.message_types):
            final_preview_ids = self.item.previews
        elif isinstance(self.item, MassMessageModel):
            final_preview_ids = self.item.previews
        else:
            final_preview_ids = []
        return final_preview_ids

    def get_date(self):
        temp_date = self.item.created_at
        assert temp_date
        if isinstance(temp_date, str):
            try:
                temp_date = float(temp_date)
            except:
                pass
        default_date = datetime.now()
        if temp_date == "-001-11-30T00:00:00+00:00":
            date_object = default_date
        else:
            if not temp_date:
                temp_date = default_date
            if isinstance(temp_date, int):
                timestamp = float(temp_date)
                date_object = datetime.fromtimestamp(timestamp)
            elif isinstance(temp_date, float):
                date_object = datetime.fromtimestamp(temp_date)
            elif isinstance(temp_date, str):
                date_object = datetime.fromisoformat(temp_date)
            else:
                date_object = temp_date
        return date_object

    def get_medias(self, content_metadata: "ContentMetadata"):
        final_assets: list[MediaMetadata] = []
        for asset_metadata in self.item.media:
            raw_media_type = asset_metadata["type"]
            if "mimetype" in asset_metadata:
                raw_media_type: str = asset_metadata["mimetype"].split("/")[0]
            author = self.item.get_author()
            media_type = author.get_api().MediaTypes().find_by_value(raw_media_type)
            if "createdAt" in asset_metadata:
                if isinstance(asset_metadata["createdAt"], str):
                    media_created_at = datetime.fromisoformat(
                        asset_metadata["createdAt"]
                    )
                elif asset_metadata["createdAt"] == None:
                    media_created_at = self.item.created_at
                else:
                    media_created_at = datetime.fromtimestamp(
                        asset_metadata["createdAt"]
                    )
            else:
                media_created_at = content_metadata.created_at
            new_asset = MediaMetadata(
                asset_metadata["id"],
                media_type,
                content_manager=content_metadata.content_manager,
                content_metadata=content_metadata,
                created_at=media_created_at,
            )
            new_asset.author = author
            new_asset.user_id = author.id
            media_manager = content_metadata.content_manager.media_manager
            if new_asset.id:
                media_manager.medias[new_asset.id] = new_asset
            else:
                media_manager.invalid_medias.append(new_asset)
            main_url = self.item.url_picker(asset_metadata)
            preview_url = self.item.preview_url_picker(asset_metadata)
            authed = author.get_authed()
            if authed.drm:
                new_asset.drm = bool(authed.drm.has_drm(asset_metadata))
            new_asset.urls = []
            for url in [main_url, preview_url].copy():
                if url:
                    if url.hostname:
                        subdomain = url.hostname.split(".")[1]
                        if any(subdomain in nm for nm in invalid_subdomains):
                            subdomain = url.hostname.split(".")[1]
                            if "upload" in subdomain:
                                continue
                            if "convert" in subdomain:
                                continue
                    new_asset.urls.append(url.geturl())

            if new_asset.id and new_asset.id in content_metadata.preview_media_ids:
                new_asset.preview = True
            new_asset.__raw__ = asset_metadata
            final_assets.append(new_asset)
        return final_assets

    def resolve_archived(self):
        archived = False
        if isinstance(self.item, ultima_scraper_api.post_types):
            archived = self.item.isArchived
        return archived

    def resolve_paid(self):
        if hasattr(self.item, "price"):
            if self.item.price and self.item.price > 0:
                if all(media["canView"] for media in self.item.media):
                    return True
            return False
        else:
            return None

    def get_receiver_id(self):
        if isinstance(self.item, ultima_scraper_api.message_types):
            return self.item.get_receiver().id


class ContentMetadata:
    def __init__(
        self,
        content_id: int,
        api_type: str,
        content_manager: ContentManager,
    ) -> None:
        self.content_id = content_id
        self.user_id: int | None = None
        self.receiver_id: int | None = None
        self.queue_id: int | None = None
        self.text: str | None = None
        self.preview_media_ids: list[int] | list[dict[str, Any]] = []
        self.archived: bool = False
        self.medias: list[MediaMetadata] = []
        self.api_type = api_type
        self.price: float | None = None
        self.paid: bool | None = False
        self.deleted: bool = False
        self.is_from_queue: bool = False
        self.__raw__: Any | None = None
        self.__soft__: Any = None
        self.__db_content__: content_model_types | None = None
        self.__legacy__ = False
        self.content_manager = content_manager

    async def resolve_extractor(self, result: ApiExtractor | DBContentExtractor):
        self.content_id = result.get_id()
        self.user_id = result.get_user_id()
        self.receiver_id = result.get_receiver_id()
        self.queue_id = result.get_queue_id()
        self.is_from_queue = bool(result.get_is_from_queue())
        self.text = result.get_text()
        self.preview_media_ids = result.get_preview_ids()
        self.archived = result.resolve_archived()
        self.price = getattr(result.item, "price", 0) or 0
        self.paid = result.resolve_paid()
        self.deleted = False
        self.created_at: datetime = result.get_date()
        self.medias: list[MediaMetadata] = result.get_medias(self)
        self.__raw__: Any | None = None
        self.__soft__ = result.item
        self.__media_types__ = None

    def find_media(self, media_id: int | None = None, urls: list[str] = []):
        for asset in self.medias:
            if asset.id == media_id:
                return asset
        for asset in self.medias:
            for url in urls:
                url = urlparse(url)
                found_url = asset.find_by_url(url)
                if found_url:
                    return asset

    def find_media_metadata_by_id(self, media_id: int):
        for asset in self.medias:
            if asset.id == media_id:
                return asset

    def find_media_by_url(self, urls: list[str]):
        for asset in self.medias:
            for url in urls:
                url = urlparse(url)
                found_url = asset.find_by_url(url)
                if found_url:
                    return asset

    def get_mass_message_stat(self):
        if isinstance(self.__soft__, MassMessageModel):
            return self.__soft__.mass_message_stat


class MediaMetadata:
    def __init__(
        self,
        media_id: int,
        media_type: str,
        content_manager: ContentManager,
        content_metadata: ContentMetadata | None = None,
        urls: list[str] = [],
        preview: bool = False,
        created_at: datetime | None = None,
        drm: bool = False,
    ) -> None:
        self.id = int(media_id) if media_id is not None else None
        if content_metadata:
            self.user_id = content_metadata.user_id
        self.media_type = media_type
        self.urls: list[str] = urls
        self.preview = preview
        self.directory: Path | None = None
        self.filename: str | None = None
        self.size = 0
        self.linked = None
        self.drm = drm
        self.key: str = ""
        self.created_at = created_at
        self.__content_metadata__ = content_metadata
        self.__db_media__: DBMediaModel | None = None
        self.content_manager = content_manager
        self.drm_download_paths: list[Path] = []
        assert self.id
        self.content_manager.media_manager.medias[self.id] = self

    def get_author(self):
        return self.author

    def raw_extractor(self, author: user_types, raw_media: dict[str, Any]):
        self.author = author
        self.user_id = author.id
        assert self.id
        raw_media_type = raw_media["type"]
        if "mimetype" in raw_media:
            raw_media_type: str = raw_media["mimetype"].split("/")[0]
        self.media_type = author.get_api().MediaTypes().find_by_value(raw_media_type)
        self.drm = bool(author.get_authed().drm.has_drm(raw_media))
        main_url = self.url_picker(raw_media)
        preview_url = self.preview_url_picker(raw_media)
        self.urls = []
        for url in [main_url, preview_url].copy():
            if url:
                if url.hostname:
                    subdomain = url.hostname.split(".")[1]
                    if any(subdomain in nm for nm in invalid_subdomains):
                        subdomain = url.hostname.split(".")[1]
                        if "upload" in subdomain:
                            continue
                        if "convert" in subdomain:
                            continue
                self.urls.append(url.geturl())
        if (
            self.__content_metadata__
            and self.id in self.__content_metadata__.preview_media_ids
        ):
            self.preview = True

        if isinstance(raw_media["createdAt"], str):
            media_created_at = datetime.fromisoformat(raw_media["createdAt"])
        else:
            media_created_at = datetime.fromtimestamp(raw_media["createdAt"])
        self.created_at = media_created_at
        self.__raw__ = raw_media

    def url_picker(self, media_item: dict[str, Any], video_quality: str = ""):
        return url_picker(self.author, media_item, video_quality)

    def preview_url_picker(self, media_item: dict[str, Any]):
        return preview_url_picker(media_item)

    def find_by_url(self, url: ParseResult):
        for media_url in self.urls:
            media_url = urlparse(media_url)
            url_path = url.path
            if "Protected" in url.path:
                index = url.path.index("/files")
                url_path = url.path[index:]
            if media_url.path == url_path:
                return self

    def get_filepath(self):
        assert self.directory and self.filename
        return Path(self.directory, self.filename)

    def get_content_metadata(self):
        return self.__content_metadata__


class MetadataManager:
    def __init__(
        self,
        subscription: user_types,
        content_manager: ContentManager,
        filesystem_manager: FilesystemManager,
        db_manager: SqliteDatabase | None = None,
    ) -> None:
        self.subscription = subscription
        self.content_manager = content_manager
        self.filesystem_manager = filesystem_manager
        self.db_manager = db_manager
        self.metadatas = self.find_metadatas()
        self.update_metadata_filepaths()
        self.redundant_metadatas: list[Path] = []
        self.content_metadatas: list[ContentMetadata] = []
        self.legacy_content_metadatas: list[ContentMetadata] = []

    def merge_content_and_directories(self, unformatted_set: list[dict[str, Any]]):
        temp_content: set[ContentMetadata] = set()
        temp_directories: set[Path] = set()
        for item in unformatted_set:
            if item:
                temp_content.update(item["content"])
                temp_directories.update(item["directories"])
        final_content = list(temp_content)
        final_directories = list(temp_directories)
        final_content.sort(key=lambda obj: obj.content_id)
        return final_content, final_directories

    def find_metadatas(self):
        found_metadatas: list[Path] = []
        for file_ in self.filesystem_manager.get_file_manager(
            self.subscription.id
        ).files:
            if file_.suffix in [".db", ".json"]:
                found_metadatas.append(file_)
        return found_metadatas

    async def process_legacy_metadata(self):
        # Ignore every other metadata fixer, THIS is the one that will merge all legacy metadata (.json and .db) into user_data.
        self.fix_json()
        # Needs work
        # self.fix_sqlite()
        performer_file_manager = self.filesystem_manager.get_file_manager(
            self.subscription.id
        )
        performer_directory_manager = performer_file_manager.directory_manager
        db_manager = DatabaseManager().get_sqlite_db(
            performer_directory_manager.user.metadata_directory.joinpath("user_data.db")
        )
        db_manager.import_metadata(self.legacy_content_metadatas)
        for legacy_filepath in self.metadatas:
            if (
                legacy_filepath.suffix != ".json"
                or "__legacy__" in legacy_filepath.parts
            ):
                continue
            if legacy_filepath and legacy_filepath.exists():
                new_metadata_filepath = (
                    performer_directory_manager.user.metadata_directory.joinpath(
                        "__legacy__", legacy_filepath.name
                    )
                )
                new_metadata_filepath.parent.mkdir(parents=True, exist_ok=True)
                self.filesystem_manager.move(legacy_filepath, new_metadata_filepath)
        await self.fix_archived_db()
        self.metadatas = self.find_metadatas()

    def fix_json(self):
        def merge_statuses(unmerged_status: dict[str, Any]):
            merged_status: list[dict[str, Any]] = [
                val for lst in unmerged_status.values() for val in lst
            ]
            return merged_status

        content_manager = self.content_manager
        for metadata_filepath in self.metadatas:
            if (
                metadata_filepath.suffix != ".json"
                or "__legacy__" in metadata_filepath.parts
                or "Mass Messages" in metadata_filepath.as_posix()
                or "Chats.json" == metadata_filepath.name
            ):
                continue
            final_content_type = None
            archive = False
            if metadata_filepath.stat().st_size == 0:
                metadata_filepath.unlink()
                continue
            new_metadata_set: list[dict[str, Any]] | dict[str, Any] = (
                main_helper.import_json(metadata_filepath)
            )
            content_types = self.subscription.get_api().CategorizedContent().get_keys()
            final_stem = metadata_filepath.stem
            patterns = []
            underscore_pattern = r"(_\d+)"
            parentheses_pattern = r"(\(\d+\))"
            patterns.extend([underscore_pattern, parentheses_pattern])

            for pattern in patterns:
                matches = re.findall(pattern, final_stem)
                if matches:
                    final_stem = (
                        final_stem.removesuffix(matches[0]).strip().replace(" ", "")
                    )
            if final_stem in content_types:
                final_content_type = final_stem
            else:
                for item in content_types:
                    if item in metadata_filepath.parts:
                        final_content_type = item
                        break
                if final_stem == "Archived" or "Archived" in metadata_filepath.parts:
                    archive = True
                    final_content_type = "Posts"
                if "Posts" in final_stem:
                    final_content_type = "Posts"
                if not final_content_type:
                    # If we get an key error here, we need to move the json file to correct folder or we can resolve it by getting content_type by looking at the directory path
                    # directory_set = set()
                    # merged = merge_statuses(new_metadata_set)
                    # for item in merged:
                    #     directory_set.add(item["directory"])
                    if isinstance(new_metadata_set, dict):
                        if (
                            "type" in new_metadata_set
                            and "content_type" not in new_metadata_set
                        ):
                            item = new_metadata_set["valid"][0]
                            directory = item["directory"]
                            content_types = (
                                self.subscription.get_api().CategorizedContent()
                            )
                            temp_content_type = content_types.path_to_key(
                                Path(directory)
                            )
                            if all(
                                temp_content_type
                                == content_types.path_to_key(Path(item["directory"]))
                                for item in new_metadata_set["valid"]
                            ):
                                final_content_type = temp_content_type
                                if not final_content_type:
                                    if metadata_filepath.parent.stem == "Metadata":
                                        continue
                        else:
                            if "content_type" not in new_metadata_set:
                                continue
                            final_content_type = new_metadata_set["content_type"]
            assert (
                final_content_type
            ), "Content type (Posts,etc) not set before fixing JSON"

            final_metadata_set = {}
            content_json = {}
            if isinstance(new_metadata_set, list):
                for temp_set in new_metadata_set:
                    content_json[temp_set["type"]] = {
                        "valid": temp_set["valid"],
                        "invalid": temp_set.get("invalid", []),
                    }
                final_metadata_set["version"] = 1.8
                final_metadata_set["content"] = content_json
                pass
            elif (
                "type" not in new_metadata_set
                and "version" not in new_metadata_set
                and "valid" in new_metadata_set
            ):
                content_json[metadata_filepath.stem] = {
                    "valid": new_metadata_set["valid"],
                    "invalid": new_metadata_set["invalid"],
                }
                final_metadata_set["version"] = 1.9
                final_metadata_set["content"] = content_json
                pass
            elif "type" in new_metadata_set:
                # Usually means there's another json file we'd have to merge into this, like Images.json and Videos.json.

                class MetadataType(TypedDict):
                    version: float | None
                    content_type: str
                    content: dict[str, Any]

                temp_content_json: MetadataType = {
                    "version": 2.0,
                    "content_type": final_content_type,
                    "content": {},
                }
                temp_content_json["content"][new_metadata_set["type"]] = {
                    "valid": new_metadata_set["valid"],
                    "invalid": new_metadata_set["invalid"],
                }
                content_json = temp_content_json["content"]
                final_metadata_set = temp_content_json
                pass
            else:
                content_json = new_metadata_set.get("content", new_metadata_set)
            if final_metadata_set:
                final_metadata_set["content_type"] = final_content_type
                main_helper.export_json(final_metadata_set, metadata_filepath)
            for media_type, status in content_json.items():
                merged_status = merge_statuses(status)
                if merged_status:
                    for item_json in merged_status:
                        if not final_content_type:
                            raise Exception("content type not found")

                        def assign_metadata(
                            content_metadata: ContentMetadata, item_json: dict[str, Any]
                        ):
                            content_metadata.__legacy__ = True
                            content_metadata.created_at = item_json["postedAt"]
                            content_metadata.paid = item_json.get("paid", False)
                            content_metadata.price = item_json.get("price", 0)
                            content_metadata.text = item_json["text"]
                            content_metadata.user_id = self.subscription.id
                            content_metadata.archived = archive
                            for asset_json in item_json.get("medias", [item_json]):
                                urls = asset_json.get("links") or []
                                if "link" in asset_json:
                                    urls.append(asset_json["link"])

                                media_metadata = content_metadata.find_media(
                                    asset_json.get("media_id"), urls
                                )
                                if not media_metadata:
                                    media_id = asset_json.get("media_id")
                                    if not media_id:
                                        continue
                                    media_metadata = MediaMetadata(
                                        media_id,
                                        media_type,
                                        content_manager,
                                        content_metadata=content_metadata,
                                        urls=urls,
                                    )
                                    pass
                                media_metadata.directory = Path(asset_json["directory"])
                                media_metadata.filename = asset_json["filename"]
                                media_metadata.size = asset_json.get("size", 0)
                                content_metadata.medias.append(media_metadata)

                        content_metadata = None
                        if isinstance(item_json, list):
                            for item_item_json in item_json:
                                content_metadata = self.find_content_metadata_by_id(
                                    item_item_json["post_id"],
                                    content_metadats=self.legacy_content_metadatas,
                                )
                                if not content_metadata:
                                    content_metadata = ContentMetadata(
                                        item_item_json["post_id"],
                                        final_content_type,
                                        content_manager,
                                    )
                                    self.legacy_content_metadatas.append(
                                        content_metadata
                                    )
                                    pass
                                assign_metadata(content_metadata, item_item_json)
                                pass
                            pass
                        else:
                            content_metadata = self.find_content_metadata_by_id(
                                item_json["post_id"],
                                content_metadats=self.legacy_content_metadatas,
                            )
                            if not content_metadata:
                                content_metadata = ContentMetadata(
                                    item_json["post_id"],
                                    final_content_type,
                                    content_manager,
                                )
                                self.legacy_content_metadatas.append(content_metadata)
                            assign_metadata(content_metadata, item_json)

        return self.legacy_content_metadatas

    async def fix_archived_db(
        self,
    ):
        api = self.subscription.get_api()
        filesystem_manager_user = self.filesystem_manager.get_file_manager(
            self.subscription.id
        )
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
                for api_type, _value in api.CategorizedContent():
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
                                .filter(table_name.post_id == item.post_id)  # type: ignore
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
                    "__legacy__",
                    archived_database_path.name,
                )
                new_filepath.parent.mkdir(exist_ok=True)
                filesystem_manager_user.rename_path(
                    archived_database_path, new_filepath
                )
                await filesystem_manager_user.refresh_files()

    def export(self, api_type: str, datas: list[dict[str, Any]]):
        if api_type == "Posts":
            self.db_manager
        pass

    def find_content_metadata_by_id(
        self, id: int, content_metadats: list[ContentMetadata] = []
    ):
        content_metadats = content_metadats or self.content_metadatas
        for content in content_metadats:
            if content.content_id == id:
                return content

    def update_metadata_filepaths(self):
        filesystem_manager = self.filesystem_manager.get_file_manager(
            self.subscription.id
        )

        directory_manager = filesystem_manager.directory_manager
        if directory_manager:
            for i, metadata_filepath in enumerate(self.metadatas):
                new_m_f = directory_manager.user.metadata_directory.joinpath(
                    metadata_filepath.name
                )
                if metadata_filepath != new_m_f:
                    if (
                        "__legacy__" in metadata_filepath.parts
                        or ".db" != metadata_filepath.suffix
                    ):
                        continue
                    counter = 0
                    while True:
                        if not new_m_f.exists():
                            # If there's metadata present already before the directory is created, we'll create it here
                            directory_manager.user.metadata_directory.mkdir(
                                exist_ok=True, parents=True
                            )
                            self.filesystem_manager.move(metadata_filepath, new_m_f)
                            break
                        else:
                            new_m_f = new_m_f.with_stem(
                                f"{metadata_filepath.stem}_{counter}"
                            )
                            counter += 1
                self.metadatas[i] = new_m_f
