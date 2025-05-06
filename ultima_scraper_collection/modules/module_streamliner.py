from __future__ import annotations

import asyncio
import copy
from typing import Any

import ultima_scraper_api
from sqlalchemy import and_, or_, select
from sqlalchemy.orm import joinedload
from ultima_scraper_api.apis.onlyfans.classes.auth_model import OnlyFansAuthModel
from ultima_scraper_api.apis.onlyfans.classes.user_model import (
    create_user as OnlyFansUserModel,
)
from ultima_scraper_api.helpers.main_helper import ProgressBar
from ultima_scraper_collection.config import site_config_types
from ultima_scraper_collection.managers.content_manager import (
    ContentManager,
    MediaManager,
)
from ultima_scraper_collection.managers.download_manager import DownloadManager
from ultima_scraper_collection.managers.filesystem_manager import FilesystemManager
from ultima_scraper_collection.managers.metadata_manager.metadata_manager import (
    MediaMetadata,
    MetadataManager,
)
from ultima_scraper_collection.managers.server_manager import ServerManager
from ultima_scraper_db.databases.ultima_archive.schemas.templates.site import (
    FilePathModel as DBFilePathModel,
)
from ultima_scraper_db.databases.ultima_archive.schemas.templates.site import (
    MediaModel as DBMediaModel,
)
from ultima_scraper_db.databases.ultima_archive.schemas.templates.site import (
    MessageModel as DBMessageModel,
)
from ultima_scraper_db.databases.ultima_archive.schemas.templates.site import UserModel
from ultima_scraper_db.databases.ultima_archive.schemas.templates.site import (
    UserModel as DBUserModel,
)
from ultima_scraper_renamer.reformat import ReformatManager

auth_types = ultima_scraper_api.auth_types
user_types = ultima_scraper_api.user_types
message_types = ultima_scraper_api.message_types
error_types = ultima_scraper_api.error_types
subscription_types = ultima_scraper_api.subscription_types
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from ultima_scraper_collection.managers.datascraper_manager.datascrapers.fansly import (
        FanslyDataScraper,
    )
    from ultima_scraper_collection.managers.datascraper_manager.datascrapers.onlyfans import (
        OnlyFansDataScraper,
    )

    datascraper_types = OnlyFansDataScraper | FanslyDataScraper


async def find_earliest_non_downloaded_message(
    user: user_types, datascraper: "datascraper_types"
):
    authed = user.get_authed()
    site_api = datascraper.server_manager.ultima_archive_db_api.get_site_api(
        authed.get_api().site_name
    )
    earliest_non_downloaded_message = None
    found_media = False
    db_performer = datascraper.db_performers[user.id]
    db_content_manager = db_performer.content_manager
    assert db_content_manager, "Content manager not found"
    db_messages = db_content_manager.messages
    for db_message in db_messages:
        if (db_message.user_id == authed.id and db_message.receiver_id == user.id) or (
            db_message.user_id == user.id and db_message.receiver_id == authed.id
        ):
            all_media_downloaded = False
            if db_message.media:
                found_media = True
            else:
                continue
            for media in db_message.media:
                for filepath in media.filepaths:
                    if not filepath.message:
                        continue
                    if filepath.downloaded:
                        all_media_downloaded = True
                        break

            if not all_media_downloaded:
                if (
                    earliest_non_downloaded_message is None
                    or db_message.created_at
                    < earliest_non_downloaded_message.created_at
                ):
                    earliest_non_downloaded_message = db_message
    if found_media:
        earliest_non_downloaded_message = db_messages[0]
    await site_api.schema.session.commit()
    return earliest_non_downloaded_message


class StreamlinedDatascraper:
    def __init__(
        self, datascraper: datascraper_types, server_manager: ServerManager
    ) -> None:
        self.datascraper = datascraper
        self.filesystem_manager = FilesystemManager()
        self.media_types = self.datascraper.api.MediaTypes()
        self.user_list: set[user_types] = set()
        self.db_performers: dict[int, UserModel] = {}
        self.metadata_manager_users: dict[int, MetadataManager] = {}
        self.server_manager: ServerManager = server_manager
        self.content_managers: dict[int, ContentManager] = {}
        self.media_managers: dict[int, MediaManager] = {}

    def find_metadata_manager(self, user_id: int):
        return self.metadata_manager_users[user_id]

    def resolve_content_manager(self, user: user_types):
        content_manager = self.content_managers.get(user.id)
        authed = user.get_authed()
        if content_manager:
            if content_manager.authed.id != authed.id:
                content_manager = ContentManager(authed)
        else:
            content_manager = ContentManager(authed)
        self.content_managers[user.id] = content_manager
        return content_manager

    def create_media_manager(self, user: user_types):
        if user.id not in self.media_managers:
            self.media_managers[user.id] = MediaManager()
        return self.media_managers[user.id]

    def get_archive_db_api(self):
        return self.server_manager.ultima_archive_db_api

    async def configure_datascraper_jobs(self):
        api = self.datascraper.api
        site_config = self.datascraper.site_config
        available_jobs = site_config.jobs.scrape
        option_manager = self.datascraper.option_manager
        performer_options = option_manager.performer_options
        assert option_manager.subscription_options, "Subscription options not found"
        valid_user_list: set[user_types] = set(
            option_manager.subscription_options.final_choices
        )
        scraping_subscriptions = site_config.jobs.scrape.subscriptions
        identifiers = []
        if performer_options:
            identifiers = performer_options.return_auto_choice()
        if not available_jobs.subscriptions:
            for authed in api.auths.values():
                authed.subscriptions = []
        if available_jobs.messages:
            chat_users: list[user_types] = []
            if identifiers:
                for authed in api.auths.values():
                    for identifier in identifiers:
                        chat_user = await authed.get_user(identifier)
                        if isinstance(chat_user, user_types):
                            chat_users.append(chat_user)
            else:
                chat_users = await self.get_chat_users()
            [
                user.scrape_whitelist.append("Messages")
                for user in chat_users
                if not user.is_subscribed() or not scraping_subscriptions
            ]
            [valid_user_list.add(x) for x in chat_users]

        if available_jobs.paid_contents:
            for authed in self.datascraper.api.auths.values():
                paid_contents = await authed.get_paid_content()
                if not isinstance(paid_contents, error_types):
                    for paid_content in paid_contents:
                        author = paid_content.get_author()
                        if identifiers:
                            found = await author.match_identifiers(identifiers)
                            if not found:
                                continue
                        if author:
                            performer = authed.find_user(
                                identifier=author.id,
                            )
                            if performer:
                                performer.job_whitelist.append("PaidContents")
                                performer.scrape_whitelist.clear()
                                valid_user_list.add(performer)
        from ultima_scraper_api.apis.fansly.classes.user_model import (
            create_user as FYUserModel,
        )

        for user in valid_user_list:
            if isinstance(user, FYUserModel) and user.following:
                user.scrape_whitelist.clear()
                pass
            pass
        # Need to filter out own profile with is_performer,etc
        final_valid_user_set = {
            user
            for user in valid_user_list
            if user.username not in user.get_authed().blacklist
        }

        self.user_list = final_valid_user_set
        return final_valid_user_set

    # Prepares the API links to be scraped
    async def scrape_vault(
        self, user: user_types, db_user: UserModel, content_type: str
    ):
        current_job = user.get_current_job()
        if not current_job:
            return
        authed: auth_types = user.get_authed()
        site_config = self.datascraper.site_config
        if (
            isinstance(authed, OnlyFansAuthModel)
            and user.is_authed_user()
            and user.is_performer()
        ):
            vault = await authed.get_vault_lists()
            vault_item = vault.resolve(name=content_type)
            assert vault_item, f"Vault item {content_type} not found"
            vault_item_medias = await vault_item.get_medias()
            media_metadatas: list[MediaMetadata] = []
            for vault_item_media in vault_item_medias:
                content_manager = self.resolve_content_manager(user)
                media_metadata = MediaMetadata(
                    vault_item_media["id"],
                    vault_item_media["type"],
                    content_manager=content_manager,
                )
                media_metadata.raw_extractor(user, vault_item_media)
                reformat_manager = ReformatManager(authed, self.filesystem_manager)
                reformat_item = reformat_manager.prepare_reformat(media_metadata)
                file_directory = reformat_item.reformat(
                    site_config.download_setup.directory_format
                )
                reformat_item.directory = file_directory
                file_path = reformat_item.reformat(
                    site_config.download_setup.filename_format
                )
                media_metadata.directory = file_directory
                media_metadata.filename = file_path.name
                media_metadatas.append(media_metadata)
        current_job.done = True

    async def prepare_filesystem(self, performer: user_types):
        await self.filesystem_manager.create_directory_manager(
            self.site_config, performer  # type:ignore
        )
        await self.filesystem_manager.format_directories(performer)
        metadata_manager = MetadataManager(
            performer,
            self.resolve_content_manager(performer),
            self.filesystem_manager,
        )
        await metadata_manager.process_legacy_metadata()
        self.metadata_manager_users[performer.id] = metadata_manager
        return metadata_manager

    async def paid_content_scraper(self, authed: auth_types):
        paid_contents = await authed.get_paid_content()
        datascraper = self.datascraper
        assert datascraper, "Datascraper not found"
        unique_suppliers: set[user_types] = set()
        for paid_content in paid_contents:
            supplier = paid_content.get_author()
            await self.prepare_filesystem(supplier)
            content_manager = datascraper.resolve_content_manager(supplier)
            content_type = paid_content.get_content_type()
            result = await datascraper.media_scraper(
                paid_content, supplier, content_type  # type:ignore
            )
            content_manager.set_content(
                content_type,
                result["content"],
            )
            unique_suppliers.add(supplier)

        for supplier in unique_suppliers:
            if isinstance(supplier, OnlyFansUserModel):
                content_manager = datascraper.resolve_content_manager(supplier)
                supplier.cache.messages.activate()
                contents = await supplier.get_mass_messages()
                supplier.cache.messages.deactivate()

                for content in contents:
                    content_type = content.get_content_type()
                    result = await datascraper.media_scraper(
                        content, supplier, content_type  # type:ignore
                    )
                    content_manager.set_content(
                        content_type,
                        result["content"],
                    )

    async def prepare_scraper(
        self,
        user: user_types,
        metadata_manager: MetadataManager,
        content_type: str,
        master_set: list[Any] = [],
    ):
        authed = user.get_authed()
        current_job = user.get_current_job()
        if not current_job:
            return
        temp_master_set: list[Any] = copy.copy(master_set)
        if not temp_master_set and not current_job.ignore:
            match content_type:
                case "Stories":
                    temp_master_set.extend(await self.datascraper.get_all_stories(user))
                    pass
                case "Posts":
                    temp_master_set = await self.datascraper.get_all_posts(user)
                    # temp_master_set = [
                    #     x for x in temp_master_set if x.id in [1264067231, 306722777]
                    # ]
                    pass
                case "Messages":
                    db_message = await find_earliest_non_downloaded_message(
                        user, self.datascraper
                    )
                    cutoff_id = db_message.id if db_message else None
                    temp_master_set = await user.get_messages(cutoff_id=cutoff_id)
                case "Chats":
                    if isinstance(authed, OnlyFansAuthModel):
                        if user.is_authed_user():
                            chats = await authed.get_chats()
                            temp_master_set = []
                            for chat in chats:
                                messages = await chat.get_messages()
                                temp_master_set.extend(messages)
                case "Highlights":
                    pass
                case "MassMessages":
                    if isinstance(authed, OnlyFansAuthModel):
                        if user.is_authed_user():
                            mass_message_stats = await authed.get_mass_message_stats()
                            temp_master_set = []
                            for mass_message_stat in mass_message_stats:
                                mass_message = (
                                    await mass_message_stat.get_mass_message()
                                )
                                temp_master_set.append(mass_message)
                        else:
                            db_message = await find_earliest_non_downloaded_message(
                                user, self.datascraper
                            )
                            cutoff_id = db_message.id if db_message else None
                            mass_messages = await user.get_mass_messages(
                                message_cutoff_id=cutoff_id
                            )
                            temp_master_set.extend(mass_messages)
                case _:
                    raise Exception(f"{content_type} is an invalid choice")
        # Adding paid content and removing duplicates by id
        if isinstance(user, ultima_scraper_api.onlyfans_classes.user_model.create_user):
            for paid_content in await user.get_paid_contents(content_type):
                temp_master_set.append(paid_content)
            pass
            temp_master_set = list(
                {getattr(obj, "id"): obj for obj in temp_master_set}.values()
            )
        await self.process_scraped_content(
            temp_master_set, content_type, user, metadata_manager
        )
        current_job.done = True

    async def process_scraped_content(
        self,
        master_set: list[dict[str, Any]],
        api_type: str,
        subscription: user_types,
        metadata_manager: MetadataManager,
    ):
        if not master_set:
            return False
        unrefined_set = []
        tasks = [
            asyncio.create_task(
                self.datascraper.media_scraper(x, subscription, api_type)  # type:ignore
            )
            for x in master_set
        ]
        unrefined_set: list[dict[str, Any]] = await ProgressBar(
            f"Processing Scraped {api_type}"
        ).gather(tasks)
        new_metadata = metadata_manager.merge_content_and_directories(unrefined_set)
        final_content, _final_directories = new_metadata
        if new_metadata:
            new_metadata_content = final_content
            content_manager = self.resolve_content_manager(subscription)
            content_manager.set_content(api_type, new_metadata_content)
            if new_metadata_content:
                pass
        else:
            print(f"No {api_type} found.")
        return True

    # Downloads scraped content
    async def prepare_downloads(
        self, performer: user_types, db_performer: DBUserModel, api_type: str
    ):
        site_db_api = self.server_manager.ultima_archive_db_api.find_site_api(
            self.datascraper.api.site_name
        )
        current_job = performer.get_current_job()
        global_settings = performer.get_api().get_global_settings()
        filesystem_manager = self.datascraper.filesystem_manager
        performer_directory_manager = filesystem_manager.get_directory_manager(
            performer.id
        )
        filesystem_manager = self.datascraper.filesystem_manager
        content_manager = self.resolve_content_manager(performer)
        db_medias = db_performer.content_manager.get_media_manager().medias
        final_download_set: set[MediaMetadata] = set()
        for db_media in db_medias.values():
            content_info = None
            if api_type == "Uncategorized":
                media_metadata = content_manager.media_manager.medias.get(db_media.id)
            else:
                media_metadata = content_manager.find_media(
                    category=api_type, media_id=db_media.id
                )
            if not media_metadata:
                continue
            if api_type == "Uncategorized":
                await db_media.awaitable_attrs.content_media_assos
                if db_media.content_media_assos:
                    continue
                if len(db_media.filepaths) > 1:
                    continue
            else:
                content_metadata = media_metadata.get_content_metadata()
                if not content_metadata:
                    continue
                db_content = await db_media.find_content(
                    api_type, content_metadata.content_id
                )
                if not db_content:
                    continue
                content_info = (db_content.id, api_type)
            db_filepath = db_media.find_filepath(content_info)
            if db_filepath:
                if media_metadata and media_metadata.urls:
                    media_metadata.__db_media__ = db_media
                    final_download_set.add(media_metadata)
        total_media_count = len(final_download_set)
        non_downloaded = [
            x for x in final_download_set if not x.get_filepath().exists()
        ]
        if non_downloaded:
            pass
        download_media_count = len(non_downloaded)
        directory = performer_directory_manager.user.download_directory
        if final_download_set:
            string = "Processing Download:\n"
            string += f"Name: {performer.username} | Type: {api_type} | Downloading: {download_media_count} | Total: {total_media_count} | Directory: {directory}\n"
            print(string)
        download_manager = DownloadManager(
            performer.get_authed(),
            filesystem_manager,
            final_download_set,
            global_settings.tools.reformatter.active,
        )
        await download_manager.bulk_download()
        await site_db_api.schema.session.commit()
        if current_job:
            current_job.done = True

    async def manage_subscriptions(
        self,
        authed: auth_types,
        identifiers: list[int | str] = [],
        refresh: bool = True,
    ):
        temp_subscriptions: list[subscription_types] = []
        results = await self.datascraper.get_all_subscriptions(
            authed, identifiers, refresh
        )
        site_settings = authed.api.get_site_settings()
        if not site_settings:
            return temp_subscriptions
        results.sort(key=lambda x: x.user.is_me(), reverse=True)
        for result in results:
            temp_subscriptions.append(result)
        authed.subscriptions = temp_subscriptions
        return authed.subscriptions

    async def account_setup(
        self,
        auth: auth_types,
        site_config: site_config_types,
        identifiers: list[int | str] | list[str] = [],
    ) -> tuple[bool, list[subscription_types]]:
        status = False
        subscriptions: list[subscription_types] = []

        if auth.is_authed() and site_config:
            authed = auth
            # metadata_filepath = (
            #     authed.directory_manager.profile.metadata_directory.joinpath(
            #         "Mass Messages.json"
            #     )
            # )
            # if authed.isPerformer:
            #     imported = main_helper.import_json(metadata_filepath)
            #     if "auth" in imported:
            #         imported = imported["auth"]
            #     mass_messages = await authed.get_mass_messages(resume=imported)
            #     if mass_messages:
            #         main_helper.export_json(mass_messages, metadata_filepath)
            authed.blacklist = await authed.get_blacklist(site_config.blacklists)
            if identifiers or site_config.jobs.scrape.subscriptions:
                subscriptions.extend(
                    await self.manage_subscriptions(
                        authed, identifiers=identifiers  # type: ignore
                    )
                )
            status = True
        return status, subscriptions

    async def get_chat_users(self):
        chat_users: list[user_types] = []
        for authed in self.datascraper.api.auths:
            chats = await authed.get_chats()
            for chat in chats:
                username: str = chat["withUser"].username
                subscription = await authed.get_subscription(identifier=username)
                if not subscription:
                    subscription = chat["withUser"]
                    chat_users.append(subscription)
        return chat_users

    async def get_performer(self, authed: auth_types, db_performer: DBUserModel):
        if authed.id == db_performer.id:
            performer = authed.user
        else:
            subscriptions = await authed.get_subscriptions(
                identifiers=[db_performer.id]
            )
            if not subscriptions:
                paid_contents = await authed.get_paid_content(
                    performer_id=db_performer.id
                )
                if not paid_contents:
                    # performer = await authed.get_user(db_performer.id)
                    # if not performer.subscribed_by_data:
                    #     return None
                    return None
                else:
                    performer = [
                        x.get_author()
                        for x in paid_contents
                        if x.get_author().id == db_performer.id
                    ][0]
                    temp_performer = await authed.get_user(performer.id, refresh=True)
                    if not temp_performer:
                        performer.is_deleted = True
            else:
                performer = subscriptions[0].user
                if not performer.is_subscribed():
                    paid_contents = await authed.get_paid_content(
                        performer_id=db_performer.id
                    )
        if isinstance(
            performer, ultima_scraper_api.onlyfans_classes.user_model.create_user
        ):
            if performer.is_blocked:
                await performer.unblock()
        performer.add_aliases([x.username for x in db_performer.aliases])
        performer.username = performer.get_usernames()[0]
        return performer
