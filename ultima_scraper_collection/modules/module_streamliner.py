from __future__ import annotations

import copy
from itertools import product
from typing import Any, Optional

import ultima_scraper_api
import ultima_scraper_api.classes.make_settings as make_settings
from sqlalchemy.exc import OperationalError
from tqdm.asyncio import tqdm_asyncio
from ultima_scraper_collection.managers.database_manager.connections.sqlite.models.api_model import (
    ApiModel,
)
from ultima_scraper_collection.managers.database_manager.connections.sqlite.sqlite_database import (
    DBCollection,
)
from ultima_scraper_collection.managers.database_manager.database_manager import (
    DatabaseManager,
)
from ultima_scraper_collection.managers.download_manager import DownloadManager
from ultima_scraper_collection.managers.filesystem_manager import FilesystemManager
from ultima_scraper_collection.managers.metadata_manager.metadata_manager import (
    MetadataManager,
)
from ultima_scraper_renamer import renamer

auth_types = ultima_scraper_api.auth_types
user_types = ultima_scraper_api.user_types
message_types = ultima_scraper_api.message_types
error_types = ultima_scraper_api.error_types

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from ultima_scraper_collection.managers.datascraper_manager.datascrapers.fansly import (
        FanslyDataScraper,
    )
    from ultima_scraper_collection.managers.datascraper_manager.datascrapers.onlyfans import (
        OnlyFansDataScraper,
    )
    from ultima_scraper_collection.managers.metadata_manager.metadata_manager import (
        ContentMetadata,
        MediaMetadata,
    )

    datascraper_types = OnlyFansDataScraper | FanslyDataScraper


class download_session(tqdm_asyncio):
    def start(
        self,
        unit: str = "B",
        unit_scale: bool = True,
        miniters: int = 1,
        tsize: int = 0,
    ):
        self.unit = unit
        self.unit_scale = unit_scale
        self.miniters = miniters
        self.total = 0
        self.colour = "Green"
        if tsize:
            tsize = int(tsize)
            self.total += tsize

    def update_total_size(self, tsize: Optional[int]):
        if tsize:
            tsize = int(tsize)
            self.total += tsize


class StreamlinedDatascraper:
    def __init__(self, datascraper: datascraper_types) -> None:
        self.datascraper = datascraper
        self.filesystem_manager = FilesystemManager()
        self.content_types = self.datascraper.api.ContentTypes()
        self.media_types = self.datascraper.api.MediaTypes()
        self.user_list: set[user_types] = set()
        self.metadata_manager_users: dict[int, MetadataManager] = {}

    async def configure_datascraper_jobs(self):
        api = self.datascraper.api
        site_settings = api.get_site_settings()
        available_jobs = site_settings.jobs.scrape
        option_manager = self.datascraper.option_manager
        performer_options = option_manager.performer_options
        valid_user_list: set[user_types] = set(
            option_manager.subscription_options.final_choices
        )
        scraping_subscriptions = (
            self.datascraper.api.get_site_settings().jobs.scrape.subscriptions
        )
        identifiers = []
        if performer_options:
            identifiers = performer_options.return_auto_choice()
        if not available_jobs.subscriptions:
            for authed in api.auths:
                authed.subscriptions = []
        if available_jobs.messages:
            chat_users: list[user_types] = []
            if identifiers:
                for authed in api.auths:
                    for identifier in identifiers:
                        chat_user = await authed.get_user(identifier)
                        if isinstance(chat_user, user_types):
                            chat_users.append(chat_user)
            else:
                chat_users = await self.get_chat_users()
            [
                user.scrape_whitelist.append("Messages")
                for user in chat_users
                if not await user.is_subscribed() or not scraping_subscriptions
            ]
            [valid_user_list.add(x) for x in chat_users]

        if available_jobs.paid_contents:
            for authed in self.datascraper.api.auths:
                paid_contents = await authed.get_paid_content()
                if not isinstance(paid_contents, error_types):
                    for paid_content in paid_contents:
                        author = paid_content.get_author()
                        if identifiers:
                            found = await author.match_identifiers(identifiers)
                            if not found:
                                continue
                        if author:
                            performer = await authed.get_subscription(
                                identifier=author.id,
                            )
                            if performer:
                                performer.job_whitelist.append("PaidContents")
                                performer.scrape_whitelist.clear()
                                valid_user_list.add(performer)
        # Need to filter out own profile with is_performer,etc
        final_valid_user_set = {
            user
            for user in valid_user_list
            if user.username not in user.get_authed().blacklist
        }

        self.user_list = final_valid_user_set
        return final_valid_user_set

    # Prepares the API links to be scraped

    async def prepare_scraper(
        self,
        subscription: user_types,
        metadata_manager: MetadataManager,
        content_type: str,
        master_set: list[Any] = [],
    ):
        authed = subscription.get_authed()
        current_job = subscription.get_current_job()
        if not current_job:
            return
        temp_master_set: list[Any] = copy.copy(master_set)
        if not temp_master_set:
            match content_type:
                case "Stories":
                    temp_master_set.extend(
                        await self.datascraper.get_all_stories(subscription)
                    )
                    pass
                case "Posts":
                    temp_master_set = await self.datascraper.get_all_posts(subscription)
                    pass
                case "Messages":
                    pass
                    unrefined_set: list[
                        message_types | Any
                    ] = await subscription.get_messages()
                    mass_messages = getattr(authed, "mass_messages")
                    if subscription.is_me() and mass_messages:
                        mass_messages = getattr(authed, "mass_messages")
                        # Need access to a creator's account to fix this
                        # unrefined_set2 = await self.datascraper.process_mass_messages(
                        #     authed,
                        #     mass_messages,
                        # )
                        # unrefined_set += unrefined_set2
                    temp_master_set = unrefined_set
                case "Archived":
                    pass
                case "Chats":
                    pass
                case "Highlights":
                    pass
                case "MassMessages":
                    pass
                case _:
                    raise Exception(f"{content_type} is an invalid choice")
        await self.process_scraped_content(
            temp_master_set, content_type, subscription, metadata_manager
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
        # if  api_type != "Posts":
        #     return
        authed = subscription.get_authed()
        subscription_directory_manager = self.filesystem_manager.get_directory_manager(
            subscription.id
        )
        formatted_metadata_directory = (
            subscription_directory_manager.user.metadata_directory
        )
        unrefined_set = []
        with authed.get_pool() as pool:
            print(f"Processing Scraped {api_type}")
            tasks = pool.starmap(
                self.datascraper.media_scraper,
                product(
                    master_set,
                    [subscription],
                    [subscription_directory_manager.root_download_directory],
                    [api_type],
                ),
            )
            settings = {"colour": "MAGENTA"}
            unrefined_set: list[dict[str, Any]] = await tqdm_asyncio.gather(
                *tasks, **settings
            )
            new_metadata = metadata_manager.merge_content_and_directories(unrefined_set)
            final_content, _final_directories = new_metadata
            metadata_path = formatted_metadata_directory.joinpath("user_data.db")
            metadata_manager.db_manager = DatabaseManager().get_sqlite_db(metadata_path)
            if new_metadata:
                new_metadata_content = final_content
                metadata_manager.content_metadatas.extend(new_metadata_content)
                print("Processing metadata.")
                subscription.scrape_manager.set_scraped(api_type, new_metadata_content)
                if new_metadata_content:
                    import_status = metadata_manager.db_manager.import_metadata(
                        new_metadata_content, api_type
                    )
                    if import_status:
                        Session = metadata_manager.db_manager.session_factory
                        if authed.api.config.settings.helpers.renamer:
                            print(f"{subscription.username}: Renaming files.")
                            _new_metadata_object = await renamer.start(
                                subscription,
                                subscription_directory_manager,
                                api_type,
                                Session,
                            )
                        pass
            else:
                print(f"No {api_type} found.")
        return True

    # Downloads scraped content

    async def prepare_downloads(self, performer: user_types, api_type: str):
        metadata_manager = self.metadata_manager_users[performer.id]
        global_settings = performer.get_api().get_global_settings()
        site_settings = performer.get_api().get_site_settings()
        if not (global_settings and site_settings):
            return
        filesystem_manager = self.datascraper.filesystem_manager
        performer_directory_manager = filesystem_manager.get_directory_manager(
            performer.id
        )
        directory = performer_directory_manager.user.download_directory
        current_job = performer.get_current_job()

        metadata_path = performer_directory_manager.user.metadata_directory.joinpath(
            "user_data.db"
        )
        user_data_db = DatabaseManager().get_sqlite_db(metadata_path)
        if user_data_db and user_data_db.name.exists():
            database_session = user_data_db.session
            db_collection = DBCollection()
            database = db_collection.database_picker("user_data")
            api_table = database.table_picker(api_type)
            media_table = database.media_table
            overwrite_files = site_settings.overwrite_files
            final_download_set: set[ContentMetadata] = set()
            final_media_set: set[MediaMetadata] = set()
            total_media_count = 0
            if database:
                db_content_dict = {}
                db_posts: list[ApiModel] = database_session.query(api_table)
                for db_post in db_posts:
                    if db_post.post_id not in db_content_dict:
                        db_content_dict[db_post.post_id] = db_post
                    else:
                        raise Exception("Duplicate key in db_content_dict")

                content_metadatas = metadata_manager.content_metadatas
                for content_metadata in content_metadatas:
                    if content_metadata.api_type == api_type:
                        db_post = db_content_dict[content_metadata.content_id]
                        if content_metadata.content_id == db_post.post_id:
                            content_metadata.__db_content__ = db_post
                            db_media_query = (
                                database_session.query(media_table)
                                .filter(media_table.post_id == db_post.post_id)
                                .filter(media_table.api_type == api_type)
                            )
                            if overwrite_files:
                                db_post.medias = db_media_query.all()
                            else:
                                db_post.medias = db_media_query.filter(
                                    media_table.downloaded == False
                                ).all()
                            if db_post.medias:
                                final_download_set.add(content_metadata)
                                [
                                    final_media_set.add(x.id)
                                    for x in content_metadata.medias
                                ]
                                total_media_count += len(content_metadata.medias)
                download_manager = DownloadManager(
                    filesystem_manager,
                    performer.get_session_manager(),
                    final_download_set,
                    global_settings.helpers.reformat_media,
                )
                download_count = len(final_media_set)
                duplicate_count = total_media_count - download_count
                string = "Processing Download:\n"
                string += f"Name: {performer.username} | Type: {api_type} | Downloading: {download_count} | Total: {total_media_count} | Duplicates: {duplicate_count} | Directory: {directory}\n"
                if total_media_count:
                    print(string)
                    _result = await download_manager.bulk_download()
                    pass
                while True:
                    try:
                        database_session.commit()
                        break
                    except OperationalError:
                        database_session.rollback()
                database_session.close()
            current_job.done = True

    async def manage_subscriptions(
        self,
        authed: auth_types,
        identifiers: list[int | str] = [],
        refresh: bool = True,
    ):
        temp_subscriptions: list[user_types] = []
        results = await self.datascraper.get_all_subscriptions(
            authed, identifiers, refresh
        )
        site_settings = authed.api.get_site_settings()
        if not site_settings:
            return temp_subscriptions
        ignore_type = site_settings.ignore_type
        results.sort(key=lambda x: x.is_me(), reverse=True)
        for result in results:
            # await result.create_directory_manager(user=True)
            subscribePrice = result.subscribePrice
            if ignore_type in ["paid"]:
                if subscribePrice > 0:
                    continue
            if ignore_type in ["free"]:
                if subscribePrice == 0:
                    continue
            temp_subscriptions.append(result)
        authed.subscriptions = temp_subscriptions
        return authed.subscriptions

    async def account_setup(
        self,
        auth: auth_types,
        datascraper: datascraper_types,
        site_settings: make_settings.SiteSettings,
        identifiers: list[int | str] | list[str] = [],
    ) -> tuple[bool, list[user_types]]:
        status = False
        subscriptions: list[user_types] = []
        authed = await auth.login()

        if authed.active and site_settings:
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
            authed.blacklist = await authed.get_blacklist(
                authed.api.get_site_settings().blacklists
            )
            if identifiers or site_settings.jobs.scrape.subscriptions:
                subscriptions.extend(
                    await datascraper.manage_subscriptions(
                        authed, identifiers=identifiers  # type: ignore
                    )
                )
            status = True
        elif (
            auth.auth_details.email
            and auth.auth_details.password
            and site_settings.browser.auth
        ):
            # domain = "https://onlyfans.com"
            # oflogin.login(auth, domain, auth.session_manager.get_proxy())
            pass
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
