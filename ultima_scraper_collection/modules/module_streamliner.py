from __future__ import annotations

import copy
from itertools import product
from typing import Any, Optional

import ultima_scraper_api
import ultima_scraper_api.apis.fansly.classes as fansly_classes
import ultima_scraper_api.classes.make_settings as make_settings
from sqlalchemy.exc import OperationalError
from tqdm.asyncio import tqdm_asyncio
from ultima_scraper_api.helpers import main_helper
from ultima_scraper_renamer import renamer

from ultima_scraper_collection.managers.database_manager.connections.sqlite.models.media_model import (
    TemplateMediaModel,
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
        self.media_types = self.datascraper.api.Locations()
        self.user_list: list[user_types] = []

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
                        author = await paid_content.get_author()
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
        for user in valid_user_list:
            if user.username in authed.blacklist:
                valid_user_list.remove(user)
            pass
        self.user_list = valid_user_list
        return valid_user_list

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
                    pass
                    temp_master_set.extend(
                        await self.datascraper.get_all_stories(subscription)
                    )
                    pass
                case "Posts":
                    temp_master_set = await subscription.get_posts()
                    # Archived Posts
                    if isinstance(subscription, fansly_classes.user_model.create_user):
                        collections = await subscription.get_collections()
                        for collection in collections:
                            temp_master_set.append(
                                await subscription.get_collection_content(collection)
                            )
                        pass
                    else:
                        temp_master_set += await subscription.get_archived_posts()
                case "Messages":
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
            new_metadata: dict[str, Any] = await main_helper.format_media_set(
                unrefined_set
            )
            metadata_path = formatted_metadata_directory.joinpath("user_data.db")
            metadata_manager.db_manager = DatabaseManager().get_sqlite_db(metadata_path)
            legacy_metadata_path = formatted_metadata_directory.joinpath(
                api_type + ".db"
            )
            if new_metadata:
                new_metadata_content: list[dict[str, Any]] = new_metadata["content"]
                print("Processing metadata.")
                (
                    old_metadata,
                    delete_metadatas,
                ) = await metadata_manager.process_legacy_metadata(
                    subscription,
                    api_type,
                    metadata_path,
                    subscription_directory_manager,
                )
                new_metadata_content.extend(old_metadata)
                subscription.set_scraped(api_type, new_metadata_content)
                result = await metadata_manager.process_metadata(
                    legacy_metadata_path,
                    new_metadata_content,
                    api_type,
                    subscription,
                    delete_metadatas,
                )
                if result:
                    import_status = metadata_manager.db_manager.import_metadata(
                        api_type, result
                    )
                    if import_status:
                        Session = metadata_manager.db_manager.session_factory
                        if authed.api.config.settings.helpers.renamer:
                            print("Renaming files.")
                            _new_metadata_object = await renamer.start(
                                subscription,
                                subscription_directory_manager,
                                api_type,
                                Session,
                            )
                        metadata_manager.delete_metadatas()
                        pass
            else:
                print(f"No {api_type} found.")
        return True

    # Downloads scraped content

    async def prepare_downloads(self, subscription: user_types, api_type: str):
        global_settings = subscription.get_api().get_global_settings()
        site_settings = subscription.get_api().get_site_settings()
        if not (global_settings and site_settings):
            return
        filesystem_manager = self.datascraper.filesystem_manager
        subscription_directory_manager = filesystem_manager.get_directory_manager(
            subscription.id
        )
        directory = subscription_directory_manager.root_download_directory
        current_job = subscription.get_current_job()

        metadata_path = subscription_directory_manager.user.metadata_directory.joinpath(
            "user_data.db"
        )
        user_data_db = DatabaseManager().get_sqlite_db(metadata_path)
        if user_data_db and user_data_db.name.exists():
            database_session = user_data_db.session
            db_collection = DBCollection()
            database = db_collection.database_picker("user_data")
            if database:
                media_table = database.media_table
                overwrite_files = site_settings.overwrite_files
                if overwrite_files:
                    download_list: list[TemplateMediaModel] = (
                        database_session.query(media_table)
                        .filter(media_table.api_type == api_type)
                        .all()
                    )
                    media_set_count = len(download_list)
                else:
                    download_list: list[TemplateMediaModel] = (
                        database_session.query(media_table)
                        .filter(media_table.downloaded == False)
                        .filter(media_table.api_type == api_type)
                    )
                    media_set_count = user_data_db.get_count(download_list)
                    pass
                # Unique download list
                final_download_list: set[TemplateMediaModel] = set()
                for download_item in download_list:
                    found = [
                        x
                        for x in download_list
                        if download_item.media_id == x.media_id
                        and download_item.api_type == x.api_type
                    ]
                    final_download_list.add(found[0])
                download_manager = DownloadManager(
                    filesystem_manager,
                    subscription.get_session_manager(),
                    final_download_list,
                    global_settings.helpers.reformat_media,
                )
                location = ""
                download_count = len(final_download_list)
                duplicate_count = media_set_count - download_count
                string = "Download Processing\n"
                string += f"Name: {subscription.username} | Type: {api_type} | Downloading: {download_count} | Total: {media_set_count} | Duplicates: {duplicate_count} {location} | Directory: {directory}\n"
                if media_set_count:
                    print(string)
                    result = await download_manager.bulk_download()
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
