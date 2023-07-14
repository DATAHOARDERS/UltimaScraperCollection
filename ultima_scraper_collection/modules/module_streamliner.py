from __future__ import annotations

import copy
from itertools import product
from typing import Any, Optional

import ultima_scraper_api
from sqlalchemy import and_, or_, select
from tqdm.asyncio import tqdm_asyncio
from ultima_scraper_collection.config import site_config_types
from ultima_scraper_collection.managers.download_manager import DownloadManager
from ultima_scraper_collection.managers.filesystem_manager import FilesystemManager
from ultima_scraper_collection.managers.metadata_manager.metadata_manager import (
    MetadataManager,
)
from ultima_scraper_collection.managers.server_manager import ServerManager
from ultima_scraper_db.databases.ultima.schemas.templates.site import (
    MessageModel as DBMessageModel,
)

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
    from ultima_scraper_collection.managers.metadata_manager.metadata_manager import (
        ContentMetadata,
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
    def __init__(
        self, datascraper: datascraper_types, server_manager: ServerManager
    ) -> None:
        self.datascraper = datascraper
        self.filesystem_manager = FilesystemManager()
        self.content_types = self.datascraper.api.ContentTypes()
        self.media_types = self.datascraper.api.MediaTypes()
        self.user_list: set[user_types] = set()
        self.metadata_manager_users: dict[int, MetadataManager] = {}
        self.server_manager: ServerManager = server_manager

    async def configure_datascraper_jobs(self):
        api = self.datascraper.api
        site_config = self.datascraper.site_config
        available_jobs = site_config.jobs.scrape
        option_manager = self.datascraper.option_manager
        performer_options = option_manager.performer_options
        assert option_manager.subscription_options
        valid_user_list: set[user_types] = set(
            option_manager.subscription_options.final_choices
        )
        scraping_subscriptions = site_config.jobs.scrape.subscriptions
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
                            performer = authed.find_user_by_identifier(
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
        if not temp_master_set:
            match content_type:
                case "Stories":
                    temp_master_set.extend(await self.datascraper.get_all_stories(user))
                    pass
                case "Posts":
                    temp_master_set = await self.datascraper.get_all_posts(user)
                case "Messages":
                    site_db = await self.server_manager.get_site_db(
                        authed.get_api().site_name
                    )
                    temp_db_messages = await site_db.session.scalars(
                        select(DBMessageModel)
                        .where(
                            or_(
                                and_(
                                    DBMessageModel.user_id == authed.id,
                                    DBMessageModel.receiver_id == user.id,
                                ),
                                and_(
                                    DBMessageModel.user_id == user.id,
                                    DBMessageModel.receiver_id == authed.id,
                                ),
                            )
                        )
                        .order_by(DBMessageModel.id.desc())
                    )
                    db_messages = temp_db_messages.all()
                    last_db_message = None
                    cutoff_id = None
                    if db_messages:
                        if all(x.verified for x in db_messages):
                            last_db_message = db_messages[0]
                            cutoff_id = last_db_message.id
                    unrefined_set: list[message_types | Any] = await user.get_messages(
                        cutoff_id=cutoff_id
                    )

                    mass_messages = getattr(authed, "mass_messages")
                    if user.is_me() and mass_messages:
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
        # Adding paid content and removing duplicates by id
        if isinstance(user, ultima_scraper_api.onlyfans_classes.user_model.create_user):
            for paid_content in await user.get_paid_contents(content_type):
                temp_master_set.append(paid_content)
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
        # if  api_type != "Posts":
        #     return
        authed = subscription.get_authed()
        unrefined_set = []
        with authed.get_pool() as pool:
            print(f"Processing Scraped {api_type}")
            tasks = pool.starmap(
                self.datascraper.media_scraper,
                product(
                    master_set,
                    [subscription],
                    [api_type],
                ),
            )
            settings = {"colour": "MAGENTA"}
            unrefined_set: list[dict[str, Any]] = await tqdm_asyncio.gather(
                *tasks, **settings
            )
            new_metadata = metadata_manager.merge_content_and_directories(unrefined_set)
            final_content, _final_directories = new_metadata
            if new_metadata:
                new_metadata_content = final_content
                subscription.content_manager.set_content(api_type, new_metadata_content)
                if new_metadata_content:
                    pass
            else:
                print(f"No {api_type} found.")
        return True

    # Downloads scraped content

    async def prepare_downloads(self, performer: user_types, api_type: str):
        current_job = performer.get_current_job()
        if not getattr(performer.content_manager.categorized, api_type):
            current_job.done = True
            return
        api = performer.get_api()
        assert current_job
        site_db = await self.server_manager.get_site_db(api.site_name, self.datascraper)
        db_user = await site_db.get_user(performer.id)
        assert db_user
        await db_user.awaitable_attrs.content_manager
        content_manager = await db_user.content_manager.init()
        db_contents = await content_manager.get_contents(api_type)
        final_download_set: set[ContentMetadata] = set()
        total_media_count = 0
        download_media_count = 0
        for db_content in db_contents:
            await site_db.session.refresh(db_content, ["media"])
            # To find duplicates, we must check if media belongs to any other content type via association table
            found_content = performer.content_manager.find_content(
                db_content.id, api_type
            )
            from ultima_scraper_collection.managers.metadata_manager.metadata_manager import (
                ContentMetadata,
                DBContentExtractor,
            )

            if found_content:
                found_content.__db_content__ = db_content
                final_download_set.add(found_content)
                download_media_count += len(found_content.medias)
            else:
                continue
                await db_content.awaitable_attrs.media
                content_metadata = ContentMetadata(db_content.id, api_type)
                extractor = DBContentExtractor(db_content)
                extractor.__api__ = api
                await content_metadata.resolve_extractor(extractor)
                content_metadata.__db_content__ = db_content
                final_download_set.add(content_metadata)
                total_media_count += len(db_content.media)
            pass

        global_settings = performer.get_api().get_global_settings()
        filesystem_manager = self.datascraper.filesystem_manager
        performer_directory_manager = filesystem_manager.get_directory_manager(
            performer.id
        )
        directory = performer_directory_manager.user.download_directory
        string = "Processing Download:\n"
        string += f"Name: {performer.username} | Type: {api_type} | Downloading: {download_media_count} | Total: {total_media_count} | Directory: {directory}\n"
        print(string)
        download_manager = DownloadManager(
            performer.get_authed(),
            filesystem_manager,
            final_download_set,
            global_settings.tools.reformatter.active,
        )
        _result = await download_manager.bulk_download()
        await site_db.session.commit()
        current_job.done = True
        return

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
