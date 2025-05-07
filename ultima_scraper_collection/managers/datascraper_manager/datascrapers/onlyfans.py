import asyncio
from datetime import datetime, timezone
from pathlib import Path
from typing import TYPE_CHECKING, Any

from sqlalchemy import select
from sqlalchemy.orm import joinedload
from ultima_scraper_api.apis.onlyfans.classes.mass_message_model import MassMessageModel
from ultima_scraper_api.apis.onlyfans.onlyfans import OnlyFansAPI
from ultima_scraper_db.databases.ultima_archive.schemas.templates.site import PostModel
from ultima_scraper_renamer.reformat import ReformatManager

from ultima_scraper_collection.config import Sites
from ultima_scraper_collection.managers.metadata_manager.metadata_manager import (
    ApiExtractor,
    ContentMetadata,
)
from ultima_scraper_collection.managers.option_manager import OptionManager
from ultima_scraper_collection.managers.server_manager import ServerManager
from ultima_scraper_collection.modules.module_streamliner import StreamlinedDatascraper

if TYPE_CHECKING:
    from ultima_scraper_api.apis.onlyfans.classes.auth_model import OnlyFansAuthModel
    from ultima_scraper_api.apis.onlyfans.classes.hightlight_model import (
        HighlightModel,
    )
    from ultima_scraper_api.apis.onlyfans.classes.message_model import MessageModel
    from ultima_scraper_api.apis.onlyfans.classes.post_model import PostModel
    from ultima_scraper_api.apis.onlyfans.classes.story_model import StoryModel
    from ultima_scraper_api.apis.onlyfans.classes.user_model import UserModel


class OnlyFansDataScraper(StreamlinedDatascraper):
    def __init__(
        self,
        api: OnlyFansAPI,
        option_manager: OptionManager,
        server_manager: ServerManager,
        site_config: Sites.OnlyFansAPIConfig,
    ) -> None:
        self.api = api
        self.option_manager = option_manager
        self.site_config = site_config
        StreamlinedDatascraper.__init__(self, self, server_manager)

    # Scrapes the API for content
    async def media_scraper(
        self,
        content_result: "StoryModel | PostModel | MessageModel|MassMessageModel",
        subscription: "UserModel",
        api_type: str,
    ) -> dict[str, Any]:
        api_type = self.api.convert_api_type_to_key(content_result)
        authed = subscription.get_authed()
        site_config = self.site_config
        new_set: dict[str, Any] = {"content": []}
        directories: list[Path] = []
        if api_type == "Stories":
            pass
        if api_type == "Posts":
            pass
        if api_type == "Messages":
            pass
        content_metadata = ContentMetadata(
            content_result.id, api_type, self.resolve_content_manager(subscription)
        )

        try:
            await content_metadata.resolve_extractor(ApiExtractor(content_result))
            for asset in content_metadata.medias:
                if asset.urls:
                    reformat_manager = ReformatManager(authed, self.filesystem_manager)
                    reformat_item = reformat_manager.prepare_reformat(asset)
                    if reformat_item.api_type == "Messages":
                        if (
                            content_metadata.queue_id
                            and content_metadata.__soft__.is_mass_message()
                        ):
                            reformat_item.api_type = "MassMessages"
                    file_directory = reformat_item.reformat(
                        site_config.download_setup.directory_format
                    )
                    reformat_item.directory = file_directory
                    file_path = reformat_item.reformat(
                        site_config.download_setup.filename_format
                    )
                    asset.directory = file_directory
                    asset.filename = file_path.name
                    if file_directory not in directories:
                        directories.append(file_directory)
            new_set["content"].append(content_metadata)
            new_set["directories"] = directories
        except Exception as e:
            print(e)
            pass
        return new_set

    async def get_all_stories(self, subscription: "UserModel"):
        """
        get_all_stories(subscription: UserModel)

        This function returns a list of all stories and highlights from the given subscription.

        Arguments:
        subscription (UserModel): An instance of the UserModel class.

        Returns:
        list[create_highlight | StoryModel]: A list containing all stories and highlights from the subscription.
        """
        master_set: list[HighlightModel | StoryModel] = []
        master_set.extend(await subscription.get_stories())
        master_set.extend(await subscription.get_archived_stories())
        highlights = await subscription.get_highlights()
        valid_highlights: list[HighlightModel | StoryModel] = []
        for highlight in highlights:
            resolved_highlight = await subscription.get_highlights(
                hightlight_id=highlight.id
            )
            valid_highlights.extend(resolved_highlight)
        master_set.extend(valid_highlights)
        return master_set

    async def get_all_posts(self, performer: "UserModel") -> list["PostModel"]:
        async with self.get_archive_db_api().create_site_api(
            performer.get_api().site_name
        ) as db_site_api:
            after_date = None
            # db_performer = await db_site_api.get_user(performer.id)
            # await db_performer.awaitable_attrs._posts
            # result = await db_performer.last_subscription_downloaded_at()
            # if result:
            #     after_date = result.downloaded_at

            posts = await performer.get_posts(after_date=after_date)
            archived_posts = await performer.get_posts(label="archived")
            private_archived_posts = await performer.get_posts(label="private_archived")
            session = db_site_api.get_session()
            posts_with_comments = (
                select(PostModel)
                .options(joinedload(PostModel.comments))
                .filter(PostModel.comments.any())
                .where(PostModel.user_id == performer.id)
                .order_by(PostModel.created_at.desc())
            )
            results = await session.scalars(posts_with_comments)
            db_posts = results.unique().all()
            threshold_date = (
                db_posts[0].created_at
                if db_posts
                else datetime.min.replace(tzinfo=timezone.utc)
            )
            latest_post_dates = [x.created_at for x in db_posts if x.comments]
            threshold_date = (
                latest_post_dates[-1]
                if latest_post_dates
                else datetime.min.replace(tzinfo=timezone.utc)
            )
            tasks = [
                x.get_comments()
                for x in performer.scrape_manager.scraped.Posts.values()
                if x.created_at > threshold_date
            ]
            if len(tasks) > 800:
                tasks = tasks[: len(tasks) // 4]
            # await asyncio.gather(*tasks)
            return posts + archived_posts + private_archived_posts

    async def get_all_subscriptions(
        self,
        authed: "OnlyFansAuthModel",
        identifiers: list[int | str] = [],
        refresh: bool = True,
    ):
        """
        get_all_subscriptions(authed: AuthModel, identifiers: list[int | str] = [], refresh: bool = True)

        This function returns a list of all subscriptions from the given authenticated user.

        Arguments:
        authed (AuthModel): An instance of the AuthModel class.
        identifiers (list[int | str], optional): A list of identifiers (username or id) for the subscriptions. Defaults to an empty list.
        refresh (bool, optional): A flag indicating whether to refresh the list of subscriptions. Defaults to True.

        Returns:
        list[create_subscription]: A list of all subscriptions, sorted by expiredAt, from the authenticated user.
        """
        subscriptions = await authed.get_subscriptions(
            identifiers=identifiers, refresh=refresh, sub_type="active"
        )
        subscriptions.sort(key=lambda x: x.subscribed_by_expire_date)
        return subscriptions
