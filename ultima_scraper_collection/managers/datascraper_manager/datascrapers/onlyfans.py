import asyncio
from pathlib import Path
from typing import TYPE_CHECKING, Any

from ultima_scraper_api.apis.onlyfans.onlyfans import OnlyFansAPI
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
    from ultima_scraper_api.apis.onlyfans.classes.auth_model import AuthModel
    from ultima_scraper_api.apis.onlyfans.classes.hightlight_model import (
        create_highlight,
    )
    from ultima_scraper_api.apis.onlyfans.classes.message_model import create_message
    from ultima_scraper_api.apis.onlyfans.classes.post_model import create_post
    from ultima_scraper_api.apis.onlyfans.classes.story_model import create_story
    from ultima_scraper_api.apis.onlyfans.classes.user_model import create_user


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
        content_result: "create_story | create_post | create_message",
        subscription: "create_user",
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

        content_metadata = ContentMetadata(content_result.id, api_type)
        await content_metadata.resolve_extractor(ApiExtractor(content_result))
        for asset in content_metadata.medias:
            if asset.urls:
                reformat_manager = ReformatManager(authed, self.filesystem_manager)
                reformat_item = reformat_manager.prepare_reformat(asset)
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
        return new_set

    async def get_all_stories(self, subscription: "create_user"):
        """
        get_all_stories(subscription: create_user)

        This function returns a list of all stories and highlights from the given subscription.

        Arguments:
        subscription (create_user): An instance of the create_user class.

        Returns:
        list[create_highlight | create_story]: A list containing all stories and highlights from the subscription.
        """
        master_set: list[create_highlight | create_story] = []
        master_set.extend(await subscription.get_stories())
        master_set.extend(await subscription.get_archived_stories())
        highlights = await subscription.get_highlights()
        valid_highlights: list[create_highlight | create_story] = []
        for highlight in highlights:
            resolved_highlight = await subscription.get_highlights(
                hightlight_id=highlight.id
            )
            valid_highlights.extend(resolved_highlight)
        master_set.extend(valid_highlights)
        return master_set

    async def get_all_posts(self, subscription: "create_user"):
        temp_master_set = await subscription.get_posts()

        # get_archived_posts uses normal posts
        await subscription.get_archived_posts()
        await asyncio.gather(*[x.get_comments() for x in temp_master_set])
        return temp_master_set

    async def get_all_subscriptions(
        self,
        authed: "AuthModel",
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
