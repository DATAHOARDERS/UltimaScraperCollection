from pathlib import Path
from typing import TYPE_CHECKING, Any

from ultima_scraper_api.apis.fansly.fansly import FanslyAPI
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
    from ultima_scraper_api.apis.fansly.classes.auth_model import AuthModel
    from ultima_scraper_api.apis.fansly.classes.message_model import MessageModel
    from ultima_scraper_api.apis.fansly.classes.post_model import PostModel
    from ultima_scraper_api.apis.fansly.classes.story_model import StoryModel
    from ultima_scraper_api.apis.fansly.classes.user_model import UserModel


class FanslyDataScraper(StreamlinedDatascraper):
    def __init__(
        self,
        api: FanslyAPI,
        option_manager: OptionManager,
        server_manager: ServerManager,
        site_config: Sites.FanslyAPIConfig,
    ) -> None:
        self.api = api
        self.option_manager = option_manager
        self.site_config = site_config
        StreamlinedDatascraper.__init__(self, self, server_manager)

    # Scrapes the API for content
    async def media_scraper(
        self,
        content_result: "StoryModel | PostModel | MessageModel",
        subscription: "UserModel",
        api_type: str,
    ) -> dict[str, Any]:
        authed = subscription.get_authed()
        site_config = self.site_config
        new_set: dict[str, Any] = {"content": []}
        directories: list[Path] = []
        if api_type == "Stories":
            pass
        if api_type == "Archived":
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

    async def get_all_stories(self, subscription: "UserModel"):
        """
        get_all_stories(subscription: UserModel)

        This function returns a list of all stories and archived stories from the given subscription.

        Arguments:
        subscription (UserModel): An instance of the UserModel class.

        Returns:
        list[StoryModel]: A list containing all stories and archived stories from the subscription.
        """
        master_set: list["StoryModel"] = []
        master_set.extend(await subscription.get_stories())
        # master_set.extend(await subscription.get_archived_stories())
        return master_set

    async def get_all_posts(self, subscription: "UserModel"):
        temp_master_set = await subscription.get_posts()
        collections = await subscription.get_collections()
        for collection in collections:
            temp_master_set.append(
                await subscription.get_collection_content(collection)
            )
        return temp_master_set

    async def get_all_subscriptions(
        self,
        authed: "AuthModel",
        identifiers: list[int | str] = [],
        refresh: bool = True,
    ):
        """
        get_all_subscriptions(authed: AuthModel, identifiers: list[int | str] = [], refresh: bool = True)

        This function returns a list of all subscriptions, including both subscriptions and followings,
        from the given authenticated user.

        Arguments:
        authed (AuthModel): An instance of the AuthModel class.
        identifiers (list[int | str], optional): A list of identifiers (username or id) for the subscriptions. Defaults to an empty list.
        refresh (bool, optional): A flag indicating whether to refresh the list of subscriptions. Defaults to True.

        Returns:
        list[create_subscription]: A list of all subscriptions, including both subscriptions and followings, from the authenticated user.
        """
        authed.followed_users = await authed.get_followings(identifiers=identifiers)
        subscriptions = await authed.get_subscriptions(
            identifiers=identifiers, refresh=refresh, sub_type="active"
        )
        subscriptions.sort(key=lambda x: x.ends_at)
        return subscriptions
