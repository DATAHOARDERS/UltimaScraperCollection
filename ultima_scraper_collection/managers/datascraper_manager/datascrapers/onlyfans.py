from pathlib import Path
from typing import Any, Union

from ultima_scraper_api.apis.onlyfans.classes.auth_model import create_auth
from ultima_scraper_api.apis.onlyfans.classes.hightlight_model import create_highlight
from ultima_scraper_api.apis.onlyfans.classes.message_model import create_message
from ultima_scraper_api.apis.onlyfans.classes.post_model import create_post
from ultima_scraper_api.apis.onlyfans.classes.story_model import create_story
from ultima_scraper_api.apis.onlyfans.classes.user_model import create_user
from ultima_scraper_api.apis.onlyfans.onlyfans import OnlyFansAPI
from ultima_scraper_renamer.reformat import ReformatManager

from ultima_scraper_collection.managers.metadata_manager.metadata_manager import (
    ContentMetadata,
)
from ultima_scraper_collection.managers.option_manager import OptionManager
from ultima_scraper_collection.modules.module_streamliner import StreamlinedDatascraper


class OnlyFansDataScraper(StreamlinedDatascraper):
    def __init__(self, api: OnlyFansAPI, option_manager: OptionManager) -> None:
        self.api = api
        self.option_manager = option_manager
        StreamlinedDatascraper.__init__(self, self)

    # Scrapes the API for content
    async def media_scraper(
        self,
        post_result: Union[create_story, create_post, create_message],
        subscription: create_user,
        formatted_directory: Path,
        api_type: str,
    ) -> dict[str, Any]:
        authed = subscription.get_authed()
        api = authed.api
        site_settings = api.get_site_settings()
        if not site_settings:
            return {}
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
        from ultima_scraper_collection.managers.metadata_manager.metadata_manager import (
            Extractor,
        )

        content_metadata = ContentMetadata(post_result.id, api_type)
        content_metadata.resolve_extractor(Extractor(post_result))
        matches = [
            s for s in site_settings.ignored_keywords if s in content_metadata.text
        ]
        if matches:
            print("Ignoring - ", f"PostID: {content_metadata.content_id}")
            return {}
        for asset in content_metadata.medias:
            if asset.urls:
                reformat_manager = ReformatManager(authed, self.filesystem_manager)
                reformat_item = reformat_manager.prepare_reformat(asset)
                file_directory = reformat_item.reformat(
                    site_settings.file_directory_format
                )
                reformat_item.directory = file_directory
                file_path = reformat_item.reformat(site_settings.filename_format)
                asset.directory = file_directory
                asset.filename = file_path.name

                if file_directory not in directories:
                    directories.append(file_directory)
        new_set["content"].append(content_metadata)
        new_set["directories"] = directories
        return new_set

    async def get_all_stories(self, subscription: create_user):
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

    async def get_all_posts(self, subscription: create_user):
        temp_master_set = await subscription.get_posts()
        # get_archived_posts uses normal posts
        await subscription.get_archived_posts()
        return temp_master_set

    async def get_all_subscriptions(
        self,
        authed: create_auth,
        identifiers: list[int | str] = [],
        refresh: bool = True,
    ):
        """
        get_all_subscriptions(authed: create_auth, identifiers: list[int | str] = [], refresh: bool = True)

        This function returns a list of all subscriptions from the given authenticated user.

        Arguments:
        authed (create_auth): An instance of the create_auth class.
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
