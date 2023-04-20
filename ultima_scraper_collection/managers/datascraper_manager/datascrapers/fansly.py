import os
from datetime import datetime
from pathlib import Path
from typing import Any, Union
from urllib.parse import urlparse

from ultima_scraper_api.apis.fansly.classes.auth_model import create_auth
from ultima_scraper_api.apis.fansly.classes.collection_model import create_collection
from ultima_scraper_api.apis.fansly.classes.message_model import create_message
from ultima_scraper_api.apis.fansly.classes.post_model import create_post
from ultima_scraper_api.apis.fansly.classes.story_model import create_story
from ultima_scraper_api.apis.fansly.classes.user_model import create_user
from ultima_scraper_api.apis.fansly.fansly import FanslyAPI

from ultima_scraper_collection.managers.metadata_manager.metadata_manager import (
    ContentMetadata,
)
from ultima_scraper_collection.managers.option_manager import OptionManager
from ultima_scraper_collection.modules.module_streamliner import StreamlinedDatascraper


class FanslyDataScraper(StreamlinedDatascraper):
    def __init__(self, api: FanslyAPI, option_manager: OptionManager) -> None:
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
    ):
        from ultima_scraper_renamer.reformat import prepare_reformat

        authed = subscription.get_authed()
        api = authed.api
        site_settings = api.get_site_settings()
        if not site_settings:
            return
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
        download_path = formatted_directory
        model_username = subscription.username
        date_format = site_settings.date_format
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
            return
        for asset in content_metadata.medias:
            filename = urlparse(asset.urls[0]).path.split("/")[-1]
            name, ext = filename.rsplit(".", 1)
            final_api_type = (
                os.path.join("Archived", api_type)
                if content_metadata.archived
                else api_type
            )
            option: dict[str, Any] = {}
            option = option | content_metadata.__dict__
            option["site_name"] = api.site_name
            option["media_id"] = asset.id
            option["filename"] = name
            option["api_type"] = final_api_type
            option["media_type"] = asset.media_type
            option["ext"] = ext
            option["profile_username"] = authed.username
            option["model_username"] = model_username
            option["date_format"] = date_format
            option["postedAt"] = asset.created_at
            option["text_length"] = site_settings.text_length
            option["directory"] = download_path
            option["preview"] = asset.preview
            option["archived"] = content_metadata.archived

            prepared_format = prepare_reformat(option)
            file_directory = await prepared_format.reformat_2(
                site_settings.file_directory_format
            )
            prepared_format.directory = file_directory
            file_path = await prepared_format.reformat_2(site_settings.filename_format)
            asset.directory = file_directory
            asset.filename = file_path.name
            if file_directory not in directories:
                directories.append(file_directory)
        new_set["content"].append(content_metadata)
        new_set["directories"] = directories
        return new_set
        locations = self.media_types
        for media_type, alt_media_types in locations.__dict__.items():
            date_today = datetime.now()
            master_date = datetime.strftime(date_today, "%d-%m-%Y %H:%M:%S")
            file_directory_format = site_settings.file_directory_format
            post_id = post_result.id
            new_post: dict[str, Any | dict[str, Any]] = {}
            new_post["medias"] = []
            new_post["archived"] = False
            rawText = ""
            text = ""
            _previews = []
            date = None
            price = None

            if isinstance(post_result, create_story):
                date = post_result.createdAt
            if isinstance(post_result, create_post):
                if post_result.isReportedByMe:
                    continue
                rawText = post_result.rawText
                text = post_result.text
                _previews = post_result.previews
                date = post_result.createdAt
                price = post_result.price
                new_post["archived"] = post_result.isArchived
            if isinstance(post_result, create_collection):
                if post_result.isReportedByMe:
                    continue
                rawText = post_result.rawText
                text = post_result.text
                _previews = post_result.preview
                date = post_result.createdAt
                price = post_result.price
                new_post["archived"] = post_result.isArchived
            if isinstance(post_result, create_message):
                if post_result.isReportedByMe:
                    continue
                text = post_result.text
                _previews = post_result.previews
                date = post_result.createdAt
                price = post_result.price
                if api_type == "Mass Messages":
                    media_user = post_result.fromUser
                    media_username = media_user.username
                    if media_username != model_username:
                        continue
            final_text = rawText if rawText else text

            if date == "-001-11-30T00:00:00+00:00":
                date_string = master_date
                date_object = datetime.strptime(master_date, "%d-%m-%Y %H:%M:%S")
            elif isinstance(date, int):
                timestamp = float(date)
                date_object = datetime.fromtimestamp(timestamp)
                date_string = date_object.replace(tzinfo=None).strftime(
                    "%d-%m-%Y %H:%M:%S"
                )
            else:
                if not date:
                    date = master_date
                date_object = datetime.fromisoformat(date)
                date_string = date_object.replace(tzinfo=None).strftime(
                    "%d-%m-%Y %H:%M:%S"
                )
                master_date = date_string
            new_post["post_id"] = post_id
            new_post["user_id"] = subscription.id
            if isinstance(post_result, create_message):
                new_post["user_id"] = post_result.fromUser.id

            new_post["text"] = final_text
            new_post["postedAt"] = date_string
            new_post["paid"] = False
            new_post["preview_media_ids"] = [int(d["id"]) for d in post_result.previews]
            new_post["api_type"] = api_type
            new_post["price"] = 0
            if price is None:
                price = 0
            if price:
                if all(media["canView"] for media in post_result.media):
                    new_post["paid"] = True

            new_post["price"] = price
            # We can store previews and accesible media in .media
            for media in post_result.media:
                media_id = media["id"]
                preview_link = ""
                link = await post_result.link_picker(media, site_settings.video_quality)
                matches = ["us", "uk", "ca", "ca2", "de"]

                if not link:
                    continue
                url = urlparse(link)
                if not url.hostname:
                    continue
                new_media: dict[str, Any | dict[str, Any]] = dict()
                new_media["media_id"] = media_id
                new_media["links"] = []
                new_media["media_type"] = media_type
                new_media["preview"] = False
                new_media["created_at"] = new_post["postedAt"]
                if isinstance(post_result, create_story):
                    date_object = datetime.fromisoformat(media["createdAt"])
                    date_string = date_object.replace(tzinfo=None).strftime(
                        "%d-%m-%Y %H:%M:%S"
                    )
                    new_media["created_at"] = date_string
                if int(media_id) in new_post["preview_media_ids"]:
                    new_media["preview"] = True
                for xlink in link, preview_link:
                    if xlink:
                        new_media["links"].append(xlink)
                        break
                mime_type: str = media["mimetype"].split("/")[0]
                if mime_type not in alt_media_types:
                    continue
                matches = [s for s in site_settings.ignored_keywords if s in final_text]
                if matches:
                    print("Matches: ", matches)
                    continue
                filename = link.rsplit("/", 1)[-1]
                filename, ext = os.path.splitext(filename)
                ext = ext.__str__().replace(".", "").split("?")[0]
                final_api_type = (
                    os.path.join("Archived", api_type)
                    if new_post["archived"]
                    else api_type
                )
                option: dict[str, Any] = {}
                option = option | new_post
                option["site_name"] = api.site_name
                option["media_id"] = media_id
                option["filename"] = filename
                option["api_type"] = final_api_type
                option["media_type"] = media_type
                option["ext"] = ext
                option["profile_username"] = authed.username
                option["model_username"] = model_username
                option["date_format"] = date_format
                option["postedAt"] = new_media["created_at"]
                option["text_length"] = site_settings.text_length
                option["directory"] = download_path
                option["preview"] = new_media["preview"]
                option["archived"] = new_post["archived"]

                prepared_format = prepare_reformat(option)
                file_directory = await prepared_format.reformat_2(file_directory_format)
                prepared_format.directory = file_directory
                file_path = await prepared_format.reformat_2(
                    site_settings.filename_format
                )
                new_media["directory"] = os.path.join(file_directory)
                new_media["filename"] = os.path.basename(file_path)
                if file_directory not in directories:
                    directories.append(file_directory)
                new_media["linked"] = None
                for k, v in subscription.temp_scraped:
                    if k == api_type:
                        continue
                    if k == "Archived":
                        v = getattr(v, api_type, [])
                    if v:
                        for post in v:
                            found_medias: list[dict[str, Any]] = []
                            medias = post.media
                            if medias:
                                for temp_media in medias:
                                    temp_filename = temp_media.get("filename")
                                    if temp_filename:
                                        if temp_filename == new_media["filename"]:
                                            found_medias.append(temp_media)
                                    else:
                                        continue
                            if found_medias:
                                for found_media in found_medias:
                                    found_media["linked"] = api_type
                                new_media["linked"] = post["api_type"]
                                new_media[
                                    "filename"
                                ] = f"linked_{new_media['filename']}"
                new_post["medias"].append(new_media)
            found_post = [x for x in new_set["content"] if x["post_id"] == post_id]
            if found_post:
                found_post = found_post[0]
                found_post["medias"] += new_post["medias"]
            else:
                new_set["content"].append(new_post)
        new_set["directories"] = directories
        return new_set

    async def get_all_stories(self, subscription: create_user):
        """
        get_all_stories(subscription: create_user)

        This function returns a list of all stories and archived stories from the given subscription.

        Arguments:
        subscription (create_user): An instance of the create_user class.

        Returns:
        list[create_story]: A list containing all stories and archived stories from the subscription.
        """
        master_set: list[create_story] = []
        master_set.extend(await subscription.get_stories())
        # master_set.extend(await subscription.get_archived_stories())
        return master_set

    async def get_all_posts(self, subscription: create_user):
        temp_master_set = await subscription.get_posts()
        collections = await subscription.get_collections()
        for collection in collections:
            temp_master_set.append(
                await subscription.get_collection_content(collection)
            )
        return temp_master_set

    async def get_all_subscriptions(
        self,
        authed: create_auth,
        identifiers: list[int | str] = [],
        refresh: bool = True,
    ):
        """
        get_all_subscriptions(authed: create_auth, identifiers: list[int | str] = [], refresh: bool = True)

        This function returns a list of all subscriptions, including both subscriptions and followings,
        from the given authenticated user.

        Arguments:
        authed (create_auth): An instance of the create_auth class.
        identifiers (list[int | str], optional): A list of identifiers (username or id) for the subscriptions. Defaults to an empty list.
        refresh (bool, optional): A flag indicating whether to refresh the list of subscriptions. Defaults to True.

        Returns:
        list[create_subscription]: A list of all subscriptions, including both subscriptions and followings, from the authenticated user.
        """
        results = await authed.get_followings(identifiers=identifiers)
        results2 = await authed.get_subscriptions(
            identifiers=identifiers, refresh=refresh
        )
        for result2 in results2:
            for found in [x for x in results if x.username == result2.username]:
                result2.subscribedByData = found.subscribedByData
                results.remove(found)
        results.extend(results2)
        results = [x for x in results if x.subscribedByData or x.following]
        results.sort(key=lambda x: x.subscribedByData["endsAt"])
        return results
