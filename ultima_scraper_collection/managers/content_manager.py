from itertools import chain
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from ultima_scraper_api.managers.session_manager import AuthedSession
    from ultima_scraper_collection.managers.metadata_manager.metadata_manager import (
        MediaMetadata,
    )


class DefaultCategorizedContent:
    def __init__(self) -> None:
        self.MassMessages: dict[int, dict[str, Any]] = {}
        self.Stories: dict[int, dict[str, Any]] = {}
        self.Posts: dict[int, dict[str, Any]] = {}
        self.Chats: dict[int, dict[str, Any]] = {}
        self.Messages: dict[int, dict[str, Any]] = {}
        self.Highlights: dict[int, dict[str, Any]] = {}

    def __iter__(self):
        for attr, value in self.__dict__.items():
            yield attr, value

    def find_content(self, content_id: int, content_type: str):
        return getattr(self, content_type)[content_id]


class ContentManager:
    def __init__(self, auth_session: "AuthedSession") -> None:
        self.auth_session = auth_session
        self.categorized = DefaultCategorizedContent()
        self.media_manager: MediaManager = MediaManager()

    def get_contents(self, content_type: str):
        return getattr(self.categorized, content_type)

    def set_content(self, content_type: str, scraped: list[Any]):
        for content in scraped:
            content_item = getattr(self.categorized, content_type)
            content_item[content.content_id] = content

    def find_content(self, content_id: int, content_type: str):
        found_content = None
        try:
            found_content = getattr(self.categorized, content_type)[content_id]
        except KeyError:
            pass
        return found_content

    def find_media(self, category: str, media_id: int):
        content_items = getattr(self.categorized, category)
        for content in content_items.values():
            for media in content.medias:
                if media.id == media_id:
                    return media

    def get_all_media_ids(self):
        return list(chain(*[x for x in self.categorized.__dict__.values()]))


class MediaManager:
    def __init__(self) -> None:
        self.medias: dict[int, "MediaMetadata"] = {}
        self.invalid_medias: list["MediaMetadata"] = []
