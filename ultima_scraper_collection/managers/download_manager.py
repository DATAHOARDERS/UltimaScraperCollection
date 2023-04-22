import asyncio
from pathlib import Path

from aiohttp import ClientResponse
from ultima_scraper_api.helpers import main_helper
from ultima_scraper_api.managers.session_manager import SessionManager
from ultima_scraper_collection.managers.database_manager.connections.sqlite.models.media_model import (
    TemplateMediaModel,
)
from ultima_scraper_collection.managers.filesystem_manager import FilesystemManager
from ultima_scraper_collection.managers.metadata_manager.metadata_manager import (
    ContentMetadata,
    MediaMetadata,
)


class DownloadManager:
    def __init__(
        self,
        filesystem_manager: FilesystemManager,
        session_manager: SessionManager,
        content_list: set[ContentMetadata] = set(),
        reformat: bool = True,
    ) -> None:
        self.filesystem_manager = filesystem_manager
        self.session_manager = session_manager
        self.content_list: set[ContentMetadata] = content_list
        self.errors: list[TemplateMediaModel] = []
        self.reformat = reformat

    async def bulk_download(self):
        final_list: list[MediaMetadata] = [
            self.download(media_item)
            for download_item in self.content_list
            for media_item in download_item.medias
        ]
        _result = await asyncio.gather(*final_list, return_exceptions=True)

    async def download(self, download_item: MediaMetadata):
        attempt = 0
        content_metadata = download_item.__content_metadata__
        db_content = content_metadata.__db_content__
        if not db_content or not download_item.id:
            return
        db_media = db_content.find_media(download_item.id)
        if not db_media:
            return
        while attempt < self.session_manager.max_attempts + 1:
            try:
                async with self.session_manager.semaphore:
                    result = await self.session_manager.request(download_item.urls[0])
                    if not download_item.directory:
                        raise Exception(
                            f"{download_item.id} has no directory\n {download_item}"
                        )
                    download_path = Path(
                        download_item.directory, download_item.filename
                    )
                    if result.status == 403:
                        attempt += 1
                        # test = await content_metadata._soft_.refresh()
                        continue
                    async with result as response:
                        download = await self.check(db_media, response)
                        if not download:
                            break
                        failed = await self.filesystem_manager.write_data(
                            response, download_path
                        )
                        if not failed:
                            timestamp = db_media.created_at.timestamp()
                            await main_helper.format_image(
                                download_path,
                                timestamp,
                                self.reformat,
                            )
                            db_media.size = response.content_length
                            db_media.downloaded = True
                            break
                        elif failed == 1:
                            # Server Disconnect Error
                            continue
                        elif failed == 2:
                            # Resource Not Found Error
                            break
                        pass
                    pass
            except asyncio.TimeoutError as e:
                pass
            except Exception as e:
                print(e)
                pass

    async def check(self, download_item: TemplateMediaModel, response: ClientResponse):
        filepath = Path(download_item.directory, download_item.filename)
        response_status = False
        if response.status == 200:
            response_status = True
            if response.content_length:
                download_item.size = response.content_length
            else:
                pass

        if filepath.exists():
            if filepath.stat().st_size == response.content_length:
                download_item.downloaded = True
            else:
                if download_item.downloaded:
                    return
                return download_item
        else:
            if response_status:
                # Can produce false positives due to the same reason below
                return download_item
            else:
                # Reached this point because it probably exists in the folder but under a different content category
                pass
