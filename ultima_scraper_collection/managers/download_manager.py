import asyncio
from pathlib import Path

from aiohttp import ClientResponse
from ultima_scraper_api.helpers import main_helper
from ultima_scraper_api.managers.session_manager import SessionManager

from ultima_scraper_collection.managers.database_manager.connections.sqlite.models.media_model import (
    TemplateMediaModel,
)
from ultima_scraper_collection.managers.filesystem_manager import FilesystemManager


class DownloadManager:
    def __init__(
        self,
        filesystem_manager: FilesystemManager,
        session_manager: SessionManager,
        download_list: set[TemplateMediaModel] = set(),
        reformat: bool = True,
    ) -> None:
        self.filesystem_manager = filesystem_manager
        self.session_manager = session_manager
        self.download_list: set[TemplateMediaModel] = download_list
        self.errors: list[TemplateMediaModel] = []
        self.reformat = reformat

    async def bulk_download(self):
        _result = await asyncio.gather(
            *[self.download(x) for x in self.download_list], return_exceptions=True
        )
        pass

    async def download(self, download_item: TemplateMediaModel):
        while True:
            result = await self.session_manager.request(download_item.link)
            download_path = Path(download_item.directory, download_item.filename)
            async with self.session_manager.semaphore:
                async with result as response:
                    download = await self.check(download_item, response)
                    if not download:
                        break
                    failed = await self.filesystem_manager.write_data(
                        response, download_path
                    )
                    if not failed:
                        timestamp = download_item.created_at.timestamp()
                        await main_helper.format_image(
                            download_path,
                            timestamp,
                            self.reformat,
                        )
                        download_item.size = response.content_length
                        download_item.downloaded = True
                        break
                    elif failed == 1:
                        # Server Disconnect Error
                        continue
                    elif failed == 2:
                        # Resource Not Found Error
                        break
                    pass
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
