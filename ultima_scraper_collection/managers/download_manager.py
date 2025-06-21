import asyncio
import copy
import os
import shutil
import traceback
from pathlib import Path
from typing import TYPE_CHECKING, Any
from urllib.parse import urlparse

import aiofiles
import ffmpeg
import ultima_scraper_api
from aiohttp import ClientResponse
from rich.progress import (
    BarColumn,
    Progress,
    SpinnerColumn,
    TextColumn,
    TimeElapsedColumn,
)
from tqdm import tqdm
from ultima_scraper_api import auth_types
from ultima_scraper_api.apis.onlyfans.classes.mass_message_model import MassMessageModel
from ultima_scraper_api.helpers import main_helper
from ultima_scraper_api.managers.session_manager import EXCEPTION_TEMPLATE
from ultima_scraper_db.databases.ultima_archive.schemas.templates.site import (
    MediaModel,
    MessageModel,
)
from ultima_scraper_renamer.reformat import ReformatManager

from ultima_scraper_collection.managers.database_manager.connections.sqlite.models.media_model import (
    TemplateMediaModel,
)
from ultima_scraper_collection.managers.filesystem_manager import FilesystemManager
from ultima_scraper_collection.managers.metadata_manager.metadata_manager import (
    MediaMetadata,
    invalid_subdomains,
)
from ultima_scraper_collection.managers.resource_manager import ResourceManager

if TYPE_CHECKING:
    from dashboard import WorkerDashboardApp

import time

user_types = ultima_scraper_api.user_types


class DownloadItem:
    def __init__(self, media_item: MediaMetadata, download_manager: "DownloadManager"):
        self.authed = media_item.get_author().get_authed()
        self.download_manager = download_manager
        self.resource_manager = download_manager.resource_manager
        self.media_item = media_item
        self.drm = self.media_item.drm
        self.request_session = (
            media_item.get_author().get_authed().auth_session.active_session
        )
        self.drm_check_downloaded_task = asyncio.create_task(self.resolve_drm_files())
        self.head_task = asyncio.create_task(self._head_request())
        self.drm_file_exists: bool | None = None
        self.head_request: ClientResponse | None = None
        self.drm_media_items: list[MediaMetadata] = []
        self.drm_download_paths: list[Path] = []

    def calculate_parts(self, file_size: int) -> int:
        """
        Determine how many parts to split a file into.

        - file_size: bytes
        - max_total_conns: current total allowed system concurrency
        - buffer_size: bytes per part download

        Returns number of parts for this file.
        """
        max_total_conns = self.resource_manager.get_current_concurrency()
        buffer_size = self.resource_manager.get_current_buffer_size()
        if file_size <= buffer_size:
            return 1
        max_parts_possible = -(
            -file_size // buffer_size
        )  # Ceiling division to cover all bytes
        max_parts_possible = max(1, max_parts_possible)

        # Now limit it to avoid oversplitting beyond system concurrency
        parts = min(max_parts_possible, max_total_conns)

        return parts

    async def resolve_drm_files(self):
        if not self.drm:
            return
        download_item = self.media_item
        content_metadata = download_item.__content_metadata__
        authed = self.authed
        reformat_manager = ReformatManager(
            authed, self.download_manager.filesystem_manager
        )
        assert reformat_manager.filesystem_manager.directory_manager
        site_config = reformat_manager.filesystem_manager.directory_manager.site_config
        drm = authed.drm
        media_item = download_item.__raw__
        assert drm and media_item
        mpd = await drm.get_mpd(media_item)
        pssh = await drm.get_pssh(mpd)
        # responses: list[ClientResponse] = []

        if pssh:
            if content_metadata:
                soft_data = content_metadata.__soft__
                raw_data = soft_data.__raw__.copy()
                if (
                    isinstance(soft_data, MassMessageModel)
                    and soft_data
                    and soft_data.author.is_authed_user()
                ):
                    raw_data["responseType"] = ""
            else:
                raw_data = {"responseType": ""}
            license = await drm.get_license(raw_data, media_item, pssh)
            keys = await drm.get_keys(license)
            content_key = keys[-1]
            key = f"{content_key.kid.hex}:{content_key.key.hex()}"
            download_item.key = key
            video_url, audio_url = [
                drm.get_video_url(mpd, media_item),
                drm.get_audio_url(mpd, media_item),
            ]
            download_item.urls = [video_url]
            reformat_item = reformat_manager.prepare_reformat(download_item)
            file_directory = reformat_item.reformat(
                site_config.download_setup.directory_format
            )
            reformat_item.directory = file_directory
            file_path = reformat_item.reformat(
                site_config.download_setup.filename_format
            )
            download_item.directory = file_directory
            download_item.filename = file_path.name
            for media_url in video_url, audio_url:
                drm_download_item = copy.copy(download_item)
                drm_download_item = reformat_manager.drm_format(
                    media_url, drm_download_item
                )
                self.drm_media_items.append(drm_download_item)

        #         signature_str = await drm.get_signature(media_item)
        #         response = await authed.auth_session.request(
        #             media_url, premade_settings="", custom_cookies=signature_str
        #         )
        #         responses.append(response)
        # return responses
        return self.drm_media_items

    async def _head_request(self):
        parsed_url = urlparse(self.media_item.urls[0])
        main_url = parsed_url.geturl()
        assert parsed_url.hostname
        subdomain = parsed_url.hostname.split(".")[0]
        if any(
            subdomain in invalid_subdomain for invalid_subdomain in invalid_subdomains
        ):
            return False
        if self.drm:
            drm = self.authed.drm
            assert drm
            signature_str = await drm.get_signature(self.media_item.__raw__)
            abc = await self.authed.auth_session.request(
                main_url,
                "HEAD",
                premade_settings="",
                custom_cookies=signature_str,
            )
            pass
        else:
            abc = await self.authed.auth_session.request(main_url, "HEAD")
        self.head_request = abc
        if abc.status == 200:
            return self.head_request
        else:
            return False

    async def download_part(
        self,
        media_metadata: MediaMetadata,
        start_byte: int,
        end_byte: int,
    ):
        async with self.download_manager.semaphore:
            original_filepath = media_metadata.get_filepath()
            parts_folder = original_filepath.parent.joinpath("__parts__")
            os.makedirs(parts_folder, exist_ok=True)
            ext = original_filepath.suffix
            download_path = parts_folder.joinpath(
                f"{original_filepath.stem}_{start_byte}_{end_byte}{ext}"
            )
            temp_download_path = Path(f"{download_path}.part")

            authed = self.authed
            max_http_attempts = 10
            http_attempts = 0

            while True:
                existing_size = (
                    temp_download_path.stat().st_size
                    if temp_download_path.exists()
                    else 0
                )
                if existing_size == end_byte:
                    # If the existing file is already complete, return it
                    await aiofiles.os.rename(
                        str(temp_download_path), str(download_path)
                    )
                    return download_path

                headers = {"Range": f"bytes={start_byte+existing_size}-{end_byte}"}

                signature_str = ""
                if media_metadata.drm:
                    assert authed.drm
                    signature_str = await authed.drm.get_signature(
                        media_metadata.__raw__
                    )
                    if download_path not in self.drm_download_paths:
                        self.drm_download_paths.append(download_path)

                response = await authed.auth_session.request(
                    media_metadata.urls[0],
                    premade_settings="",
                    custom_cookies=signature_str,
                    range_header=headers,
                )

                total_length = existing_size
                expected_total_size = existing_size + (response.content_length or 0)
                status_code = 0

                if response.status in (200, 206):
                    try:
                        async with aiofiles.open(temp_download_path, mode="ab") as f:
                            buffer = bytearray()

                            async for data in response.content.iter_chunked(65536):
                                buffer.extend(data)
                                total_length += len(data)
                                resource_buffer = (
                                    self.resource_manager.get_current_buffer_size()
                                )
                                if len(buffer) >= resource_buffer:
                                    await f.write(buffer)
                                    buffer.clear()

                            if buffer:
                                await f.write(buffer)

                        if expected_total_size and total_length != expected_total_size:
                            status_code = 1  # download incomplete
                            await aiofiles.os.remove(str(temp_download_path))
                        else:
                            await aiofiles.os.rename(
                                str(temp_download_path), str(download_path)
                            )

                    except EXCEPTION_TEMPLATE as _e:
                        status_code = 1
                    except Exception as _e:
                        raise Exception(
                            f"Unknown Error: {_e} | Username: {authed.username}"
                        )

                else:
                    status_code = 2  # HTTP error

                await response.release()

                if status_code == 0:
                    return download_path  # success

                if status_code == 1:
                    # immediate retry
                    continue

                if status_code == 2:
                    http_attempts += 1
                    if http_attempts >= max_http_attempts:
                        return status_code  # give up after max attempts

                    await asyncio.sleep(1)  # backoff could be added here if desired

    async def download(self):
        resource_manager = self.resource_manager
        download_path = self.media_item.get_filepath()
        temp_download_path = Path(f"{download_path}.part")
        existing_size = 0
        if temp_download_path.exists():
            existing_size = temp_download_path.stat().st_size
        result = await self.head_task
        if not result:
            await self.download_manager.increase_completed()

            return False
        assert result
        if download_path.exists():
            local_size = download_path.stat().st_size
            if local_size == result.content_length:

                await self.download_manager.increase_completed()

                return True

        file_size = result.content_length or 0
        # file_size = 1701850177 * 2 or 0
        num_parts = self.calculate_parts(file_size)
        download_paths: list[Path] = []
        part_tasks: list[asyncio.Task[Any]] = []
        part_ranges: list[tuple[int, int]] = []
        part_buffer_size = resource_manager.get_current_buffer_size()
        for num_part in range(num_parts):
            start_byte = num_part * part_buffer_size + existing_size
            end_byte = min(start_byte + part_buffer_size - 1, file_size - 1)
            part_ranges.append((start_byte, end_byte))
            part_tasks.append(
                asyncio.create_task(
                    self.download_part(self.media_item, start_byte, end_byte)
                )
            )

        download_paths = await asyncio.gather(*part_tasks)
        dest_path = self.media_item.get_filepath()
        if len(download_paths) == 1:
            # If only one part, just rename it to the final destination
            if download_paths[0] is not None:
                await aiofiles.os.rename(str(download_paths[0]), str(dest_path))
            await self.download_manager.increase_completed()
            return True

        def merge_video_parts(
            dest_path: Path,
            download_paths: list[Path],
            buffer_size: int = 64 * 1024 * 1024,
        ):
            with open(dest_path, "wb") as final_file:
                for temp_path in download_paths:
                    with open(temp_path, "rb") as part_file:
                        shutil.copyfileobj(part_file, final_file, length=buffer_size)
                    os.remove(temp_path)

        await self.download_manager.increase_completed()

        await asyncio.to_thread(
            merge_video_parts,
            dest_path,
            download_paths,
            self.download_manager.resource_manager.get_current_buffer_size(),
        )

        # async with aiofiles.open(dest_path, "wb") as final_file:
        #     for temp_path in download_paths:
        #         async with aiofiles.open(temp_path, "rb") as part_file:
        #             while True:
        #                 data = await part_file.read(
        #                     self.download_manager.resource_manager.get_current_buffer_size()
        #                 )
        #                 if not data:
        #                     break
        #                 await final_file.write(data)
        #         await aiofiles.os.remove(str(temp_path))
        return True

    async def get_remote_file_size(self, url: str) -> int:

        assert self.authed.drm
        signature_str = await self.authed.drm.get_signature(self.media_item.__raw__)
        resp = await self.authed.auth_session.request(
            url, premade_settings="", custom_cookies=signature_str
        )
        if resp.status != 200:
            raise Exception(
                f"Failed to fetch headers for {url}, status code: {resp.status}"
            )
        content_length = resp.headers.get("Content-Length")
        if content_length is None:
            raise Exception(f"No Content-Length header for {url}")
        return int(content_length)

    async def download_drm_files(self):
        download_tasks: list[asyncio.Task[Any]] = []
        download_path = self.media_item.get_filepath()
        if download_path.exists():
            dashboard = self.download_manager.dashboard
            if dashboard:
                completed = self.download_manager.completed
                completed += 1
                total = self.download_manager.total
                dashboard.set_download_task(
                    self.download_manager.worker_id,
                    (completed / total) * 100 if total else 100,
                    f"{self.download_manager.completed}/{self.download_manager.total}",
                )
            return True
        for drm_item in self.drm_media_items:
            file_size = await self.get_remote_file_size(
                drm_item.urls[0]
            )  # implement this
            task = self.download_part(drm_item, 0, file_size)
            download_tasks.append(task)
        await asyncio.gather(*download_tasks)
        return True


class DownloadManager:
    def __init__(
        self,
        authed: auth_types,
        filesystem_manager: FilesystemManager,
        media_set: set[MediaMetadata] = set(),
        reformat: bool = True,
        dashboard: "WorkerDashboardApp | None" = None,  # Add dashboard argument
        worker_id: int = 0,  # Add worker_id argument
        resource_manager: ResourceManager = ResourceManager(),
    ) -> None:
        self.authed = authed
        self.filesystem_manager = filesystem_manager
        self.resource_manager = resource_manager
        self.auth_session = self.authed.auth_session
        self.requester = self.authed.get_requester()
        self.content_list: set[MediaMetadata] = media_set
        self.errors: list[TemplateMediaModel] = []
        self.reformat = reformat
        self.reformat_manager = ReformatManager(self.authed, filesystem_manager)
        self.dashboard: "WorkerDashboardApp|None" = dashboard
        self.worker_id = worker_id
        self.total = 0
        self.completed = 0
        self.semaphore = asyncio.Semaphore(64)

    async def increase_completed(self):
        self.completed += 1
        if self.dashboard:
            total = self.total
            self.dashboard.set_download_task(
                self.worker_id,
                (self.completed / total) * 100 if total else 100,
                f"{self.completed}/{total}",
            )

    async def check_total_size(self):
        async def get_file_size(file: MediaMetadata):
            if len(file.urls) > 1:
                return 0  # Skip files with multiple URLs as in your logic
            response = await self.auth_session.active_session.head(file.urls[0])
            return response.content_length if response.content_length else 0

        tasks = [get_file_size(file) for file in self.content_list]
        sizes = await asyncio.gather(*tasks)
        return sum(sizes)

    async def process_download_item(self, download_item: DownloadItem):
        db_media = download_item.media_item.__db_media__
        assert db_media
        content = download_item.media_item.get_content_metadata()
        if content:
            db_content = content.__db_content__
            assert db_content
            if isinstance(db_content, MessageModel):
                if db_content.queue_id:
                    db_filepath = db_media.find_filepath(
                        (db_content.queue_id, "MassMessages")
                    )
                else:
                    db_filepath = db_media.find_filepath()
            else:
                db_filepath = db_media.find_filepath()
        else:
            db_filepath = db_media.find_filepath()
        assert download_item.media_item.directory
        assert download_item.media_item.filename
        assert db_media.created_at
        final_filepath = download_item.media_item.directory.joinpath(
            download_item.media_item.filename
        )
        if final_filepath.exists():
            final_size = final_filepath.stat().st_size
            timestamp = db_media.created_at.timestamp()
            await main_helper.format_file(
                final_filepath,
                timestamp,
                self.reformat,
            )
            if db_media and db_filepath:
                if not db_filepath.preview:
                    db_media.size = download_item.media_item.size = final_size
                else:
                    if final_size > db_media.size:
                        db_media.size = final_size
                db_filepath.downloaded = True

    async def bulk_download(
        self,
        performer: user_types,
        api_type: str,
        download_media_count: int,
        total_media_count: int,
    ):
        queue: asyncio.Queue[tuple[Path, list[Path], asyncio.Future[Any]]] = (
            asyncio.Queue()
        )
        worker_task = asyncio.create_task(self.ffmpeg_worker(queue))
        download_items = [DownloadItem(x, self) for x in self.content_list]
        self.total = len(download_items)

        # Prepare download tasks
        download_tasks: list[asyncio.Task[Any]] = []
        for item in download_items:
            await item.drm_check_downloaded_task
            if item.drm:
                download_tasks.append(asyncio.create_task(item.download_drm_files()))
            else:
                download_tasks.append(asyncio.create_task(item.download()))

        await asyncio.gather(*download_tasks)

        drm_items = [x for x in download_items if x.drm]
        authed_drm = self.authed.drm
        dashboard = self.dashboard
        worker_id = self.worker_id
        total = len(drm_items)
        header = f"{performer.username} | {api_type} ({download_media_count}/{total_media_count})"

        async def decrypt_and_merge():
            # DRM post-processing: decrypt and merge
            completed = 0
            if drm_items and authed_drm:
                for item in drm_items:
                    try:
                        if (
                            item.media_item.directory is not None
                            and item.media_item.filename is not None
                        ):
                            final_path = (
                                item.media_item.directory / item.media_item.filename
                            )
                        else:
                            # Handle the error or skip processing if either is None
                            continue
                        if dashboard:
                            dashboard.set_download_task(
                                worker_id,
                                (completed / total) * 100 if total else 100,
                                f"{header} [DRM Decrypting: {item.media_item.filename}]",
                            )
                        decrypted_paths: list[Path] = []

                        for path in item.drm_download_paths:
                            output = await asyncio.to_thread(
                                authed_drm.decrypt_file,
                                path,
                                item.media_item.key,
                                temp_output_path=self.authed.get_api()
                                .get_global_settings()
                                .drm.decrypt_media_path,
                            )
                            if not output:
                                raise Exception("No output_filepath")
                            decrypted_paths.append(output)
                        # Group decrypted paths by extension
                        paths_by_ext: dict[str, list[Path]] = {}
                        for path in decrypted_paths:
                            ext = path.suffix.lower()
                            paths_by_ext.setdefault(ext, []).append(path)
                        # If there are multiple decrypted files for a given extension, merge them into one file
                        for ext, paths in paths_by_ext.items():
                            if len(paths) > 1:
                                merged_path = Path(
                                    paths[0].as_posix().split(".dec")[0]
                                ).with_suffix(f".dec{ext}")
                                async with aiofiles.open(
                                    merged_path, "wb"
                                ) as merged_file:
                                    for temp_path in sorted(paths):
                                        async with aiofiles.open(
                                            temp_path, "rb"
                                        ) as part_file:
                                            while True:
                                                data = await part_file.read(64 * 1024)
                                                if not data:
                                                    break
                                                await merged_file.write(data)
                                        await aiofiles.os.remove(str(temp_path))
                                # Replace the list of paths with the merged file
                                paths_by_ext[ext] = [merged_path]
                        # Update decrypted_paths to only include the merged (or single) files
                        decrypted_paths = [paths[0] for paths in paths_by_ext.values()]
                        if dashboard:
                            dashboard.set_download_task(
                                worker_id,
                                (completed / total) * 100 if total else 100,
                                f"{header} [DRM Merging: {item.media_item.filename}]",
                            )
                        future = asyncio.get_event_loop().create_future()
                        await queue.put((final_path, decrypted_paths, future))
                        await future
                        completed += 1
                        if dashboard:
                            dashboard.set_download_task(
                                worker_id,
                                (completed / total) * 100 if total else 100,
                                f"{header} [DRM {completed}/{total}]",
                            )
                    except Exception as e:
                        print(
                            f"Error in DRM processing: {e} | Authed: {self.authed.username}"
                        )
                        print(traceback.format_exc())
                        await asyncio.sleep(2)
                        pass

        await decrypt_and_merge()

        # Post-process all items
        process_tasks = [
            self.process_download_item(item) for item in download_items if item
        ]
        await asyncio.gather(*process_tasks)

        await queue.join()
        worker_task.cancel()
        await asyncio.gather(worker_task, return_exceptions=True)

        if dashboard:
            dashboard.set_download_task(
                worker_id, 100, f"{performer.username} | {api_type} (Done)"
            )
        if download_items:
            return True
        return False

    def get_format(self, filepath: Path) -> str:
        """Get the container format of a media file using ffprobe."""
        try:
            probe = ffmpeg.probe(filepath.as_posix())
            return probe["format"][
                "format_name"
            ]  # Short format name (e.g., 'mp4', 'mov')
        except ffmpeg.Error as e:
            raise Exception(f"Error probing file {filepath}: {e.stderr.decode()}")

    def get_preferred_format(self, filepath: Path) -> str:
        """Extract the preferred container format from the file."""
        try:
            probe = ffmpeg.probe(filepath.as_posix())
            format_names = probe["format"][
                "format_name"
            ]  # E.g., "mov,mp4,m4a,3gp,3g2,mj2"
            formats = format_names.split(",")  # Split into a list
            # Prioritize 'mp4' if available, otherwise take the first format
            return "mp4" if "mp4" in formats else formats[0]
        except ffmpeg.Error as e:
            raise Exception(f"Error probing file {filepath}: {e.stderr.decode()}")

    async def format_media(
        self, output_filepath: Path, decrypted_media_paths: list[Path]
    ):
        # If you have decrypted video and audio to merge
        if len(decrypted_media_paths) > 1:
            try:
                dec_video_path, dec_audio_path = decrypted_media_paths
            except Exception as e:
                pass
            video_input = ffmpeg.input(dec_video_path)  # type:ignore
            audio_input = ffmpeg.input(dec_audio_path)  # type:ignore

            try:
                # Dynamically determine the preferred format
                output_format = self.get_preferred_format(dec_video_path)

                temp_output_filepath = output_filepath.with_suffix(".part")
                _ffmpeg_output = ffmpeg.output(  # type:ignore
                    video_input,  # type:ignore
                    audio_input,  # type:ignore
                    temp_output_filepath.as_posix(),
                    vcodec="copy",
                    acodec="copy",
                    f=output_format,  # Dynamically set the format
                ).run(capture_stdout=True, capture_stderr=True, overwrite_output=True)

                temp_output_filepath.rename(output_filepath)
                return True
            except ffmpeg.Error as _e:
                return False
        return True

    async def ffmpeg_worker(
        self, queue: asyncio.Queue[tuple[Path, list[Path], asyncio.Future[Any]]]
    ):
        while True:
            try:
                output_filepath, decrypted_media_paths, future = await queue.get()
            except Exception as e:
                print(e)
                pass
                continue
            try:
                result = await self.format_media(output_filepath, decrypted_media_paths)
                future.set_result(result)
                for path in decrypted_media_paths:
                    path.unlink()
            except Exception as e:
                future.set_exception(e)
            finally:
                queue.task_done()
