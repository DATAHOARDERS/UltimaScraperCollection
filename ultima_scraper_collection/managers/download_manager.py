import asyncio
import copy
import traceback
from pathlib import Path
from urllib.parse import urlparse

import ffmpeg
from aiohttp import ClientConnectionError, ClientResponse
from alive_progress import alive_bar
from ultima_scraper_api import auth_types
from ultima_scraper_api.apis.onlyfans.classes.mass_message_model import MassMessageModel
from ultima_scraper_api.helpers import main_helper
from ultima_scraper_collection.managers.database_manager.connections.sqlite.models.media_model import (
    TemplateMediaModel,
)
from ultima_scraper_collection.managers.filesystem_manager import FilesystemManager
from ultima_scraper_collection.managers.metadata_manager.metadata_manager import (
    MediaMetadata,
    invalid_subdomains,
)
from ultima_scraper_db.databases.ultima_archive.schemas.templates.site import (
    MediaModel,
    MessageModel,
)
from ultima_scraper_renamer.reformat import ReformatManager


class DownloadManager:
    def __init__(
        self,
        authed: auth_types,
        filesystem_manager: FilesystemManager,
        media_set: set[MediaMetadata] = set(),
        reformat: bool = True,
    ) -> None:
        self.authed = authed
        self.filesystem_manager = filesystem_manager
        self.auth_session = self.authed.auth_session
        self.requester = self.authed.get_requester()
        self.content_list: set[MediaMetadata] = media_set
        self.errors: list[TemplateMediaModel] = []
        self.reformat = reformat
        self.reformat_manager = ReformatManager(self.authed, filesystem_manager)
        self.bar = None

    async def check_total_size(self):
        async def get_file_size(file: MediaMetadata):
            if len(file.urls) > 1:
                return 0  # Skip files with multiple URLs as in your logic
            response = await self.auth_session.active_session.head(file.urls[0])
            return response.content_length if response.content_length else 0

        tasks = [get_file_size(file) for file in self.content_list]
        sizes = await asyncio.gather(*tasks)
        return sum(sizes)

    async def process_download_item(self, download_item: MediaMetadata):
        try:
            db_media = download_item.__db_media__
        except Exception as _e:
            pass
        assert db_media
        content = download_item.get_content_metadata()
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
        if isinstance(download_item, MediaMetadata):
            final_filepath = download_item.directory.joinpath(download_item.filename)
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
                        db_media.size = download_item.size = final_size
                    else:
                        if final_size > db_media.size:
                            db_media.size = final_size
                    db_filepath.downloaded = True

    async def bulk_download(self):
        queue: asyncio.Queue[tuple[Path, list[Path]]] = asyncio.Queue()
        worker_task = asyncio.create_task(self.ffmpeg_worker(queue))
        final_list = [
            self.download(media_item, queue) for media_item in self.content_list
        ]
        global_settings = self.authed.get_api().get_global_settings()
        decrypt_media_path = global_settings.drm.decrypt_media_path
        if final_list:
            with alive_bar(
                len(self.content_list),
                title="Downloading Media Items",
                spinner="dots",
                bar="smooth",
                enrich_print=True,
            ) as bar:
                self.bar = bar  # Allow updates within `self.download` if needed

                # Run tasks asynchronously and track progress
                results = await asyncio.gather(*final_list, return_exceptions=True)

            drm_results = [
                x for x in results if x and isinstance(x, MediaMetadata) and x.drm
            ]
            if drm_results:
                with alive_bar(
                    len(drm_results),
                    title="Processing DRM-protected files",
                    spinner="dots",
                    bar="smooth",
                    enrich_print=True,
                ) as bar:
                    authed_drm = self.authed.drm
                    for download_item in drm_results:
                        try:
                            final_download_path = download_item.directory.joinpath(
                                download_item.filename
                            )

                            if authed_drm and download_item.drm:
                                bar.text(f"Decrypting: {download_item.filename}")
                                decrypted_media_paths: list[Path] = []
                                for download_path in download_item.drm_download_paths:
                                    output_filepath = authed_drm.decrypt_file(
                                        download_path,
                                        download_item.key,
                                        temp_output_path=decrypt_media_path,
                                    )
                                    if not output_filepath:
                                        raise Exception("No output_filepath")
                                    decrypted_media_paths.append(output_filepath)

                                bar.text(f"Merging: {download_item.filename}")
                                future = asyncio.get_event_loop().create_future()
                                task = (
                                    final_download_path,
                                    decrypted_media_paths,
                                    future,
                                )
                                await queue.put(task)
                                await future
                            bar()
                        except Exception as e:
                            pass

            tasks = []
            for download_item in results:
                if not download_item:
                    continue
                tasks.append(self.process_download_item(download_item))
            await asyncio.gather(*tasks)
        await queue.join()
        worker_task.cancel()
        await asyncio.gather(worker_task, return_exceptions=True)
        pass

    async def drm_download(self, download_item: MediaMetadata):
        content_metadata = download_item.__content_metadata__
        authed = self.authed
        reformat_manager = ReformatManager(authed, self.filesystem_manager)
        assert reformat_manager.filesystem_manager.directory_manager
        site_config = reformat_manager.filesystem_manager.directory_manager.site_config
        drm = authed.drm
        media_item = download_item.__raw__
        assert drm and media_item
        mpd = await drm.get_mpd(media_item)
        pssh = await drm.get_pssh(mpd)
        responses: list[ClientResponse] = []

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

                signature_str = await drm.get_signature(media_item)
                response = await authed.auth_session.request(
                    media_url, premade_settings="", custom_cookies=signature_str
                )
                responses.append(response)
        return responses

    async def download(
        self,
        download_item: MediaMetadata,
        ffmpeg_queue: asyncio.Queue[tuple[Path, list[Path]]],
    ):
        try:
            if not download_item.urls:
                return
            attempt = 0
            try:
                db_media = download_item.__db_media__
            except Exception as _e:
                pass
            assert db_media
            await db_media.awaitable_attrs.content_media_assos
            p_url = urlparse(download_item.urls[0])
            assert p_url.hostname
            subdomain = p_url.hostname.split(".")[0]
            if any(subdomain in nm for nm in invalid_subdomains):
                return

            authed = self.authed
            authed_drm = authed.drm

            async with self.auth_session.semaphore:
                while (
                    attempt < self.auth_session.get_session_manager().max_attempts + 1
                ):
                    try:
                        if download_item.drm:

                            if not authed_drm:
                                break
                            responses = await self.drm_download(download_item)
                        else:
                            responses = [
                                await self.requester.request(download_item.urls[0])
                            ]
                        if all(response.status != 200 for response in responses):
                            attempt += 1
                            continue
                        if not download_item.directory:
                            raise Exception(
                                f"{download_item.id} has no directory\n {download_item}"
                            )
                        final_size = 0
                        error = None
                        process_responses = True
                        if download_item.drm and await self.drm_check_downloaded(
                            download_item
                        ):
                            process_responses = False
                        if process_responses:
                            for response in responses:
                                download_path, error = await self.writer(
                                    response,
                                    download_item,
                                    encrypted=bool(download_item.key),
                                )
                                if error:
                                    attempt += 1
                                    break
                                if download_item.drm and download_path:
                                    if (
                                        download_path
                                        not in download_item.drm_download_paths
                                    ):
                                        download_item.drm_download_paths.append(
                                            download_path
                                        )
                                # if authed_drm and download_item.drm and download_path:
                                #     output_filepath = authed_drm.decrypt_file(
                                #         download_path, download_item.key
                                #     )
                                #     if not output_filepath:
                                #         raise Exception("No output_filepath")
                                #     decrypted_media_paths.append(output_filepath)
                                if response.content_length:
                                    final_size += response.content_length
                        if error == 1:
                            # Server Disconnect Error
                            continue
                        elif error == 2:
                            # Resource Not Found Error
                            break
                        assert download_item.filename
                        download_path = download_item.directory.joinpath(
                            download_item.filename
                        )
                        break
                    except asyncio.TimeoutError as _e:
                        continue
                    except ClientConnectionError as _e:
                        attempt += 1
                        continue
                    except Exception as _e:
                        traceback.print_exc()
            self.bar()
            return download_item
        except Exception as _e:
            pass

    async def writer(
        self,
        result: ClientResponse,
        download_item: MediaMetadata,
        encrypted: bool = True,
    ):
        async with result as response:
            if download_item.drm and encrypted:
                download_item = copy.copy(download_item)
                download_item = self.reformat_manager.drm_format(
                    response.url.human_repr(), download_item
                )
                downloaded = await self.drm_check_downloaded(download_item)
                if downloaded:
                    return download_item.get_filepath(), None
            assert download_item.directory and download_item.filename
            download_path = Path(download_item.directory, download_item.filename)
            db_media = copy.copy(download_item.__db_media__)
            db_media.directory = download_item.directory
            db_media.filename = download_item.filename
            download = await self.check(db_media, response)
            if not download:
                return download_path, None
            failed = await self.filesystem_manager.write_data(response, download_path)
            return download_path, failed

    async def drm_check_downloaded(self, download_item: MediaMetadata):
        download_path = download_item.get_filepath()
        if ".enc" in download_path.name:
            download_path = Path(download_path.as_posix().replace(".enc", ".dec"))
        if download_path.exists():
            try:
                ffmpeg.probe(download_path.as_posix())
                return True
            except ffmpeg.Error as _e:
                return False
        return False

    async def check(self, download_item: MediaModel, response: ClientResponse):
        # Checks if we should download item or not // True | False
        filepath = Path(download_item.directory, download_item.filename)
        response_status = False
        if response.status == 200:
            response_status = True
            if response.content_length:
                download_item.size = response.content_length

        if filepath.exists():
            try:
                if filepath.stat().st_size == response.content_length:
                    return False
                else:
                    return True
            except Exception as _e:
                pass
        else:
            if response_status:
                # Can produce false positives due to the same reason below
                return True
            else:
                # Reached this point because it probably exists in the folder but under a different content category
                pass

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
            dec_video_path, dec_audio_path = decrypted_media_paths
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

    async def ffmpeg_worker(self, queue: asyncio.Queue[tuple[Path, list[Path]]]):
        while True:
            output_filepath, decrypted_media_paths, future = await queue.get()
            try:
                result = await self.format_media(output_filepath, decrypted_media_paths)
                future.set_result(result)
                for path in decrypted_media_paths:
                    path.unlink()
            except Exception as e:
                future.set_exception(e)
            finally:
                queue.task_done()
