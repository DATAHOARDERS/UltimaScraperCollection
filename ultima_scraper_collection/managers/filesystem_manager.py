from __future__ import annotations

import copy
import os
import shutil
from datetime import datetime
from pathlib import Path
from typing import TYPE_CHECKING, Any, Generator, Literal

import ultima_scraper_api
from aiohttp.client_reqrep import ClientResponse
from ultima_scraper_api.classes.prepare_directories import FormatTypes
from ultima_scraper_api.helpers.main_helper import open_partial
from ultima_scraper_api.managers.session_manager import EXCEPTION_TEMPLATE
from ultima_scraper_renamer.reformat import prepare_reformat

if TYPE_CHECKING:

    api_types = ultima_scraper_api.api_types
    user_types = ultima_scraper_api.user_types


class FilesystemManager:
    def __init__(self) -> None:
        self.user_data_directory = Path("__user_data__")
        self.trash_directory = self.user_data_directory.joinpath("trash")
        self.profiles_directory = self.user_data_directory.joinpath("profiles")
        self.settings_directory = Path("__settings__")
        self.ignore_files = ["desktop.ini", ".DS_Store", ".DS_store", "@eaDir"]
        self.directory_manager: DirectoryManager | None = None
        self.directory_manager_users: dict[int, DirectoryManager] = {}
        self.file_manager_users: dict[int, FileManager] = {}

    def __iter__(self):
        for each in self.__dict__.values():
            yield each

    def check(self):
        for directory in self:
            if isinstance(directory, Path):
                directory.mkdir(exist_ok=True)

    def move(self, src: Path, trg: Path):
        shutil.move(src, trg)

    def remove_mandatory_files(
        self, files: list[Path] | Generator[Path, None, None], keep: list[str] = []
    ):
        folders = [x for x in files if x.name not in self.ignore_files]
        if keep:
            folders = [x for x in files if x.name in keep]
        return folders

    def get_directory_manager(self, user_id: int):
        return self.directory_manager_users[user_id]

    def get_file_manager(self, user_id: int):
        return self.file_manager_users[user_id]

    def activate_directory_manager(self, api: api_types):
        from ultima_scraper_api.helpers import main_helper

        site_settings = api.get_site_settings()
        root_metadata_directory = main_helper.check_space(
            site_settings.metadata_directories
        )
        root_download_directory = main_helper.check_space(
            site_settings.download_directories
        )
        self.directory_manager = DirectoryManager(
            site_settings,
            root_metadata_directory,
            root_download_directory,
        )

    def trash(self):
        pass

    async def write_data(
        self, response: ClientResponse, download_path: Path, callback: Any = None
    ):
        status_code = 0
        if response.status == 200:
            total_length = 0
            os.makedirs(os.path.dirname(download_path), exist_ok=True)
            partial_path: str | None = None
            try:
                with open_partial(download_path) as f:
                    partial_path = f.name
                    try:
                        async for data in response.content.iter_chunked(4096):
                            f.write(data)
                            length = len(data)
                            total_length += length
                            if callback:
                                callback(length)
                    except EXCEPTION_TEMPLATE as _e:
                        status_code = 1
                    except Exception as _e:
                        raise Exception(f"Unknown Error: {_e}")
            except:
                if partial_path:
                    os.unlink(partial_path)
                raise
            else:
                if status_code:
                    os.unlink(partial_path)
                else:
                    try:
                        os.replace(partial_path, download_path)
                    except OSError:
                        pass
        else:
            if response.content_length:
                pass
                # progress_bar.update_total_size(-response.content_length)
            status_code = 2
        return status_code

    async def create_directory_manager(self, api: api_types, user: user_types):

        # profile_directory = filesystem_directory_manager.profile.root_directory.joinpath(
        #     self.username
        # )
        if self.directory_manager:
            directory_manager = DirectoryManager(
                api.get_site_settings(),
                self.directory_manager.root_metadata_directory,
                self.directory_manager.root_download_directory,
            )
            option = {
                "site_name": api.site_name,
                "profile_username": user.username,
                "model_username": user.username,
                "directory": directory_manager.root_download_directory,
            }
            pr_rt_rd = prepare_reformat(option)
            _f_d_p = await pr_rt_rd.remove_non_unique(
                directory_manager, "file_directory_format"
            )
            # f_d_p.mkdir(parents=True, exist_ok=True)
            option["directory"] = self.directory_manager.root_metadata_directory
            pr_rt_rm = prepare_reformat(option)
            _f_d_p_2 = await pr_rt_rm.remove_non_unique(
                self.directory_manager, "metadata_directory_format"
            )
            # f_d_p_2.mkdir(parents=True, exist_ok=True)
            self.directory_manager_users[user.id] = directory_manager
            self.file_manager_users[user.id] = FileManager(directory_manager)
            return directory_manager

    async def format_directories(self, subscription: user_types) -> DirectoryManager:

        directory_manager = self.get_directory_manager(subscription.id)
        file_manager = self.get_file_manager(subscription.id)
        authed = subscription.get_authed()
        api = authed.api
        site_settings = authed.api.get_site_settings()
        if site_settings:
            authed_username = authed.username
            subscription_username = subscription.username
            site_name = authed.api.site_name
            p_r = prepare_reformat()
            prepared_metadata_format = await p_r.standard(
                site_name,
                authed_username,
                subscription_username,
                datetime.today(),
                site_settings.date_format,
                site_settings.text_length,
                directory_manager.root_metadata_directory,
            )
            string = await prepared_metadata_format.reformat_2(
                site_settings.metadata_directory_format
            )
            directory_manager.user.metadata_directory = Path(string)
            prepared_download_format = copy.copy(prepared_metadata_format)
            prepared_download_format.directory = (
                directory_manager.root_download_directory
            )
            string = await prepared_download_format.reformat_2(
                site_settings.file_directory_format
            )
            formtatted_root_download_directory = (
                await prepared_download_format.remove_non_unique(
                    directory_manager, "file_directory_format"
                )
            )
            directory_manager.user.download_directory = (
                formtatted_root_download_directory
            )
            await file_manager.set_default_files(
                prepared_metadata_format, prepared_download_format
            )
            _metadata_filepaths = await file_manager.find_metadata_files(
                legacy_files=False
            )
            # I forgot why we need to set default file twice
            await file_manager.set_default_files(
                prepared_metadata_format, prepared_download_format
            )
            user_metadata_directory = directory_manager.user.metadata_directory
            _user_download_directory = directory_manager.user.download_directory
            legacy_metadata_directory = user_metadata_directory
            directory_manager.user.legacy_metadata_directories.append(
                legacy_metadata_directory
            )
            items = api.ContentTypes().__dict__.items()
            for api_type, _ in items:
                legacy_metadata_directory_2 = user_metadata_directory.joinpath(api_type)
                directory_manager.user.legacy_metadata_directories.append(
                    legacy_metadata_directory_2
                )
            legacy_model_directory = directory_manager.root_download_directory.joinpath(
                site_name, subscription_username
            )
            directory_manager.user.legacy_download_directories.append(
                legacy_model_directory
            )
        return directory_manager


from ultima_scraper_api.classes.make_settings import SiteSettings


class DirectoryManager:
    def __init__(
        self,
        site_settings: SiteSettings,
        root_metadata_directory: Path = Path(),
        root_download_directory: Path = Path(),
    ) -> None:
        self.root_directory = Path()
        self.root_metadata_directory = Path(root_metadata_directory)
        self.root_download_directory = Path(root_download_directory)
        # self.profile = self.ProfileDirectories(Path(profile_directory))
        self.user = self.UserDirectories()
        formats = FormatTypes(site_settings)
        string, status = formats.check_rules()
        if not status:
            print(string)
            exit(0)
        self.formats = formats
        pass

    def create_directories(self):
        # self.profile.create_directories()
        self.root_metadata_directory.mkdir(exist_ok=True)
        self.root_download_directory.mkdir(exist_ok=True)

    def delete_empty_directories(
        self, directory: Path, filesystem_manager: FilesystemManager
    ):
        for root, dirnames, _files in os.walk(directory, topdown=False):
            for dirname in dirnames:
                full_path = os.path.realpath(os.path.join(root, dirname))
                contents = os.listdir(full_path)
                if not contents:
                    shutil.rmtree(full_path, ignore_errors=True)
                else:
                    content_paths = [Path(full_path, content) for content in contents]
                    contents = filesystem_manager.remove_mandatory_files(content_paths)
                    if not contents:
                        shutil.rmtree(full_path, ignore_errors=True)

        if os.path.exists(directory) and not os.listdir(directory):
            os.rmdir(directory)

    # class ProfileDirectories:
    #     def __init__(self, root_directory: Path) -> None:
    #         self.root_directory = Path(root_directory)
    #         self.metadata_directory = self.root_directory.joinpath("Metadata")
    #     def create_directories(self):
    #         self.root_directory.mkdir(exist_ok=True)

    class UserDirectories:
        def __init__(self) -> None:
            self.metadata_directory = Path()
            self.download_directory = Path()
            self.legacy_download_directories: list[Path] = []
            self.legacy_metadata_directories: list[Path] = []

        def find_legacy_directory(
            self,
            directory_type: Literal["metadata", "download"] = "metadata",
            api_type: str = "",
        ):
            match directory_type:
                case "metadata":
                    directories = self.legacy_metadata_directories
                case _:
                    directories = self.legacy_download_directories
            final_directory = directories[0]
            for directory in directories:
                for part in directory.parts:
                    if api_type in part:
                        return directory
            return final_directory

    async def walk(self, directory: Path):
        all_files: list[Path] = []
        for root, _subdirs, files in os.walk(directory):
            x = [Path(root, x) for x in files]
            all_files.extend(x)
        return all_files


class FileManager:
    def __init__(self, directory_manager: DirectoryManager) -> None:
        self.files: list[Path] = []
        self.directory_manager = directory_manager

    async def set_default_files(
        self,
        prepared_metadata_format: prepare_reformat,
        prepared_download_format: prepare_reformat,
    ):
        self.files = []
        await self.add_files(prepared_metadata_format, "metadata_directory_format")
        await self.add_files(prepared_download_format, "file_directory_format")

    async def add_files(self, reformatter: prepare_reformat, format_key: str):
        directory_manager = self.directory_manager
        formatted_directory = await reformatter.remove_non_unique(
            directory_manager, format_key
        )
        files: list[Path] = []
        files = await directory_manager.walk(formatted_directory)
        self.files.extend(files)
        return files

    async def find_metadata_files(self, legacy_files: bool = True):
        new_list: list[Path] = []
        for filepath in self.files:
            if not legacy_files:
                if "__legacy_metadata__" in filepath.parts:
                    continue
            match filepath.suffix:
                case ".db":
                    red_list = ["thumbs.db"]
                    status = [x for x in red_list if x == filepath.name.lower()]
                    if status:
                        continue
                    new_list.append(filepath)
                case ".json":
                    new_list.append(filepath)
                case _:
                    pass
        return new_list
