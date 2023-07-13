from __future__ import annotations

import copy
import os
import shutil
from datetime import datetime
from pathlib import Path
from typing import TYPE_CHECKING, Any, Generator, Literal

from ultima_scraper_api.classes.prepare_metadata import format_attributes
import ultima_scraper_api
from aiohttp.client_reqrep import ClientResponse
from ultima_scraper_api.helpers.main_helper import open_partial
from ultima_scraper_api.managers.session_manager import EXCEPTION_TEMPLATE
from ultima_scraper_renamer.reformat import ReformatItem


if TYPE_CHECKING:
    api_types = ultima_scraper_api.api_types
    user_types = ultima_scraper_api.user_types
    from ultima_scraper_collection import datascraper_types
    from ultima_scraper_collection.config import site_config_types


class FilesystemManager:
    def __init__(self) -> None:
        self.user_data_directory = Path("__user_data__")
        self.trash_directory = self.user_data_directory.joinpath("trash")
        self.profiles_directory = self.user_data_directory.joinpath("profiles")
        self.devices_directory = self.user_data_directory.joinpath("devices")
        self.settings_directory = Path("__user_data__")
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

    def activate_directory_manager(self, site_config: site_config_types):
        from ultima_scraper_collection.helpers import main_helper

        root_metadata_directory = main_helper.check_space(
            site_config.metadata_setup.directories
        )
        root_download_directory = main_helper.check_space(
            site_config.download_setup.directories
        )
        self.directory_manager = DirectoryManager(
            site_config,
            root_metadata_directory,
            root_download_directory,
        )

    def trash(self):
        pass

    async def write_data(
        self, response: ClientResponse, download_path: Path, callback: Any = None
    ):
        status_code = None
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

    async def create_directory_manager(
        self, datascraper: datascraper_types, user: user_types
    ):
        # profile_directory = filesystem_directory_manager.profile.root_directory.joinpath(
        #     self.username
        # )
        api = datascraper.api
        if self.directory_manager:
            directory_manager = DirectoryManager(
                datascraper.site_config,
                self.directory_manager.root_metadata_directory,
                self.directory_manager.root_download_directory,
            )
            option = {
                "site_name": api.site_name,
                "profile_username": user.username,
                "model_username": user.username,
                "directory": directory_manager.root_download_directory,
            }
            reformat_item_fd = ReformatItem(option)
            _f_d_p = reformat_item_fd.remove_non_unique(
                directory_manager, "file_directory_format"
            )
            # f_d_p.mkdir(parents=True, exist_ok=True)
            option["directory"] = self.directory_manager.root_metadata_directory
            reformat_item_md = ReformatItem(option)
            _f_d_p_2 = reformat_item_md.remove_non_unique(
                self.directory_manager, "metadata_directory_format"
            )
            # f_d_p_2.mkdir(parents=True, exist_ok=True)
            self.directory_manager_users[user.id] = directory_manager
            self.file_manager_users[user.id] = FileManager(directory_manager)
            return directory_manager

    async def format_directories(self, subscription: user_types) -> DirectoryManager:
        directory_manager = self.get_directory_manager(subscription.id)
        site_config = directory_manager.site_config
        file_manager = self.get_file_manager(subscription.id)
        authed = subscription.get_authed()
        api = authed.api
        authed_username = authed.username
        subscription_username = subscription.username
        site_name = authed.api.site_name
        reformat_item = ReformatItem()
        reformat_item.site_name = site_name
        reformat_item.profile_username = authed_username
        reformat_item.model_username = subscription_username
        reformat_item.date = datetime.today()
        download_setup = site_config.download_setup
        reformat_item.date_format = download_setup.date_format
        reformat_item.text_length = download_setup.text_length
        reformat_item.directory = directory_manager.root_metadata_directory
        metadata_setup = site_config.metadata_setup
        string = reformat_item.reformat(metadata_setup.directory_format)
        directory_manager.user.metadata_directory = Path(string)
        reformat_item_2 = copy.copy(reformat_item)
        reformat_item_2.directory = directory_manager.root_download_directory
        string = reformat_item_2.reformat(metadata_setup.directory_format)
        formtatted_root_download_directory = reformat_item_2.remove_non_unique(
            directory_manager, "file_directory_format"
        )
        directory_manager.user.download_directory = formtatted_root_download_directory
        await file_manager.set_default_files(reformat_item, reformat_item_2)
        _metadata_filepaths = await file_manager.find_metadata_files(legacy_files=False)
        # I forgot why we need to set default file twice
        await file_manager.set_default_files(reformat_item, reformat_item_2)
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


class DirectoryManager:
    def __init__(
        self,
        site_config: site_config_types,
        root_metadata_directory: Path = Path(),
        root_download_directory: Path = Path(),
    ) -> None:
        self.root_directory = Path()
        self.root_metadata_directory = Path(root_metadata_directory)
        self.root_download_directory = Path(root_download_directory)
        # self.profile = self.ProfileDirectories(Path(profile_directory))
        self.user = self.UserDirectories()
        self.site_config = site_config
        formats = FormatTypes(site_config)
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
        prepared_metadata_format: ReformatItem,
        prepared_download_format: ReformatItem,
    ):
        self.files = []
        await self.add_files(prepared_metadata_format, "metadata_directory_format")
        await self.add_files(prepared_download_format, "file_directory_format")

    async def add_files(self, reformatter: ReformatItem, format_key: str):
        directory_manager = self.directory_manager
        formatted_directory = reformatter.remove_non_unique(
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


class FormatTypes:
    def __init__(self, site_settings: site_config_types) -> None:
        self.metadata_directory_format = site_settings.metadata_setup.directory_format
        self.file_directory_format = site_settings.download_setup.directory_format
        self.filename_format = site_settings.download_setup.filename_format

    def check_rules(self):
        """Checks for invalid filepath

        Returns:
            tuple(str,bool): Returns a string which explains invalid filepath format
        """
        bool_status = True
        wl = []
        invalid_list = []
        string = ""
        for key, _value in self:
            if key == "file_directory_format":
                bl = format_attributes()
                wl = [v for _k, v in bl.__dict__.items()]
                bl = bl.whitelist(wl)
                invalid_list = []
                for b in bl:
                    if b in self.file_directory_format.as_posix():
                        invalid_list.append(b)
            if key == "filename_format":
                bl = format_attributes()
                wl = [v for _k, v in bl.__dict__.items()]
                bl = bl.whitelist(wl)
                invalid_list = []
                for b in bl:
                    if b in self.filename_format.as_posix():
                        invalid_list.append(b)
            if key == "metadata_directory_format":
                wl = [
                    "{site_name}",
                    "{first_letter}",
                    "{model_id}",
                    "{profile_username}",
                    "{model_username}",
                ]
                bl = format_attributes().whitelist(wl)
                invalid_list: list[str] = []
                for b in bl:
                    if b in self.metadata_directory_format.as_posix():
                        invalid_list.append(b)
            if invalid_list:
                string += f"You cannot use {','.join(invalid_list)} in {key}. Use any from this list {','.join(wl)}"
                bool_status = False

        return string, bool_status

    def check_unique(self):
        values: list[str] = []
        unique = []
        new_format_copied = copy.deepcopy(self)
        option: dict[str, Any] = {}
        option["string"] = ""
        option["bool_status"] = True
        option["unique"] = new_format_copied
        f = format_attributes()
        for key, value in self:
            value: Path
            if key == "file_directory_format":
                unique = ["{media_id}", "{model_username}"]
                values = list(value.parts)
                option["unique"].file_directory_format = unique
            elif key == "filename_format":
                values = []
                unique = ["{media_id}", "{filename}"]
                for _key2, value2 in f:
                    if value2 in value.as_posix():
                        values.append(value2)
                option["unique"].filename_format = unique
            elif key == "metadata_directory_format":
                unique = ["{model_username}"]
                values = list(value.parts)
                option["unique"].metadata_directory_format = unique
            if key != "filename_format":
                e = [x for x in values if x in unique]
            else:
                e = [x for x in unique if x in values]
            if e:
                setattr(option["unique"], key, e)
            else:
                option[
                    "string"
                ] += f"{key} is a invalid format since it has no unique identifiers. Use any from this list {','.join(unique)}\n"
                option["bool_status"] = False
        return option

    def __iter__(self):
        for attr, value in self.__dict__.items():
            yield attr, value
