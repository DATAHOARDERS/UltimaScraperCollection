from __future__ import annotations

import copy
import hashlib
import os
import shutil
from pathlib import Path
from typing import TYPE_CHECKING, Any, Generator, Literal

import aiofiles
import ultima_scraper_api
from aiohttp import ClientResponse
from aiohttp.client_reqrep import ClientResponse
from ultima_scraper_api.helpers.main_helper import open_partial
from ultima_scraper_api.managers.session_manager import EXCEPTION_TEMPLATE
from ultima_scraper_collection.helpers import main_helper as usc_helper
from ultima_scraper_renamer.reformat import (
    FormatAttributes,
    ReformatItem,
    ReformatManager,
)

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
        self.devices_directory = self.user_data_directory.joinpath("drm_device")
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
        root_metadata_directory = usc_helper.check_space(
            site_config.metadata_setup.directories
        )
        root_download_directory = usc_helper.check_space(
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
        self,
        response: ClientResponse,
        download_path: Path,
        callback: Any = None,
        buffer_size: int = 64 * 1024 * 1024,  # 512MB buffer
    ):
        status_code = None
        if response.status == 200:
            total_length = 0
            os.makedirs(os.path.dirname(download_path), exist_ok=True)

            async with aiofiles.open(f"{download_path}.part", mode="wb") as f:
                buffer = bytearray()  # Buffer to hold chunks before writing to disk
                try:
                    async for data in response.content.iter_chunked(
                        65536
                    ):  # 64KB chunks from network
                        buffer.extend(data)  # Add chunk to buffer
                        length = len(data)
                        total_length += length

                        if len(buffer) >= buffer_size:
                            await f.write(buffer)
                            buffer.clear()  # Clear buffer after writing

                        if callback:
                            callback(length)
                except EXCEPTION_TEMPLATE as _e:
                    status_code = 1
                except Exception as _e:
                    raise Exception(f"Unknown Error: {_e}")
                else:
                    if status_code:
                        await aiofiles.os.remove(f"{download_path}.part")
                    else:
                        try:
                            if buffer:  # Write any remaining buffered data
                                await f.write(buffer)
                            await aiofiles.os.rename(
                                f"{download_path}.part", download_path
                            )
                        except OSError as _e:
                            raise Exception(f"File rename failed: {_e}")
        else:
            if response.content_length:
                pass
            status_code = 2
        return status_code

    async def create_option(
        self,
        datascraper: datascraper_types,
        username: str,
        directory: Path,
        format_key: str,
    ):
        api = datascraper.api
        option = {
            "site_name": api.site_name,
            "profile_username": username,
            "model_username": username,
            "directory": directory,
        }
        reformat_item_fd = ReformatItem(option)
        assert self.directory_manager
        f_d_p = reformat_item_fd.remove_non_unique(self.directory_manager, format_key)
        return f_d_p

    async def create_directory_manager(
        self, site_config: site_config_types, user: user_types
    ):
        if self.directory_manager:
            final_download_directory = await self.discover_main_directory(user)
            final_root_download_directory = (
                self.directory_manager.root_download_directory
            )
            for directory in site_config.download_setup.directories:
                assert directory.path
                if directory.path.as_posix() in final_download_directory.as_posix():
                    final_root_download_directory = directory.path
                    break
            directory_manager = DirectoryManager(
                site_config,
                self.directory_manager.root_metadata_directory,
                final_root_download_directory,
            )
            self.directory_manager_users[user.id] = directory_manager
            self.file_manager_users[user.id] = FileManager(directory_manager)
            return directory_manager

    async def discover_main_metadata_directory(self, subscription: user_types):
        usernames = subscription.get_usernames(ignore_id=False)
        valid_usernames = subscription.get_usernames(ignore_id=True)
        authed = subscription.get_authed()
        reformat_manager = ReformatManager(authed, self)
        directory_manager = self.directory_manager
        site_config = directory_manager.site_config
        final_store_directory = None
        for username in usernames:
            for store_directory in [
                x.path for x in site_config.metadata_setup.directories if x.path
            ]:
                download_directory_reformat_item = (
                    reformat_manager.prepare_user_reformat(
                        subscription, store_directory, username=username
                    )
                )
                formatted_download_directory = (
                    download_directory_reformat_item.reformat(
                        site_config.metadata_setup.directory_format
                    )
                )
                final_store_directory = formatted_download_directory
                if final_store_directory.exists():
                    if username == f"u{subscription.id}":
                        if valid_usernames:
                            download_directory_reformat_item = (
                                reformat_manager.prepare_user_reformat(
                                    subscription,
                                    store_directory,
                                    username=valid_usernames[-1],
                                )
                            )
                            formatted_download_directory = (
                                download_directory_reformat_item.reformat(
                                    site_config.metadata_setup.directory_format
                                )
                            )
                            if not formatted_download_directory.exists():
                                formatted_download_directory.mkdir(
                                    exist_ok=True, parents=True
                                )
                                final_store_directory.rename(
                                    formatted_download_directory
                                )
                                final_store_directory = formatted_download_directory
                            else:
                                final_store_directory = formatted_download_directory
                            return final_store_directory
                    else:
                        return final_store_directory
        return final_store_directory

    async def discover_main_directory(self, subscription: user_types):
        usernames = subscription.get_usernames(ignore_id=False)
        if f"u{subscription.id}" not in usernames:
            usernames.append(f"u{subscription.id}")
        valid_usernames = subscription.get_usernames(ignore_id=True)
        authed = subscription.get_authed()
        reformat_manager = ReformatManager(authed, self)
        directory_manager = self.directory_manager
        assert directory_manager
        site_config = directory_manager.site_config
        store_directories = [
            x.path for x in site_config.download_setup.directories if x.path
        ]

        for username in usernames:
            for store_directory in store_directories:
                download_directory_reformat_item = (
                    reformat_manager.prepare_user_reformat(
                        subscription, store_directory, username=username
                    )
                )
                formatted_download_directory = (
                    download_directory_reformat_item.remove_non_unique(
                        directory_manager, "file_directory_format"
                    )
                )

                if formatted_download_directory.exists():
                    if username == f"u{subscription.id}" and valid_usernames:
                        download_directory_reformat_item = (
                            reformat_manager.prepare_user_reformat(
                                subscription,
                                store_directory,
                                username=valid_usernames[-1],
                            )
                        )
                        new_formatted_download_directory = (
                            download_directory_reformat_item.remove_non_unique(
                                directory_manager, "file_directory_format"
                            )
                        )
                        if not new_formatted_download_directory.exists():
                            # formatted_download_directory.mkdir(
                            #     exist_ok=True, parents=True
                            # )
                            formatted_download_directory.rename(
                                new_formatted_download_directory
                            )
                            formatted_download_directory = (
                                new_formatted_download_directory
                            )
                        return formatted_download_directory
                    return formatted_download_directory

        download_directory_reformat_item = reformat_manager.prepare_user_reformat(
            subscription, directory_manager.root_download_directory, username=username
        )
        formatted_download_directory = (
            download_directory_reformat_item.remove_non_unique(
                directory_manager, "file_directory_format"
            )
        )
        return formatted_download_directory

    async def discover_alternative_directories(self, subscription: user_types):
        usernames = subscription.get_usernames(ignore_id=False)
        authed = subscription.get_authed()
        reformat_manager = ReformatManager(authed, self)
        directory_manager = self.get_directory_manager(subscription.id)
        site_config = directory_manager.site_config
        for username in usernames:
            for alt_download_directory in [
                x.path for x in site_config.download_setup.directories if x.path
            ]:
                alt_download_directory_reformat_item = (
                    reformat_manager.prepare_user_reformat(
                        subscription, alt_download_directory, username=username
                    )
                )
                formatted_alt_download_directory = (
                    alt_download_directory_reformat_item.remove_non_unique(
                        directory_manager, "file_directory_format"
                    )
                )
                if (
                    formatted_alt_download_directory
                    == directory_manager.user.download_directory
                ):
                    continue
                if formatted_alt_download_directory.exists():
                    directory_manager.user.alt_download_directories.append(
                        formatted_alt_download_directory
                    )
        return directory_manager.user.alt_download_directories

    async def format_directories(self, performer: user_types) -> DirectoryManager:
        authed = performer.get_authed()
        directory_manager = self.get_directory_manager(performer.id)
        file_manager = self.get_file_manager(performer.id)

        final_metadata_directory = await self.discover_main_metadata_directory(
            performer
        )
        directory_manager.user.metadata_directory = final_metadata_directory

        final_download_directory = await self.discover_main_directory(performer)
        directory_manager.user.download_directory = final_download_directory

        api = authed.api
        performer_username = performer.get_usernames(ignore_id=True)[-1]
        site_name = authed.api.site_name
        alt_directories = await self.discover_alternative_directories(performer)
        await file_manager.set_default_files()
        _metadata_filepaths = await file_manager.find_metadata_files(legacy_files=False)
        # for metadata_filepath in metadata_filepaths:
        #     if file_manager.directory_manager.user.metadata_directory.as_posix() in metadata_filepath.parent.as_posix():
        #         continue
        #     new_filepath = file_manager.directory_manager.user.metadata_directory.joinpath(metadata_filepath.name)
        #     if new_filepath.exists():
        #         new_filepath = usc_helper.find_unused_filename(
        #             new_filepath
        #         )
        #         if new_filepath.exists():
        #             breakpoint()
        #     file_manager.rename_path(metadata_filepath, new_filepath)
        #     pass
        # alt_files = await usc_helper.walk(
        #     file_manager.directory_manager.user.download_directory
        # )
        # if not alt_files:
        #     shutil.rmtree(file_manager.directory_manager.user.download_directory)
        await file_manager.merge_alternative_directories(alt_directories)
        user_metadata_directory = directory_manager.user.metadata_directory
        assert user_metadata_directory
        _user_download_directory = directory_manager.user.download_directory
        legacy_metadata_directory = user_metadata_directory
        directory_manager.user.legacy_metadata_directories.append(
            legacy_metadata_directory
        )
        items = api.CategorizedContent()
        for api_type, _ in items:
            legacy_metadata_directory_2 = user_metadata_directory.joinpath(api_type)
            directory_manager.user.legacy_metadata_directories.append(
                legacy_metadata_directory_2
            )
        legacy_model_directory = directory_manager.root_download_directory.joinpath(
            site_name, performer_username
        )
        directory_manager.user.legacy_download_directories.append(
            legacy_model_directory
        )
        return directory_manager


class DirectoryManager:
    def __init__(
        self,
        site_config: site_config_types,
        root_metadata_directory: Path,
        root_download_directory: Path,
    ) -> None:
        self.root_directory = Path()
        self.root_metadata_directory = Path(root_metadata_directory)
        self.root_download_directory = Path(root_download_directory)
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
            self.metadata_directory: Path | None = None
            self.download_directory: Path | None = None
            self.alt_download_directories: list[Path] = []
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
        return await usc_helper.walk(directory)


class FileManager:
    def __init__(self, directory_manager: DirectoryManager) -> None:
        self.files: list[Path] = []
        self.directory_manager = directory_manager

    async def set_default_files(
        self,
    ):
        assert self.directory_manager.user.metadata_directory
        assert self.directory_manager.user.download_directory
        await self.update_files(self.directory_manager.user.metadata_directory)
        await self.update_files(self.directory_manager.user.download_directory)

    async def refresh_files(self):
        return await self.set_default_files()

    async def update_files(self, directory: Path):
        directory_manager = self.directory_manager
        files = await directory_manager.walk(directory)
        self.files.extend(files)
        return files

    def add_file(self, filepath: Path):
        self.files.append(filepath)
        return True

    def remove_file(self, filepath: Path):
        if filepath in self.files:
            self.files.remove(filepath)
            return True
        return False

    def rename_path(self, old_filepath: Path, new_filepath: Path):
        self.remove_file(old_filepath)
        self.add_file(new_filepath)
        new_filepath.parent.mkdir(exist_ok=True, parents=True)
        shutil.move(old_filepath, new_filepath)
        return True

    def delete_path(self, filepath: Path):
        if filepath.is_dir():
            filepath.rmdir()
        else:
            self.remove_file(filepath)
            filepath.unlink(missing_ok=True)
        return True

    async def cleanup(self):
        unique: set[Path] = set()
        await self.refresh_files()
        for valid_file in self.find_string_in_path("__drm__"):
            self.delete_path(valid_file)
            unique.add(valid_file.parent)
        for unique_file in unique:
            self.delete_path(unique_file)
        return True

    def find_string_in_path(self, string: str):
        valid_files: list[Path] = []
        for file in self.files:
            if string in file.as_posix():
                valid_files.append(file)
        return valid_files

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

    async def merge_alternative_directories(self, alt_directories: list[Path]):
        directory_manager = self.directory_manager
        assert directory_manager.user.download_directory
        for alt_download_directory in alt_directories:
            alt_files = await directory_manager.walk(alt_download_directory)
            for alt_file in alt_files:
                new_filepath = Path(
                    alt_file.as_posix().replace(
                        alt_download_directory.as_posix(),
                        directory_manager.user.download_directory.as_posix(),
                    )
                )

                if alt_file.suffix in [".json", ".db"]:
                    if new_filepath.exists():
                        new_filepath = usc_helper.find_unused_filename(new_filepath)
                        if new_filepath.exists():
                            breakpoint()
                if new_filepath.exists():
                    old_checksum = hashlib.md5(alt_file.read_bytes()).hexdigest()
                    new_checksum = hashlib.md5(new_filepath.read_bytes()).hexdigest()
                    if old_checksum == new_checksum:
                        self.delete_path(alt_file)
                    else:
                        old_size = alt_file.stat().st_size
                        new_size = new_filepath.stat().st_size
                        if old_size > new_size:
                            self.rename_path(alt_file, new_filepath)
                        elif new_size > old_size:
                            self.delete_path(alt_file)
                        elif old_size == new_size:
                            if usc_helper.is_image_valid(new_filepath):
                                self.delete_path(alt_file)
                            elif usc_helper.is_image_valid(alt_file):
                                self.rename_path(alt_file, new_filepath)
                else:
                    self.rename_path(alt_file, new_filepath)

            alt_files = await usc_helper.walk(alt_download_directory)
            if not alt_files:
                shutil.rmtree(alt_download_directory)


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
                bl = FormatAttributes()
                wl = [v for _k, v in bl.__dict__.items()]
                bl = bl.whitelist(wl)
                invalid_list = []
                for b in bl:
                    if b in self.file_directory_format.as_posix():
                        invalid_list.append(b)
            if key == "filename_format":
                bl = FormatAttributes()
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
                bl = FormatAttributes().whitelist(wl)
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
        f = FormatAttributes()
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
