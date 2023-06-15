from pathlib import Path

from pydantic import BaseModel
from ultima_scraper_api.config import (
    FanslyAPI,
    GlobalAPI,
    OnlyFansAPI,
    Settings,
    Sites,
    UltimaScraperAPIConfig,
)


class Jobs(BaseModel):
    class Scrape(BaseModel):
        subscriptions: bool = True
        messages: bool = True
        paid_contents: bool = True

    class Metadata:
        content: bool = True
        comments: bool = True

    scrape: Scrape = Scrape()
    metadata: Scrape = Scrape()


class Directory(BaseModel):
    path: Path | None = None
    minimum_space: int = -1
    store: bool = True
    overflow: bool = True


class GlobalXPathSetup(BaseModel):
    directories: list[Directory] = [Directory(path=Path().absolute())]
    directory_format: str = ""


class DownloadPathSetup(GlobalXPathSetup):
    filename_format: str = ""
    text_length: int = 255
    date_format: str = "%Y-%m-%d"
    overwrite_files: bool = True


class Trash(BaseModel):
    cleanup: bool = True


class ToolSettings(BaseModel):
    active: bool = True


class Renamer(ToolSettings):
    pass


class Reformatter(ToolSettings):
    pass


class Downloader(ToolSettings):
    pass


class SSHConnection(BaseModel):
    host: str | None = None
    port: int = 22
    username: str = "UltimaRoot"
    private_key_filepath: Path | None = Path() or None
    private_key_password: str | None = None
    local_bind_port: list[int] = [10022]


class DatabaseInfo(BaseModel):
    name: str = "UltimaDB"
    username: str = "postgres"
    password: str = "password"
    port: int = 5432


class Tools(BaseModel):
    renamer: Renamer = Renamer()
    reformatter: Reformatter = Reformatter()
    downloader: Downloader = Downloader()


class UltimaScraperCollectionConfig(UltimaScraperAPIConfig):
    class _Settings(Settings):
        auto_site_choice: str = ""
        tools: Tools = Tools()
        trash = Trash()
        infinite_loop: bool = False
        exit_on_completetion: bool = True

    class _Sites(Sites):
        class _GlobalAPI(GlobalAPI):
            auto_profile_choice: int | str | bool | None = None
            auto_performer_choice: int | str | bool | None = None
            auto_content_choice: int | str | bool | None = None
            auto_media_choice: int | str | bool | None = None
            jobs = Jobs()
            metadata_setup = GlobalXPathSetup()
            metadata_setup.directory_format = (
                "{site_name}/{first_letter}/{model_username}/Metadata"
            )
            download_setup = DownloadPathSetup()
            download_setup.directory_format = "{site_name}/{first_letter}/{model_username}/{api_type}/{value}/{media_type}"
            download_setup.filename_format = "{filename}.{ext}"
            video_quality = "source"
            pass

        class _OnlyFansAPI(OnlyFansAPI, _GlobalAPI):
            pass

        class _FanslyAPI(FanslyAPI, _GlobalAPI):
            pass

        onlyfans: _OnlyFansAPI = _OnlyFansAPI()
        fansly: _FanslyAPI = _FanslyAPI()

    settings: _Settings = _Settings()
    site_apis: _Sites = _Sites()
