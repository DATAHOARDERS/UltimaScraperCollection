from pathlib import Path

from pydantic import BaseModel, StrictBool, StrictInt, StrictStr
from ultima_scraper_api.config import (
    FanslyAPIConfig,
    GlobalAPI,
    OnlyFansAPIConfig,
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
    directories: list[Directory] = [Directory(path=Path("__user_data__").absolute())]
    directory_format: Path = Path()


class DownloadPathSetup(GlobalXPathSetup):
    filename_format: Path = Path()
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
    username: str | None = None
    private_key_filepath: Path | None = None
    private_key_password: str | None = None
    host: str | None = None
    port: int = 22


class DatabaseInfo(BaseModel):
    name: str = "ultima"
    username: str | None = None
    password: str | None = None
    host: str = "localhost"
    port: int = 5432
    ssh: SSHConnection = SSHConnection()


class Database(BaseModel):
    connection_info: DatabaseInfo = DatabaseInfo()
    main: bool = True
    active: bool = True


class Tools(BaseModel):
    renamer: Renamer = Renamer()
    reformatter: Reformatter = Reformatter()
    downloader: Downloader = Downloader()


auto_types = list[int | str] | StrictInt | StrictStr | StrictBool | None


class GlobalAPI(GlobalAPI):
    auto_profile_choice: auto_types = None
    auto_performer_choice: auto_types = None
    auto_content_choice: auto_types = None
    auto_media_choice: auto_types = None
    jobs = Jobs()
    metadata_setup = GlobalXPathSetup()
    metadata_setup.directory_format = (
        "{site_name}/{first_letter}/{model_username}/Metadata"
    )  # type: ignore
    download_setup = DownloadPathSetup()
    download_setup.directory_format = (
        "{site_name}/{first_letter}/{model_username}/{api_type}/{value}/{media_type}"
    )  # type: ignore
    download_setup.filename_format = "{filename}.{ext}"  # type: ignore
    video_quality = "source"
    blacklists: list[str] = []


class Sites(Sites):
    class OnlyFansAPIConfig(OnlyFansAPIConfig, GlobalAPI):
        pass

    class FanslyAPIConfig(FanslyAPIConfig, GlobalAPI):
        pass

    onlyfans: OnlyFansAPIConfig = OnlyFansAPIConfig(auto_content_choice=True)
    fansly: FanslyAPIConfig = FanslyAPIConfig()


site_config_types = Sites.OnlyFansAPIConfig | Sites.FanslyAPIConfig


class UltimaScraperCollectionConfig(UltimaScraperAPIConfig):
    class Settings(Settings):
        auto_site_choice: str = ""
        databases: list[Database] = [Database()]
        tools: Tools = Tools()
        trash = Trash()
        infinite_loop: bool = False
        exit_on_completion: bool = True

        def get_main_database(self):
            return [x for x in self.databases if x.main][0]

    settings: Settings = Settings()
    site_apis: Sites = Sites()
