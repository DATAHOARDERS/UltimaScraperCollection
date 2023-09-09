from datetime import datetime
from pathlib import Path
from typing import TYPE_CHECKING, Any

import ultima_scraper_api
from alembic import command
from alembic.config import Config
from alembic.migration import MigrationContext
from sqlalchemy import create_engine, func
from sqlalchemy.orm import DeclarativeBase, scoped_session, sessionmaker
from ultima_scraper_collection.managers.database_manager.connections.sqlite.models import (
    user_database,
)

if TYPE_CHECKING:
    from ultima_scraper_collection.managers.metadata_manager.metadata_manager import (
        ContentMetadata,
    )

user_types = ultima_scraper_api.user_types


class DBCollection(object):
    def __init__(self) -> None:
        self.user_database = user_database

    def database_picker(self, database_name: str):
        match database_name:
            case "user_data":
                database = self.user_database
            case _:
                raise Exception(f'"{database_name}" is an invalid database name')
        return database


class SqliteDatabase(DeclarativeBase):
    def __init__(self) -> None:
        self.name = Path()
        self.info = ""
        self.session_factory = sessionmaker()
        self.session = self.create_session()
        self.root_directory = Path(__file__).parent
        self.alembic_directory = Path()
        self.migration_directory = Path()

    def init_db(self, name: Path, legacy: bool = False):
        self.name: Path = name
        self.info = f"sqlite:///{name}"
        self.session_factory = sessionmaker(bind=self.create_engine(), autocommit=False)
        self.session = self.create_session()
        self.alembic_directory = Path(__file__).parent.joinpath(
            f"{'databases' if not legacy else 'legacy_databases'}",
            self.name.stem.lower(),
            "alembic",
        )
        self.migration_directory = self.alembic_directory.parent.joinpath("migration")
        return self

    def create_engine(self):
        return create_engine(
            self.info,
            connect_args={},
        )

    def create_session(self):
        return scoped_session(self.session_factory)

    def execute(self, statement: Any):
        result = self.session.execute(statement)
        return result

    def generate_migration(self):
        if not self.session.bind:
            return
        conn = self.session.bind.engine.connect()
        context = MigrationContext.configure(conn)
        current_rev = context.get_current_revision()
        alembic_cfg = Config(self.migration_directory.joinpath("alembic.ini"))
        alembic_cfg.set_main_option(
            "script_location",
            self.migration_directory.joinpath("alembic").as_posix(),
        )
        alembic_cfg.set_main_option("sqlalchemy.url", self.info)
        if not current_rev:
            _ggg = command.revision(alembic_cfg, autogenerate=True)
        else:
            _ggg = command.revision(alembic_cfg, autogenerate=True, head=current_rev)
        self.run_migrations()
        return True

    def run_migrations(self, legacy: bool = False) -> None:
        while True:
            try:
                migration_directory = (
                    self.alembic_directory.parent.joinpath("migration")
                    if legacy
                    else self.migration_directory
                )

                alembic_cfg = Config(migration_directory.joinpath("alembic.ini"))
                alembic_cfg.set_main_option(
                    "script_location",
                    migration_directory.joinpath("alembic").as_posix(),
                )
                alembic_cfg.set_main_option("sqlalchemy.url", self.info)
                command.upgrade(alembic_cfg, "head")
                break
            except Exception as e:
                print(e)
                pass

    def revert_migration(self):
        while True:
            try:
                alembic_cfg = Config(self.migration_directory.joinpath("alembic.ini"))
                alembic_cfg.set_main_option(
                    "script_location",
                    self.migration_directory.joinpath("alembic").as_posix(),
                )
                alembic_cfg.set_main_option("sqlalchemy.url", self.info)
                command.downgrade(alembic_cfg, "-1")
                break
            except Exception as e:
                print(e)
                pass

    def import_metadata(
        self, datas: list["ContentMetadata"], api_type: str | None = None
    ):
        database_path = self.name
        database_path.parent.mkdir(parents=True, exist_ok=True)
        self.run_migrations()
        db_collection = DBCollection()
        database = db_collection.database_picker(database_path.stem)
        database_session = self.session
        for post in datas:
            if post.api_type:
                api_type = post.api_type
            api_table = database.table_picker(api_type)
            if not api_table:
                return
            post_id = post.content_id
            post_created_at_string = post.created_at
            date_object = None
            if post_created_at_string:
                try:
                    date_object = datetime.fromisoformat(post_created_at_string)
                    pass
                except Exception as _e:
                    date_object = datetime.strptime(
                        post_created_at_string, "%d-%m-%Y %H:%M:%S"
                    )
                    pass
            result = database_session.query(api_table)
            post_db = result.filter_by(post_id=post_id).first()
            if not post_db:
                post_db = api_table()
            else:
                pass
            if api_type == "Messages":
                post_db.user_id = post.user_id
            post_db.post_id = post_id
            post_db.text = post.text
            post_db.price = post.price
            post_db.paid = post.paid
            post_db.archived = post.archived
            if date_object:
                post_db.created_at = date_object
            database_session.add(post_db)
            for media in post.medias:
                if media.media_type == "Texts":
                    continue
                media_created_at_string = media.created_at
                if not isinstance(media_created_at_string, datetime):
                    if isinstance(media_created_at_string, int):
                        date_object = datetime.fromtimestamp(media_created_at_string)
                    else:
                        try:
                            date_object = datetime.fromisoformat(
                                media_created_at_string
                            )
                        except Exception as _e:
                            date_object = datetime.strptime(
                                post_created_at_string, "%d-%m-%Y %H:%M:%S"
                            )
                            pass
                media_id = media.id
                result = database_session.query(database.media_table)
                media_db = result.filter_by(post_id=post_id, media_id=media_id).first()
                if not media_db:
                    media_db = result.filter_by(
                        filename=media.filename, created_at=date_object
                    ).first()
                    if not media_db:
                        media_db = database.media_table()
                else:
                    pass
                if (
                    post.__legacy__
                    and media_db.media_id != media.id
                    and media_db.media_id
                ):
                    media_id = media_db.media_id

                media_db.media_id = media_id
                media_db.post_id = post_id
                media_db.size = media.size if media_db.size is None else media_db.size
                media_db.link = media.urls[0] if media.urls else None
                media_db.preview = media.preview
                media_db.directory = (
                    media.directory.as_posix() if media.directory else None
                )
                media_db.filename = media.filename
                media_db.api_type = api_type
                media_db.media_type = media.media_type
                media_db.linked = media.linked
                if date_object:
                    media_db.created_at = date_object
                database_session.add(media_db)
        database_session.commit()
        database_session.close()
        return True

    def legacy_sqlite_updater(
        self,
        api_type: str,
        subscription: user_types,
    ):
        final_result: list[dict[str, Any]] = []
        legacy_metadata_path = self.name
        if legacy_metadata_path.exists():
            self.run_migrations(legacy=True)
            database_name = "user_data"
            database_session = self.session
            db_collection = DBCollection()
            database = db_collection.database_picker(database_name)
            if database:
                if api_type == "Messages":
                    api_table_table = database.table_picker(api_type, True)
                else:
                    api_table_table = database.table_picker(api_type)
                media_table_table = database.media_table.media_legacy_table
                if api_table_table:
                    result = database_session.query(api_table_table).all()
                    result2 = database_session.query(media_table_table).all()
                    for item in result:
                        for item2 in result2:
                            if item.post_id != item2.post_id:
                                continue
                            item.medias.append(item2)
                        item.user_id = subscription.id
                        final_result.append(item)
            database_session.close()
        return final_result

    def find_table(self, name: str):
        table = [x for x in self.metadata.sorted_tables if x.name == name]
        if table:
            return table[0]

    def get_count(self, q: Any):
        count_q = q.statement.with_only_columns(func.count()).order_by(None)
        count: int = q.session.execute(count_q).scalar()
        return count
