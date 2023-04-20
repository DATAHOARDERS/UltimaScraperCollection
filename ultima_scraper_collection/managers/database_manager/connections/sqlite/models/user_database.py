from typing import Optional, cast

import sqlalchemy
from sqlalchemy.orm.decl_api import declarative_base

from ultima_scraper_collection.managers.database_manager.connections.sqlite.models.api_model import (
    ApiModel,
)
from ultima_scraper_collection.managers.database_manager.connections.sqlite.models.media_model import (
    TemplateMediaModel,
)

Base = declarative_base()
LegacyBase = declarative_base()


class profiles_table(Base):
    __tablename__ = "profiles"
    id = sqlalchemy.Column(sqlalchemy.Integer, primary_key=True)
    user_id = cast(int, sqlalchemy.Column(sqlalchemy.Integer, nullable=False))
    username = sqlalchemy.Column(sqlalchemy.String, unique=True, nullable=False)


class stories_table(ApiModel, Base):
    ApiModel.__tablename__ = "stories"


class posts_table(ApiModel, Base):
    ApiModel.__tablename__ = "posts"


class messages_table(ApiModel, Base):
    ApiModel.__tablename__ = "messages"
    user_id = cast(Optional[int], sqlalchemy.Column(sqlalchemy.Integer))

    class api_legacy_table(ApiModel, LegacyBase):
        pass


class products_table(ApiModel, Base):
    ApiModel.__tablename__ = "products"
    title = sqlalchemy.Column(sqlalchemy.String)


class others_table(ApiModel, Base):
    ApiModel.__tablename__ = "others"


# class comments_table(api_table,Base):
#     api_table.__tablename__ = "comments"


class media_table(TemplateMediaModel, Base):
    class media_legacy_table(TemplateMediaModel().legacy_2(LegacyBase), LegacyBase):
        pass


def table_picker(table_name: str, legacy: bool = False):
    match table_name:
        case "Stories" | "Highlights":
            table = stories_table
        case "Posts":
            table = posts_table
        case "Messages" | "Chats" | "MassMessages":
            table = messages_table if not legacy else messages_table().api_legacy_table
        case "Products":
            table = products_table
        case "Others":
            table = others_table
        case _:
            raise Exception(f'"{table_name}" is an invalid table name')
    return table
