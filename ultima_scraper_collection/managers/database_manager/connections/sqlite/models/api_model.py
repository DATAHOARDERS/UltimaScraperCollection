### api_table.py ###

from datetime import datetime
from typing import cast

import sqlalchemy
from sqlalchemy.orm import declarative_base
from ultima_scraper_collection.managers.database_manager.connections.sqlite.models.media_model import (
    TemplateMediaModel,
)

LegacyBase = declarative_base()


class ApiModel:
    __tablename__ = ""
    id = sqlalchemy.Column(sqlalchemy.Integer, primary_key=True)
    post_id = cast(
        int, sqlalchemy.Column(sqlalchemy.Integer, unique=True, nullable=False)
    )
    text = cast(str, sqlalchemy.Column(sqlalchemy.String))
    price = cast(int, sqlalchemy.Column(sqlalchemy.Integer))
    paid = sqlalchemy.Column(sqlalchemy.Integer)
    archived = cast(bool, sqlalchemy.Column(sqlalchemy.Boolean, default=False))
    created_at = cast(datetime, sqlalchemy.Column(sqlalchemy.TIMESTAMP))
    medias: list[TemplateMediaModel] = []

    def legacy(self, table_name: str):
        class legacy_api_table(LegacyBase):
            __tablename__ = table_name
            id = sqlalchemy.Column(sqlalchemy.Integer, primary_key=True)
            text = sqlalchemy.Column(sqlalchemy.String)
            price = sqlalchemy.Column(sqlalchemy.Integer)
            paid = sqlalchemy.Column(sqlalchemy.Integer)
            created_at = sqlalchemy.Column(sqlalchemy.DATETIME)

        return legacy_api_table

    def convert(self):
        item = self.__dict__
        item.pop("_sa_instance_state")
        return item

    def find_media(self, media_id: int):
        for db_media in self.medias:
            if db_media.media_id == media_id:
                return db_media
