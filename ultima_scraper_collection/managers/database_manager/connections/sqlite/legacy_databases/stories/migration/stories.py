# type: ignore
### posts.py ###

from sqlalchemy.orm import declarative_base

from ultima_scraper_collection.managers.database_manager.connections.sqlite.models.api_model import (
    ApiModel,
)
from ultima_scraper_collection.managers.database_manager.connections.sqlite.models.media_model import (
    TemplateMediaModel,
)

Base = declarative_base()


class api_table(ApiModel, Base):
    ApiModel.__tablename__ = "stories"


class TemplateMediaModel(TemplateMediaModel, Base):
    pass
