from sqlalchemy import MetaData
from sqlalchemy.orm import DeclarativeBase


class Base(DeclarativeBase):
    metadata = MetaData(naming_convention={"pk": "pk_%(table_name)s"})
