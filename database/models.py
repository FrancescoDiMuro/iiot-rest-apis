from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship
from sqlalchemy import ForeignKey, String
from typing import List
from datetime import datetime, timezone

TIMESTAMP_FORMAT: str = '%Y%m%dT%H%M%S'

class Base(DeclarativeBase):
    pass

class Tags(Base):
    __tablename__ = 'tags'

    id: Mapped[int] = mapped_column(primary_key=True, nullable=False)
    name: Mapped[str] = mapped_column(String(30), nullable=False)
    description: Mapped[str] = mapped_column(String(40), nullable=False)
    address: Mapped[str] = mapped_column(nullable=False)
    collection_interval: Mapped[str] = mapped_column(nullable=False)
    low_limit: Mapped[float] = mapped_column(nullable=False)
    high_limit: Mapped[float] = mapped_column(nullable=False)
    egu: Mapped[str] = mapped_column(nullable=False)
    created_at: Mapped[str] = mapped_column(nullable=False, server_default=datetime.now(timezone.utc).strftime(TIMESTAMP_FORMAT))
    updated_at: Mapped[str] = mapped_column(nullable=False, server_default=datetime.now(timezone.utc).strftime(TIMESTAMP_FORMAT))
    deleted_at: Mapped[str] = mapped_column(nullable=True)
    data: Mapped[List["Data"]] = relationship()


class Data(Base):
    __tablename__ = 'test'

    id: Mapped[int] = mapped_column(primary_key=True, nullable=False)
    timestamp: Mapped[str] = mapped_column(nullable=False)
    value: Mapped[float] = mapped_column(nullable=False)
    tag_id: Mapped[int] = mapped_column(ForeignKey('tags.id'))