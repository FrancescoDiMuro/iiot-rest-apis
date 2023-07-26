from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column
from sqlalchemy import String

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
    created_at: Mapped[str] = mapped_column(nullable=False)
    updated_at: Mapped[str] = mapped_column(nullable=False)
    deleted_at: Mapped[str] = mapped_column(nullable=True)