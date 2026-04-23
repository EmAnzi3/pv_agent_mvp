from __future__ import annotations

from datetime import datetime

from sqlalchemy import DateTime, ForeignKey, Integer, String, Text, UniqueConstraint
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship


class Base(DeclarativeBase):
    pass


class Source(Base):
    __tablename__ = "sources"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    name: Mapped[str] = mapped_column(String(100), unique=True, nullable=False)
    base_url: Mapped[str | None] = mapped_column(String(500), nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)


class RawItem(Base):
    __tablename__ = "raw_items"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    source_name: Mapped[str] = mapped_column(String(100), nullable=False)
    external_id: Mapped[str] = mapped_column(String(255), nullable=False)
    source_url: Mapped[str | None] = mapped_column(String(1000), nullable=True)
    payload_json: Mapped[str] = mapped_column(Text, nullable=False)
    fetched_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)

    __table_args__ = (UniqueConstraint("source_name", "external_id", name="uq_raw_source_external"),)


class ProjectMaster(Base):
    __tablename__ = "projects_master"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    project_key: Mapped[str] = mapped_column(String(255), unique=True, nullable=False)
    project_name: Mapped[str] = mapped_column(String(500), nullable=False)
    proponent: Mapped[str | None] = mapped_column(String(255), nullable=True)
    region: Mapped[str | None] = mapped_column(String(100), nullable=True)
    province: Mapped[str | None] = mapped_column(String(100), nullable=True)
    municipalities: Mapped[str | None] = mapped_column(Text, nullable=True)
    project_type: Mapped[str | None] = mapped_column(String(100), nullable=True)
    power_mw: Mapped[str | None] = mapped_column(String(50), nullable=True)
    status_normalized: Mapped[str | None] = mapped_column(String(100), nullable=True)
    primary_source: Mapped[str | None] = mapped_column(String(100), nullable=True)
    primary_url: Mapped[str | None] = mapped_column(String(1000), nullable=True)
    updated_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

    events: Mapped[list["ProjectEvent"]] = relationship(back_populates="project", cascade="all, delete-orphan")


class ProjectEvent(Base):
    __tablename__ = "project_events"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    project_id: Mapped[int] = mapped_column(ForeignKey("projects_master.id"), nullable=False)
    event_type: Mapped[str] = mapped_column(String(100), nullable=False)
    status_raw: Mapped[str | None] = mapped_column(String(255), nullable=True)
    status_normalized: Mapped[str | None] = mapped_column(String(100), nullable=True)
    source_name: Mapped[str] = mapped_column(String(100), nullable=False)
    source_url: Mapped[str | None] = mapped_column(String(1000), nullable=True)
    details_json: Mapped[str | None] = mapped_column(Text, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)

    project: Mapped[ProjectMaster] = relationship(back_populates="events")
