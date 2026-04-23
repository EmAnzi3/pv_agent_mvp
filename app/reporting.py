from __future__ import annotations

import csv
from datetime import datetime, timedelta
from pathlib import Path

from sqlalchemy import select
from sqlalchemy.orm import Session

from app.config import settings
from app.models import ProjectEvent, ProjectMaster


def _ensure_reports_dir() -> Path:
    path = Path(settings.reports_dir)
    path.mkdir(parents=True, exist_ok=True)
    return path


class ReportBuilder:
    def __init__(self, db: Session) -> None:
        self.db = db

    def build_daily_reports(self) -> list[Path]:
        reports_dir = _ensure_reports_dir()
        stamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
        files: list[Path] = []

        files.append(self._build_new_projects_report(reports_dir / f"new_projects_{stamp}.csv"))
        files.append(self._build_status_changes_report(reports_dir / f"status_changes_{stamp}.csv"))
        files.append(self._build_snapshot_report(reports_dir / f"projects_snapshot_{stamp}.csv"))
        return files

    def _build_new_projects_report(self, path: Path) -> Path:
        since = datetime.utcnow() - timedelta(days=1)
        rows = self.db.scalars(
            select(ProjectEvent).where(ProjectEvent.event_type == "NEW_PROJECT", ProjectEvent.created_at >= since)
        ).all()
        with path.open("w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerow(["project_id", "event_type", "status", "source", "source_url", "created_at"])
            for row in rows:
                writer.writerow([row.project_id, row.event_type, row.status_normalized, row.source_name, row.source_url, row.created_at.isoformat()])
        return path

    def _build_status_changes_report(self, path: Path) -> Path:
        since = datetime.utcnow() - timedelta(days=1)
        rows = self.db.scalars(
            select(ProjectEvent).where(ProjectEvent.event_type == "STATUS_CHANGE", ProjectEvent.created_at >= since)
        ).all()
        with path.open("w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerow(["project_id", "event_type", "status", "source", "source_url", "created_at"])
            for row in rows:
                writer.writerow([row.project_id, row.event_type, row.status_normalized, row.source_name, row.source_url, row.created_at.isoformat()])
        return path

    def _build_snapshot_report(self, path: Path) -> Path:
        rows = self.db.scalars(select(ProjectMaster)).all()
        with path.open("w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerow(["project_name", "proponent", "region", "province", "municipalities", "project_type", "power_mw", "status", "source", "url", "updated_at"])
            for row in rows:
                writer.writerow([
                    row.project_name,
                    row.proponent,
                    row.region,
                    row.province,
                    row.municipalities,
                    row.project_type,
                    row.power_mw,
                    row.status_normalized,
                    row.primary_source,
                    row.primary_url,
                    row.updated_at.isoformat() if row.updated_at else None,
                ])
        return path
