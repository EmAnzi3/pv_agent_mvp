from __future__ import annotations

import json
from datetime import datetime
from typing import Iterable

from sqlalchemy import select
from sqlalchemy.orm import Session

from app.collectors.base import CollectorResult
from app.dedupe import build_project_key
from app.models import ProjectEvent, ProjectMaster, RawItem, Source
from app.normalizers import as_list, normalize_power_to_mw, normalize_project_type, normalize_status


class PipelineRunSummary(dict):
    pass


class IngestionPipeline:
    def __init__(self, db: Session) -> None:
        self.db = db

    def ensure_source(self, name: str, base_url: str | None) -> None:
        existing = self.db.scalar(select(Source).where(Source.name == name))
        if existing:
            return
        self.db.add(Source(name=name, base_url=base_url))
        self.db.commit()

    def process_collector_results(
        self,
        source_name: str,
        source_url: str | None,
        results: Iterable[CollectorResult],
    ) -> PipelineRunSummary:
        self.ensure_source(source_name, source_url)
        new_projects = 0
        changed_projects = 0
        raw_items_saved = 0

        for item in results:
            if self._save_raw_item(source_name, item):
                raw_items_saved += 1

            payload = item.payload
            project_name = payload.get("title") or item.title
            proponent = payload.get("proponent")
            region = payload.get("region")
            province = payload.get("province")
            municipalities = as_list(payload.get("municipalities"))
            power_mw = normalize_power_to_mw(payload.get("power"))
            status_raw = payload.get("status_raw")
            status_normalized = normalize_status(status_raw)
            project_type = normalize_project_type(project_name, payload.get("project_type_hint"))
            project_key = build_project_key(project_name, proponent, region, municipalities, power_mw)

            existing_project = self.db.scalar(
                select(ProjectMaster).where(ProjectMaster.project_key == project_key)
            )

            if not existing_project:
                existing_project = ProjectMaster(
                    project_key=project_key,
                    project_name=project_name[:500],
                    proponent=proponent,
                    region=region,
                    province=province,
                    municipalities=", ".join(municipalities) if municipalities else None,
                    project_type=project_type,
                    power_mw=power_mw,
                    status_normalized=status_normalized,
                    primary_source=source_name,
                    primary_url=item.source_url,
                    updated_at=datetime.utcnow(),
                )
                self.db.add(existing_project)
                self.db.flush()
                self._add_event(existing_project.id, "NEW_PROJECT", status_raw, status_normalized, source_name, item.source_url, payload)
                new_projects += 1
            else:
                has_change = False
                if existing_project.status_normalized != status_normalized:
                    has_change = True
                if (existing_project.power_mw or "") != (power_mw or ""):
                    has_change = True
                if has_change:
                    existing_project.status_normalized = status_normalized
                    existing_project.power_mw = power_mw
                    existing_project.updated_at = datetime.utcnow()
                    self._add_event(existing_project.id, "STATUS_CHANGE", status_raw, status_normalized, source_name, item.source_url, payload)
                    changed_projects += 1

        self.db.commit()
        return PipelineRunSummary(
            source_name=source_name,
            raw_items_saved=raw_items_saved,
            new_projects=new_projects,
            changed_projects=changed_projects,
        )

    def _save_raw_item(self, source_name: str, item: CollectorResult) -> bool:
        existing = self.db.scalar(
            select(RawItem).where(
                RawItem.source_name == source_name,
                RawItem.external_id == item.external_id,
            )
        )
        if existing:
            return False
        self.db.add(
            RawItem(
                source_name=source_name,
                external_id=item.external_id,
                source_url=item.source_url,
                payload_json=json.dumps(item.payload, ensure_ascii=False),
            )
        )
        self.db.flush()
        return True

    def _add_event(
        self,
        project_id: int,
        event_type: str,
        status_raw: str | None,
        status_normalized: str | None,
        source_name: str,
        source_url: str | None,
        payload: dict,
    ) -> None:
        self.db.add(
            ProjectEvent(
                project_id=project_id,
                event_type=event_type,
                status_raw=status_raw,
                status_normalized=status_normalized,
                source_name=source_name,
                source_url=source_url,
                details_json=json.dumps(payload, ensure_ascii=False),
            )
        )
