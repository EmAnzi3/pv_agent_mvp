from __future__ import annotations

import json
from datetime import datetime

from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from app.collectors.base import CollectorResult
from app.dedupe import build_project_key
from app.models import ProjectEvent, ProjectMaster, RawItem, Source
from app.power_utils import parse_power_to_mw


def normalize_text(value: str | None) -> str | None:
    if value is None:
        return None

    cleaned = " ".join(str(value).replace("\xa0", " ").split()).strip()
    return cleaned or None


def truncate_text(value: str | None, max_len: int) -> str | None:
    if not value:
        return None

    value = normalize_text(value)
    if not value:
        return None

    if len(value) <= max_len:
        return value

    if max_len <= 3:
        return value[:max_len]

    return value[: max_len - 3] + "..."


def normalize_list(value) -> str | None:
    if value is None:
        return None

    if isinstance(value, list):
        cleaned = [normalize_text(str(x)) for x in value if normalize_text(str(x))]
        return ", ".join(cleaned) if cleaned else None

    return normalize_text(str(value))


def normalize_power_to_mw(value: str | None) -> str | None:
    parsed = parse_power_to_mw(value)
    return str(parsed) if parsed is not None else None


def normalize_status(status_raw: str | None) -> str | None:
    if not status_raw:
        return None

    value = normalize_text(status_raw)
    if not value:
        return None

    lowered = value.lower()

    if any(x in lowered for x in ["in corso", "in itinere", "verifica amministrativa"]):
        return "in_corso"

    if any(x in lowered for x in ["chiuso", "concluso", "valutato", "parere via espresso"]):
        return "concluso"

    if any(x in lowered for x in ["archiviato", "archiviata"]):
        return "archiviato"

    if any(x in lowered for x in ["positivo", "favorevole"]):
        return "positivo"

    if any(x in lowered for x in ["negativo", "sfavorevole"]):
        return "negativo"

    return truncate_text(value, 100)


class IngestionPipeline:
    def __init__(self, db: Session):
        self.db = db

    def process_collector_results(
        self,
        source_name: str,
        source_url: str | None,
        results: list[CollectorResult],
    ) -> dict:
        self._ensure_source(source_name, source_url)

        raw_items_saved = 0
        new_projects = 0
        changed_projects = 0

        for result in results:
            payload = result.payload or {}

            external_id = truncate_text(result.external_id, 255)
            if not external_id:
                continue

            item_url = truncate_text(result.source_url or source_url, 1000)

            raw_saved = self._save_raw_item(
                source_name=source_name,
                external_id=external_id,
                source_url=item_url,
                payload=payload,
            )
            if raw_saved:
                raw_items_saved += 1

            project_name = truncate_text(
                payload.get("title")
                or payload.get("project_name")
                or result.title,
                500,
            )

            if not project_name:
                continue

            proponent = truncate_text(
                payload.get("proponent")
                or payload.get("proponente"),
                255,
            )

            region = truncate_text(
                payload.get("region")
                or payload.get("regione"),
                100,
            )

            province = truncate_text(
                payload.get("province")
                or payload.get("provincia"),
                100,
            )

            municipalities = normalize_list(
                payload.get("municipalities")
                or payload.get("comuni")
                or payload.get("municipality")
                or payload.get("comune")
            )

            project_type = truncate_text(
                payload.get("procedure")
                or payload.get("procedura")
                or payload.get("project_type")
                or payload.get("project_type_hint"),
                100,
            )

            status_raw = truncate_text(
                payload.get("status_raw")
                or payload.get("status")
                or payload.get("stato"),
                255,
            )

            status_normalized = normalize_status(status_raw)

            # Potenza:
            # Prima analizziamo il titolo completo/testo progetto.
            # Solo dopo usiamo payload["power"], perché alcuni collector
            # possono avere già estratto male valori parziali.
            power_mw = None

            power_candidates = [
                payload.get("title"),
                result.title,
                project_name,
                payload.get("project_name"),
                payload.get("power"),
                payload.get("potenza"),
            ]

            for candidate in power_candidates:
                parsed_power = normalize_power_to_mw(candidate)
                if parsed_power is not None:
                    power_mw = truncate_text(parsed_power, 50)
                    break

            project_key = build_project_key(
                project_name=project_name,
                proponent=proponent,
                region=region,
                municipalities=municipalities,
                power_mw=power_mw,
            )
            project_key = truncate_text(project_key, 255)

            if not project_key:
                continue

            now = datetime.utcnow()

            existing_project = (
                self.db.query(ProjectMaster)
                .filter(ProjectMaster.project_key == project_key)
                .first()
            )

            if existing_project is None:
                project = ProjectMaster(
                    project_key=project_key,
                    project_name=project_name,
                    proponent=proponent,
                    region=region,
                    province=province,
                    municipalities=municipalities,
                    project_type=project_type,
                    power_mw=power_mw,
                    status_normalized=status_normalized,
                    primary_source=truncate_text(source_name, 100),
                    primary_url=item_url,
                    updated_at=now,
                )

                self.db.add(project)

                try:
                    self.db.flush()
                except IntegrityError:
                    self.db.rollback()
                    existing_project = (
                        self.db.query(ProjectMaster)
                        .filter(ProjectMaster.project_key == project_key)
                        .first()
                    )
                    if existing_project is None:
                        continue

                    changed = self._update_existing_project(
                        existing_project=existing_project,
                        project_name=project_name,
                        proponent=proponent,
                        region=region,
                        province=province,
                        municipalities=municipalities,
                        project_type=project_type,
                        power_mw=power_mw,
                        status_normalized=status_normalized,
                        source_name=source_name,
                        item_url=item_url,
                        status_raw=status_raw,
                    )
                    if changed:
                        changed_projects += 1
                    continue

                self._add_event(
                    project_id=project.id,
                    event_type="new_project",
                    status_raw=status_raw,
                    status_normalized=status_normalized,
                    source_name=source_name,
                    source_url=item_url,
                    details={
                        "project_name": project_name,
                        "proponent": proponent,
                        "region": region,
                        "province": province,
                        "municipalities": municipalities,
                        "project_type": project_type,
                        "power_mw": power_mw,
                    },
                )

                new_projects += 1

            else:
                changed = self._update_existing_project(
                    existing_project=existing_project,
                    project_name=project_name,
                    proponent=proponent,
                    region=region,
                    province=province,
                    municipalities=municipalities,
                    project_type=project_type,
                    power_mw=power_mw,
                    status_normalized=status_normalized,
                    source_name=source_name,
                    item_url=item_url,
                    status_raw=status_raw,
                )

                if changed:
                    changed_projects += 1

        self.db.commit()

        return {
            "source_name": source_name,
            "raw_items_saved": raw_items_saved,
            "new_projects": new_projects,
            "changed_projects": changed_projects,
        }

    def _update_existing_project(
        self,
        existing_project: ProjectMaster,
        project_name: str,
        proponent: str | None,
        region: str | None,
        province: str | None,
        municipalities: str | None,
        project_type: str | None,
        power_mw: str | None,
        status_normalized: str | None,
        source_name: str,
        item_url: str | None,
        status_raw: str | None,
    ) -> bool:
        changed_fields = {}

        if (existing_project.project_name or "") != (project_name or ""):
            changed_fields["project_name"] = {
                "old": existing_project.project_name,
                "new": project_name,
            }
            existing_project.project_name = project_name

        if (existing_project.proponent or "") != (proponent or ""):
            changed_fields["proponent"] = {
                "old": existing_project.proponent,
                "new": proponent,
            }
            existing_project.proponent = proponent

        if (existing_project.region or "") != (region or ""):
            changed_fields["region"] = {
                "old": existing_project.region,
                "new": region,
            }
            existing_project.region = region

        if (existing_project.province or "") != (province or ""):
            changed_fields["province"] = {
                "old": existing_project.province,
                "new": province,
            }
            existing_project.province = province

        if (existing_project.municipalities or "") != (municipalities or ""):
            changed_fields["municipalities"] = {
                "old": existing_project.municipalities,
                "new": municipalities,
            }
            existing_project.municipalities = municipalities

        if (existing_project.project_type or "") != (project_type or ""):
            changed_fields["project_type"] = {
                "old": existing_project.project_type,
                "new": project_type,
            }
            existing_project.project_type = project_type

        if (existing_project.power_mw or "") != (power_mw or ""):
            changed_fields["power_mw"] = {
                "old": existing_project.power_mw,
                "new": power_mw,
            }
            existing_project.power_mw = power_mw

        if (existing_project.status_normalized or "") != (status_normalized or ""):
            changed_fields["status_normalized"] = {
                "old": existing_project.status_normalized,
                "new": status_normalized,
            }
            existing_project.status_normalized = status_normalized

        if (existing_project.primary_url or "") != (item_url or ""):
            changed_fields["primary_url"] = {
                "old": existing_project.primary_url,
                "new": item_url,
            }
            existing_project.primary_url = item_url

        existing_project.primary_source = truncate_text(source_name, 100)
        existing_project.updated_at = datetime.utcnow()

        if changed_fields:
            self._add_event(
                project_id=existing_project.id,
                event_type="project_changed",
                status_raw=status_raw,
                status_normalized=status_normalized,
                source_name=source_name,
                source_url=item_url,
                details=changed_fields,
            )
            return True

        return False

    def _ensure_source(self, source_name: str, source_url: str | None) -> None:
        source_name = truncate_text(source_name, 100)
        source_url = truncate_text(source_url, 500)

        if not source_name:
            return

        existing = (
            self.db.query(Source)
            .filter(Source.name == source_name)
            .first()
        )

        if existing is not None:
            if source_url and existing.base_url != source_url:
                existing.base_url = source_url
                self.db.flush()
            return

        source = Source(
            name=source_name,
            base_url=source_url,
            created_at=datetime.utcnow(),
        )

        self.db.add(source)

        try:
            self.db.flush()
        except IntegrityError:
            self.db.rollback()

            existing = (
                self.db.query(Source)
                .filter(Source.name == source_name)
                .first()
            )

            if existing is not None and source_url and existing.base_url != source_url:
                existing.base_url = source_url
                self.db.flush()

    def _save_raw_item(
        self,
        source_name: str,
        external_id: str,
        source_url: str | None,
        payload: dict,
    ) -> bool:
        source_name = truncate_text(source_name, 100) or "unknown"
        external_id = truncate_text(external_id, 255) or "unknown"
        source_url = truncate_text(source_url, 1000)

        existing = (
            self.db.query(RawItem)
            .filter(
                RawItem.source_name == source_name,
                RawItem.external_id == external_id,
            )
            .first()
        )

        if existing is not None:
            return False

        raw_item = RawItem(
            source_name=source_name,
            external_id=external_id,
            source_url=source_url,
            payload_json=json.dumps(payload, ensure_ascii=False),
            fetched_at=datetime.utcnow(),
        )

        self.db.add(raw_item)

        try:
            self.db.flush()
            return True
        except IntegrityError:
            self.db.rollback()
            return False

    def _add_event(
        self,
        project_id: int,
        event_type: str,
        status_raw: str | None,
        status_normalized: str | None,
        source_name: str,
        source_url: str | None,
        details: dict | None = None,
    ) -> None:
        event = ProjectEvent(
            project_id=project_id,
            event_type=truncate_text(event_type, 100) or "event",
            status_raw=truncate_text(status_raw, 255),
            status_normalized=truncate_text(status_normalized, 100),
            source_name=truncate_text(source_name, 100) or "unknown",
            source_url=truncate_text(source_url, 1000),
            details_json=json.dumps(details or {}, ensure_ascii=False),
            created_at=datetime.utcnow(),
        )

        self.db.add(event)