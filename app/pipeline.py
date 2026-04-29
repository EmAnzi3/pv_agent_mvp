from __future__ import annotations

import hashlib
import json
import re
import unicodedata
from datetime import datetime
from decimal import Decimal, InvalidOperation
from typing import Any

from sqlalchemy.orm import Session

from app.collectors.base import CollectorResult
from app.models import ProjectEvent, ProjectMaster, RawItem, Source


def truncate_text(value: Any, max_length: int | None) -> Any:
    """
    Tronca solo stringhe/testi.
    Lascia int, float, Decimal, datetime, None ecc. invariati.
    """
    if value is None:
        return None

    if max_length is None:
        return value

    if isinstance(value, (int, float, Decimal, datetime)):
        return value

    value = str(value)

    if len(value) <= max_length:
        return value

    return value[:max_length]


def normalize_text(value: Any) -> str | None:
    if value is None:
        return None

    text = str(value)
    text = text.replace("\ufeff", "")
    text = text.replace("\xa0", " ")
    text = text.replace("\r", " ")
    text = text.replace("\n", " ")
    text = re.sub(r"\s+", " ", text).strip()

    if not text:
        return None

    return text


def normalize_for_key(value: Any) -> str:
    text = normalize_text(value)
    if not text:
        return ""

    text = text.lower()
    text = unicodedata.normalize("NFKD", text)
    text = "".join(ch for ch in text if not unicodedata.combining(ch))
    text = re.sub(r"[^a-z0-9]+", " ", text)
    text = re.sub(r"\s+", " ", text).strip()

    return text


def normalize_list(value: Any) -> str | None:
    if value is None:
        return None

    if isinstance(value, list):
        cleaned = []
        seen = set()

        for item in value:
            text = normalize_text(item)
            if not text:
                continue

            key = normalize_for_key(text)
            if key in seen:
                continue

            seen.add(key)
            cleaned.append(text)

        if not cleaned:
            return None

        return truncate_text(", ".join(cleaned), 500)

    text = normalize_text(value)
    if not text:
        return None

    parts = re.split(r"[;,|]", text)
    cleaned = []
    seen = set()

    for part in parts:
        item = normalize_text(part)
        if not item:
            continue

        key = normalize_for_key(item)
        if key in seen:
            continue

        seen.add(key)
        cleaned.append(item)

    if not cleaned:
        return None

    return truncate_text(", ".join(cleaned), 500)


def normalize_status(status_raw: str | None) -> str | None:
    value = normalize_text(status_raw)
    if not value:
        return None

    lowered = value.lower()

    if any(x in lowered for x in ["in corso", "in itinere", "avviata", "avviato", "verifica amministrativa"]):
        return "in_corso"

    if any(x in lowered for x in ["conclusa", "concluso", "chiusa", "chiuso", "valutato", "parere via espresso"]):
        return "concluso"

    if any(x in lowered for x in ["archiviata", "archiviato", "improcedibile"]):
        return "archiviato"

    if any(x in lowered for x in ["positivo", "positiva", "favorevole", "compatibile"]):
        return "positivo"

    if any(x in lowered for x in ["negativo", "negativa", "sfavorevole", "non compatibile"]):
        return "negativo"

    if any(x in lowered for x in ["sospesa", "sospeso", "sospensione"]):
        return "sospeso"

    return truncate_text(value, 100)


def parse_italian_number(value: str) -> Decimal | None:
    if not value:
        return None

    text = str(value).strip()
    text = text.replace(" ", "")

    if "," in text and "." in text:
        # Caso italiano: 37.688,4 -> 37688.4
        if text.rfind(",") > text.rfind("."):
            text = text.replace(".", "").replace(",", ".")
        else:
            text = text.replace(",", "")
    elif "," in text:
        text = text.replace(",", ".")

    try:
        return Decimal(text)
    except InvalidOperation:
        return None


def normalize_power_to_mw(value: Any) -> float | None:
    """
    Estrae una potenza da testo e la converte in MW.

    Gestisce:
    - MW
    - MWP
    - KW
    - KWP

    Regola pratica:
    - se trova kW/kWp divide per 1000
    - se trova MW/MWp lascia in MW
    """
    text = normalize_text(value)
    if not text:
        return None

    text_norm = text.lower()
    text_norm = text_norm.replace("mwp", "mw")
    text_norm = text_norm.replace("mw p", "mw")
    text_norm = text_norm.replace("kwp", "kw")
    text_norm = text_norm.replace("kw p", "kw")

    patterns = [
        r"potenza(?:\s+\w+){0,8}?\s*(?:pari\s+a|di|da)?\s*([0-9]+(?:[.,][0-9]+)*(?:[.,][0-9]+)?)\s*(mw|kw)\b",
        r"([0-9]+(?:[.,][0-9]+)*(?:[.,][0-9]+)?)\s*(mw|kw)\b",
    ]

    candidates: list[tuple[Decimal, str]] = []

    for pattern in patterns:
        for match in re.finditer(pattern, text_norm, flags=re.IGNORECASE):
            number_raw = match.group(1)
            unit = match.group(2).lower()

            number = parse_italian_number(number_raw)
            if number is None:
                continue

            candidates.append((number, unit))

        if candidates:
            break

    if not candidates:
        return None

    # Preferisco valori MW espliciti.
    # Se ci sono solo kW, converto.
    mw_candidates = [(n, u) for n, u in candidates if u == "mw"]
    chosen_number, chosen_unit = mw_candidates[0] if mw_candidates else candidates[0]

    if chosen_unit == "kw":
        chosen_number = chosen_number / Decimal("1000")

    if chosen_number <= 0:
        return None

    # Evita numeri palesemente assurdi derivati da match sbagliati.
    # 2000 MW fotovoltaici in una singola procedura regionale è possibile solo come errore di parsing.
    if chosen_number > Decimal("1000"):
        return None

    return float(chosen_number)


def json_dumps_safe(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, default=str)


def values_different(old: Any, new: Any) -> bool:
    old_norm = normalize_text(old)
    new_norm = normalize_text(new)
    return old_norm != new_norm


def build_project_key(
    project_name: str | None,
    proponent: str | None,
    region: str | None,
    municipalities: str | None,
    power_mw: float | str | None,
    source_url: str | None = None,
    external_id: str | None = None,
) -> str | None:
    """
    Chiave progetto stabile.

    Priorità:
    1. URL fonte, se presente e valido.
       Questo è importante soprattutto per Sicilia, perché molte righe hanno titoli simili.
    2. external_id, se presente.
    3. hash semantico su nome + proponente + regione + comuni + potenza.
    """
    url = normalize_text(source_url)
    if url and url.startswith("http"):
        raw = f"url|{url}"
        return hashlib.sha1(raw.encode("utf-8")).hexdigest()

    ext = normalize_text(external_id)
    if ext:
        raw = f"external|{ext}"
        return hashlib.sha1(raw.encode("utf-8")).hexdigest()

    name_key = normalize_for_key(project_name)
    proponent_key = normalize_for_key(proponent)
    region_key = normalize_for_key(region)
    municipalities_key = normalize_for_key(municipalities)

    power_key = ""
    if power_mw is not None:
        try:
            power_key = str(round(float(power_mw), 6))
        except Exception:
            power_key = normalize_for_key(power_mw)

    raw = "|".join(
        [
            name_key,
            proponent_key,
            region_key,
            municipalities_key,
            power_key,
        ]
    )

    raw = raw.strip("|")

    if not raw:
        return None

    return hashlib.sha1(raw.encode("utf-8")).hexdigest()


def get_payload_value(payload: dict[str, Any], *keys: str) -> Any:
    for key in keys:
        value = payload.get(key)
        if value not in (None, "", [], {}):
            return value
    return None


def merge_value(old: Any, new: Any, max_length: int | None = None) -> Any:
    """
    Aggiorna solo se il nuovo valore è pieno.
    """
    if new in (None, "", [], {}):
        return old

    if max_length:
        new = truncate_text(new, max_length)

    return new


class IngestionPipeline:
    def __init__(self, db: Session):
        self.db = db

    def process_collector_results(
        self,
        source_name: str,
        source_url: str | None,
        results: list[CollectorResult],
    ) -> dict[str, Any]:
        source_name = truncate_text(source_name, 100)
        source_url = truncate_text(source_url, 1000)

        self._ensure_source(source_name, source_url)

        raw_items_saved = 0
        new_projects = 0
        changed_projects = 0
        unchanged_projects = 0
        events_added = 0
        skipped = 0

        now = datetime.utcnow()

        for result in results:
            try:
                payload = result.payload or {}
                if not isinstance(payload, dict):
                    payload = {"value": payload}

                external_id = truncate_text(
                    normalize_text(result.external_id)
                    or normalize_text(payload.get("external_id"))
                    or normalize_text(result.source_url)
                    or normalize_text(result.title),
                    500,
                )

                item_url = truncate_text(
                    normalize_text(result.source_url)
                    or normalize_text(payload.get("source_url"))
                    or source_url,
                    1000,
                )

                title = truncate_text(
                    normalize_text(payload.get("title"))
                    or normalize_text(payload.get("project_name"))
                    or normalize_text(result.title),
                    500,
                )

                if not external_id and not title:
                    skipped += 1
                    continue

                self._upsert_raw_item(
                    source_name=source_name,
                    external_id=external_id or title,
                    source_url=item_url,
                    payload=payload,
                    fetched_at=now,
                )
                raw_items_saved += 1

                project_name = truncate_text(
                    normalize_text(
                        get_payload_value(
                            payload,
                            "project_name",
                            "title",
                            "name",
                            "oggetto",
                        )
                    )
                    or normalize_text(result.title),
                    500,
                )

                if not project_name:
                    skipped += 1
                    continue

                proponent = truncate_text(
                    normalize_text(
                        get_payload_value(
                            payload,
                            "proponent",
                            "proponente",
                            "applicant",
                            "company",
                            "societa",
                            "società",
                        )
                    ),
                    255,
                )

                region = truncate_text(
                    normalize_text(
                        get_payload_value(
                            payload,
                            "region",
                            "regione",
                        )
                    ),
                    100,
                )

                province = truncate_text(
                    normalize_text(
                        get_payload_value(
                            payload,
                            "province",
                            "provincia",
                            "prov",
                        )
                    ),
                    100,
                )

                municipalities = normalize_list(
                    get_payload_value(
                        payload,
                        "municipalities",
                        "municipality",
                        "comuni",
                        "comune",
                    )
                )
                municipalities = truncate_text(municipalities, 500)

                project_type = truncate_text(
                    normalize_text(
                        get_payload_value(
                            payload,
                            "procedure",
                            "procedura",
                            "project_type",
                            "project_type_hint",
                            "tipologia",
                        )
                    ),
                    100,
                )

                status_raw = truncate_text(
                    normalize_text(
                        get_payload_value(
                            payload,
                            "status_raw",
                            "status",
                            "stato",
                        )
                    ),
                    255,
                )

                status_normalized = normalize_status(status_raw)

                power_mw = None

                power_candidates = [
                    get_payload_value(payload, "power_mw"),
                    get_payload_value(payload, "potenza_mw"),
                    get_payload_value(payload, "power"),
                    get_payload_value(payload, "potenza"),
                    project_name,
                    result.title,
                ]

                for candidate in power_candidates:
                    if candidate in (None, "", [], {}):
                        continue

                    if isinstance(candidate, (int, float, Decimal)):
                        try:
                            candidate_float = float(candidate)
                            if candidate_float > 0:
                                power_mw = candidate_float
                                break
                        except Exception:
                            pass

                    parsed_power = normalize_power_to_mw(candidate)
                    if parsed_power is not None:
                        power_mw = parsed_power
                        break

                project_key = build_project_key(
                    project_name=project_name,
                    proponent=proponent,
                    region=region,
                    municipalities=municipalities,
                    power_mw=power_mw,
                    source_url=item_url,
                    external_id=external_id,
                )
                project_key = truncate_text(project_key, 255)

                if not project_key:
                    skipped += 1
                    continue

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
                        primary_source=source_name,
                        primary_url=item_url,
                        updated_at=now,
                    )

                    self.db.add(project)
                    self.db.flush()

                    self._add_event(
                        project_id=project.id,
                        event_type="project_created",
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
                            "payload": payload,
                        },
                    )

                    new_projects += 1
                    events_added += 1
                    continue

                changed_fields: dict[str, Any] = {}

                def update_field(
                    attr: str,
                    new_value: Any,
                    max_length: int | None = None,
                    overwrite_empty_only: bool = False,
                ) -> None:
                    nonlocal changed_fields

                    if new_value in (None, "", [], {}):
                        return

                    if max_length:
                        new_value = truncate_text(new_value, max_length)

                    old_value = getattr(existing_project, attr)

                    if overwrite_empty_only and old_value not in (None, "", [], {}):
                        return

                    if values_different(old_value, new_value):
                        setattr(existing_project, attr, new_value)
                        changed_fields[attr] = {
                            "old": old_value,
                            "new": new_value,
                        }

                update_field("project_name", project_name, 500)
                update_field("proponent", proponent, 255)
                update_field("region", region, 100)
                update_field("province", province, 100, overwrite_empty_only=True)
                update_field("municipalities", municipalities, 500, overwrite_empty_only=True)
                update_field("project_type", project_type, 100)

                if power_mw is not None and existing_project.power_mw is None:
                    existing_project.power_mw = power_mw
                    changed_fields["power_mw"] = {
                        "old": None,
                        "new": power_mw,
                    }

                if status_normalized and values_different(existing_project.status_normalized, status_normalized):
                    existing_project.status_normalized = status_normalized
                    changed_fields["status_normalized"] = {
                        "old": existing_project.status_normalized,
                        "new": status_normalized,
                    }

                if item_url and values_different(existing_project.primary_url, item_url):
                    # Non sovrascrivo una primary_url valida con roba sospetta.
                    if str(item_url).startswith("http"):
                        existing_project.primary_url = item_url
                        changed_fields["primary_url"] = {
                            "old": existing_project.primary_url,
                            "new": item_url,
                        }

                if source_name and values_different(existing_project.primary_source, source_name):
                    existing_project.primary_source = source_name
                    changed_fields["primary_source"] = {
                        "old": existing_project.primary_source,
                        "new": source_name,
                    }

                if changed_fields:
                    existing_project.updated_at = now

                    self._add_event(
                        project_id=existing_project.id,
                        event_type="project_changed",
                        status_raw=status_raw,
                        status_normalized=status_normalized,
                        source_name=source_name,
                        source_url=item_url,
                        details={
                            "changed_fields": changed_fields,
                            "payload": payload,
                        },
                    )

                    changed_projects += 1
                    events_added += 1
                else:
                    unchanged_projects += 1

            except Exception:
                self.db.rollback()
                raise

        self.db.commit()

        return {
            "source_name": source_name,
            "raw_items_saved": raw_items_saved,
            "new_projects": new_projects,
            "changed_projects": changed_projects,
            "unchanged_projects": unchanged_projects,
            "events_added": events_added,
            "skipped": skipped,
        }

    def _upsert_raw_item(
        self,
        source_name: str,
        external_id: str | None,
        source_url: str | None,
        payload: dict[str, Any],
        fetched_at: datetime,
    ) -> None:
        source_name = truncate_text(source_name, 100)
        external_id = truncate_text(external_id, 500)
        source_url = truncate_text(source_url, 1000)

        payload_json = json_dumps_safe(payload)

        existing = (
            self.db.query(RawItem)
            .filter(RawItem.source_name == source_name)
            .filter(RawItem.external_id == external_id)
            .first()
        )

        if existing is not None:
            existing.source_url = source_url
            existing.payload_json = payload_json
            existing.fetched_at = fetched_at
            return

        raw_item = RawItem(
            source_name=source_name,
            external_id=external_id,
            source_url=source_url,
            payload_json=payload_json,
            fetched_at=fetched_at,
        )

        self.db.add(raw_item)

    def _add_event(
        self,
        project_id: int,
        event_type: str,
        status_raw: str | None,
        status_normalized: str | None,
        source_name: str,
        source_url: str | None,
        details: dict[str, Any],
    ) -> None:
        event = ProjectEvent(
            project_id=project_id,
            event_type=truncate_text(event_type, 100),
            status_raw=truncate_text(status_raw, 255),
            status_normalized=truncate_text(status_normalized, 100),
            source_name=truncate_text(source_name, 100),
            source_url=truncate_text(source_url, 1000),
            details_json=json_dumps_safe(details),
            created_at=datetime.utcnow(),
        )

        self.db.add(event)

    def _ensure_source(self, source_name: str | None, source_url: str | None) -> None:
        source_name = truncate_text(source_name, 100)
        source_url = truncate_text(source_url, 1000)

        if not source_name:
            return

        existing = (
            self.db.query(Source)
            .filter(Source.name == source_name)
            .first()
        )

        source_url_attr = None
        for candidate in ("url", "source_url", "base_url"):
            if hasattr(Source, candidate):
                source_url_attr = candidate
                break

        if existing is not None:
            if source_url_attr and source_url:
                old_url = getattr(existing, source_url_attr, None)
                if values_different(old_url, source_url):
                    setattr(existing, source_url_attr, source_url)
            return

        source = Source(name=source_name)

        if source_url_attr and source_url:
            setattr(source, source_url_attr, source_url)

        self.db.add(source)
        self.db.flush()