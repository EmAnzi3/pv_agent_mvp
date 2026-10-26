from __future__ import annotations

import hashlib
import html
import json
import re
from typing import Any

from app.wind_agents.base import AgentFinding, BaseWindAgent


API_BASE_URL = "https://api.regione.toscana.it"
API_URL_TEMPLATE = API_BASE_URL + "/C01/suap-dt/v1/avvisi/eventiPubblici/{page_index}/{page_size}"
PUBLIC_URL = "https://servizi.patti.regione.toscana.it/star-info/avvisiPubblici"

WIND_TERMS = (
    "eolico",
    "eolica",
    "parco eolico",
    "impianto eolico",
    "aerogenerator",
    "repowering",
)


class ToscanaWindAgent(BaseWindAgent):
    """Wind adaptation of pv_agent_mvp's Toscana GeA collector.

    Reuses the same regional API and JSON-normalisation strategy, replacing the
    PV filter with a wind-only filter. Findings remain source-level and never
    promote or mutate canonical projects directly.
    """

    agent_name = "institutional_watch"
    source_name = "Regione Toscana GeA"
    base_url = PUBLIC_URL

    def __init__(self, page_size: int = 100, max_pages: int = 30) -> None:
        super().__init__()
        self.page_size = page_size
        self.max_pages = max_pages

    @staticmethod
    def _clean(value: object) -> str:
        return re.sub(r"\s+", " ", str(value or "")).strip()

    @staticmethod
    def _normalize_key(value: str) -> str:
        return re.sub(r"[^a-z0-9]", "", value.lower())

    @classmethod
    def _is_wind(cls, text: str) -> bool:
        lowered = cls._clean(text).lower()
        return any(term in lowered for term in WIND_TERMS)

    @classmethod
    def _first_non_empty(cls, data: dict[str, Any], keys: list[str]) -> str | None:
        normalized = {cls._normalize_key(k): v for k, v in data.items()}
        for key in keys:
            value = data.get(key)
            if value is None:
                value = normalized.get(cls._normalize_key(key))
            if value is None or isinstance(value, (dict, list)):
                continue
            text = cls._clean(value)
            if text:
                return text
        return None

    @classmethod
    def _expand_item(cls, item: dict[str, Any]) -> dict[str, Any]:
        expanded = dict(item)
        contenuto = item.get("contenuto")
        if isinstance(contenuto, str) and contenuto.strip():
            parsed = None
            for candidate in (contenuto, html.unescape(contenuto)):
                try:
                    parsed = json.loads(candidate)
                    break
                except Exception:
                    pass
            if isinstance(parsed, dict):
                for key, value in parsed.items():
                    expanded.setdefault(key, value)
        for key, value in list(expanded.items()):
            if isinstance(value, dict):
                for sub_key, sub_value in value.items():
                    expanded.setdefault(sub_key, sub_value)
                    expanded.setdefault(f"{key}_{sub_key}", sub_value)
        return expanded

    @staticmethod
    def _extract_items(data: Any) -> list[dict[str, Any]]:
        if isinstance(data, list):
            return [item for item in data if isinstance(item, dict)]
        if not isinstance(data, dict):
            return []
        for key in ("elements", "content", "data", "items", "result", "results", "eventi", "avvisi"):
            value = data.get(key)
            if isinstance(value, list):
                return [item for item in value if isinstance(item, dict)]
        for value in data.values():
            if isinstance(value, list) and all(isinstance(item, dict) for item in value):
                return value
        return []

    def _fetch_page(self, page_index: int) -> Any:
        url = API_URL_TEMPLATE.format(page_index=page_index, page_size=self.page_size)
        headers = {
            "Accept": "application/json, text/plain, */*",
            "Content-Type": "application/json",
            "Origin": "https://servizi.patti.regione.toscana.it",
            "Referer": "https://servizi.patti.regione.toscana.it/",
            "User-Agent": "Wind-Radar-Agent/0.6",
            "X-Domain": "GEA",
            "X-Ente": "GEA",
            "Cache-Control": "no-cache",
        }
        params = {"dominio": "GEA", "codiceTerritorio": "GEA"}
        body = {"sortField": "stato", "order": "desc", "sezione": "GEA"}
        response = self.session.post(url, params=params, json=body, headers=headers, timeout=90)
        response.raise_for_status()
        return response.json()

    @classmethod
    def _power_mw(cls, text: str) -> float | None:
        for match in re.finditer(
            r"(?<![\d.,])([0-9]+(?:[.\s][0-9]{3})*(?:,[0-9]+)?|[0-9]+(?:\.[0-9]+)?)\s*MW\b",
            text,
            flags=re.IGNORECASE,
        ):
            raw = match.group(1).replace(" ", "")
            if "," in raw:
                raw = raw.replace(".", "").replace(",", ".")
            try:
                value = float(raw)
            except ValueError:
                continue
            if 0 < value < 5000:
                return value
        return None

    @classmethod
    def _procedure(cls, text: str) -> str | None:
        lowered = cls._clean(text).lower()
        if "paur" in lowered or "procedimento autorizzatorio unico" in lowered:
            return "PAUR"
        if "verifica" in lowered or "assoggettabil" in lowered:
            return "VERIFICA"
        if "valutazione di impatto ambientale" in lowered or re.search(r"\bvia\b", lowered):
            return "VIA"
        return None

    @classmethod
    def _status(cls, text: str) -> str | None:
        lowered = cls._clean(text).lower()
        for needle, label in (
            ("favorevole con prescrizioni", "Favorevole con prescrizioni"),
            ("favorevole", "Favorevole"),
            ("archiviat", "Archiviato"),
            ("conclus", "Concluso"),
            ("integrazioni", "Integrazioni"),
            ("osservazioni", "Osservazioni"),
            ("in corso", "In corso"),
        ):
            if needle in lowered:
                return label
        return None

    @classmethod
    def _url(cls, data: dict[str, Any]) -> str:
        for key in ("url", "link", "href", "dettaglio", "urlDettaglio", "linkDettaglio", "documentazione"):
            value = data.get(key)
            if value and cls._clean(value).startswith("http"):
                return cls._clean(value)
        item_id = cls._first_non_empty(data, ["id", "idEvento", "idAvviso", "codice", "identificativo"])
        return f"{PUBLIC_URL}?id={item_id}" if item_id else PUBLIC_URL

    @classmethod
    def _external_id(cls, data: dict[str, Any], title: str, date: str | None, url: str) -> str:
        source_id = cls._first_non_empty(data, ["id", "idEvento", "idAvviso", "codice", "identificativo"])
        if source_id:
            return f"TOSCANA-GEA-{source_id}"
        raw = "|".join([date or "", title, url])
        return "TOSCANA-GEA-" + hashlib.sha1(raw.encode("utf-8")).hexdigest()[:16]

    def fetch(self) -> list[AgentFinding]:
        findings: list[AgentFinding] = []
        seen: set[str] = set()

        for page_index in range(self.max_pages):
            data = self._fetch_page(page_index)
            items = self._extract_items(data)
            if not items:
                break

            for item in items:
                expanded = self._expand_item(item)
                raw_text = self._clean(
                    " | ".join(
                        str(value)
                        for value in expanded.values()
                        if value is not None and not isinstance(value, (dict, list))
                    )
                )
                if not raw_text or not self._is_wind(raw_text):
                    continue

                title = self._first_non_empty(
                    expanded,
                    ["descrizione", "oggetto", "titolo", "denominazione", "intervento", "nome", "procedimento"],
                ) or raw_text[:700]
                date = self._first_non_empty(
                    expanded,
                    ["dataInizioPubblicazione", "dataPubblicazione", "dataUltimoAggiornamento", "dataFinePubblicazione", "dataProtocollo", "createdAt", "updatedAt"],
                )
                proponent = self._first_non_empty(
                    expanded,
                    ["proponente", "richiedente", "soggettoProponente", "intestatario", "societa", "società", "azienda"],
                )
                municipality = self._first_non_empty(
                    expanded,
                    ["comune", "comuni", "localizzazione", "ubicazione", "territorio"],
                )
                province = self._first_non_empty(expanded, ["provincia", "siglaProvincia"])
                procedure = self._first_non_empty(
                    expanded,
                    ["tipoProcedimento", "procedimento", "procedura", "tipologia", "tipo"],
                ) or self._procedure(raw_text)
                status = self._first_non_empty(
                    expanded,
                    ["stato", "status", "statoProcedimento", "fase"],
                ) or self._status(raw_text)
                source_url = self._url(expanded)
                external_id = self._external_id(expanded, title, date, source_url)
                if external_id in seen:
                    continue
                seen.add(external_id)

                findings.append(
                    AgentFinding(
                        external_id=external_id,
                        source_name=self.source_name,
                        source_url=source_url,
                        title=title[:700],
                        finding_type="project_source",
                        payload={
                            "project_name": title[:700],
                            "proponent": proponent,
                            "region": "Toscana",
                            "province": province,
                            "municipality": municipality,
                            "power_mw": self._power_mw(raw_text),
                            "procedure": procedure,
                            "status_raw": status,
                            "source_date": date,
                            "sector": "eolico",
                            "source_grade_ceiling": "A1",
                            "project_specific": True,
                            "source_adapter_origin": "pv_agent_mvp/toscana.py",
                        },
                    )
                )

            if len(items) < self.page_size:
                break

        return findings
