from __future__ import annotations

import ast
import html
import json
import re
from datetime import date

from bs4 import BeautifulSoup

from app.wind_agents.base import AgentFinding, BaseWindAgent


MAP_URL = "https://atos.arrr.it/mappa_fer.php?mn=fer&mnin=mappafer"
DETAIL_TEMPLATE = "https://atos.arrr.it/scheda_impianto_fer.php?mn=fer&id_impianto={id_impianto}"


class ToscanaAtosWindAgent(BaseWindAgent):
    """Wind adaptation of the ATOS Toscana FER collector from pv_agent_mvp.

    It intentionally avoids hard-coding the PV source-type ids: the map is
    requested without a technology filter and rows are retained only when the
    marker/title/detail explicitly identifies wind. This makes the adapter less
    brittle if ATOS changes numeric source codes.
    """

    agent_name = "institutional_watch"
    source_name = "ATOS Toscana FER"
    base_url = MAP_URL

    def __init__(self, max_details: int = 500) -> None:
        super().__init__()
        self.max_details = max_details

    @staticmethod
    def _clean(value: object) -> str:
        return re.sub(r"\s+", " ", str(value or "")).strip()

    @classmethod
    def _is_wind_text(cls, value: object) -> bool:
        text = cls._clean(value).lower()
        return any(term in text for term in ("eolico", "eolica", "aerogenerator", "wind"))

    def _fetch_map_results(self) -> str:
        self.session.headers.update(
            {
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
                "Origin": "https://atos.arrr.it",
                "Referer": MAP_URL,
            }
        )
        initial = self.session.get(MAP_URL, timeout=90)
        initial.raise_for_status()

        # Same form mechanics used by pv_agent_mvp, but without tipologia_fonte[]
        # so the adapter does not depend on undocumented numeric wind ids.
        payload = [
            ("n_page", "1"),
            ("prima", "1"),
            ("from", ""),
            ("azione", ""),
            ("op", ""),
            ("mn", "fer"),
            ("stmn", ""),
            ("id_provincia_combo", " "),
            ("id_provincia", ""),
            ("codice_comunale_combo", ""),
            ("codice_comunale", ""),
            ("denominazione", ""),
            ("denominazione_ditta", ""),
            ("id_tipo_autorizzazione", " "),
            ("stato_autorizzazione", " "),
        ]
        response = self.session.post(MAP_URL, data=payload, timeout=90, allow_redirects=True)
        response.raise_for_status()
        return response.text

    @staticmethod
    def _strip_js_comments(source: str) -> str:
        source = re.sub(r"/\*.*?\*/", "", source, flags=re.DOTALL)
        source = re.sub(r"(^|\s)//.*?$", r"\1", source, flags=re.MULTILINE)
        return source

    @classmethod
    def _extract_array(cls, text: str, declaration: str = "const discariche") -> list:
        start = text.find(declaration)
        if start == -1:
            raise ValueError(f"ATOS declaration not found: {declaration}")
        equals = text.find("=", start)
        opening = text.find("[", equals)
        if opening == -1:
            raise ValueError("ATOS array opening not found")

        depth = 0
        quote: str | None = None
        escaped = False
        closing = None
        for index in range(opening, len(text)):
            char = text[index]
            if quote is not None:
                if escaped:
                    escaped = False
                elif char == "\\":
                    escaped = True
                elif char == quote:
                    quote = None
                continue
            if char in {"'", '"'}:
                quote = char
                continue
            if char == "[":
                depth += 1
            elif char == "]":
                depth -= 1
                if depth == 0:
                    closing = index + 1
                    break
        if closing is None:
            raise ValueError("ATOS array closing not found")

        raw = html.unescape(text[opening:closing])
        raw = cls._strip_js_comments(raw).strip()
        try:
            value = json.loads(raw)
        except Exception:
            normalized = re.sub(r"\bnull\b", "None", raw, flags=re.I)
            normalized = re.sub(r"\bundefined\b", "None", normalized, flags=re.I)
            normalized = re.sub(r"\btrue\b", "True", normalized, flags=re.I)
            normalized = re.sub(r"\bfalse\b", "False", normalized, flags=re.I)
            value = ast.literal_eval(normalized)
        if not isinstance(value, list):
            raise ValueError("ATOS marker payload is not a list")
        return value

    @classmethod
    def _marker(cls, row: list) -> dict:
        def at(index: int):
            if index >= len(row):
                return None
            value = row[index]
            return cls._clean(html.unescape(value)) if isinstance(value, str) else value

        plant_id = at(10)
        return {
            "title": at(0),
            "latitude": at(1),
            "longitude": at(2),
            "province": at(4),
            "municipality": at(5),
            "proponent": at(7),
            "authorization_type": at(9),
            "id_impianto": plant_id,
            "icon": at(11),
            "authorization_status": at(12),
            "detail_url": DETAIL_TEMPLATE.format(id_impianto=plant_id) if plant_id not in (None, "") else None,
            "row_text": " | ".join(cls._clean(item) for item in row if item not in (None, "")),
        }

    @classmethod
    def _between(cls, text: str, start: str, end: str) -> str | None:
        match = re.search(re.escape(start) + r"\s+(.*?)\s+" + re.escape(end), text, flags=re.I)
        return cls._clean(match.group(1)) if match else None

    @classmethod
    def _parse_power(cls, value: object) -> float | None:
        text = cls._clean(value)
        match = re.search(r"[0-9]+(?:[.,][0-9]+)*", text)
        if not match:
            return None
        number = match.group(0)
        if "," in number and "." in number:
            number = number.replace(".", "").replace(",", ".") if number.rfind(",") > number.rfind(".") else number.replace(",", "")
        elif "," in number:
            number = number.replace(",", ".")
        try:
            parsed = float(number)
        except ValueError:
            return None
        return parsed if 0 < parsed < 5000 else None

    @classmethod
    def _detail(cls, html_text: str) -> dict:
        soup = BeautifulSoup(html_text, "html.parser")
        text = cls._clean(soup.get_text(" ", strip=True))
        source_type = cls._between(text, "Tipologia Fonte", "Potenza MW")
        return {
            "raw_text": text,
            "title": cls._between(text, "Denominazione Impianto", "Denominazione Ditta"),
            "proponent": cls._between(text, "Denominazione Ditta", "Comune"),
            "municipality": cls._between(text, "Comune", "Sigla Provincia"),
            "province": cls._between(text, "Sigla Provincia", "Autorizzazione Vigente"),
            "authorization_type": cls._between(text, "Tipo di Autorizzazione", "Tipologia Fonte"),
            "source_type": source_type,
            "power_mw": cls._parse_power(cls._between(text, "Potenza MW", "Dati dell'ultimo Atto")),
        }

    @classmethod
    def _last_act_date(cls, text: str) -> str | None:
        marker = re.search(r"dati\s+dell[’']ultimo\s+atto", text, flags=re.I)
        segment = text[marker.end():marker.end() + 2500] if marker else text[:2500]
        dates: list[date] = []
        for match in re.finditer(r"\b(\d{1,2})[/-](\d{1,2})[/-](\d{4})\b", segment):
            day, month, year = match.groups()
            try:
                dates.append(date(int(year), int(month), int(day)))
            except ValueError:
                pass
        for match in re.finditer(r"\b(\d{4})-(\d{1,2})-(\d{1,2})\b", segment):
            year, month, day = match.groups()
            try:
                dates.append(date(int(year), int(month), int(day)))
            except ValueError:
                pass
        valid = [item for item in dates if item <= date.today()]
        return max(valid).isoformat() if valid else None

    def fetch(self) -> list[AgentFinding]:
        html_text = self._fetch_map_results()
        rows = self._extract_array(html_text)
        markers = [self._marker(row) for row in rows if isinstance(row, (list, tuple))]

        # Prefer marker-level wind identification to avoid hitting every ATOS plant.
        candidates = [
            marker
            for marker in markers
            if marker.get("id_impianto")
            and (
                self._is_wind_text(marker.get("icon"))
                or self._is_wind_text(marker.get("title"))
                or self._is_wind_text(marker.get("row_text"))
            )
        ][: self.max_details]

        findings: list[AgentFinding] = []
        seen: set[str] = set()
        for marker in candidates:
            detail_url = marker.get("detail_url")
            if not detail_url:
                continue
            response = self.session.get(detail_url, timeout=90)
            response.raise_for_status()
            detail = self._detail(response.text)
            if not self._is_wind_text(detail.get("source_type") or detail.get("raw_text")):
                continue

            plant_id = str(marker["id_impianto"])
            external_id = f"TOSCANA-ATOS-WIND-{plant_id}"
            if external_id in seen:
                continue
            seen.add(external_id)

            raw_text = detail.get("raw_text") or ""
            last_act = self._last_act_date(raw_text)
            findings.append(
                AgentFinding(
                    external_id=external_id,
                    source_name=self.source_name,
                    source_url=detail_url,
                    title=detail.get("title") or marker.get("title") or f"Impianto ATOS {plant_id}",
                    finding_type="project_source",
                    payload={
                        "project_name": detail.get("title") or marker.get("title"),
                        "proponent": detail.get("proponent") or marker.get("proponent"),
                        "region": "Toscana",
                        "province": (detail.get("province") or marker.get("province") or "").upper() or None,
                        "municipality": detail.get("municipality") or marker.get("municipality"),
                        "power_mw": detail.get("power_mw"),
                        "authorization_type": detail.get("authorization_type") or marker.get("authorization_type"),
                        "status_raw": marker.get("authorization_status"),
                        "source_type": detail.get("source_type"),
                        "last_act_date": last_act,
                        "latitude": marker.get("latitude"),
                        "longitude": marker.get("longitude"),
                        "atos_id": plant_id,
                        "sector": "eolico",
                        "source_grade_ceiling": "A1",
                        "project_specific": True,
                        "source_adapter_origin": "pv_agent_mvp/toscana_atos.py",
                    },
                )
            )

        return findings
