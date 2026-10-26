from __future__ import annotations

import hashlib
import re
from urllib.parse import urljoin

from bs4 import BeautifulSoup

from app.wind_agents.base import AgentFinding, BaseWindAgent


START_URL = (
    "https://www.regione.lazio.it/imprese/"
    "tutela-ambientale-difesa-suolo/"
    "valutazione-impatto-ambientale-progetti"
)

WIND_TERMS = (
    "eolico",
    "eolica",
    "parco eolico",
    "impianto eolico",
    "aerogenerator",
    "repowering",
)

PROVINCE_NAME_TO_CODE = {
    "frosinone": "FR",
    "latina": "LT",
    "rieti": "RI",
    "roma": "RM",
    "viterbo": "VT",
}


class LazioWindAgent(BaseWindAgent):
    """Wind adaptation of the mature Lazio collector in pv_agent_mvp.

    It keeps the same source, pagination and block-parsing strategy, but flips
    the domain filter from PV to wind. Findings remain raw institutional
    intelligence and never mutate canonical projects directly.
    """

    agent_name = "institutional_watch"
    source_name = "Regione Lazio VIA/PAUR"
    base_url = START_URL

    def __init__(self, max_pages: int = 120) -> None:
        super().__init__()
        self.max_pages = max_pages

    @staticmethod
    def _clean(text: str | None) -> str:
        return re.sub(r"\s+", " ", text or "").strip()

    @classmethod
    def _is_wind(cls, text: str) -> bool:
        lowered = cls._clean(text).lower()
        return any(term in lowered for term in WIND_TERMS)

    @classmethod
    def _field(cls, text: str, field: str, stop_fields: list[str]) -> str | None:
        stops = "|".join(re.escape(item) for item in stop_fields)
        match = re.search(
            rf"\b{re.escape(field)}\s*:\s*(.+?)(?=\s+(?:{stops})\s*:|$)",
            text,
            flags=re.IGNORECASE,
        )
        return cls._clean(match.group(1)) if match else None

    @classmethod
    def _title(cls, text: str) -> str | None:
        cleaned = re.sub(
            r"\bData\s+arrivo\s*:\s*[0-9]{2}/[0-9]{2}/[0-9]{4}",
            "",
            text,
            flags=re.IGNORECASE,
        )
        cleaned = re.sub(
            r"\bScarica\s+Elaborati\s+Progettuali\b",
            "",
            cleaned,
            flags=re.IGNORECASE,
        )
        cleaned = re.split(
            r"\bResponsabile\s*:|\bTipologia\s*:|\bEmail\s*:|"
            r"\bProponente\s*:|\bComune\s*:",
            cleaned,
            maxsplit=1,
            flags=re.IGNORECASE,
        )[0]
        cleaned = cls._clean(cleaned).strip(" -–—:;")
        return cleaned or None

    @classmethod
    def _municipality(cls, text: str) -> str | None:
        match = re.search(
            r"\bComune\s*:\s*(.+?)\s*-\s*Provincia\s*:",
            text,
            flags=re.IGNORECASE,
        )
        return cls._clean(match.group(1)) if match else None

    @classmethod
    def _province(cls, text: str) -> str | None:
        match = re.search(
            r"\bProvincia\s*:\s*([A-ZÀ-ÚA-Za-zà-ú'’ ]+?)(?:\s+Allegato|\s*$|\s+\*)",
            text,
            flags=re.IGNORECASE,
        )
        if not match:
            return None
        value = cls._clean(match.group(1))
        return PROVINCE_NAME_TO_CODE.get(value.lower(), value)

    @classmethod
    def _status(cls, text: str) -> str | None:
        lowered = cls._clean(text).lower()
        checks = (
            ("favorevole con prescrizioni", "Favorevole con prescrizioni"),
            ("favorevole", "Favorevole"),
            ("archiviato", "Archiviato"),
            ("archiviata", "Archiviato"),
            ("improcedibile", "Improcedibile"),
            ("rinviato a via", "Rinviato a VIA"),
            ("escluso da via", "Escluso da VIA"),
            ("esclusa da via", "Escluso da VIA"),
            ("conclus", "Concluso"),
            ("procedimento in corso", "In corso"),
        )
        for needle, label in checks:
            if needle in lowered:
                return label
        return None

    @classmethod
    def _power_mw(cls, text: str) -> float | None:
        # Wind pages normally expose MW directly; do not infer from WTG count.
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
        match = re.search(r"\bTipologia\s*:\s*(VIA|VERIFICA|PAUR|VAS)\b", text, re.I)
        return match.group(1).upper() if match else None

    @classmethod
    def _date(cls, text: str) -> str | None:
        match = re.search(r"\bData\s+arrivo\s*:\s*([0-9]{2}/[0-9]{2}/[0-9]{4})", text, re.I)
        return match.group(1) if match else None

    @classmethod
    def _next_url(cls, soup: BeautifulSoup, current_url: str) -> str | None:
        for anchor in soup.find_all("a", href=True):
            if "pagina successiva" in cls._clean(anchor.get_text(" ", strip=True)).lower():
                return urljoin(current_url, anchor["href"])
        link = soup.find("link", attrs={"rel": "next"})
        if link and link.get("href"):
            return urljoin(current_url, link["href"])
        return None

    @classmethod
    def _external_id(cls, title: str, date: str | None, proponent: str | None, municipality: str | None) -> str:
        raw = "|".join([date or "", title, proponent or "", municipality or ""])
        return "LAZIO-WIND-" + hashlib.sha1(raw.encode("utf-8")).hexdigest()[:16]

    def fetch(self) -> list[AgentFinding]:
        findings: list[AgentFinding] = []
        seen: set[str] = set()
        visited: set[str] = set()
        url: str | None = START_URL
        page_no = 1

        while url and page_no <= self.max_pages and url not in visited:
            visited.add(url)
            response = self.session.get(url, timeout=90)
            response.raise_for_status()
            soup = BeautifulSoup(response.content.decode("utf-8", errors="replace"), "html.parser")

            for block in soup.find_all("li"):
                text = self._clean(block.get_text(" ", strip=True))
                if "Data arrivo" not in text:
                    continue
                if "Proponente" not in text and "Comune" not in text:
                    continue
                if not self._is_wind(text):
                    continue

                title = self._title(text)
                if not title:
                    continue
                date = self._date(text)
                proponent = self._field(
                    text,
                    "Proponente",
                    ["Comune", "Provincia", "Responsabile", "Tipologia", "Email"],
                )
                municipality = self._municipality(text)
                province = self._province(text)
                procedure = self._procedure(text)
                status = self._status(text)
                detail_url = url
                first_link = block.find("a", href=True)
                if first_link:
                    detail_url = urljoin(url, first_link["href"])

                external_id = self._external_id(title, date, proponent, municipality)
                if external_id in seen:
                    continue
                seen.add(external_id)

                findings.append(
                    AgentFinding(
                        external_id=external_id,
                        source_name=self.source_name,
                        source_url=detail_url,
                        title=title,
                        finding_type="project_source",
                        payload={
                            "project_name": title,
                            "proponent": proponent,
                            "region": "Lazio",
                            "province": province,
                            "municipality": municipality,
                            "power_mw": self._power_mw(text),
                            "procedure": procedure,
                            "status_raw": status,
                            "date_received": date,
                            "sector": "eolico",
                            "source_grade_ceiling": "A1",
                            "project_specific": True,
                            "source_adapter_origin": "pv_agent_mvp/lazio.py",
                        },
                    )
                )

            url = self._next_url(soup, url)
            page_no += 1

        return findings
