from __future__ import annotations

import hashlib
import re
from datetime import date
from urllib.parse import urljoin

from bs4 import BeautifulSoup

from app.wind_agents.base import AgentFinding, BaseWindAgent


BASE_TEMPLATE = "https://www.regione.veneto.it/web/vas-via-vinca-nuvv/progetti-{year}"
WIND_TERMS = (
    "eolico",
    "eolica",
    "parco eolico",
    "impianto eolico",
    "aerogenerator",
    "repowering",
)
STATUS_VALUES = {
    "In verifica amministrativa",
    "In itinere",
    "In itinere - 10bis",
    "Parere VIA espresso. In Itinere CDS",
    "Archiviato",
    "Valutato",
}
PROVINCE_NAMES = {
    "belluno": "BL",
    "padova": "PD",
    "rovigo": "RO",
    "treviso": "TV",
    "venezia": "VE",
    "verona": "VR",
    "vicenza": "VI",
}


class VenetoWindAgent(BaseWindAgent):
    """Wind adaptation of pv_agent_mvp's Veneto annual VIA project pages."""

    agent_name = "institutional_watch"
    source_name = "Regione Veneto VIA/VAS"
    base_url = BASE_TEMPLATE.format(year=date.today().year)

    def __init__(self, years: list[int] | None = None) -> None:
        super().__init__()
        current = date.today().year
        self.years = years or [current, current - 1]

    @staticmethod
    def _clean(value: object) -> str:
        return re.sub(r"\s+", " ", str(value or "")).strip()

    @classmethod
    def _is_wind(cls, text: str) -> bool:
        lowered = cls._clean(text).lower()
        return any(term in lowered for term in WIND_TERMS)

    @classmethod
    def _context_lines(cls, link) -> list[str]:
        parent = link.parent
        if not parent:
            return [cls._clean(link.get_text(" ", strip=True))]
        lines = [cls._clean(x) for x in parent.get_text("\n", strip=True).splitlines() if cls._clean(x)]
        if len(lines) < 3 and parent.parent:
            lines = [cls._clean(x) for x in parent.parent.get_text("\n", strip=True).splitlines() if cls._clean(x)]
        out: list[str] = []
        for line in lines:
            if line and line not in out:
                out.append(line)
        return out

    @classmethod
    def _proponent(cls, lines: list[str]) -> str | None:
        for line in lines:
            match = re.search(r"Proponente\s*:\s*(.+)", line, flags=re.I)
            if match:
                value = cls._clean(match.group(1)).strip(" -–—:;,. ")
                return value or None
        return None

    @classmethod
    def _status(cls, lines: list[str]) -> str | None:
        for line in lines:
            cleaned = cls._clean(line)
            if cleaned in STATUS_VALUES:
                return cleaned
        joined = cls._clean(" ".join(lines)).lower()
        for value in STATUS_VALUES:
            if value.lower() in joined:
                return value
        return None

    @classmethod
    def _municipalities(cls, text: str) -> list[str]:
        for pattern in (
            r"Comuni\s+di\s+localizzazione\s*:\s*(.+?)(?:\.|$)",
            r"Comune\s+di\s+localizzazione\s*:\s*(.+?)(?:\.|$)",
            r"(?:nel|nei)\s+Comuni?\s+di\s+(.+?)(?:\.|;|$)",
        ):
            match = re.search(pattern, text, flags=re.I)
            if not match:
                continue
            raw = re.sub(r"\([A-Z]{2}\)", "", match.group(1)).replace(";", ",")
            out: list[str] = []
            for part in re.split(r",|\s+e\s+|\s+ed\s+", raw, flags=re.I):
                item = cls._clean(part).strip(" -–—:;,.()")
                if item and item.lower() not in {x.lower() for x in out}:
                    out.append(item)
            return out[:12]
        return []

    @classmethod
    def _province(cls, text: str) -> str | None:
        match = re.search(r"\b(BL|PD|RO|TV|VE|VR|VI)\b|\((BL|PD|RO|TV|VE|VR|VI)\)", text, flags=re.I)
        if match:
            return (match.group(1) or match.group(2)).upper()
        lowered = cls._clean(text).lower()
        for name, code in PROVINCE_NAMES.items():
            if name in lowered:
                return code
        return None

    @classmethod
    def _power_mw(cls, text: str) -> float | None:
        for match in re.finditer(
            r"(?<![\d.,])([0-9]+(?:[.\s][0-9]{3})*(?:,[0-9]+)?|[0-9]+(?:\.[0-9]+)?)\s*(MWp|MW)\b",
            text,
            flags=re.I,
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

    @staticmethod
    def _external_id(title: str, source_url: str) -> str:
        raw = f"{title}|{source_url}".lower()
        return "VENETO-WIND-" + hashlib.sha1(raw.encode("utf-8")).hexdigest()[:18]

    def fetch(self) -> list[AgentFinding]:
        unique: dict[str, AgentFinding] = {}
        for year in self.years:
            page_url = BASE_TEMPLATE.format(year=year)
            response = self.session.get(page_url, timeout=90)
            if response.status_code == 404:
                continue
            response.raise_for_status()
            soup = BeautifulSoup(response.text, "html.parser")

            for link in soup.find_all("a", href=True):
                title = self._clean(link.get_text(" ", strip=True))
                if not title:
                    continue
                lines = self._context_lines(link)
                context = self._clean(" | ".join(lines))
                combined = self._clean(f"{title} {context}")
                if not self._is_wind(combined):
                    continue

                source_url = urljoin(page_url, self._clean(link.get("href") or ""))
                external_id = self._external_id(title, source_url)
                if external_id in unique:
                    continue

                unique[external_id] = AgentFinding(
                    external_id=external_id,
                    source_name=self.source_name,
                    source_url=source_url,
                    title=title[:700],
                    finding_type="project_source",
                    payload={
                        "project_name": title[:700],
                        "proponent": self._proponent(lines),
                        "region": "Veneto",
                        "province": self._province(combined),
                        "municipalities": self._municipalities(combined),
                        "power_mw": self._power_mw(combined),
                        "procedure": "VIA/VAS",
                        "status_raw": self._status(lines),
                        "source_year": year,
                        "sector": "eolico",
                        "source_grade_ceiling": "A1",
                        "project_specific": True,
                        "source_adapter_origin": "pv_agent_mvp/veneto.py",
                    },
                )

        return list(unique.values())
