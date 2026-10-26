from __future__ import annotations

import hashlib
import re
from urllib.parse import urljoin

from bs4 import BeautifulSoup

from app.wind_agents.base import AgentFinding, BaseWindAgent


BASE_URL = "https://serviziambiente.regione.emilia-romagna.it/viavasweb/"
WIND_TERMS = (
    "eolico",
    "eolica",
    "parco eolico",
    "impianto eolico",
    "aerogenerator",
    "repowering",
)
PROVINCE_NAME_TO_CODE = {
    "BOLOGNA": "BO",
    "FERRARA": "FE",
    "FORLI-CESENA": "FC",
    "FORLÌ-CESENA": "FC",
    "MODENA": "MO",
    "PARMA": "PR",
    "PIACENZA": "PC",
    "RAVENNA": "RA",
    "REGGIO EMILIA": "RE",
    "RIMINI": "RN",
}
LABELS = {
    "titolo",
    "proponente",
    "stato",
    "tipo procedura",
    "tipologia progetto o piano",
    "localizzazione",
    "comune",
    "provincia/citta metropolitana",
    "provincia/città metropolitana",
    "altre localizzazioni",
    "protocollo di attivazione",
    "numero",
    "data",
    "documenti",
    "pubblicazioni",
}


class EmiliaRomagnaWindAgent(BaseWindAgent):
    """Wind adaptation of pv_agent_mvp's Emilia-Romagna VIA/VAS collector."""

    agent_name = "institutional_watch"
    source_name = "Regione Emilia-Romagna VIA/VAS"
    base_url = BASE_URL

    @staticmethod
    def _clean(value: object) -> str:
        return re.sub(r"\s+", " ", str(value or "")).strip()

    @classmethod
    def _norm(cls, value: object) -> str:
        return cls._clean(value).lower().translate(str.maketrans("àèéìòù", "aeeiou"))

    @classmethod
    def _is_wind(cls, text: str) -> bool:
        norm = cls._norm(text)
        return any(term in norm for term in WIND_TERMS)

    @classmethod
    def _detail_links(cls, soup: BeautifulSoup) -> list[str]:
        out: list[str] = []
        for anchor in soup.find_all("a", href=True):
            href = (anchor.get("href") or "").strip()
            if "/ricerca/dettaglio/" not in href:
                continue
            absolute = urljoin(BASE_URL, href)
            if absolute not in out:
                out.append(absolute)
        return out

    @classmethod
    def _lines(cls, soup: BeautifulSoup) -> list[str]:
        return [cls._clean(line) for line in soup.get_text("\n", strip=True).splitlines() if cls._clean(line)]

    @classmethod
    def _is_label(cls, value: str) -> bool:
        return cls._norm(value).rstrip(":") in LABELS

    @classmethod
    def _value_after(cls, lines: list[str], label: str) -> str | None:
        wanted = cls._norm(label)
        for idx, line in enumerate(lines):
            norm = cls._norm(line).rstrip(":")
            if norm == wanted:
                for candidate in lines[idx + 1 : idx + 5]:
                    value = cls._clean(candidate)
                    if not value:
                        continue
                    if cls._is_label(value):
                        return None
                    return value
            if norm.startswith(wanted + " ") and ":" in line:
                value = cls._clean(line.split(":", 1)[1])
                if value and not cls._is_label(value):
                    return value
        return None

    @classmethod
    def _protocol(cls, lines: list[str], wanted: str) -> str | None:
        for idx, line in enumerate(lines):
            if cls._norm(line).rstrip(":") != "protocollo di attivazione":
                continue
            for j in range(idx + 1, min(idx + 10, len(lines))):
                if cls._norm(lines[j]).rstrip(":") == wanted:
                    for candidate in lines[j + 1 : j + 4]:
                        if cls._is_label(candidate):
                            return None
                        value = cls._clean(candidate)
                        if value:
                            return value
        return None

    @classmethod
    def _province(cls, value: str | None) -> str | None:
        text = cls._clean(value)
        if not text:
            return None
        upper = text.upper()
        match = re.search(r"\b(BO|FE|FC|MO|PR|PC|RA|RE|RN)\b|\((BO|FE|FC|MO|PR|PC|RA|RE|RN)\)", upper)
        if match:
            return (match.group(1) or match.group(2)).upper()
        normalized = cls._norm(upper).replace("'", "")
        for name, code in PROVINCE_NAME_TO_CODE.items():
            if cls._norm(name).replace("'", "") in normalized:
                return code
        return None

    @classmethod
    def _municipalities(cls, primary: str | None, other: str | None, title: str) -> list[str]:
        out: list[str] = []

        def add(value: str) -> None:
            item = cls._clean(value).strip(" -–—:;,.()")
            item = re.sub(r"^(?:Comune|Comuni)\s+di\s+", "", item, flags=re.I)
            if item and len(item) <= 100 and item.lower() not in {x.lower() for x in out}:
                out.append(item)

        if primary:
            add(primary)
        if other:
            for part in re.split(r"[,;]+|\s+e\s+|\s+ed\s+", other, flags=re.I):
                if "prov" in part.lower() and ":" in part:
                    part = part.split(":", 1)[1]
                add(part)
        if not out:
            for match in re.finditer(
                r"(?:nel|nei|localizzato\s+nel|localizzato\s+nei)\s+Comuni?\s+di\s+(.+?)(?:\s*\([A-Z]{2}\)|\.|;|$)",
                title,
                flags=re.I,
            ):
                raw = re.sub(r"\([A-Z]{2}\)", "", match.group(1))
                for part in re.split(r",|\s+e\s+|\s+ed\s+", raw, flags=re.I):
                    add(part)
                break
        return out[:15]

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
    def _external_id(detail_url: str) -> str:
        return "ER-WIND-" + hashlib.sha1(detail_url.encode("utf-8")).hexdigest()[:18]

    def fetch(self) -> list[AgentFinding]:
        response = self.session.get(BASE_URL, timeout=90)
        response.raise_for_status()
        soup = BeautifulSoup(response.text, "html.parser")
        findings: list[AgentFinding] = []

        for detail_url in self._detail_links(soup):
            detail = self.session.get(detail_url, timeout=90)
            detail.raise_for_status()
            lines = self._lines(BeautifulSoup(detail.text, "html.parser"))
            if not lines:
                continue
            title = self._value_after(lines, "Titolo")
            if not title:
                continue
            proponent = self._value_after(lines, "Proponente")
            status = self._value_after(lines, "Stato")
            procedure = self._value_after(lines, "Tipo Procedura")
            typology = self._value_after(lines, "Tipologia progetto o piano")
            municipality = self._value_after(lines, "Comune")
            province_raw = self._value_after(lines, "Provincia/Città Metropolitana")
            other_locations = self._value_after(lines, "Altre localizzazioni")
            combined = self._clean(" ".join(x for x in [title, proponent, status, procedure, typology, municipality, province_raw, other_locations] if x))
            if not self._is_wind(combined):
                continue

            findings.append(
                AgentFinding(
                    external_id=self._external_id(detail_url),
                    source_name=self.source_name,
                    source_url=detail_url,
                    title=title[:700],
                    finding_type="project_source",
                    payload={
                        "project_name": title[:700],
                        "proponent": proponent,
                        "region": "Emilia-Romagna",
                        "province": self._province(province_raw or combined),
                        "municipalities": self._municipalities(municipality, other_locations, title),
                        "power_mw": self._power_mw(combined),
                        "procedure": procedure,
                        "status_raw": status,
                        "typology": typology,
                        "protocol_number": self._protocol(lines, "numero"),
                        "protocol_date": self._protocol(lines, "data"),
                        "sector": "eolico",
                        "source_grade_ceiling": "A1",
                        "project_specific": True,
                        "source_adapter_origin": "pv_agent_mvp/emilia_romagna.py",
                    },
                )
            )
        return findings
