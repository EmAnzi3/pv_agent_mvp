from __future__ import annotations

import hashlib
import html
import re
from urllib.parse import urljoin, urlparse

from bs4 import BeautifulSoup

from app.wind_agents.base import AgentFinding, BaseWindAgent


BASE_URL = "https://www.va.regione.umbria.it"
SOURCE_PAGES = (
    ("https://www.va.regione.umbria.it/via/elenco-dei-procedimenti-di-valutazione-di-impatto-ambientale", "VIA/PAUR"),
    ("https://www.va.regione.umbria.it/via/elenco-dei-procedimenti-di-verifica-di-assoggettabilita-a-via", "Verifica di assoggettabilità a VIA"),
    ("https://www.va.regione.umbria.it/via/valutazione-preliminare", "Valutazione preliminare"),
)
WIND_TERMS = ("eolico", "eolica", "parco eolico", "impianto eolico", "aerogenerator", "repowering")
PROVINCE_CODES = {"PG", "TR"}


class UmbriaWindAgent(BaseWindAgent):
    """Wind adaptation of pv_agent_mvp's Umbria VIA/PAUR list collector."""

    agent_name = "institutional_watch"
    source_name = "Regione Umbria VIA/PAUR"
    base_url = BASE_URL

    @staticmethod
    def _clean(value: object) -> str:
        return " ".join(html.unescape(str(value or "")).replace("\xa0", " ").split()).strip()

    @classmethod
    def _norm(cls, value: object) -> str:
        text = cls._clean(value).lower()
        return text.translate(str.maketrans("àèéìòù", "aeeiou"))

    @classmethod
    def _is_wind(cls, text: str) -> bool:
        norm = cls._norm(text)
        return any(term in norm for term in WIND_TERMS)

    @classmethod
    def _project_link(cls, table, page_url: str) -> str | None:
        for anchor in table.find_all("a", href=True):
            label = cls._clean(anchor.get_text(" ", strip=True))
            if label and cls._is_wind(label):
                return urljoin(page_url, anchor.get("href") or "").split("#", 1)[0]
        for anchor in table.find_all("a", href=True):
            href = anchor.get("href") or ""
            if href and not href.startswith("#"):
                return urljoin(page_url, href).split("#", 1)[0]
        return None

    @classmethod
    def _title(cls, text: str) -> str | None:
        before = re.split(r"\bSoggetto\s+proponente\s*:", text, maxsplit=1, flags=re.I)[0]
        title = cls._clean(before).strip(" -–—")
        for marker in (
            "Oggetto Procedimento Autorità procedente Stato del procedimento",
            "Oggetto Procedimento Autorita procedente Stato del procedimento",
        ):
            if marker in title:
                title = cls._clean(title.split(marker, 1)[-1])
        title = re.sub(r"^\s*P\.A\.U\.R\._\s*", "P.A.U.R. - ", title, flags=re.I)
        title = re.sub(r"\s+-\s+\([0-9]{1,3}-[0-9]{1,3}-[0-9]{4}\)\s*$", "", title)
        return cls._clean(title)[:900] or None

    @classmethod
    def _proponent(cls, text: str) -> str | None:
        match = re.search(
            r"\bSoggetto\s+proponente\s*:\s*(.+?)(?=\s+Termine\s+per\s+la\s+presentazione|\s+Data\s+di\s+pubblicazione|\s+Verifica\s+di|\s+Provvedimento\s+autorizzatorio|\s+VIA\s+|\s+P\.A\.U\.R\.|\s*$)",
            text,
            flags=re.I,
        )
        if not match:
            return None
        value = cls._clean(match.group(1)).strip(" .,:;–—-")
        return value[:250] if len(value) >= 3 else None

    @classmethod
    def _province(cls, text: str) -> str | None:
        for code in re.findall(r"\(([A-Z]{2})\)", text or ""):
            if code in PROVINCE_CODES:
                return code
        norm = cls._norm(text)
        if "perugia" in norm:
            return "PG"
        if "terni" in norm:
            return "TR"
        return None

    @classmethod
    def _municipalities(cls, text: str) -> list[str]:
        out: list[str] = []
        for match in re.finditer(
            r"\b(?:nel|nei|nella|nelle)?\s*Comuni?\s+di\s+(.+?)\s*\((PG|TR)\)",
            text,
            flags=re.I,
        ):
            chunk = re.split(r"\s+Soggetto\s+proponente\s*:", match.group(1), maxsplit=1, flags=re.I)[0]
            for part in re.split(r",|\s+e\s+|\s+ed\s+", chunk, flags=re.I):
                value = cls._clean(part).strip(" .,:;–—-()[]\"'")
                if value and value.lower() not in {x.lower() for x in out}:
                    out.append(value)
        return out[:12]

    @classmethod
    def _power_mw(cls, text: str) -> float | None:
        pattern = r"(?<![\d.,])(\d{1,3}(?:[.\s'’]\d{3})+(?:[,.]\d+)?|\d+[,.]\d+|\d+)\s*(MWp|MW)\b"
        for match in re.finditer(pattern, text, flags=re.I):
            number = match.group(1).replace(" ", "").replace("'", "").replace("’", "")
            if "," in number:
                number = number.replace(".", "").replace(",", ".")
            elif number.count(".") > 1:
                number = number.replace(".", "")
            try:
                value = float(number)
            except ValueError:
                continue
            if 0 < value < 5000:
                return value
        return None

    @classmethod
    def _status(cls, text: str) -> str | None:
        norm = cls._norm(text)
        if " in corso" in f" {norm} ":
            return "In corso"
        if " concluso" in f" {norm} " or " conclusa" in f" {norm} ":
            return "Concluso"
        return None

    @classmethod
    def _authority(cls, text: str) -> str | None:
        for label in ("Regione Umbria", "Ministero Ambiente", "Regione Toscana", "Regione Marche"):
            if label.lower() in text.lower():
                return label
        return None

    @classmethod
    def _external_id(cls, source_url: str, title: str) -> str:
        parsed = urlparse(source_url)
        stable = parsed.path.strip("/") or source_url or title
        digest = hashlib.sha1(cls._norm(stable).encode("utf-8")).hexdigest()[:16]
        return f"UMBRIA-WIND-{digest}"

    def fetch(self) -> list[AgentFinding]:
        unique: dict[str, AgentFinding] = {}
        for page_url, procedure in SOURCE_PAGES:
            response = self.session.get(page_url, timeout=90)
            response.raise_for_status()
            soup = BeautifulSoup(response.content.decode("utf-8", errors="replace"), "html.parser")

            for table in soup.find_all("table"):
                text = self._clean(table.get_text(" ", strip=True))
                if "Soggetto proponente" not in text or not self._is_wind(text):
                    continue
                authority = self._authority(text)
                if authority and "Umbria" not in authority:
                    continue
                title = self._title(text)
                if not title:
                    continue
                source_url = self._project_link(table, page_url) or page_url
                external_id = self._external_id(source_url, title)
                if external_id in unique:
                    continue
                unique[external_id] = AgentFinding(
                    external_id=external_id,
                    source_name=self.source_name,
                    source_url=source_url,
                    title=title[:700],
                    finding_type="project_source",
                    payload={
                        "project_name": title[:900],
                        "proponent": self._proponent(text),
                        "region": "Umbria",
                        "province": self._province(text),
                        "municipalities": self._municipalities(text),
                        "power_mw": self._power_mw(text),
                        "procedure": procedure,
                        "status_raw": self._status(text),
                        "authority": authority,
                        "sector": "eolico",
                        "source_grade_ceiling": "A1",
                        "project_specific": True,
                        "source_adapter_origin": "pv_agent_mvp/umbria.py",
                    },
                )
        return list(unique.values())
