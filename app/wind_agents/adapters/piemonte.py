from __future__ import annotations

import hashlib
import re
from datetime import date
from urllib.parse import urljoin

from bs4 import BeautifulSoup

from app.wind_agents.base import AgentFinding, BaseWindAgent


START_URL = "http://www.sistemapiemonte.it/skvia/HomePage.do?ricerca=ArchivioProgetti"
CHANGE_COMPETENZA_URL = (
    "http://www.sistemapiemonte.it/skvia/"
    "cpRicercaArchivioProgetti!handleCbAutoritaCompetente_VALUE_CHANGED.do"
    "?confermacbAutoritaCompetente=conferma"
)
SEARCH_KEYWORDS = ("eolico", "eolica", "parco eolico", "repowering")
WIND_TERMS = ("eolico", "eolica", "parco eolico", "impianto eolico", "aerogenerator", "repowering")
PROVINCE_NAMES = {
    "alessandria": "AL",
    "asti": "AT",
    "biella": "BI",
    "cuneo": "CN",
    "novara": "NO",
    "torino": "TO",
    "verbania": "VB",
    "verbano-cusio-ossola": "VB",
    "vercelli": "VC",
}


class PiemonteWindAgent(BaseWindAgent):
    """Wind adaptation of pv_agent_mvp's Piemonte SKVIA archive search."""

    agent_name = "institutional_watch"
    source_name = "Regione Piemonte SKVIA"
    base_url = START_URL

    def __init__(self, years: list[int] | None = None) -> None:
        super().__init__()
        current = date.today().year
        self.years = years or [current, current - 1, current - 2, current - 3]
        self.session.headers.update(
            {
                "User-Agent": "Wind-Radar-Agent/0.6",
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            }
        )

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
    def _form(cls, soup: BeautifulSoup, current_url: str) -> tuple[str, dict[str, str]]:
        form = soup.find("form", {"id": "cpRicercaArchivioProgetti"}) or soup.find("form")
        if form is None:
            raise RuntimeError("Piemonte SKVIA form not found")
        action = urljoin(current_url, form.get("action") or "")
        data: dict[str, str] = {}
        for field in form.find_all(["input", "select", "textarea"]):
            name = field.get("name")
            if not name:
                continue
            if field.name == "select":
                selected = field.find("option", selected=True)
                data[name] = selected.get("value") if selected else ""
                continue
            field_type = (field.get("type") or "").lower()
            if field_type in {"submit", "button", "image", "checkbox"}:
                continue
            data[name] = field.get("value") or ""
        return action, data

    @staticmethod
    def _base_payload(base: dict[str, str]) -> dict[str, str]:
        data = dict(base)
        data["appDataRicercaArchivioProgetti.competenza"] = "REGIONE PIEMONTE"
        data["appDataRicercaArchivioProgetti.tipologia"] = ""
        data["appDataRicercaArchivioProgetti.annoRegistro"] = ""
        data["appDataRicercaArchivioProgetti.codice"] = ""
        data["appDataRicercaArchivioProgetti.denominazioneProgetto"] = ""
        data["__checkbox_appDataRicercaArchivioProgetti.flagLeggeObiettivo"] = ""
        data["__checkbox_appDataRicercaArchivioProgetti.incidenza"] = ""
        data["appDataRicercaArchivioProgetti.cat"] = ""
        data["appDataRicercaArchivioProgetti.codIstatProvincia"] = ""
        data["appDataRicercaArchivioProgetti.istatComune"] = ""
        data["appDataRicercaArchivioProgetti.flagStato"] = ""
        data["appDataCodiceSitoReteNaturaSelezionato"] = ""
        data["appDataRicercaArchivioProgetti.idParco"] = ""
        return data

    @classmethod
    def _result_rows(cls, soup: BeautifulSoup, page_url: str) -> list[dict]:
        table = soup.find("table", {"id": "row_tElencoProgetti"}) or soup.find("table", {"id": "wpRisultatiRicercaArchivioProgetti"})
        if table is None:
            return []
        rows: list[dict] = []
        for tr in table.find_all("tr"):
            cells = [cls._clean(td.get_text(" ", strip=True)) for td in tr.find_all(["td", "th"])]
            cells = [c for c in cells if c]
            if len(cells) < 5:
                continue
            joined = cls._clean(" | ".join(cells))
            lowered = joined.lower()
            if "autorità competente" in lowered and "codice pratica" in lowered:
                continue
            if "risultati trovati" in lowered or "scarica in excel" in lowered or "scarica in pdf" in lowered:
                continue
            if "regione piemonte" not in lowered:
                continue
            authority = cells[0]
            code = cells[1] if len(cells) > 1 else ""
            title = cells[2] if len(cells) > 2 else ""
            municipality = cells[3] if len(cells) > 3 else ""
            status = cells[-1] if cells else ""
            if not code or not title:
                continue
            detail_url = page_url
            for anchor in tr.find_all("a", href=True):
                href = anchor.get("href") or ""
                if href and not href.startswith("mailto:"):
                    detail_url = urljoin(page_url, href)
                    break
            rows.append(
                {
                    "authority": authority,
                    "code": code,
                    "title": title,
                    "municipality": municipality,
                    "status": status,
                    "url": detail_url,
                    "raw_text": joined,
                }
            )
        return rows

    @classmethod
    def _proponent(cls, text: str) -> str | None:
        parts = [cls._clean(p) for p in text.split(",") if cls._clean(p)]
        markers = ("s.r.l", "srl", "s.p.a", "spa", "soc agr", "soc. agr", "società agricola", "societa agricola", "green energy")
        for part in parts:
            if any(marker in part.lower() for marker in markers):
                return part.strip(" .,-;:")
        return None

    @classmethod
    def _province(cls, text: str) -> str | None:
        match = re.search(r"\b(AL|AT|BI|CN|NO|TO|VB|VC)\b|\((AL|AT|BI|CN|NO|TO|VB|VC)\)", text, flags=re.I)
        if match:
            return (match.group(1) or match.group(2)).upper()
        norm = cls._norm(text)
        for name, code in PROVINCE_NAMES.items():
            if name in norm:
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

    @classmethod
    def _procedure(cls, code: str) -> str | None:
        upper = cls._clean(code).upper()
        if "PAUR" in upper:
            return "PAUR"
        if "VIA" in upper:
            return "VIA"
        if "VER" in upper or "SCREEN" in upper:
            return "VERIFICA"
        return None

    @classmethod
    def _external_id(cls, row: dict) -> str:
        code = cls._clean(row.get("code") or "")
        if code:
            return "PIEMONTE-WIND-" + re.sub(r"[^A-Za-z0-9._-]+", "-", code).strip("-")
        raw = cls._clean(f"{row.get('title')}|{row.get('url')}")
        return "PIEMONTE-WIND-" + hashlib.sha1(raw.encode("utf-8")).hexdigest()[:18]

    def fetch(self) -> list[AgentFinding]:
        home = self.session.get(START_URL, timeout=90, allow_redirects=True)
        home.raise_for_status()
        home_soup = BeautifulSoup(home.content.decode("utf-8", errors="replace"), "html.parser")
        _, base_data = self._form(home_soup, home.url)

        changed = self.session.post(
            CHANGE_COMPETENZA_URL,
            data=self._base_payload(base_data),
            timeout=90,
            allow_redirects=True,
            headers={
                "Referer": home.url,
                "Origin": "http://www.sistemapiemonte.it",
                "Content-Type": "application/x-www-form-urlencoded",
            },
        )
        changed.raise_for_status()
        changed_soup = BeautifulSoup(changed.content.decode("utf-8", errors="replace"), "html.parser")
        search_action, base_after = self._form(changed_soup, changed.url)

        unique: dict[str, AgentFinding] = {}
        for keyword in SEARCH_KEYWORDS:
            for year in self.years:
                data = dict(base_after)
                data["appDataRicercaArchivioProgetti.competenza"] = "REGIONE PIEMONTE"
                data["appDataRicercaArchivioProgetti.denominazioneProgetto"] = keyword
                data["appDataRicercaArchivioProgetti.annoRegistro"] = str(year)
                data["appDataRicercaArchivioProgetti.tipologia"] = ""
                data["appDataRicercaArchivioProgetti.flagStato"] = ""
                data["method:handleBtRicercaArchivioProgetti_CLICKED"] = "Ricerca"
                response = self.session.post(
                    search_action,
                    data=data,
                    timeout=90,
                    allow_redirects=True,
                    headers={
                        "Referer": changed.url,
                        "Origin": "http://www.sistemapiemonte.it",
                        "Content-Type": "application/x-www-form-urlencoded",
                    },
                )
                response.raise_for_status()
                soup = BeautifulSoup(response.content.decode("utf-8", errors="replace"), "html.parser")
                for row in self._result_rows(soup, response.url):
                    combined = self._clean(f"{row['title']} {row['raw_text']}")
                    if not self._is_wind(combined):
                        continue
                    external_id = self._external_id(row)
                    if external_id in unique:
                        continue
                    title = row["title"]
                    municipality = self._clean(row.get("municipality") or "").strip(" -–—:;,.()")
                    unique[external_id] = AgentFinding(
                        external_id=external_id,
                        source_name=self.source_name,
                        source_url=row.get("url") or START_URL,
                        title=title[:700],
                        finding_type="project_source",
                        payload={
                            "project_name": title[:700],
                            "proponent": self._proponent(title),
                            "region": "Piemonte",
                            "province": self._province(combined),
                            "municipalities": [municipality] if municipality else [],
                            "power_mw": self._power_mw(combined),
                            "procedure": self._procedure(row.get("code") or ""),
                            "status_raw": self._clean(row.get("status") or "") or None,
                            "practice_code": row.get("code"),
                            "source_year": year,
                            "sector": "eolico",
                            "source_grade_ceiling": "A1",
                            "project_specific": True,
                            "source_adapter_origin": "pv_agent_mvp/piemonte.py",
                        },
                    )
        return list(unique.values())
