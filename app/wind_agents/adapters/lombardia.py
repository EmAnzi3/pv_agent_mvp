from __future__ import annotations

import hashlib
import re
from datetime import date
from urllib.parse import urljoin

from app.wind_agents.base import AgentFinding, BaseWindAgent


BASE_URL = "https://www.silvia.servizirl.it/silviaweb/"
TIPO_PROCEDURA_LIST = "1,2,3,5,15"
TARGET_SECTORS = {"2", "8"}
WIND_TERMS = (
    "eolico",
    "eolica",
    "parco eolico",
    "impianto eolico",
    "aerogenerator",
    "repowering",
)
PROVINCE_NAME_TO_CODE = {
    "bergamo": "BG",
    "brescia": "BS",
    "como": "CO",
    "cremona": "CR",
    "lecco": "LC",
    "lodi": "LO",
    "mantova": "MN",
    "milano": "MI",
    "monza": "MB",
    "monza e brianza": "MB",
    "pavia": "PV",
    "sondrio": "SO",
    "varese": "VA",
}


class LombardiaWindAgent(BaseWindAgent):
    """Wind adaptation of pv_agent_mvp's Regione Lombardia SILVIA API collector."""

    agent_name = "institutional_watch"
    source_name = "Regione Lombardia SILVIA"
    base_url = BASE_URL

    def __init__(self, years: list[int] | None = None) -> None:
        super().__init__()
        current = date.today().year
        self.years = years or [current, current - 1, current - 2]

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
    def _first(cls, row: dict, keys: list[str]) -> str | None:
        normalized = {cls._norm(k).replace(" ", ""): v for k, v in row.items()}
        for key in keys:
            value = row.get(key)
            if value in (None, ""):
                value = normalized.get(cls._norm(key).replace(" ", ""))
            if isinstance(value, (dict, list)) or value in (None, ""):
                continue
            text = cls._clean(value)
            if text:
                return text
        return None

    def _load_sectors(self) -> list[str]:
        response = self.session.get(urljoin(BASE_URL, "getAllSettori.html"), timeout=90)
        response.raise_for_status()
        data = response.json()
        if not isinstance(data, list):
            raise RuntimeError("Lombardia getAllSettori did not return a list")
        found: list[str] = []
        for row in data:
            if not isinstance(row, dict):
                continue
            sector_id = row.get("idSettore") or row.get("id_settore") or row.get("id")
            if str(sector_id) in TARGET_SECTORS:
                found.append(str(sector_id))
        return sorted(set(found))

    def _search(self, sector_id: str, year: int) -> list[dict]:
        params = {
            "tipoProcedura": TIPO_PROCEDURA_LIST,
            "rgroupAutorita": "",
            "codiceProcedura": "",
            "descrProcedura": "",
            "idMacroStato": "",
            "interessati": "",
            "strFiltroEnte": "",
            "optionSettore": sector_id,
            "dataAvvioDa": "",
            "dataAvvioA": "",
            "dataDepositoDa": "",
            "dataDepositoA": "",
            "checkedAutorita": "",
            "checkedTipologiaProg": "",
            "tipoProponente": "",
            "idReferenteSelect": "",
            "descrProponente": "",
            "idTipoEnte": "",
            "idEnteACSelected": "",
            "accTipoEnte": "",
            "accTipoProc": "",
            "annoAvvio": str(year),
            "idSett": sector_id,
        }
        response = self.session.get(urljoin(BASE_URL, "avviaRicercaProcedura.html"), params=params, timeout=90)
        response.raise_for_status()
        data = response.json()
        return [row for row in data if isinstance(row, dict)] if isinstance(data, list) else []

    @classmethod
    def _status(cls, row: dict) -> str | None:
        macro = row.get("macroStato")
        if isinstance(macro, dict):
            value = cls._clean(macro.get("descrMacroStato") or "")
            if value:
                return value
        return cls._first(row, ["descrMacroStato", "stato", "descrStato", "descStato"])

    @classmethod
    def _procedure(cls, row: dict) -> str | None:
        return cls._first(row, ["group", "descrTipoProcedura", "tipoProcedura", "descTipoProcedura", "proceduraTipo"])

    @classmethod
    def _proponent(cls, row: dict) -> str | None:
        return cls._first(
            row,
            ["proponenti", "proponente", "descrProponente", "descrEnteAzienda", "enteProponente", "referente", "richiedente"],
        )

    @classmethod
    def _province(cls, text: str) -> str | None:
        match = re.search(r"\b(BG|BS|CO|CR|LC|LO|MN|MI|MB|PV|SO|VA)\b|\((BG|BS|CO|CR|LC|LO|MN|MI|MB|PV|SO|VA)\)", text, flags=re.I)
        if match:
            return (match.group(1) or match.group(2)).upper()
        norm = cls._norm(text)
        for name, code in PROVINCE_NAME_TO_CODE.items():
            if name in norm:
                return code
        return None

    @classmethod
    def _municipalities(cls, title: str) -> list[str]:
        out: list[str] = []
        for pattern in (
            r"(?:nel|nei)\s+Comuni?\s+di\s+(.+?)(?:\s*\([A-Z]{2}\)|\.|;|$)",
            r"Comune\s+di\s+(.+?)(?:\s*\([A-Z]{2}\)|\.|;|$)",
        ):
            match = re.search(pattern, title, flags=re.I)
            if not match:
                continue
            raw = re.sub(r"\([A-Z]{2}\)", "", match.group(1))
            for part in re.split(r",|\s+e\s+|\s+ed\s+", raw, flags=re.I):
                item = cls._clean(part).strip(" -–—:;,.()")
                if item and item.lower() not in {x.lower() for x in out}:
                    out.append(item)
            break
        return out[:12]

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
    def _external_id(cls, row: dict, title: str) -> str:
        proc_id = row.get("idProgetto") or row.get("idProcedura") or row.get("id_procedura") or row.get("id") or row.get("idStudio")
        if proc_id:
            return f"LOMBARDIA-WIND-{proc_id}"
        raw = f"{title}|{cls._proponent(row) or ''}"
        return "LOMBARDIA-WIND-" + hashlib.sha1(raw.encode("utf-8")).hexdigest()[:18]

    def fetch(self) -> list[AgentFinding]:
        unique: dict[str, AgentFinding] = {}
        sectors = self._load_sectors()
        if not sectors:
            raise RuntimeError("Lombardia SILVIA target sectors 2/8 not returned")

        for sector_id in sectors:
            for year in self.years:
                for row in self._search(sector_id, year):
                    title = self._first(
                        row,
                        ["descrProgetto", "descrProcedura", "titolo", "oggetto", "descrizione", "descProcedura", "nomeProcedura", "procedura"],
                    )
                    if not title or not self._is_wind(title):
                        continue
                    proponent = self._proponent(row)
                    status = self._status(row)
                    procedure = self._procedure(row)
                    proc_id = row.get("idProgetto") or row.get("idProcedura") or row.get("id_procedura") or row.get("id") or row.get("idStudio")
                    detail_url = urljoin(BASE_URL, f"#/scheda-sintesi/{proc_id}") if proc_id else BASE_URL
                    external_id = self._external_id(row, title)
                    if external_id in unique:
                        continue
                    combined = self._clean(" ".join(x for x in [title, proponent, status, procedure] if x))
                    unique[external_id] = AgentFinding(
                        external_id=external_id,
                        source_name=self.source_name,
                        source_url=detail_url,
                        title=title[:700],
                        finding_type="project_source",
                        payload={
                            "project_name": title[:700],
                            "proponent": proponent,
                            "region": "Lombardia",
                            "province": self._province(combined),
                            "municipalities": self._municipalities(title),
                            "power_mw": self._power_mw(combined),
                            "procedure": procedure,
                            "status_raw": status,
                            "source_year": year,
                            "sector_id": sector_id,
                            "sector": "eolico",
                            "source_grade_ceiling": "A1",
                            "project_specific": True,
                            "source_adapter_origin": "pv_agent_mvp/lombardia.py",
                        },
                    )
        return list(unique.values())
