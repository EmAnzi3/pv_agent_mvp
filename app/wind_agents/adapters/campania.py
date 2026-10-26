from __future__ import annotations

import hashlib
import re
from datetime import date, datetime
from urllib.parse import urljoin

from bs4 import BeautifulSoup

from app.wind_agents.base import AgentFinding, BaseWindAgent


BASE_URL = "http://viavas.regione.campania.it"
SEARCH_URL = (
    "http://viavas.regione.campania.it/"
    "opencms/opencms/VIAVAS/VIA_files_new/Ricerca_Avanzata.html"
)
SEARCH_KEYWORDS = ("eolico", "eolica", "repowering eolico")
WIND_TERMS = ("eolico", "eolica", "aerogenerator", "repowering", "parco eolico")


class CampaniaWindAgent(BaseWindAgent):
    """Wind adaptation of pv_agent_mvp's Campania VIA/PAUR collector."""

    agent_name = "institutional_watch"
    source_name = "Regione Campania VIA/PAUR"
    base_url = SEARCH_URL

    def __init__(self, min_year: int | None = None) -> None:
        super().__init__()
        self.min_date = datetime(min_year or (date.today().year - 1), 1, 1)

    @staticmethod
    def _clean(value: object) -> str:
        return re.sub(r"\s+", " ", str(value or "")).strip()

    @classmethod
    def _norm(cls, value: object) -> str:
        return cls._clean(value).lower()

    @classmethod
    def _is_wind(cls, text: str) -> bool:
        lowered = cls._norm(text)
        return any(term in lowered for term in WIND_TERMS)

    @staticmethod
    def _header(value: str) -> str:
        value = re.sub(r"[^a-zA-Z0-9à-ùÀ-Ù]+", "_", value.strip().lower()).strip("_")
        replacements = str.maketrans("àèéìòù", "aeeiou")
        return value.translate(replacements)

    def _post_search(self, keyword: str) -> str:
        payload = {
            "stato": "",
            "tipo": "",
            "nome": "",
            "titolo": keyword,
            "provincia": "",
            "button_provincia": "Applica",
            "comune": "",
            "esito": "",
            "action_RB": "start",
            "submit": "Cerca",
        }
        headers = {
            "User-Agent": "Wind-Radar-Agent/0.6",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "it-IT,it;q=0.9,en;q=0.7",
            "Content-Type": "application/x-www-form-urlencoded",
            "Origin": BASE_URL,
            "Referer": SEARCH_URL,
            "Host": "viavas.regione.campania.it",
        }
        response = self.session.post(
            SEARCH_URL,
            data=payload,
            headers=headers,
            timeout=90,
            allow_redirects=False,
        )
        if response.is_redirect or response.is_permanent_redirect:
            redirected = urljoin(SEARCH_URL, response.headers.get("Location", ""))
            if "www.regione.campania.it" in redirected:
                raise RuntimeError(f"unexpected Campania redirect: {redirected}")
            response = self.session.get(redirected, headers=headers, timeout=90, allow_redirects=False)
        response.raise_for_status()
        return response.content.decode("utf-8", errors="replace")

    @classmethod
    def _project_url(cls, node, page_url: str) -> str:
        for anchor in node.find_all("a", href=True):
            absolute = urljoin(page_url, anchor.get("href") or "")
            if "/Progetti/" in absolute or absolute.endswith(".via") or absolute.endswith(".viavi"):
                return absolute
        return page_url

    @classmethod
    def _rows(cls, html_page: str) -> list[dict]:
        soup = BeautifulSoup(html_page, "html.parser")
        rows: list[dict] = []
        for table in soup.find_all("table"):
            table_norm = cls._norm(table.get_text(" ", strip=True))
            if not all(token in table_norm for token in ("data di presentazione", "cup", "proponente")):
                continue
            headers: list[str] = []
            for tr in table.find_all("tr"):
                cells = tr.find_all(["th", "td"])
                values = [cls._clean(cell.get_text(" ", strip=True)) for cell in cells]
                if not values:
                    continue
                normalized_headers = [cls._header(value) for value in values]
                if all(token in normalized_headers for token in ("data_di_presentazione", "cup", "proponente")):
                    headers = normalized_headers
                    continue
                if not headers or len(values) < 4:
                    continue
                record = {header: values[i] if i < len(values) else "" for i, header in enumerate(headers)}
                record["source_url"] = cls._project_url(tr, SEARCH_URL)
                record["raw_text"] = cls._clean(" | ".join(values))
                rows.append(record)
        return rows

    @classmethod
    def _parse_date(cls, value: str) -> datetime | None:
        for fmt in ("%d/%m/%Y", "%d-%m-%Y", "%Y-%m-%d"):
            try:
                return datetime.strptime(cls._clean(value), fmt)
            except ValueError:
                pass
        return None

    @classmethod
    def _power_mw(cls, text: str) -> float | None:
        for match in re.finditer(
            r"(?<![\d.,])([0-9]+(?:[.\s][0-9]{3})*(?:,[0-9]+)?|[0-9]+(?:\.[0-9]+)?)\s*MW\b",
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
    def _municipalities(cls, territory: str, title: str) -> list[str]:
        values: list[str] = []
        if territory:
            for part in re.split(r"[,;/]+|\s+e\s+", territory, flags=re.I):
                item = cls._clean(part).strip(" -–—:;,.()")
                if item and item.lower() not in {v.lower() for v in values}:
                    values.append(item)
        if values:
            return values[:12]
        for match in re.finditer(
            r"(?:Comune|Comuni)\s+di\s+(.+?)(?:\s*\([A-Z]{2}\)|\.|;|$)",
            title,
            flags=re.I,
        ):
            for part in re.split(r",|\s+e\s+", match.group(1), flags=re.I):
                item = cls._clean(part).strip(" -–—:;,.()")
                if item and item.lower() not in {v.lower() for v in values}:
                    values.append(item)
            if values:
                break
        return values[:12]

    @staticmethod
    def _province(text: str) -> str | None:
        match = re.search(r"\b(AV|BN|CE|NA|SA)\b|\((AV|BN|CE|NA|SA)\)", text, flags=re.I)
        if not match:
            return None
        return (match.group(1) or match.group(2)).upper()

    @classmethod
    def _procedure(cls, text: str) -> str | None:
        lowered = cls._norm(text)
        if "paur" in lowered or "provvedimento autorizzatorio unico" in lowered:
            return "PAUR"
        if "verifica" in lowered or "assoggettabil" in lowered:
            return "VERIFICA"
        if "valutazione di impatto ambientale" in lowered or re.search(r"\bvia\b", lowered):
            return "VIA"
        return None

    @staticmethod
    def _external_id(cup: str, date_text: str, title: str, proponent: str, source_url: str) -> str:
        if cup:
            return f"CAMPANIA-WIND-{cup}"
        raw = "|".join([date_text, title, proponent, source_url])
        return "CAMPANIA-WIND-" + hashlib.sha1(raw.encode("utf-8")).hexdigest()[:18]

    def fetch(self) -> list[AgentFinding]:
        unique: dict[str, AgentFinding] = {}
        for keyword in SEARCH_KEYWORDS:
            html_page = self._post_search(keyword)
            for row in self._rows(html_page):
                project = self._clean(row.get("progetto") or "")
                proponent = self._clean(row.get("proponente") or "")
                raw_text = self._clean(row.get("raw_text") or "")
                full_text = f"{project} {proponent} {raw_text}"
                if not project or not self._is_wind(full_text):
                    continue
                date_text = self._clean(row.get("data_di_presentazione") or "")
                parsed_date = self._parse_date(date_text)
                if parsed_date is None or parsed_date < self.min_date:
                    continue
                source_url = self._clean(row.get("source_url") or SEARCH_URL)
                cup = self._clean(row.get("cup") or "")
                territory = self._clean(row.get("territori") or row.get("territorio") or "")
                municipalities = self._municipalities(territory, project)
                external_id = self._external_id(cup, date_text, project, proponent, source_url)
                unique.setdefault(
                    external_id,
                    AgentFinding(
                        external_id=external_id,
                        source_name=self.source_name,
                        source_url=source_url,
                        title=project[:700],
                        finding_type="project_source",
                        payload={
                            "project_name": project[:700],
                            "proponent": proponent or None,
                            "region": "Campania",
                            "province": self._province(project + " " + territory),
                            "municipalities": municipalities,
                            "power_mw": self._power_mw(project),
                            "procedure": self._procedure(project + " " + raw_text),
                            "status_raw": self._clean(row.get("esito") or "") or None,
                            "cup": cup or None,
                            "date_presented": date_text,
                            "decree": self._clean(row.get("decreto") or "") or None,
                            "sector": "eolico",
                            "source_grade_ceiling": "A1",
                            "project_specific": True,
                            "source_adapter_origin": "pv_agent_mvp/campania.py",
                        },
                    ),
                )
        return list(unique.values())
