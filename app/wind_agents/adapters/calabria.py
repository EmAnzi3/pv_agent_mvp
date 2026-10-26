from __future__ import annotations

import hashlib
import re
import time
from datetime import date, datetime
from urllib.parse import urljoin, urlparse

from bs4 import BeautifulSoup

from app.wind_agents.base import AgentFinding, BaseWindAgent


BASE_URL = "https://www.regione.calabria.it"
SOURCE_URL = "https://www.regione.calabria.it/dipartimento-per-la-sostenibilita-ambientale/avvisi-via-e-vas/"
SEARCH_TERMS = (
    "eolico",
    "eolica",
    "parco eolico",
    "repowering eolico",
    "PAUR eolico",
    "VIA eolico",
)
WIND_TERMS = ("eolico", "eolica", "aerogenerator", "repowering", "parco eolico")
PROVINCE_CODES = ("CZ", "CS", "KR", "RC", "VV")


class CalabriaWindAgent(BaseWindAgent):
    """Wind adaptation of pv_agent_mvp's Calabria regional web-search collector."""

    agent_name = "institutional_watch"
    source_name = "Regione Calabria VIA/PAUR"
    base_url = SOURCE_URL

    def __init__(self, max_search_pages_per_term: int = 6, min_year: int | None = None) -> None:
        super().__init__()
        self.max_search_pages_per_term = max(1, max_search_pages_per_term)
        self.cutoff = date(min_year or (date.today().year - 1), 1, 1)

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

    def _get_html(self, url: str) -> str | None:
        try:
            response = self.session.get(
                url,
                headers={
                    "User-Agent": "Wind-Radar-Agent/0.6",
                    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
                },
                timeout=60,
            )
            response.raise_for_status()
            return response.content.decode("utf-8", errors="replace")
        except Exception:
            return None

    @staticmethod
    def _allowed_url(url: str) -> bool:
        try:
            parsed = urlparse(url)
        except Exception:
            return False
        return parsed.scheme in {"http", "https"} and parsed.netloc.lower() in {
            "www.regione.calabria.it",
            "regione.calabria.it",
        }

    @classmethod
    def _candidate_url(cls, url: str, context: str) -> bool:
        if not cls._allowed_url(url):
            return False
        parsed = urlparse(url)
        path = parsed.path.lower()
        if path in {"", "/"} or "/page/" in path or ".pdf" in path:
            return False
        if any(token in path for token in ("wp-json", "feed", "privacy", "cookie", "contatti", "uffici")):
            return False
        return cls._is_wind(context + " " + path) or any(
            token in path for token in ("valutazione", "impatto", "assoggettabil", "paur", "via", "avvisi-via-e-vas")
        )

    def _discover(self) -> list[str]:
        urls: list[str] = []
        seen: set[str] = set()
        for term in SEARCH_TERMS:
            slug = term.replace(" ", "+")
            search_urls = [f"{BASE_URL}/?s={slug}"] + [
                f"{BASE_URL}/page/{page}/?s={slug}" for page in range(2, self.max_search_pages_per_term + 1)
            ]
            for search_url in search_urls:
                html_page = self._get_html(search_url)
                if not html_page:
                    continue
                soup = BeautifulSoup(html_page, "html.parser")
                page_text = self._clean(soup.get_text(" ", strip=True))
                if not self._is_wind(page_text):
                    continue
                for anchor in soup.find_all("a", href=True):
                    url = urljoin(search_url, anchor.get("href") or "").split("#", 1)[0]
                    parent = anchor.find_parent(["article", "li", "div", "p", "h2", "h3", "section"])
                    context = self._clean(
                        f"{anchor.get_text(' ', strip=True)} {parent.get_text(' ', strip=True) if parent else ''}"
                    )
                    if url in seen or not self._candidate_url(url, context):
                        continue
                    if not self._is_wind(context + " " + url):
                        continue
                    seen.add(url)
                    urls.append(url)
                time.sleep(0.05)
        return urls

    @classmethod
    def _title(cls, soup: BeautifulSoup, plain: str) -> str | None:
        for selector in ("h1", "h2", ".entry-title", ".page-title", "title"):
            node = soup.select_one(selector)
            if node:
                value = cls._clean(node.get_text(" ", strip=True))
                if value and len(value) > 10:
                    return value[:900]
        return plain[:900] if plain else None

    @classmethod
    def _publication_date(cls, soup: BeautifulSoup, text: str) -> str | None:
        candidates = []
        for node in soup.select("time, .date, .entry-date, .published, .post-date"):
            candidates.append(cls._clean(node.get("datetime") or node.get_text(" ", strip=True)))
        candidates.append(text[:1800])
        months = {
            "gennaio": 1, "febbraio": 2, "marzo": 3, "aprile": 4, "maggio": 5, "giugno": 6,
            "luglio": 7, "agosto": 8, "settembre": 9, "ottobre": 10, "novembre": 11, "dicembre": 12,
        }
        for candidate in candidates:
            for fmt in ("%Y-%m-%d", "%d/%m/%Y", "%d-%m-%Y"):
                match = re.search(r"\b\d{4}-\d{1,2}-\d{1,2}\b|\b\d{1,2}[/-]\d{1,2}[/-]\d{4}\b", candidate)
                if match:
                    try:
                        return datetime.strptime(match.group(0), fmt).date().isoformat()
                    except ValueError:
                        pass
            match = re.search(r"\b(\d{1,2})\s+([a-zàéèìòù]+)\s+(20\d{2})\b", candidate, flags=re.I)
            if match and match.group(2).lower() in months:
                try:
                    return date(int(match.group(3)), months[match.group(2).lower()], int(match.group(1))).isoformat()
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
    def _proponent(cls, text: str) -> str | None:
        for pattern in (
            r"Proponente\s*:?\s*(.+?)(?:\s+Comune|\s+Localizz|\s+Proced|\s+Potenza|\s+PAUR|\s+VIA|\||$)",
            r"Societ[aà]\s+proponente\s*:?\s*(.+?)(?:\s+Comune|\s+Localizz|\s+Proced|\s+Potenza|\||$)",
        ):
            match = re.search(pattern, text, flags=re.I)
            if match:
                value = cls._clean(match.group(1)).strip(" -–—:;,.")
                if 2 <= len(value) <= 220:
                    return value
        return None

    @classmethod
    def _municipalities(cls, text: str) -> list[str]:
        values: list[str] = []
        for match in re.finditer(
            r"(?:comune|comuni)\s+di\s+(.+?)(?:\s*\((?:CZ|CS|KR|RC|VV)\)|\.|;|\s+-\s+|$)",
            text,
            flags=re.I,
        ):
            for part in re.split(r",|/|\s+e\s+", match.group(1), flags=re.I):
                item = cls._clean(part).strip(" -–—:;,.()")
                if item and 2 <= len(item) <= 80 and item.lower() not in {v.lower() for v in values}:
                    values.append(item)
            if values:
                break
        return values[:12]

    @staticmethod
    def _province(text: str) -> str | None:
        match = re.search(r"\b(CZ|CS|KR|RC|VV)\b|\((CZ|CS|KR|RC|VV)\)", text, flags=re.I)
        return (match.group(1) or match.group(2)).upper() if match else None

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
    def _external_id(url: str) -> str:
        return "CALABRIA-WIND-" + hashlib.sha1(url.encode("utf-8")).hexdigest()[:18]

    def fetch(self) -> list[AgentFinding]:
        findings: list[AgentFinding] = []
        for url in self._discover():
            html_page = self._get_html(url)
            if not html_page:
                continue
            soup = BeautifulSoup(html_page, "html.parser")
            plain = self._clean(soup.get_text(" ", strip=True))
            title = self._title(soup, plain)
            combined = self._clean(f"{title or ''} {plain[:9000]}")
            if not title or not self._is_wind(combined):
                continue
            publication = self._publication_date(soup, combined)
            if publication:
                try:
                    if date.fromisoformat(publication) < self.cutoff:
                        continue
                except ValueError:
                    pass
            municipalities = self._municipalities(combined)
            findings.append(
                AgentFinding(
                    external_id=self._external_id(url),
                    source_name=self.source_name,
                    source_url=url,
                    title=title,
                    finding_type="project_source",
                    payload={
                        "project_name": title,
                        "proponent": self._proponent(combined),
                        "region": "Calabria",
                        "province": self._province(combined),
                        "municipalities": municipalities,
                        "power_mw": self._power_mw(combined),
                        "procedure": self._procedure(combined),
                        "publication_date": publication,
                        "sector": "eolico",
                        "source_grade_ceiling": "A1",
                        "project_specific": True,
                        "source_adapter_origin": "pv_agent_mvp/calabria.py",
                    },
                )
            )
            time.sleep(0.05)
        return findings
