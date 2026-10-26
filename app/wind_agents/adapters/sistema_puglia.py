from __future__ import annotations

import re
import time
import unicodedata
from urllib.parse import urljoin

from bs4 import BeautifulSoup

from app.wind_agents.base import AgentFinding, BaseWindAgent
from app.wind_agents.state import get_source_cursor, set_source_cursor


BASE_URL = "https://www.sistema.puglia.it"
DETAIL_URL_TEMPLATE = "https://www.sistema.puglia.it/portal/page/portal/SistemaPuglia/DettaglioInfo?id={id}"
CURSOR_ID = "sistema-puglia-detail-id"
INITIAL_CURSOR = 64250  # last audited high-water mark inherited from pv_agent_mvp

WIND_TERMS = (
    "eolico",
    "eolica",
    "parco eolico",
    "impianto eolico",
    "aerogenerator",
    "repowering",
)

PROVINCES = ("BA", "BT", "BR", "FG", "LE", "TA")


class SistemaPugliaWindAgent(BaseWindAgent):
    """Incremental wind watch for Sistema Puglia energy acts.

    The mature PV collector scanned a fixed ~1,500-detail-id range. For periodic
    wind monitoring this adapter uses a persistent high-water cursor plus a
    bounded forward probe and lookback, so routine runs stay finite even when
    individual legacy portal pages are slow. A future explicit backfill can
    widen the range without changing the live cadence.
    """

    agent_name = "institutional_watch"
    source_name = "Sistema Puglia Energia"
    base_url = BASE_URL

    def __init__(
        self,
        forward_probe: int = 30,
        lookback: int = 60,
        request_sleep: float = 0.05,
        request_timeout: float = 8.0,
    ) -> None:
        super().__init__()
        self.forward_probe = max(1, forward_probe)
        self.lookback = max(0, lookback)
        self.request_sleep = max(0.0, request_sleep)
        self.request_timeout = max(2.0, request_timeout)

    @staticmethod
    def _clean(value: object) -> str:
        return re.sub(r"\s+", " ", str(value or "")).strip()

    @classmethod
    def _repair_mojibake(cls, value: str) -> str:
        # Same portal quirk handled by pv_agent_mvp: several pages are more
        # reliable when decoded as Windows-1252. Keep a small repair fallback.
        replacements = {
            "Ã ": "à",
            "Ã¨": "è",
            "Ã©": "é",
            "Ã¬": "ì",
            "Ã²": "ò",
            "Ã¹": "ù",
            "â€™": "’",
            "Â": "",
        }
        for bad, good in replacements.items():
            value = value.replace(bad, good)
        return value

    @classmethod
    def _norm(cls, value: object) -> str:
        text = cls._clean(value).lower()
        text = unicodedata.normalize("NFKD", text)
        text = "".join(ch for ch in text if not unicodedata.combining(ch))
        return re.sub(r"\s+", " ", text)

    @classmethod
    def _is_wind(cls, text: str) -> bool:
        lowered = cls._norm(text)
        return any(term in lowered for term in WIND_TERMS)

    def _get_html(self, detail_id: int) -> str | None:
        url = DETAIL_URL_TEMPLATE.format(id=detail_id)
        try:
            response = self.session.get(
                url,
                headers={
                    "User-Agent": "Wind-Radar-Agent/0.6",
                    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
                    "Accept-Language": "it-IT,it;q=0.9,en;q=0.7",
                    "Referer": BASE_URL,
                },
                timeout=self.request_timeout,
                allow_redirects=True,
            )
        except Exception:
            return None
        if response.status_code != 200:
            return None
        text = response.content.decode("windows-1252", errors="replace")
        text = self._repair_mojibake(text)
        if "Data Pubblicazione" not in text and "Determinazione" not in text:
            return None
        return text

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
    def _publication_date(cls, text: str) -> str | None:
        match = re.search(
            r"Data\s+Pubblicazione\s*:?\s*([0-9]{1,2}[/-][0-9]{1,2}[/-][0-9]{4})",
            text,
            flags=re.I,
        )
        return match.group(1) if match else None

    @classmethod
    def _proponent(cls, text: str) -> str | None:
        normalized = cls._clean(text)
        patterns = (
            r"Societ[aà]\s+proponente\s*:\s*(.+?)(?:\s+-\s+Partita\s+IVA|\s+-\s+P\.?\s*IVA|\s+C\.?\s*Fisc|\s+Sede\s+Legale|\s+Data\s+Pubblicazione|\s+\[Scarica|$)",
            r"Proponente\s*:\s*(.+?)(?:\s+-\s+Partita\s+IVA|\s+-\s+P\.?\s*IVA|\s+C\.?\s*Fisc|\s+Sede\s+Legale|\s+Data\s+Pubblicazione|\s+\[Scarica|$)",
            r"Voltura\s+(?:alla\s+societ[aà]|a\s+favore\s+di)\s+(.+?)(?:\s+con\s+sede|\s+-\s+P\.?\s*IVA|\s+C\.?\s*Fisc|\s+Data\s+Pubblicazione|$)",
        )
        for pattern in patterns:
            match = re.search(pattern, normalized, flags=re.I)
            if match:
                value = cls._clean(match.group(1)).strip(" -–—:;,.")
                if 2 <= len(value) <= 220:
                    return value
        return None

    @classmethod
    def _province(cls, text: str) -> str | None:
        for code in PROVINCES:
            if re.search(rf"\({code}\)|\b{code}\b", text, flags=re.I):
                return code
        province_names = {
            "bari": "BA",
            "barletta": "BT",
            "brindisi": "BR",
            "foggia": "FG",
            "lecce": "LE",
            "taranto": "TA",
        }
        lowered = cls._norm(text)
        for name, code in province_names.items():
            if f"provincia di {name}" in lowered:
                return code
        return None

    @classmethod
    def _municipalities(cls, text: str) -> list[str]:
        values: list[str] = []
        for match in re.finditer(
            r"(?:comune|comuni)\s+di\s+(.+?)(?:\s+in\s+provincia|\s*\([A-Z]{2}\)|\.|;|\s+-\s+|$)",
            text,
            flags=re.I,
        ):
            segment = cls._clean(match.group(1))
            for part in re.split(r",|\s+e\s+", segment):
                value = cls._clean(part).strip(" -–—:;,.()")
                if value and 2 <= len(value) <= 80 and value.lower() not in {v.lower() for v in values}:
                    values.append(value)
            if values:
                break
        return values[:12]

    @classmethod
    def _procedure(cls, text: str) -> str | None:
        lowered = cls._norm(text)
        if "autorizzazione unica" in lowered or re.search(r"\bau\b", lowered):
            return "AU FER"
        if "paur" in lowered or "provvedimento autorizzatorio unico" in lowered:
            return "PAUR"
        if "valutazione di impatto ambientale" in lowered or re.search(r"\bvia\b", lowered):
            return "VIA"
        if "voltura" in lowered:
            return "Voltura"
        if "proroga" in lowered:
            return "Proroga"
        if "variante" in lowered:
            return "Variante"
        return "Atto energia"

    @classmethod
    def _pdf_url(cls, soup: BeautifulSoup, page_url: str) -> str | None:
        for anchor in soup.find_all("a", href=True):
            label = cls._norm(anchor.get_text(" ", strip=True))
            href = anchor.get("href") or ""
            if ".pdf" in href.lower() or "scarica" in label or "determinazione" in label:
                return urljoin(page_url, href)
        return None

    @classmethod
    def _title(cls, soup: BeautifulSoup, text: str, proponent: str | None, municipalities: list[str], power_mw: float | None) -> str:
        for selector in ("h1", "h2", "h3", ".title", ".titolo"):
            for node in soup.select(selector):
                candidate = cls._clean(node.get_text(" ", strip=True))
                if len(candidate) > 20 and "sistema puglia" not in cls._norm(candidate):
                    return candidate[:700]
        pieces = [piece for piece in (proponent, ", ".join(municipalities[:4]) or None, f"{power_mw:g} MW" if power_mw else None) if piece]
        if pieces:
            return " - ".join(pieces)[:700]
        match = re.search(r"(Determinazione\s+del\s+Dirigente.+?)(?:Data\s+Pubblicazione|$)", text, flags=re.I)
        return cls._clean(match.group(1))[:700] if match else "Sistema Puglia Energia - eolico"

    def fetch(self) -> list[AgentFinding]:
        raw_cursor = get_source_cursor(CURSOR_ID, str(INITIAL_CURSOR))
        try:
            cursor = int(raw_cursor or INITIAL_CURSOR)
        except ValueError:
            cursor = INITIAL_CURSOR

        upper = cursor + self.forward_probe
        lower = max(1, cursor - self.lookback)
        highest_existing = cursor
        findings: list[AgentFinding] = []

        for detail_id in range(upper, lower - 1, -1):
            html_page = self._get_html(detail_id)
            if html_page is None:
                continue
            highest_existing = max(highest_existing, detail_id)

            soup = BeautifulSoup(html_page, "html.parser")
            text = self._clean(soup.get_text(" ", strip=True))
            if not self._is_wind(text):
                if self.request_sleep:
                    time.sleep(self.request_sleep)
                continue

            page_url = DETAIL_URL_TEMPLATE.format(id=detail_id)
            proponent = self._proponent(text)
            municipalities = self._municipalities(text)
            power_mw = self._power_mw(text)
            procedure = self._procedure(text)
            publication_date = self._publication_date(text)
            title = self._title(soup, text, proponent, municipalities, power_mw)

            findings.append(
                AgentFinding(
                    external_id=f"SISTEMA-PUGLIA-WIND-{detail_id}",
                    source_name=self.source_name,
                    source_url=page_url,
                    title=title,
                    finding_type="project_source",
                    payload={
                        "project_name": title,
                        "proponent": proponent,
                        "region": "Puglia",
                        "province": self._province(text),
                        "municipalities": municipalities,
                        "power_mw": power_mw,
                        "procedure": procedure,
                        "publication_date": publication_date,
                        "pdf_url": self._pdf_url(soup, page_url),
                        "detail_id": detail_id,
                        "sector": "eolico",
                        "source_grade_ceiling": "A1",
                        "project_specific": True,
                        "source_adapter_origin": "pv_agent_mvp/sistema_puglia_energia.py",
                    },
                )
            )

            if self.request_sleep:
                time.sleep(self.request_sleep)

        set_source_cursor(
            CURSOR_ID,
            highest_existing,
            {
                "scanned_lower": lower,
                "scanned_upper": upper,
                "wind_findings": len(findings),
                "strategy": "incremental_forward_probe_plus_lookback",
                "request_timeout_seconds": self.request_timeout,
            },
        )
        return findings
