from __future__ import annotations

import re
from urllib.parse import urljoin

from bs4 import BeautifulSoup

from app.collectors.base import BaseCollector, CollectorResult
from app.config import settings


PV_KEYWORDS = [
    "fotovolta",
    "agrivolta",
    "agrovolta",
    "bess",
    "accumulo",
    "solare agrivoltaico",
]

PROCEDURE_LINES = {
    "VIA",
    "VIA MINISTERIALE",
    "VAS",
    "SCOPING VAS",
    "VERIFICA ASSOGGETTABILITÀ VIA (SCREENING)",
}


class EmiliaRomagnaCollector(BaseCollector):
    source_name = "emilia_romagna"
    base_url = "https://serviziambiente.regione.emilia-romagna.it/viavasweb/"

    def fetch(self) -> list[CollectorResult]:
        try:
            response = self.session.get(self.base_url, timeout=settings.request_timeout)
            response.raise_for_status()
        except Exception:
            return []

        soup = BeautifulSoup(response.text, "html.parser")

        # Link dettaglio in ordine di apparizione
        detail_links: list[str] = []
        for a in soup.find_all("a", href=True):
            href = (a.get("href") or "").strip()
            if "/ricerca/dettaglio/" in href:
                detail_links.append(urljoin(self.base_url, href))

        # Righe testuali pulite
        raw_lines = soup.get_text("\n", strip=True).splitlines()
        lines = [self._clean_text(x) for x in raw_lines if self._clean_text(x)]

        blocks = self._parse_blocks(lines)

        results: list[CollectorResult] = []
        seen_ids: set[str] = set()
        detail_idx = 0

        for block in blocks:
            title = (block.get("title") or "").strip()
            if not title:
                continue

            lowered_title = title.lower()
            if not any(k in lowered_title for k in PV_KEYWORDS):
                continue

            detail_url = detail_links[detail_idx] if detail_idx < len(detail_links) else self.base_url
            detail_idx += 1

            proponent = block.get("proponent")
            status_raw = block.get("status")
            procedure = block.get("procedure")
            power = self._extract_power(title)
            municipalities = self._extract_municipalities(title)

            external_id = self._build_external_id(title, proponent, detail_url)
            if external_id in seen_ids:
                continue
            seen_ids.add(external_id)

            results.append(
                CollectorResult(
                    external_id=external_id,
                    source_url=detail_url,
                    title=title[:250],
                    payload={
                        "title": title[:500],
                        "proponent": proponent,
                        "status_raw": status_raw,
                        "region": "Emilia-Romagna",
                        "province": None,
                        "municipalities": municipalities,
                        "power": power,
                        "project_type_hint": procedure or title[:500],
                    },
                )
            )

        return results

    def _parse_blocks(self, lines: list[str]) -> list[dict[str, str | None]]:
        blocks: list[dict[str, str | None]] = []
        current: dict[str, str | None] | None = None
        mode: str | None = None

        for line in lines:
            upper = line.upper().strip()

            # nuovo blocco procedurale
            if upper in PROCEDURE_LINES:
                if current and current.get("title"):
                    self._finalize_block(current)
                    blocks.append(current)
                current = {
                    "procedure": upper,
                    "title": "",
                    "proponent": "",
                    "status": "",
                }
                mode = None
                continue

            if current is None:
                continue

            if upper == "TITOLO:":
                mode = "title"
                continue
            if upper == "PROPONENTE:":
                mode = "proponent"
                continue
            if upper == "STATO:":
                mode = "status"
                continue

            if upper.startswith("AVVIO OSSERVAZIONI:") or upper.startswith("SCADENZA OSSERVAZIONI:"):
                mode = None
                continue

            if upper.startswith("DATA PRESENTAZIONE ISTANZA:"):
                mode = None
                continue

            if line == "Istanze Presentate":
                if current and current.get("title"):
                    self._finalize_block(current)
                    blocks.append(current)
                current = None
                mode = None
                continue

            if mode == "title":
                current["title"] = (current["title"] + " " + line).strip()
            elif mode == "proponent":
                current["proponent"] = (current["proponent"] + " " + line).strip()
            elif mode == "status":
                current["status"] = (current["status"] + " " + line).strip()

        if current and current.get("title"):
            self._finalize_block(current)
            blocks.append(current)

        return blocks

    def _finalize_block(self, block: dict[str, str | None]) -> None:
        for key in ("title", "proponent", "status"):
            value = block.get(key) or ""
            block[key] = self._clean_text(value)

    def _extract_power(self, text: str) -> str | None:
        m = re.search(
            r"(\d{1,3}(?:[.\s]\d{3})*(?:,\d+)?|\d+(?:,\d+)?)\s*(MWP|MW|KWP|KW)",
            text,
            flags=re.IGNORECASE,
        )
        if m:
            return f"{m.group(1)} {m.group(2)}"
        return None

    def _extract_municipalities(self, title: str) -> list[str]:
        patterns = [
            r"nel comune di\s+([A-ZÀ-Ú][A-Za-zÀ-Úà-ú'`\- ]+)",
            r"nei comuni di\s+([A-ZÀ-Ú][A-Za-zÀ-Úà-ú'`\- ,]+)",
            r"localizzato nel comune di\s+([A-ZÀ-Ú][A-Za-zÀ-Úà-ú'`\- ]+)",
            r"localizzato nei comuni di\s+([A-ZÀ-Ú][A-Za-zÀ-Úà-ú'`\- ,]+)",
            r"nei territori comunali di\s+([A-ZÀ-Ú][A-Za-zÀ-Úà-ú'`\- ,]+)",
            r"nel comune\s+di\s+([A-ZÀ-Ú][A-Za-zÀ-Úà-ú'`\- ]+)",
        ]

        out: list[str] = []
        for pattern in patterns:
            for match in re.finditer(pattern, title, flags=re.IGNORECASE):
                raw = match.group(1).strip()
                parts = re.split(r",|\se\s", raw)
                for p in parts:
                    p = p.strip(" -")
                    if p and p not in out:
                        out.append(p)
        return out

    def _build_external_id(self, title: str, proponent: str | None, detail_url: str) -> str:
        base = f"{title}|{proponent or ''}|{detail_url}".lower()
        base = re.sub(r"\s+", "-", base)
        base = re.sub(r"[^a-z0-9:/._|-]", "", base)
        return base[:250]

    def _clean_text(self, value: str) -> str:
        return " ".join((value or "").replace("\xa0", " ").split()).strip()