from __future__ import annotations

import re
from urllib.parse import urljoin

from bs4 import BeautifulSoup

from app.collectors.base import BaseCollector, CollectorResult
from app.config import settings


STATUS_VALUES = {
    "In verifica amministrativa",
    "In itinere",
    "In itinere - 10bis",
    "Parere VIA espresso. In Itinere CDS",
    "Archiviato",
    "Valutato",
}


class VenetoCollector(BaseCollector):
    source_name = "veneto"
    base_url = "https://www.regione.veneto.it/web/vas-via-vinca-nuvv/progetti-2026"

    def fetch(self) -> list[CollectorResult]:
        try:
            response = self.session.get(self.base_url, timeout=settings.request_timeout)
            response.raise_for_status()
        except Exception:
            return []

        soup = BeautifulSoup(response.text, "html.parser")

        # Prendiamo tutti i link della pagina e poi filtriamo quelli che
        # sembrano essere veri progetti FV/agrivoltaico/BESS.
        links = soup.find_all("a", href=True)
        results: list[CollectorResult] = []

        for idx, link in enumerate(links):
            title = " ".join(link.get_text(" ", strip=True).split())
            if not title:
                continue

            lowered = title.lower()
            if not any(k in lowered for k in ["fotovolta", "agrivolta", "agrovolta", "bess"]):
                continue

            href = link.get("href", "").strip()
            source_url = urljoin(self.base_url, href)

            # Recupero un blocco di testo locale attorno al link
            context_lines = self._extract_context_lines(link)
            block_text = " | ".join(context_lines)

            proponent = self._extract_proponent(context_lines)
            status_raw = self._extract_status(context_lines)
            municipalities = self._extract_municipalities(title)
            power = self._extract_power(title)

            # external_id più robusto: usa titolo + url, non l'indice puro
            external_id = self._build_external_id(title, source_url)

            results.append(
                CollectorResult(
                    external_id=external_id,
                    source_url=source_url,
                    title=title[:250],
                    payload={
                        "title": title[:500],
                        "proponent": proponent,
                        "status_raw": status_raw or block_text[:500],
                        "region": "Veneto",
                        "province": None,
                        "municipalities": municipalities,
                        "power": power or title,
                        "project_type_hint": title,
                    },
                )
            )

        return results

    def _extract_context_lines(self, link) -> list[str]:
        """
        Costruisce un piccolo contesto testuale attorno al link del progetto.
        La pagina Veneto è sostanzialmente lineare: il progetto è preceduto
        dalla riga del proponente e seguito dallo stato.
        """
        parent = link.parent
        if not parent:
            return [link.get_text(" ", strip=True)]

        text = parent.get_text("\n", strip=True)
        lines = [ln.strip() for ln in text.splitlines() if ln.strip()]

        # Se il parent non basta, sali di un livello
        if len(lines) < 3 and parent.parent:
            text = parent.parent.get_text("\n", strip=True)
            lines = [ln.strip() for ln in text.splitlines() if ln.strip()]

        # Deduplica mantenendo l'ordine
        seen = set()
        clean_lines = []
        for line in lines:
            if line not in seen:
                clean_lines.append(line)
                seen.add(line)

        return clean_lines

    def _extract_proponent(self, lines: list[str]) -> str | None:
        for line in lines:
            m = re.search(r"Proponente:\s*(.+)", line, flags=re.IGNORECASE)
            if m:
                return m.group(1).strip()
        return None

    def _extract_status(self, lines: list[str]) -> str | None:
        for line in lines:
            line_clean = " ".join(line.split())
            if line_clean in STATUS_VALUES:
                return line_clean
        return None

    def _extract_municipalities(self, title: str) -> list[str]:
        patterns = [
            r"Comuni di localizzazione:\s*(.+?)(?:\.|$)",
            r"Comune di localizzazione:\s*(.+?)(?:\.|$)",
        ]

        for pattern in patterns:
            m = re.search(pattern, title, flags=re.IGNORECASE)
            if m:
                raw = m.group(1).strip()
                raw = raw.replace(";", ",")
                parts = re.split(r",|\se\s", raw)
                out = []
                for p in parts:
                    p = p.strip(" -")
                    if p:
                        out.append(p)
                return out

        return []

    def _extract_power(self, title: str) -> str | None:
        m = re.search(
            r"(\d{1,3}(?:[.\s]\d{3})*(?:,\d+)?|\d+(?:,\d+)?)\s*(MWp|MW|Kwp|kWp)",
            title,
            flags=re.IGNORECASE,
        )
        if m:
            return f"{m.group(1)} {m.group(2)}"
        return None

    def _build_external_id(self, title: str, source_url: str) -> str:
        base = f"{title}|{source_url}".lower()
        base = re.sub(r"\s+", "-", base)
        base = re.sub(r"[^a-z0-9:/._|-]", "", base)
        return base[:250]