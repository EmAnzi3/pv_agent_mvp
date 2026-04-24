from __future__ import annotations

import html
import json
import re
from pathlib import Path
from urllib.parse import urljoin

from bs4 import BeautifulSoup

from app.collectors.base import BaseCollector, CollectorResult


BASE_URL = "https://www.regione.lazio.it"
START_URL = (
    "https://www.regione.lazio.it/imprese/"
    "tutela-ambientale-difesa-suolo/"
    "valutazione-impatto-ambientale-progetti"
)


PV_KEYWORDS = [
    "fotovoltaico",
    "fotovoltaica",
    "solare fotovoltaico",
    "solare fotovoltaica",
    "agrivoltaico",
    "agrovoltaico",
    "agro-fotovoltaico",
    "agrofotovoltaico",
    "impianto fv",
    "parco fotovoltaico",
    "centrale fotovoltaica",
    "bess",
    "accumulo",
]

NON_PV_EXCLUDE = [
    "eolico",
    "eolica",
    "parco eolico",
    "impianto eolico",
    "rifiuti",
    "discarica",
    "depuratore",
    "trattamento reflui",
    "biometano",
    "biogas",
    "allevamento",
    "cava",
    "attività estrattiva",
]


class LazioCollector(BaseCollector):
    source_name = "lazio"
    base_url = START_URL

    def fetch(self) -> list[CollectorResult]:
        debug_base = Path("/app/reports/debug_lazio")
        debug_base.mkdir(parents=True, exist_ok=True)

        results: list[CollectorResult] = []
        seen_ids: set[str] = set()
        visited_urls: set[str] = set()
        matched_blocks: list[dict] = []
        raw_blocks_sample: list[str] = []

        url = START_URL
        page_no = 1
        max_pages = 120

        while url and page_no <= max_pages:
            if url in visited_urls:
                break

            visited_urls.add(url)

            try:
                response = self.session.get(url, timeout=90)
                response.raise_for_status()
                text = response.content.decode("utf-8", errors="replace")
            except Exception as exc:
                self._write_text(debug_base / f"page_{page_no}_error.txt", str(exc))
                break

            self._write_text(debug_base / f"page_{page_no}.html", text[:500000])

            soup = BeautifulSoup(text, "html.parser")
            blocks = self._extract_project_blocks(soup)
            next_url = self._find_next_url(soup, url)

            self._write_json(
                debug_base / f"page_{page_no}_debug.json",
                {
                    "url": url,
                    "blocks": len(blocks),
                    "next_url": next_url,
                },
            )

            for block in blocks:
                raw_text = self._clean_text(block.get_text(" ", strip=True))

                if len(raw_blocks_sample) < 80:
                    raw_blocks_sample.append(raw_text)

                if not self._is_pv_related(raw_text):
                    continue

                normalized = self._normalize_block(block, url)
                if not normalized:
                    continue

                external_id = self._build_external_id(
                    normalized.get("date"),
                    normalized["title"],
                    normalized.get("proponent"),
                    normalized.get("municipality"),
                )

                if external_id in seen_ids:
                    continue

                seen_ids.add(external_id)
                matched_blocks.append(normalized)

                results.append(
                    CollectorResult(
                        external_id=external_id,
                        source_url=normalized.get("url") or url,
                        title=normalized["title"][:250],
                        payload={
                            "title": normalized["title"][:500],
                            "proponent": normalized.get("proponent"),
                            "status_raw": normalized.get("status"),
                            "region": "Lazio",
                            "province": normalized.get("province"),
                            "municipalities": (
                                [normalized["municipality"]]
                                if normalized.get("municipality")
                                else []
                            ),
                            "power": normalized.get("power"),
                            "project_type_hint": normalized.get("procedure") or "Lazio VIA",
                        },
                    )
                )

            if not next_url:
                break

            url = next_url
            page_no += 1

        self._write_json(debug_base / "raw_blocks_sample.json", raw_blocks_sample)
        self._write_json(debug_base / "matched_blocks_sample.json", matched_blocks[:100])
        self._write_json(
            debug_base / "summary.json",
            {
                "pages_visited": len(visited_urls),
                "results": len(results),
                "matched_blocks": len(matched_blocks),
                "visited_urls": list(visited_urls),
            },
        )

        return results

    def _extract_project_blocks(self, soup: BeautifulSoup) -> list:
        """
        La pagina Lazio contiene i progetti dentro elementi <li>.
        Ogni progetto contiene uno span/testo con 'Data arrivo'.
        """
        blocks = []

        for li in soup.find_all("li"):
            text = self._clean_text(li.get_text(" ", strip=True))

            if "Data arrivo" not in text:
                continue

            if "Proponente" not in text and "Comune" not in text:
                continue

            blocks.append(li)

        return blocks

    def _normalize_block(self, block, page_url: str) -> dict | None:
        text = self._clean_text(block.get_text(" ", strip=True))
        if not text:
            return None

        date = self._extract_regex(
            text,
            r"Data arrivo\s*:?\s*([0-9]{2}/[0-9]{2}/[0-9]{4})",
        )

        procedure = self._extract_regex(
            text,
            r"Tipologia\s*:?\s*(VIA|VERIFICA|PAUR|VAS)",
        )

        proponent = self._extract_regex(
            text,
            r"Proponente\s*:?\s*(.+?)(?:\s+Comune\s*:|\s+Responsabile\s*:|$)",
        )

        municipality = self._extract_last_regex(
            text,
            r"Comune\s*:?\s*(.+?)\s*-\s*Provincia\s*:?",
        )

        province = self._extract_last_regex(
            text,
            r"Provincia\s*:?\s*([A-ZÀ-ÚA-Za-zà-ú ]+?)(?:\s+Allegato|\s*$)",
        )

        status = self._extract_status(text)

        title = self._extract_title(text)
        if not title:
            return None

        power = self._extract_power_text(title) or self._extract_power_text(text)
        detail_url = self._extract_first_url(block, page_url)

        return {
            "date": date,
            "title": title,
            "procedure": procedure,
            "proponent": proponent,
            "municipality": municipality,
            "province": province,
            "status": status,
            "power": power,
            "url": detail_url or page_url,
        }

    def _extract_title(self, text: str) -> str | None:
        cleaned = text

        cleaned = re.sub(
            r"Data arrivo\s*:?\s*[0-9]{2}/[0-9]{2}/[0-9]{4}",
            "",
            cleaned,
            flags=re.IGNORECASE,
        )
        cleaned = re.sub(
            r"Scarica Elaborati Progettuali",
            "",
            cleaned,
            flags=re.IGNORECASE,
        )
        cleaned = re.sub(
            r"Responsabile\s*:?\s*.+?(?=Tipologia\s*:?|Email\s*:?|Proponente\s*:?|Comune\s*:?|$)",
            "",
            cleaned,
            flags=re.IGNORECASE,
        )
        cleaned = re.sub(
            r"Tipologia\s*:?\s*.+?(?=Email\s*:?|Proponente\s*:?|Comune\s*:?|$)",
            "",
            cleaned,
            flags=re.IGNORECASE,
        )
        cleaned = re.sub(
            r"Email\s*:?\s*.+?(?=Proponente\s*:?|Comune\s*:?|$)",
            "",
            cleaned,
            flags=re.IGNORECASE,
        )
        cleaned = re.sub(
            r"Proponente\s*:?\s*.+?(?=Comune\s*:?|$)",
            "",
            cleaned,
            flags=re.IGNORECASE,
        )
        cleaned = re.sub(
            r"Comune\s*:?\s*.+$",
            "",
            cleaned,
            flags=re.IGNORECASE,
        )

        cleaned = self._clean_text(cleaned)
        cleaned = cleaned.strip(" -–—:;")

        if not cleaned:
            return None

        return cleaned

    def _find_next_url(self, soup: BeautifulSoup, current_url: str) -> str | None:
        for a in soup.find_all("a", href=True):
            label = self._clean_text(a.get_text(" ", strip=True)).lower()

            if "pagina successiva" in label:
                return urljoin(current_url, a["href"])

        next_link = soup.find("link", attrs={"rel": "next"})
        if next_link and next_link.get("href"):
            return urljoin(current_url, next_link["href"])

        return None

    def _is_pv_related(self, text: str) -> bool:
        lowered = f" {self._normalize_for_match(text)} "

        if not any(k in lowered for k in PV_KEYWORDS):
            return False

        has_strong_pv = any(
            k in lowered
            for k in [
                "fotovoltaico",
                "fotovoltaica",
                "agrivoltaico",
                "agro-fotovoltaico",
                "agrofotovoltaico",
                "solare fotovoltaico",
                "solare fotovoltaica",
            ]
        )

        if not has_strong_pv and any(k in lowered for k in NON_PV_EXCLUDE):
            return False

        return True

    def _extract_status(self, text: str) -> str | None:
        lowered = self._normalize_for_match(text)

        if "favorevole con prescrizioni" in lowered:
            return "Favorevole con prescrizioni"

        if "favorevole" in lowered:
            return "Favorevole"

        if "archiviato" in lowered or "archiviata" in lowered:
            return "Archiviato"

        if "improcedibile" in lowered:
            return "Improcedibile"

        if "rinviato a via" in lowered or "rinviato a v.i.a" in lowered:
            return "Rinviato a VIA"

        if "esclusa dal via" in lowered or "escluso dal via" in lowered:
            return "Escluso da VIA"

        if "conclus" in lowered:
            return "Concluso"

        if "procedimento in corso" in lowered or "in corso" in lowered:
            return "In corso"

        return None

    def _extract_power_text(self, text: str | None) -> str | None:
        if not text:
            return None

        m = re.search(
            r"(?<![\d.,'’])"
            r"("
            r"(?:\d{1,3}(?:[.\s'’]\d{3})+(?:[,.]\d+)?)"
            r"|"
            r"(?:\d+[.,]\d+)"
            r"|"
            r"(?:\d+)"
            r")"
            r"\s*"
            r"(MWp|MW|kWp|KWp|kW|KW)"
            r"\b",
            text,
            flags=re.IGNORECASE,
        )

        if not m:
            return None

        return f"{m.group(1)} {m.group(2)}"

    def _extract_first_url(self, block, page_url: str) -> str | None:
        preferred: list[str] = []
        fallback: list[str] = []

        for a in block.find_all("a", href=True):
            href = a.get("href")
            label = self._clean_text(a.get_text(" ", strip=True)).lower()

            if not href:
                continue

            absolute_url = urljoin(page_url, href)

            # Evita mailto come URL principale.
            if absolute_url.startswith("mailto:"):
                continue

            if "elaborati" in label or "scarica" in label or "progettuali" in label:
                preferred.append(absolute_url)
            else:
                fallback.append(absolute_url)

        if preferred:
            return preferred[0]

        if fallback:
            return fallback[0]

        return None

    def _extract_regex(self, text: str, pattern: str) -> str | None:
        m = re.search(pattern, text, flags=re.IGNORECASE)
        if not m:
            return None

        return self._clean_text(m.group(1))

    def _extract_last_regex(self, text: str, pattern: str) -> str | None:
        matches = list(re.finditer(pattern, text, flags=re.IGNORECASE))
        if not matches:
            return None

        return self._clean_text(matches[-1].group(1))

    def _build_external_id(
        self,
        date: str | None,
        title: str,
        proponent: str | None,
        municipality: str | None,
    ) -> str:
        base = f"{date or ''}|{title}|{proponent or ''}|{municipality or ''}".lower()
        base = re.sub(r"\s+", "-", base)
        base = re.sub(r"[^a-z0-9:/._|-]", "", base)
        return base[:250]

    def _normalize_for_match(self, value: str) -> str:
        value = self._clean_text(value).lower()
        value = html.unescape(value)
        value = value.replace("à", "a")
        value = value.replace("è", "e")
        value = value.replace("é", "e")
        value = value.replace("ì", "i")
        value = value.replace("ò", "o")
        value = value.replace("ù", "u")
        return value

    def _clean_text(self, value: str) -> str:
        value = html.unescape(value or "")
        return " ".join(value.replace("\xa0", " ").split()).strip()

    def _write_text(self, path: Path, content: str) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(content, encoding="utf-8")

    def _write_json(self, path: Path, obj) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(obj, ensure_ascii=False, indent=2), encoding="utf-8")