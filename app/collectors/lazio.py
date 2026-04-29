from __future__ import annotations

import html
import json
import re
from pathlib import Path
from urllib.parse import parse_qs, unquote, urljoin, urlparse

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

PROVINCE_NAME_TO_CODE = {
    "FROSINONE": "FR",
    "LATINA": "LT",
    "RIETI": "RI",
    "ROMA": "RM",
    "VITERBO": "VT",
}


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
                            "title": normalized["title"][:700],
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
                            "procedure": normalized.get("procedure"),
                            "category": "Lazio VIA",
                            "date": normalized.get("date"),
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

    # ------------------------------------------------------------------
    # BLOCK PARSING
    # ------------------------------------------------------------------

    def _extract_project_blocks(self, soup: BeautifulSoup) -> list:
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
            r"\bData\s+arrivo\s*:\s*([0-9]{2}/[0-9]{2}/[0-9]{4})",
        )

        procedure = self._extract_regex(
            text,
            r"\bTipologia\s*:\s*(VIA|VERIFICA|PAUR|VAS)\b",
        )

        proponent = self._extract_field_value(
            text,
            field="Proponente",
            stop_fields=[
                "Comune",
                "Provincia",
                "Responsabile",
                "Tipologia",
                "Email",
            ],
        )

        municipality = self._extract_structured_municipality(text)
        province = self._extract_structured_province(text)
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

    def _extract_structured_municipality(self, text: str) -> str | None:
        matches = list(
            re.finditer(
                r"\bComune\s*:\s*(.+?)\s*-\s*Provincia\s*:",
                text,
                flags=re.IGNORECASE,
            )
        )

        if not matches:
            return None

        value = self._clean_text(matches[-1].group(1))
        return self._clean_municipality(value)

    def _extract_structured_province(self, text: str) -> str | None:
        matches = list(
            re.finditer(
                r"\bProvincia\s*:\s*([A-ZÀ-ÚA-Za-zà-ú'’ ]+?)(?:\s+Allegato|\s*$|\s+\*)",
                text,
                flags=re.IGNORECASE,
            )
        )

        if not matches:
            return None

        value = self._clean_text(matches[-1].group(1))
        return self._clean_province(value)

    def _extract_field_value(
        self,
        text: str,
        field: str,
        stop_fields: list[str],
    ) -> str | None:
        stop_pattern = "|".join(re.escape(item) for item in stop_fields)

        pattern = (
            rf"\b{re.escape(field)}\s*:\s*"
            rf"(.+?)"
            rf"(?=\s+(?:{stop_pattern})\s*:|$)"
        )

        matches = list(re.finditer(pattern, text, flags=re.IGNORECASE))

        if not matches:
            return None

        value = self._clean_text(matches[-1].group(1))
        return self._clean_entity(value)

    def _extract_title(self, text: str) -> str | None:
        cleaned = text

        cleaned = re.sub(
            r"\bData\s+arrivo\s*:\s*[0-9]{2}/[0-9]{2}/[0-9]{4}",
            "",
            cleaned,
            flags=re.IGNORECASE,
        )

        cleaned = re.sub(
            r"\bScarica\s+Elaborati\s+Progettuali\b",
            "",
            cleaned,
            flags=re.IGNORECASE,
        )

        label_pattern = (
            r"\bResponsabile\s*:|"
            r"\bTipologia\s*:|"
            r"\bEmail\s*:|"
            r"\bProponente\s*:|"
            r"\bComune\s*:"
        )

        cleaned = re.split(label_pattern, cleaned, maxsplit=1, flags=re.IGNORECASE)[0]

        cleaned = self._clean_text(cleaned)
        cleaned = cleaned.strip(" -–—:;")

        if not cleaned:
            return None

        return cleaned

    # ------------------------------------------------------------------
    # NAVIGATION
    # ------------------------------------------------------------------

    def _find_next_url(self, soup: BeautifulSoup, current_url: str) -> str | None:
        for a in soup.find_all("a", href=True):
            label = self._clean_text(a.get_text(" ", strip=True)).lower()

            if "pagina successiva" in label:
                return urljoin(current_url, a["href"])

        next_link = soup.find("link", attrs={"rel": "next"})

        if next_link and next_link.get("href"):
            return urljoin(current_url, next_link["href"])

        return None

    # ------------------------------------------------------------------
    # FILTERING
    # ------------------------------------------------------------------

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

    # ------------------------------------------------------------------
    # FIELD EXTRACTION
    # ------------------------------------------------------------------

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

        if (
            "rinviato a via" in lowered
            or "rinviato a v.i.a" in lowered
            or "rinviato alla procedura di via" in lowered
            or "rinviata alla procedura di via" in lowered
            or "rinviato alla procedura di v.i.a" in lowered
            or "rinviata alla procedura di v.i.a" in lowered
        ):
            return "Rinviato a VIA"

        if (
            "esclusa dal via" in lowered
            or "escluso dal via" in lowered
            or "esclusa da via" in lowered
            or "escluso da via" in lowered
            or "escluso dal procedimento di via" in lowered
            or "esclusa dal procedimento di via" in lowered
            or "escluso dal procedimento di v.i.a" in lowered
            or "esclusa dal procedimento di v.i.a" in lowered
        ):
            return "Escluso da VIA"

        if "conclus" in lowered:
            return "Concluso"

        if "procedimento in corso" in lowered or "in corso" in lowered:
            return "In corso"

        return None

    def _extract_power_text(self, text: str | None) -> str | None:
        if not text:
            return None

        number_unit = (
            r"([0-9]+(?:[.\s'’][0-9]+)*(?:[,.][0-9]+)?)"
            r"\s*"
            r"(MWp|MW|Mw|mW|kWp|KWp|Kwp|kW|KW)"
        )

        preferred_patterns = [
            rf"(?:potenza\s+)?di\s+picco\s+(?:totale\s+)?(?:dc\s+)?(?:pari\s+a\s+|di\s+)?{number_unit}",
            rf"potenza\s+di\s+picco\s+(?:totale\s+)?(?:dc\s+)?(?:pari\s+a\s+|di\s+)?{number_unit}",
            rf"potenza\s+(?:elettrica\s+)?installata\s+(?:di\s+)?{number_unit}",
            rf"potenza\s+complessiva\s+(?:di\s+)?{number_unit}",
            rf"potenza\s+nominale\s+complessiva\s+(?:di\s+)?{number_unit}",
            rf"potenza\s+di\s+nominale\s+pari\s+a\s+{number_unit}",
            rf"potenza\s+nominale\s+(?:in\s+dc\s+)?(?:pari\s+a\s+|di\s+)?{number_unit}",
            rf"potenza\s+in\s+dc\s+pari\s+a\s+{number_unit}",
            rf"potenza\s+in\s+immissione\s+pari\s+a\s+{number_unit}",
            rf"potenza\s+(?:pari\s+a\s+|di\s+){number_unit}",
            rf"\bda\s+{number_unit}",
        ]

        for pattern in preferred_patterns:
            match = re.search(pattern, text, flags=re.IGNORECASE)

            if match:
                return f"{match.group(1)} {match.group(2)}"

        match = re.search(
            r"(?<![\d.,'’])"
            r"("
            r"(?:\d+(?:[.\s'’]\d+)*(?:[,.]\d+)?)"
            r")"
            r"\s*"
            r"(MWp|MW|Mw|mW|kWp|KWp|Kwp|kW|KW)"
            r"\b",
            text,
            flags=re.IGNORECASE,
        )

        if not match:
            return None

        return f"{match.group(1)} {match.group(2)}"

    def _extract_first_url(self, block, page_url: str) -> str | None:
        preferred: list[str] = []
        fallback: list[str] = []

        for a in block.find_all("a", href=True):
            href = a.get("href")
            label = self._clean_text(a.get_text(" ", strip=True)).lower()

            normalized_url = self._normalize_url(href, page_url)

            if not normalized_url:
                continue

            if "elaborati" in label or "scarica" in label or "progettuali" in label:
                preferred.append(normalized_url)
            else:
                fallback.append(normalized_url)

        if preferred:
            return preferred[0]

        if fallback:
            return fallback[0]

        return None

    def _normalize_url(self, href: str | None, page_url: str) -> str | None:
        if not href:
            return None

        raw = html.unescape(str(href)).strip().strip("\"'")

        if not raw:
            return None

        if raw.startswith("mailto:"):
            return None

        parsed_raw = urlparse(raw)

        # Decodifica safelinks PRIMA di fare unquote globale, altrimenti il parametro
        # url=... rischia di inglobare anche &data=... e altra spazzatura Outlook.
        if "safelinks.protection.outlook.com" in parsed_raw.netloc:
            query = parse_qs(parsed_raw.query, keep_blank_values=True)
            target_values = query.get("url")

            if target_values:
                target = target_values[0]
                return self._normalize_url(target, page_url)

        raw_decoded = unquote(raw)
        raw_decoded = self._fix_scheme(raw_decoded)

        box_url = self._extract_box_url(raw_decoded)

        if box_url:
            return box_url

        absolute_url = urljoin(page_url, raw_decoded)
        absolute_url = self._fix_scheme(absolute_url)

        parsed_absolute = urlparse(absolute_url)

        if "safelinks.protection.outlook.com" in parsed_absolute.netloc:
            query = parse_qs(parsed_absolute.query, keep_blank_values=True)
            target_values = query.get("url")

            if target_values:
                target = target_values[0]
                return self._normalize_url(target, page_url)

        box_url = self._extract_box_url(absolute_url)

        if box_url:
            return box_url

        return absolute_url

    def _extract_box_url(self, value: str | None) -> str | None:
        if not value:
            return None

        text = html.unescape(str(value))
        text = unquote(text)
        text = self._fix_scheme(text)

        match = re.search(
            r"https?:/{1,2}(?:regionelazio\.app\.box\.com|regionelazio\.box\.com)/[^\s\"'<>|&]+",
            text,
            flags=re.IGNORECASE,
        )

        if not match:
            return None

        url = match.group(0)
        url = self._fix_scheme(url)

        # Taglia code tipiche dei safelinks.
        url = re.split(
            r"(?:&|\?)data=|(?:&|\?)sdata=|(?:&|\?)reserved=|\|",
            url,
            maxsplit=1,
            flags=re.IGNORECASE,
        )[0]

        url = url.strip(" .,:;\"'<>")

        return url or None

    def _fix_scheme(self, url: str) -> str:
        url = url.strip().strip("\"'")

        url = re.sub(r"^https:/([^/])", r"https://\1", url, flags=re.IGNORECASE)
        url = re.sub(r"^http:/([^/])", r"http://\1", url, flags=re.IGNORECASE)

        return url

    def _extract_regex(self, text: str, pattern: str) -> str | None:
        match = re.search(pattern, text, flags=re.IGNORECASE)

        if not match:
            return None

        return self._clean_text(match.group(1))

    # ------------------------------------------------------------------
    # CLEANING
    # ------------------------------------------------------------------

    def _clean_municipality(self, value: str | None) -> str | None:
        value = self._clean_text(value)

        if not value:
            return None

        value = re.sub(r"\([A-Z]{2}\)", "", value)
        value = re.sub(r"\s+Provincia\s*:.*$", "", value, flags=re.IGNORECASE)
        value = value.strip(" .,:;-–—()")

        if not value:
            return None

        if len(value) > 100:
            return None

        bad_fragments = [
            "responsabile",
            "tipologia",
            "email",
            "proponente",
            "allegato",
            "scarica",
            "elaborati",
            "progettuali",
            "potenza",
            "impianto",
            "determinazione",
        ]

        lowered = value.lower()

        if any(fragment in lowered for fragment in bad_fragments):
            return None

        return value.title()

    def _clean_province(self, value: str | None) -> str | None:
        value = self._clean_text(value)

        if not value:
            return None

        value = value.strip(" .,:;-–—()")
        upper = value.upper()

        if upper in PROVINCE_NAME_TO_CODE:
            return PROVINCE_NAME_TO_CODE[upper]

        if re.fullmatch(r"[A-Z]{2}", upper):
            return upper

        return value.title()

    def _clean_entity(self, value: str | None) -> str | None:
        value = self._clean_text(value)

        if not value:
            return None

        value = re.sub(
            r"\s+(Comune|Provincia|Responsabile|Tipologia|Email)\s*:.*$",
            "",
            value,
            flags=re.IGNORECASE,
        )

        value = value.strip(" .,:;-–—")

        return value or None

    # ------------------------------------------------------------------
    # IDS / TEXT
    # ------------------------------------------------------------------

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

    def _clean_text(self, value: str | None) -> str:
        value = html.unescape(value or "")
        return " ".join(value.replace("\xa0", " ").split()).strip()

    # ------------------------------------------------------------------
    # DEBUG
    # ------------------------------------------------------------------

    def _write_text(self, path: Path, content: str) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(content, encoding="utf-8")

    def _write_json(self, path: Path, obj) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(obj, ensure_ascii=False, indent=2), encoding="utf-8")


if __name__ == "__main__":
    collector = LazioCollector()
    items = collector.fetch()

    print(f"items: {len(items)}")

    for item in items[:100]:
        print(
            item.external_id,
            "|",
            item.title,
            "|",
            item.payload.get("proponent"),
            "|",
            item.payload.get("province"),
            "|",
            item.payload.get("municipalities"),
            "|",
            item.payload.get("power"),
            "|",
            item.payload.get("status_raw"),
            "|",
            item.source_url,
        )