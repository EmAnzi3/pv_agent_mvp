from __future__ import annotations

import re
import time
from urllib.parse import urljoin

from bs4 import BeautifulSoup

from app.collectors.base import BaseCollector, CollectorResult
from app.config import settings


BASE_URL = "https://serviziambiente.regione.emilia-romagna.it/viavasweb/"
REQUEST_SLEEP_SECONDS = 0.08


PV_KEYWORDS = [
    "fotovolta",
    "agrivolta",
    "agrovolta",
    "agro-fotovolta",
    "bess",
    "accumulo",
    "solare agrivoltaico",
]


PROVINCE_NAME_TO_CODE = {
    "BOLOGNA": "BO",
    "FERRARA": "FE",
    "FORLI-CESENA": "FC",
    "FORLÌ-CESENA": "FC",
    "FORLI' CESENA": "FC",
    "MODENA": "MO",
    "PARMA": "PR",
    "PIACENZA": "PC",
    "RAVENNA": "RA",
    "REGGIO EMILIA": "RE",
    "RIMINI": "RN",
}


STOP_LABELS = {
    "descrizione",
    "documenti",
    "ricorso giurisdizionale",
    "inoltro osservazioni",
    "titolo",
    "proponente",
    "stato",
    "tipo procedura",
    "tipologia progetto o piano",
    "localizzazione",
    "comune",
    "provincia/citta metropolitana",
    "provincia/città metropolitana",
    "altre localizzazioni",
    "protocollo di attivazione",
    "numero",
    "data",
    "data scadenza oservazioni",
    "data scadenza osservazioni",
    "pubblicazioni",
    "documenti presenti",
    "attivazione istanza",
    "progetto iniziale",
    "verifica di completezza",
    "progetto sottoposto a osservazioni",
    "altra documentazione",
    "pareri",
}


class EmiliaRomagnaCollector(BaseCollector):
    source_name = "emilia_romagna"
    base_url = BASE_URL

    def fetch(self) -> list[CollectorResult]:
        html_page = self._get_html(self.base_url)

        if not html_page:
            return []

        soup = BeautifulSoup(html_page, "html.parser")
        detail_links = self._extract_detail_links(soup)

        results: list[CollectorResult] = []
        seen_ids: set[str] = set()

        for detail_url in detail_links:
            detail_html = self._get_html(detail_url)

            if not detail_html:
                continue

            parsed = self._parse_detail_page(detail_html, detail_url)

            if not parsed:
                continue

            if not self._is_relevant(parsed):
                continue

            external_id = self._build_external_id(detail_url)

            if external_id in seen_ids:
                continue

            seen_ids.add(external_id)

            title = parsed.get("title") or ""

            results.append(
                CollectorResult(
                    external_id=external_id,
                    source_url=detail_url,
                    title=title[:250],
                    payload={
                        "title": title[:700],
                        "proponent": parsed.get("proponent"),
                        "status_raw": parsed.get("status_raw"),
                        "region": "Emilia-Romagna",
                        "province": parsed.get("province"),
                        "municipalities": parsed.get("municipalities") or [],
                        "power": parsed.get("power"),
                        "project_type_hint": parsed.get("project_type_hint"),
                        "procedure": parsed.get("procedure"),
                        "category": "Emilia-Romagna VIA-VAS",
                        "tipologia": parsed.get("tipologia"),
                        "protocol_number": parsed.get("protocol_number"),
                        "protocol_date": parsed.get("protocol_date"),
                        "detail_url": detail_url,
                    },
                )
            )

            time.sleep(REQUEST_SLEEP_SECONDS)

        return results

    # ------------------------------------------------------------------
    # HTTP
    # ------------------------------------------------------------------

    def _headers(self) -> dict[str, str]:
        return {
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/147.0.0.0 Safari/537.36"
            ),
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "it-IT,it;q=0.9,en-US;q=0.8,en;q=0.7",
            "Referer": BASE_URL,
        }

    def _get_html(self, url: str) -> str | None:
        try:
            response = self.session.get(
                url,
                headers=self._headers(),
                timeout=settings.request_timeout,
                allow_redirects=True,
            )
            response.raise_for_status()
            return response.text
        except Exception:
            return None

    # ------------------------------------------------------------------
    # LINK DISCOVERY
    # ------------------------------------------------------------------

    def _extract_detail_links(self, soup: BeautifulSoup) -> list[str]:
        links: list[str] = []
        seen: set[str] = set()

        for a in soup.find_all("a", href=True):
            href = (a.get("href") or "").strip()

            if "/ricerca/dettaglio/" not in href:
                continue

            absolute = urljoin(self.base_url, href)

            if absolute in seen:
                continue

            seen.add(absolute)
            links.append(absolute)

        return links

    # ------------------------------------------------------------------
    # DETAIL PARSING
    # ------------------------------------------------------------------

    def _parse_detail_page(self, html_page: str, detail_url: str) -> dict | None:
        soup = BeautifulSoup(html_page, "html.parser")
        lines = self._extract_clean_lines(soup)

        if not lines:
            return None

        title = self._value_after_label(lines, "Titolo")
        proponent = self._value_after_label(lines, "Proponente")
        status_raw = self._value_after_label(lines, "Stato")
        procedure = self._value_after_label(lines, "Tipo Procedura")
        tipologia = self._value_after_label(lines, "Tipologia progetto o piano")

        municipality_raw = self._value_after_label(lines, "Comune")
        province_raw = self._value_after_label(lines, "Provincia/Città Metropolitana")
        other_locations_raw = self._value_after_label(lines, "Altre localizzazioni")

        protocol_number = self._extract_protocol_number(lines)
        protocol_date = self._extract_protocol_date(lines)

        if not title:
            return None

        province = self._normalize_province(province_raw)

        municipalities = self._extract_municipalities(
            municipality_raw=municipality_raw,
            other_locations_raw=other_locations_raw,
        )

        for municipality in self._extract_municipalities_from_title(title):
            if municipality not in municipalities:
                municipalities.append(municipality)

        combined_text = " ".join(
            part
            for part in [
                title,
                proponent,
                status_raw,
                procedure,
                tipologia,
                municipality_raw,
                province_raw,
                other_locations_raw,
            ]
            if part
        )

        power = self._extract_power(combined_text)

        return {
            "title": title,
            "proponent": proponent,
            "status_raw": status_raw,
            "procedure": procedure,
            "tipologia": tipologia,
            "project_type_hint": procedure or tipologia or title,
            "province": province,
            "municipalities": municipalities,
            "power": power,
            "protocol_number": protocol_number,
            "protocol_date": protocol_date,
            "detail_url": detail_url,
        }

    def _extract_clean_lines(self, soup: BeautifulSoup) -> list[str]:
        raw_lines = soup.get_text("\n", strip=True).splitlines()
        lines: list[str] = []

        for line in raw_lines:
            cleaned = self._clean_text(line)

            if cleaned:
                lines.append(cleaned)

        return lines

    def _value_after_label(self, lines: list[str], label: str) -> str | None:
        wanted = self._normalize_label(label)

        for idx, line in enumerate(lines):
            normalized_line = self._normalize_label(line)

            if normalized_line == wanted:
                for candidate in lines[idx + 1 : idx + 5]:
                    cleaned = self._clean_text(candidate)

                    if not cleaned:
                        continue

                    if self._looks_like_label(cleaned):
                        return None

                    return cleaned

            if normalized_line.startswith(wanted + " "):
                inline_value = line.split(":", 1)[-1].strip() if ":" in line else ""
                inline_value = self._clean_text(inline_value)

                if inline_value and not self._looks_like_label(inline_value):
                    return inline_value

        return None

    def _extract_protocol_number(self, lines: list[str]) -> str | None:
        for idx, line in enumerate(lines):
            if self._normalize_label(line) != "protocollo di attivazione":
                continue

            for j in range(idx + 1, min(idx + 8, len(lines))):
                if self._normalize_label(lines[j]) == "numero":
                    return self._next_value(lines, j)

        return None

    def _extract_protocol_date(self, lines: list[str]) -> str | None:
        for idx, line in enumerate(lines):
            if self._normalize_label(line) != "protocollo di attivazione":
                continue

            for j in range(idx + 1, min(idx + 10, len(lines))):
                if self._normalize_label(lines[j]) == "data":
                    return self._next_value(lines, j)

        return None

    def _next_value(self, lines: list[str], idx: int) -> str | None:
        for candidate in lines[idx + 1 : idx + 4]:
            cleaned = self._clean_text(candidate)

            if not cleaned:
                continue

            if self._looks_like_label(cleaned):
                return None

            return cleaned

        return None

    # ------------------------------------------------------------------
    # NORMALIZATION
    # ------------------------------------------------------------------

    def _is_relevant(self, parsed: dict) -> bool:
        text = " ".join(
            str(parsed.get(key) or "")
            for key in [
                "title",
                "tipologia",
                "procedure",
                "project_type_hint",
            ]
        ).lower()

        return any(keyword in text for keyword in PV_KEYWORDS)

    def _normalize_province(self, value: str | None) -> str | None:
        if not value:
            return None

        cleaned = self._clean_text(value) or ""
        cleaned_upper = cleaned.upper().strip()

        if re.fullmatch(r"[A-Z]{2}", cleaned_upper):
            return cleaned_upper

        match = re.search(r"\(([A-Z]{2})\)", cleaned_upper)
        if match:
            return match.group(1)

        normalized = self._normalize_key(cleaned_upper)

        for province_name, code in PROVINCE_NAME_TO_CODE.items():
            if self._normalize_key(province_name) == normalized:
                return code

        return cleaned.title()

    def _extract_municipalities(
        self,
        municipality_raw: str | None,
        other_locations_raw: str | None,
    ) -> list[str]:
        municipalities: list[str] = []

        primary = self._clean_municipality(municipality_raw)

        if primary:
            municipalities.append(primary)

        for municipality in self._extract_municipalities_from_other_locations(other_locations_raw):
            if municipality not in municipalities:
                municipalities.append(municipality)

        return municipalities[:15]

    def _extract_municipalities_from_other_locations(self, value: str | None) -> list[str]:
        if not value:
            return []

        text = self._clean_text(value) or ""

        if not text:
            return []

        results: list[str] = []

        pattern = (
            r"prov\.?\s+[A-ZÀ-ÚA-Za-zà-ú'`\- ]+\s*:\s*"
            r"(.+?)(?=(?:,\s*)?prov\.?\s+[A-ZÀ-ÚA-Za-zà-ú'`\- ]+\s*:|$)"
        )

        matches = list(re.finditer(pattern, text, flags=re.IGNORECASE))

        if matches:
            for match in matches:
                chunk = match.group(1)

                for part in self._split_municipality_list(chunk):
                    cleaned = self._clean_municipality(part)

                    if cleaned and cleaned not in results:
                        results.append(cleaned)

            return results

        for part in self._split_municipality_list(text):
            cleaned = self._clean_municipality(part)

            if cleaned and cleaned not in results:
                results.append(cleaned)

        return results

    def _extract_municipalities_from_title(self, title: str | None) -> list[str]:
        if not title:
            return []

        text = self._clean_text(title) or ""

        if not text:
            return []

        results: list[str] = []

        patterns = [
            r"\bnel\s+comune\s+di\s+(.+?)(?:\s+con\s+opere|\s+e\s+opere|\s+denominato|\s+della\s+potenza|\.|$)",
            r"\bnei\s+comuni\s+di\s+(.+?)(?:\s+con\s+opere|\s+e\s+opere|\s+denominato|\s+della\s+potenza|\.|$)",
            r"\bnel\s+comune\s+di\s+(.+?)(?:\s*\([A-Z]{2}\)|,|\.|$)",
            r"\bnei\s+comuni\s+di\s+(.+?)(?:\s*\([A-Z]{2}\)|\.|$)",
            r"\blocalizzato\s+nel\s+comune\s+di\s+(.+?)(?:\s*\([A-Z]{2}\)|,|\.|$)",
            r"\blocalizzato\s+nei\s+comuni\s+di\s+(.+?)(?:\s*\([A-Z]{2}\)|\.|$)",
            r"\bopere\s+di\s+connessione\s+nei\s+comuni\s+di\s+(.+?)(?:\.|$)",
            r"\bopere\s+connesse\s+nei\s+comuni\s+di\s+(.+?)(?:\.|$)",
            r"\bopere\s+connesse\s+nel\s+comune\s+di\s+(.+?)(?:\.|$)",
            r"\bopere\s+di\s+connessione\s+nel\s+comune\s+di\s+(.+?)(?:\.|$)",
        ]

        for pattern in patterns:
            for match in re.finditer(pattern, text, flags=re.IGNORECASE):
                raw = match.group(1)

                raw = re.sub(r"\([A-Z]{2}\)", "", raw)

                raw = re.split(
                    r"\s+(?:con|e\s+relative|relative|denominato|della\s+potenza|di\s+potenza|avente)\b",
                    raw,
                    maxsplit=1,
                    flags=re.IGNORECASE,
                )[0]

                for part in self._split_municipality_list(raw):
                    cleaned = self._clean_municipality(part)

                    if cleaned and cleaned not in results:
                        results.append(cleaned)

        return results[:15]

    def _split_municipality_list(self, value: str) -> list[str]:
        value = value.replace(";", ",")
        value = re.sub(r"\s+ed\s+", ",", value, flags=re.IGNORECASE)
        value = re.sub(r"\s+e\s+", ",", value, flags=re.IGNORECASE)
        return [part.strip() for part in value.split(",") if part.strip()]

    def _clean_municipality(self, value: str | None) -> str | None:
        if not value:
            return None

        cleaned = self._clean_text(value) or ""

        if not cleaned:
            return None

        cleaned = re.sub(r"\([A-Z]{2}\)", "", cleaned)

        cleaned = re.split(
            r"\s+nelle?\s+provin(?:ce|cie)\s+di\b",
            cleaned,
            maxsplit=1,
            flags=re.IGNORECASE,
        )[0]

        cleaned = re.split(
            r"\s+in\s+provin(?:cia|ce|cie)\s+di\b",
            cleaned,
            maxsplit=1,
            flags=re.IGNORECASE,
        )[0]

        cleaned = re.sub(r"\bprov\.?\b.*$", "", cleaned, flags=re.IGNORECASE)
        cleaned = cleaned.strip(" .,:;-()")

        if not cleaned:
            return None

        if len(cleaned) > 80:
            return None

        bad_fragments = [
            "protocollo",
            "pubblicazioni",
            "documenti",
            "progetto",
            "impianto",
            "potenza",
            "stato",
            "procedura",
            "localizzazione",
            "provincia",
            "province",
            "provincie",
            "città metropolitana",
            "citta metropolitana",
            "osservazioni",
            "opere",
            "connessione",
            "rtn",
            "linea",
        ]

        lowered = cleaned.lower()

        if any(fragment in lowered for fragment in bad_fragments):
            return None

        return cleaned.title()

    def _extract_power(self, text: str | None) -> str | None:
        if not text:
            return None

        number_unit = (
            r"([0-9]+(?:[.\s][0-9]{3})*(?:[,\.][0-9]+)?|[0-9]+(?:[,\.][0-9]+)?)"
            r"\s*(MWP|MW|KWP|KW)"
        )

        preferred_patterns = [
            rf"potenza\s+nominale\s+in\s+dc\s+di\s+{number_unit}",
            rf"potenza\s+di\s+picco\s+pari\s+a\s+{number_unit}",
            rf"potenza\s+installata\s+di\s+{number_unit}",
            rf"potenza\s+complessiva\s+(?:di\s+)?{number_unit}",
            rf"potenza\s+pari\s+a\s+{number_unit}",
            rf"\bdi\s+potenza\s+{number_unit}",
            rf"\bpotenza\s+{number_unit}",
            rf"\bda\s+{number_unit}",
        ]

        for pattern in preferred_patterns:
            for match in re.finditer(pattern, text, flags=re.IGNORECASE):
                start = max(0, match.start() - 80)
                end = min(len(text), match.end() + 80)
                context = text[start:end].lower()
                before = text[start:match.start()].lower()

                is_storage_before = any(
                    word in before
                    for word in [
                        "bess",
                        "accumulo",
                        "sistema di accumulo",
                        "capacità di accumulo",
                        "capacita di accumulo",
                        "storage",
                        "batteria",
                        "batterie",
                    ]
                )

                is_plant_context = any(
                    word in context
                    for word in [
                        "impianto agrivoltaico",
                        "impianto fotovoltaico",
                        "solare agrivoltaico",
                        "progetto di un impianto",
                        "realizzazione di un impianto",
                    ]
                )

                if is_storage_before and not is_plant_context:
                    continue

                return f"{match.group(1)} {match.group(2)}"

        generic_pattern = number_unit
        candidates: list[dict[str, str | bool]] = []

        for match in re.finditer(generic_pattern, text, flags=re.IGNORECASE):
            start = max(0, match.start() - 100)
            end = min(len(text), match.end() + 100)

            before = text[start:match.start()].lower()
            context = text[start:end].lower()

            is_storage = any(
                word in before
                for word in [
                    "bess",
                    "accumulo",
                    "sistema di accumulo",
                    "capacità di accumulo",
                    "capacita di accumulo",
                    "storage",
                    "batteria",
                    "batterie",
                ]
            )

            is_plant_context = any(
                word in context
                for word in [
                    "impianto agrivoltaico",
                    "impianto fotovoltaico",
                    "solare agrivoltaico",
                    "progetto di un impianto",
                    "realizzazione di un impianto",
                ]
            )

            candidates.append(
                {
                    "value": f"{match.group(1)} {match.group(2)}",
                    "is_storage": bool(is_storage and not is_plant_context),
                }
            )

        for candidate in candidates:
            if not candidate["is_storage"]:
                return str(candidate["value"])

        if candidates:
            return str(candidates[0]["value"])

        return None

    def _build_external_id(self, detail_url: str) -> str:
        match = re.search(r"/dettaglio/(\d+)", detail_url)

        if match:
            return f"emilia_romagna_{match.group(1)}"

        cleaned = re.sub(r"\s+", "-", detail_url.lower())
        cleaned = re.sub(r"[^a-z0-9:/._-]", "", cleaned)
        return f"emilia_romagna_{cleaned}"[:250]

    def _looks_like_label(self, value: str) -> bool:
        normalized = self._normalize_label(value)
        return normalized in STOP_LABELS

    def _normalize_label(self, value: str | None) -> str:
        value = self._clean_text(value or "") or ""
        value = value.lower()
        value = value.replace("à", "a")
        value = value.replace("è", "e")
        value = value.replace("é", "e")
        value = value.replace("ì", "i")
        value = value.replace("ò", "o")
        value = value.replace("ù", "u")
        value = value.replace("città", "citta")
        value = re.sub(r"[^a-z0-9/ ]+", " ", value)
        value = " ".join(value.split())
        return value

    def _normalize_key(self, value: str | None) -> str:
        value = self._clean_text(value or "") or ""
        value = value.lower()
        value = value.replace("à", "a")
        value = value.replace("è", "e")
        value = value.replace("é", "e")
        value = value.replace("ì", "i")
        value = value.replace("ò", "o")
        value = value.replace("ù", "u")
        value = re.sub(r"[^a-z0-9]+", "_", value)
        return value.strip("_")

    def _clean_text(self, value: str | None) -> str:
        return " ".join((value or "").replace("\xa0", " ").split()).strip()


if __name__ == "__main__":
    collector = EmiliaRomagnaCollector()
    items = collector.fetch()

    print(f"items: {len(items)}")

    for item in items[:40]:
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
            item.source_url,
        )