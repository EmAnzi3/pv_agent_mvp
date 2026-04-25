from __future__ import annotations

import html
import re
from datetime import datetime
from urllib.parse import urljoin

from bs4 import BeautifulSoup

from app.collectors.base import BaseCollector, CollectorResult


BASE_URL = "http://viavas.regione.campania.it"

SEARCH_URL = (
    "http://viavas.regione.campania.it/"
    "opencms/opencms/VIAVAS/VIA_files_new/Ricerca_Avanzata.html"
)

MIN_DATE = datetime(2025, 1, 1)

SEARCH_KEYWORDS = [
    "fotovoltaico",
    "agrivoltaico",
    "agrovoltaico",
]

PV_KEYWORDS = [
    "fotovoltaico",
    "fotovoltaica",
    "agrivoltaico",
    "agrovoltaico",
    "agrofotovoltaico",
    "agro-fotovoltaico",
    "solare fotovoltaico",
    "impianto fotovoltaico",
    "impianto agrivoltaico",
    "impianto agrovoltaico",
]

NON_PV_EXCLUDE = [
    "eolico",
    "eolica",
    "rifiuti",
    "discarica",
    "depuratore",
    "depurazione",
    "cava",
    "estrattiva",
    "idraulico",
    "idraulica",
    "stradale",
    "ferroviaria",
]


class CampaniaCollector(BaseCollector):
    source_name = "campania"
    base_url = SEARCH_URL

    def fetch(self) -> list[CollectorResult]:
        results: list[CollectorResult] = []
        seen: set[str] = set()

        for keyword in SEARCH_KEYWORDS:
            try:
                html_page = self._post_search(keyword)
            except Exception:
                continue

            rows = self._parse_results_table(html_page, SEARCH_URL)

            for row in rows:
                normalized = self._normalize_row(row)

                if not normalized:
                    continue

                external_id = normalized["external_id"]

                if external_id in seen:
                    continue

                seen.add(external_id)

                results.append(
                    CollectorResult(
                        external_id=external_id,
                        source_url=normalized["source_url"],
                        title=normalized["title"][:250],
                        payload={
                            "title": normalized["title"][:500],
                            "proponent": normalized.get("proponent"),
                            "status_raw": normalized.get("status_raw"),
                            "region": "Campania",
                            "province": normalized.get("province"),
                            "municipalities": normalized.get("municipalities") or [],
                            "power": normalized.get("power"),
                            "project_type_hint": normalized.get("procedure") or "Campania VIA/PAUR",
                            "cup": normalized.get("cup"),
                            "date_presented": normalized.get("date_presented"),
                            "decree": normalized.get("decree"),
                        },
                    )
                )

        return results

    # ------------------------------------------------------------------
    # HTTP / FORM
    # ------------------------------------------------------------------

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
            "User-Agent": "PV-Agent-MVP/0.1",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "it-IT,it;q=0.9,en-US;q=0.8,en;q=0.7",
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
            location = response.headers.get("Location", "")
            redirected_url = urljoin(SEARCH_URL, location)

            if "www.regione.campania.it" in redirected_url:
                raise RuntimeError(
                    f"Redirect errato intercettato: {SEARCH_URL} -> {redirected_url}"
                )

            response = self.session.get(
                redirected_url,
                headers=headers,
                timeout=90,
                allow_redirects=False,
            )

        response.raise_for_status()
        return response.content.decode("utf-8", errors="replace")

    # ------------------------------------------------------------------
    # PARSING
    # ------------------------------------------------------------------

    def _parse_results_table(self, html_page: str, page_url: str) -> list[dict]:
        soup = BeautifulSoup(html_page, "html.parser")
        rows: list[dict] = []

        for table in soup.find_all("table"):
            table_text = self._clean_text(table.get_text(" ", strip=True))
            table_norm = self._normalize_for_match(table_text)

            if "data di presentazione" not in table_norm:
                continue

            if "cup" not in table_norm:
                continue

            if "proponente" not in table_norm:
                continue

            headers: list[str] = []

            for tr in table.find_all("tr"):
                cells = tr.find_all(["th", "td"])
                values = [self._clean_text(cell.get_text(" ", strip=True)) for cell in cells]

                if not values:
                    continue

                norm_values = [self._normalize_header(value) for value in values]

                if (
                    "data_di_presentazione" in norm_values
                    and "cup" in norm_values
                    and "proponente" in norm_values
                ):
                    headers = norm_values
                    continue

                if not headers:
                    continue

                if len(values) < 4:
                    continue

                record: dict = {}

                for idx, header in enumerate(headers):
                    if idx < len(values):
                        record[header] = values[idx]
                    else:
                        record[header] = ""

                link_url = self._extract_first_project_url(tr, page_url)
                if link_url:
                    record["source_url"] = link_url
                else:
                    record["source_url"] = page_url

                record["raw_text"] = self._clean_text(" | ".join(values))

                rows.append(record)

        return rows

    def _normalize_row(self, row: dict) -> dict | None:
        raw_text = row.get("raw_text") or ""
        project = row.get("progetto") or ""
        proponent = row.get("proponente") or ""
        cup = row.get("cup") or ""
        date_text = row.get("data_di_presentazione") or ""
        territory = row.get("territori") or ""
        status = row.get("esito") or ""
        decree = row.get("decreto") or ""
        source_url = row.get("source_url") or SEARCH_URL

        full_text = f"{project} {proponent} {raw_text}"

        if not self._is_pv_related(full_text):
            return None

        parsed_date = self._parse_date(date_text)
        if parsed_date is None:
            return None

        if parsed_date < MIN_DATE:
            return None

        title = self._clean_text(project)
        if not title:
            return None

        municipalities = self._extract_municipalities(territory, title)
        province = self._extract_province(title)
        power = self._extract_power_text(title)
        procedure = self._extract_procedure(title)

        external_id = self._build_external_id(
            cup=cup,
            date_text=date_text,
            title=title,
            proponent=proponent,
            municipality=", ".join(municipalities),
        )

        return {
            "external_id": external_id,
            "source_url": source_url,
            "title": title,
            "proponent": proponent or None,
            "status_raw": status or None,
            "province": province,
            "municipalities": municipalities,
            "power": power,
            "procedure": procedure,
            "cup": cup or None,
            "date_presented": date_text or None,
            "decree": decree or None,
        }

    # ------------------------------------------------------------------
    # EXTRACTION HELPERS
    # ------------------------------------------------------------------

    def _is_pv_related(self, text: str) -> bool:
        norm = f" {self._normalize_for_match(text)} "

        has_pv = any(keyword in norm for keyword in PV_KEYWORDS)

        if not has_pv:
            return False

        has_strong_pv = any(
            keyword in norm
            for keyword in [
                "fotovoltaico",
                "fotovoltaica",
                "agrivoltaico",
                "agrovoltaico",
                "agrofotovoltaico",
                "agro-fotovoltaico",
            ]
        )

        if not has_strong_pv and any(exclude in norm for exclude in NON_PV_EXCLUDE):
            return False

        return True

    def _parse_date(self, value: str | None) -> datetime | None:
        value = self._clean_text(value or "")

        if not value:
            return None

        for fmt in ("%d/%m/%Y", "%d-%m-%Y", "%Y-%m-%d"):
            try:
                return datetime.strptime(value, fmt)
            except ValueError:
                pass

        return None

    def _extract_first_project_url(self, node, page_url: str) -> str | None:
        for a in node.find_all("a", href=True):
            href = a.get("href") or ""

            if not href:
                continue

            absolute = urljoin(page_url, href)

            if "/Progetti/" in absolute or absolute.endswith(".via") or absolute.endswith(".viavi"):
                return absolute

        return None

    def _extract_municipalities(self, territory: str | None, title: str | None) -> list[str]:
        values: list[str] = []

        territory = self._clean_text(territory or "")
        title = self._clean_text(title or "")

        if territory:
            for part in re.split(r"[,;/]+|\s+ e \s+", territory, flags=re.IGNORECASE):
                cleaned = self._clean_municipality(part)
                if cleaned and cleaned not in values:
                    values.append(cleaned)

        if values:
            return values

        patterns = [
            r"Comune di\s+([A-ZÀ-Ú][A-Za-zÀ-Úà-ú'`\- ]+?)(?:\s*\([A-Z]{2}\)|,|\.|;|$)",
            r"Comuni di\s+([A-ZÀ-Ú][A-Za-zÀ-Úà-ú'`\- ,]+?)(?:\s*\([A-Z]{2}\)|\.|;|$)",
            r"nel Comune di\s+([A-ZÀ-Ú][A-Za-zÀ-Úà-ú'`\- ]+?)(?:\s*\([A-Z]{2}\)|,|\.|;|$)",
            r"nei Comuni di\s+([A-ZÀ-Ú][A-Za-zÀ-Úà-ú'`\- ,]+?)(?:\s*\([A-Z]{2}\)|\.|;|$)",
        ]

        for pattern in patterns:
            match = re.search(pattern, title, flags=re.IGNORECASE)
            if not match:
                continue

            raw = match.group(1)

            for part in re.split(r",|\s+e\s+", raw, flags=re.IGNORECASE):
                cleaned = self._clean_municipality(part)
                if cleaned and cleaned not in values:
                    values.append(cleaned)

        return values

    def _clean_municipality(self, value: str | None) -> str | None:
        value = self._clean_text(value or "")
        value = value.strip(" .:-,;()")

        if not value:
            return None

        if len(value) > 80:
            return None

        bad_words = [
            "provincia",
            "potenza",
            "impianto",
            "opere",
            "connessione",
            "ubicato",
            "ubicata",
            "localita",
            "località",
            "loc",
            "loc.",
            "catasto",
            "foglio",
            "particella",
            "mwp",
            "mw",
            "kwp",
            "kvac",
        ]

        lowered = value.lower()

        if any(word in lowered for word in bad_words):
            return None

        return value.upper()

    def _extract_province(self, text: str | None) -> str | None:
        text = text or ""

        match = re.search(r"\(([A-Z]{2})\)", text)
        if match:
            return match.group(1)

        provinces = {
            "AVELLINO": "AV",
            "BENEVENTO": "BN",
            "CASERTA": "CE",
            "NAPOLI": "NA",
            "SALERNO": "SA",
        }

        upper = text.upper()

        for province_name, code in provinces.items():
            if province_name in upper:
                return code

        return None

    def _extract_power_text(self, text: str | None) -> str | None:
        if not text:
            return None

        match = re.search(
            r"(?<![\d.,'’])"
            r"("
            r"(?:\d{1,3}(?:[.\s'’]\d{3})+(?:[,.]\d+)?)"
            r"|"
            r"(?:\d+[.,]\d+)"
            r"|"
            r"(?:\d+)"
            r")"
            r"\s*"
            r"(MWp|MW|MVA|MWh|kWp|KWp|kW|KW|kwp|kw|kVAC|KVAC)"
            r"\b",
            text,
            flags=re.IGNORECASE,
        )

        if not match:
            return None

        return f"{match.group(1)} {match.group(2)}"

    def _extract_procedure(self, text: str | None) -> str | None:
        norm = self._normalize_for_match(text or "")

        if "paur" in norm:
            return "PAUR"

        if "verifica" in norm:
            return "VERIFICA"

        if "valutazione impatto ambientale" in norm or " via " in f" {norm} ":
            return "VIA"

        return "Campania VIA/PAUR"

    def _build_external_id(
        self,
        cup: str | None,
        date_text: str | None,
        title: str,
        proponent: str | None,
        municipality: str | None,
    ) -> str:
        base = f"campania|{cup or ''}|{date_text or ''}|{title}|{proponent or ''}|{municipality or ''}"
        base = self._normalize_for_match(base)
        base = re.sub(r"\s+", "-", base)
        base = re.sub(r"[^a-z0-9:/._|-]", "", base)
        return base[:250]

    # ------------------------------------------------------------------
    # TEXT HELPERS
    # ------------------------------------------------------------------

    def _normalize_header(self, value: str) -> str:
        value = self._normalize_for_match(value)
        value = re.sub(r"[^a-z0-9]+", "_", value)
        value = value.strip("_")
        return value

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