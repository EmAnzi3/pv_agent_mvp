from __future__ import annotations

import csv
import hashlib
import html
import io
import json
import re
import unicodedata
from pathlib import Path
from urllib.parse import parse_qs, urljoin, urlparse

try:
    from bs4 import BeautifulSoup
except Exception:  # pragma: no cover - fallback solo se bs4 non è installato
    BeautifulSoup = None

from app.collectors.base import BaseCollector, CollectorResult


BASE_URL = "https://si-vvi.regione.sicilia.it/viavas/"

LIST_URL = (
    "https://si-vvi.regione.sicilia.it/viavas/index.php/it/component/fabrik/list/30"
    "?Itemid=332&resetfilters=1&fabrik_incsessionfilters=0"
)

# Questo è l'endpoint open-data che in passato ha restituito davvero il CSV completo.
# Le URL Fabrik restano come fallback, perché il portale Sicilia è ballerino: oggi CSV,
# domani HTML vuoto, dopodomani decide di fare teatro sperimentale.
OPEN_DATA_CSV_URL = (
    "https://dati.regione.sicilia.it/download/dataset/"
    "progetti-sottoposti-valutazione-ambientale/filesystem/"
    "progetti-sottoposti-valutazione-ambientale_csv.csv"
)

CSV_URL_CANDIDATES = [
    OPEN_DATA_CSV_URL,
    (
        "https://si-vvi.regione.sicilia.it/viavas/index.php?"
        "option=com_fabrik&view=list&listid=30&format=csv&Itemid=332"
        "&resetfilters=1&fabrik_incsessionfilters=0"
    ),
    (
        "https://si-vvi.regione.sicilia.it/viavas/index.php?"
        "option=com_fabrik&view=list&listid=30&format=raw&Itemid=332"
        "&resetfilters=1&fabrik_incsessionfilters=0"
    ),
    (
        "https://si-vvi.regione.sicilia.it/viavas/index.php/it/component/fabrik/list/30"
        "?Itemid=332&format=csv&resetfilters=1&fabrik_incsessionfilters=0"
    ),
    (
        "https://si-vvi.regione.sicilia.it/viavas/index.php/it/component/fabrik/list/30"
        "?Itemid=332&format=raw&resetfilters=1&fabrik_incsessionfilters=0"
    ),
    (
        "https://si-vvi.regione.sicilia.it/viavas/index.php/it/component/fabrik/list/30"
        "?format=csv&Itemid=332&resetfilters=1&fabrik_incsessionfilters=0"
    ),
]

CSV_URL = CSV_URL_CANDIDATES[0]

DETAIL_URL_TEMPLATE = (
    "https://si-vvi.regione.sicilia.it/viavas/index.php/it/component/fabrik/list/30"
    "?Itemid=332&procedura___oggetto_raw={raw_id}"
    "&limitstart30=0&resetfilters=1&fabrik_incsessionfilters=0"
)

COMMERCIAL_PV_KEYWORDS = [
    "fotovoltaico",
    "fotovoltaica",
    "agro fotovoltaico",
    "agro fotovoltaica",
    "agro-fotovoltaico",
    "agro-fotovoltaica",
    "agrivoltaico",
    "agrivoltaica",
    "agrovoltaico",
    "agrovoltaica",
    "parco fotovoltaico",
    "centrale fotovoltaica",
    "campo fotovoltaico",
    "impianto fv",
    "impianti fv",
    "impianto pv",
    "fv ",
    " pv ",
    "fv-",
]

BESS_KEYWORDS = [
    "storage",
    "accumulo",
    "bess",
    "energy storage",
    "sistema di accumulo",
    "accumulo elettrochimico",
]

# Esclusioni solo per casi chiaramente non pertinenti al monitoraggio di grandi impianti FV.
# Non uso una lista troppo aggressiva: in Sicilia ci sono molti agrivoltaici con opere accessorie
# e un filtro troppo severo taglia fuori roba buona.
NON_PV_EXCLUDE = [
    "parco eolico",
    "impianto eolico",
    "impianti eolici",
    "imboschimento",
    "miniera",
    "acque minerali",
    "allevamento avicolo",
]

SECONDARY_PV_EXCLUDE = [
    "ripristino dell'impianto fotovoltaico esistente",
    "ripristino impianto fotovoltaico esistente",
    "potenziamento dell'impianto fotovoltaico esistente",
    "ammodernamento dell'impianto fotovoltaico esistente",
    "revamping dell'impianto fotovoltaico esistente",
]

SICILY_PROVINCES = {"AG", "CL", "CT", "EN", "ME", "PA", "RG", "SR", "TP"}


class SiciliaCollector(BaseCollector):
    source_name = "sicilia"
    base_url = BASE_URL

    def fetch(self) -> list[CollectorResult]:
        debug_base = Path("/app/reports/debug_sicilia")
        debug_base.mkdir(parents=True, exist_ok=True)

        text, used_url, rows = self._download_rows(debug_base)

        if not rows:
            self._write_json(
                debug_base / "rows_empty.json",
                {
                    "note": "Nessuna riga letta dalle sorgenti Sicilia",
                    "used_url": used_url,
                    "text_preview": text[:2000] if text else None,
                },
            )
            return []

        self._write_text(debug_base / "sicilia_raw.csv", text[:500000])
        self._write_json(debug_base / "columns.json", list(rows[0].keys()) if rows else [])
        self._write_json(debug_base / "sample_rows.json", rows[:20])

        # Il CSV/HTML Sicilia può contenere la stessa pratica più volte.
        # Qui deduplichiamo prima di consegnare i record alla pipeline: una pratica = un raw_id/detail_url.
        matched_rows: list[dict] = []
        excluded_rows: list[dict] = []
        best_by_source_key: dict[str, dict] = {}

        for row in rows:
            normalized = self._normalize_row(row)
            if not normalized:
                continue

            title = normalized["title"]

            if not self._is_commercial_pv_project(title):
                if self._contains_any(title, COMMERCIAL_PV_KEYWORDS + BESS_KEYWORDS):
                    excluded_rows.append(row)
                continue

            matched_rows.append(row)

            source_key = self._source_key(normalized)
            previous = best_by_source_key.get(source_key)
            if previous is None or self._row_quality_score(normalized) > self._row_quality_score(previous):
                best_by_source_key[source_key] = normalized

        results: list[CollectorResult] = []

        for normalized in best_by_source_key.values():
            external_id = self._build_external_id(normalized)[:250]

            results.append(
                CollectorResult(
                    external_id=external_id,
                    source_url=normalized.get("detail_url") or used_url or LIST_URL,
                    title=normalized["title"][:250],
                    payload={
                        "title": normalized["title"][:500],
                        "project_name": normalized["title"][:500],
                        "proponent": normalized.get("proponent"),
                        "status_raw": normalized.get("status_raw"),
                        "region": "Sicilia",
                        "province": normalized.get("province"),
                        "municipalities": normalized.get("municipalities") or [],
                        "power": normalized.get("power"),
                        "procedure": normalized.get("procedure"),
                        "project_type_hint": normalized.get("procedure") or "Sicilia VIA/VAS",
                        "codice": normalized.get("codice"),
                        "raw_id": normalized.get("raw_id"),
                    },
                )
            )

        self._write_json(debug_base / "matched_rows_sample.json", matched_rows[:80])
        self._write_json(debug_base / "excluded_rows_sample.json", excluded_rows[:80])
        self._write_json(
            debug_base / "summary.json",
            {
                "used_url": used_url,
                "rows_total": len(rows),
                "matched_rows": len(matched_rows),
                "excluded_pv_like_rows": len(excluded_rows),
                "results": len(results),
            },
        )

        return results

    def _download_rows(self, debug_base: Path) -> tuple[str, str, list[dict]]:
        attempts: list[dict] = []

        for url in CSV_URL_CANDIDATES:
            try:
                response = self.session.get(url, timeout=90)
                response.raise_for_status()

                text = response.content.decode("utf-8-sig", errors="replace")
                rows = self._read_csv(text)
                first_keys = list(rows[0].keys()) if rows else []
                valid = self._looks_like_sicilia_rows(rows)

                attempts.append(
                    {
                        "url": url,
                        "status": response.status_code,
                        "content_type": response.headers.get("content-type"),
                        "bytes": len(response.content or b""),
                        "rows": len(rows),
                        "first_keys": first_keys[:40],
                        "valid": valid,
                    }
                )

                if valid:
                    self._write_json(debug_base / "download_attempts.json", attempts)
                    return text, url, rows

            except Exception as exc:
                attempts.append({"url": url, "error": str(exc)})

        # Ultima spiaggia: se l'export CSV non restituisce righe, prova a leggere la tabella HTML.
        html_text, html_rows = self._download_html_rows(debug_base)
        attempts.append(
            {
                "url": LIST_URL,
                "fallback": "html_table",
                "rows": len(html_rows),
                "valid": self._looks_like_sicilia_rows(html_rows),
            }
        )

        self._write_json(debug_base / "download_attempts.json", attempts)

        if self._looks_like_sicilia_rows(html_rows):
            return html_text, LIST_URL, html_rows

        return "", CSV_URL, []

    def _download_html_rows(self, debug_base: Path) -> tuple[str, list[dict]]:
        if BeautifulSoup is None:
            return "", []

        try:
            response = self.session.get(LIST_URL, timeout=90)
            response.raise_for_status()
            text = response.content.decode("utf-8-sig", errors="replace")
        except Exception as exc:
            self._write_text(debug_base / "html_download_error.txt", str(exc))
            return "", []

        self._write_text(debug_base / "sicilia_list_page.html", text[:1000000])

        soup = BeautifulSoup(text, "html.parser")
        tables = soup.find_all("table")
        best_rows: list[dict] = []

        for table in tables:
            headers = [self._normalize_header(th.get_text(" ", strip=True)) for th in table.find_all("th")]
            headers = [h for h in headers if h]

            if not headers:
                first_row = table.find("tr")
                if first_row:
                    headers = [
                        self._normalize_header(cell.get_text(" ", strip=True))
                        for cell in first_row.find_all(["td", "th"])
                    ]
                    headers = [h for h in headers if h]

            rows: list[dict] = []
            for tr in table.find_all("tr"):
                cells = tr.find_all("td")
                if not cells:
                    continue

                row: dict[str, str] = {}
                for idx, cell in enumerate(cells):
                    key = headers[idx] if idx < len(headers) and headers[idx] else f"col_{idx}"
                    value = cell.get_text(" ", strip=True)
                    row[key] = self._clean_text(value)

                    link = cell.find("a", href=True)
                    if link:
                        href = urljoin(BASE_URL, link.get("href"))
                        if "fabrik" in href or "procedura" in href:
                            row.setdefault("procedura_url", href)
                            raw_id = self._extract_raw_id(href)
                            if raw_id:
                                row.setdefault("procedura___oggetto_raw", raw_id)

                if any(v for v in row.values()):
                    rows.append(self._coerce_html_row(row))

            if len(rows) > len(best_rows):
                best_rows = rows

        self._write_json(debug_base / "html_rows_sample.json", best_rows[:30])
        return text, best_rows

    def _coerce_html_row(self, row: dict) -> dict:
        """
        Traduce intestazioni HTML variabili in chiavi coerenti con il CSV.
        """
        out = dict(row)

        title = self._get_first_by_contains(row, ["oggetto", "progetto", "intervento", "descrizione"])
        proponent = self._get_first_by_contains(row, ["proponente", "ditta", "richiedente", "societa", "societa_proponente"])
        procedure = self._get_first_by_contains(row, ["tipologia", "procedura", "tipo"])
        status = self._get_first_by_contains(row, ["stato", "status"])
        code = self._get_first_by_contains(row, ["codice", "id"])

        if title:
            out.setdefault("procedura_progetto_oggetto", title)
        if proponent:
            out.setdefault("proponente_progetto", proponent)
        if procedure:
            out.setdefault("procedura_tipologia", procedure)
        if status:
            out.setdefault("procedura_stato", status)
        if code:
            out.setdefault("procedura_codice", code)

        return out

    def _read_csv(self, text: str) -> list[dict]:
        if not text:
            return []

        best_rows: list[dict] = []

        for delimiter in (";", ",", "\t", "|"):
            try:
                reader = csv.DictReader(io.StringIO(text), delimiter=delimiter)
                rows: list[dict] = []

                for raw_row in reader:
                    row: dict[str, str] = {}

                    for key, value in raw_row.items():
                        if key is None:
                            continue

                        norm_key = self._normalize_header(str(key))
                        if not norm_key:
                            continue

                        row[norm_key] = self._clean_text(value or "")

                    if any(str(v).strip() for v in row.values()):
                        rows.append(row)

                if not rows:
                    continue

                if self._looks_like_sicilia_rows(rows):
                    return rows

                if len(rows) > len(best_rows):
                    best_rows = rows

            except Exception:
                continue

        return best_rows

    def _looks_like_sicilia_rows(self, rows: list[dict]) -> bool:
        if not rows:
            return False

        keys: set[str] = set()
        for row in rows[:10]:
            keys.update(row.keys())

        if "procedura_progetto_oggetto" in keys:
            return True

        if "procedura_oggetto" in keys or "oggetto" in keys or "progetto" in keys:
            return True

        if any("procedura" in key and "oggetto" in key for key in keys):
            return True

        return False

    def _normalize_row(self, row: dict) -> dict | None:
        title = self._clean_text(
            self._get_first(
                row,
                [
                    "procedura_progetto_oggetto",
                    "procedura_oggetto",
                    "oggetto",
                    "progetto",
                    "intervento",
                    "descrizione",
                ],
            )
        )

        if not title:
            return None

        codice = self._clean_text(
            self._get_first(
                row,
                [
                    "procedura_codice",
                    "codice",
                    "id",
                    "procedura_id",
                ],
            )
        )

        detail_url = self._clean_text(
            self._get_first(
                row,
                [
                    "procedura_url",
                    "url",
                    "link",
                    "detail_url",
                    "dettaglio",
                ],
            )
        )

        raw_id = self._clean_text(
            self._get_first(
                row,
                [
                    "procedura___oggetto_raw",
                    "procedura_oggetto_raw",
                    "oggetto_raw",
                    "procedura_progetto_oggetto_raw",
                ],
            )
        )

        detail_url = self._clean_detail_url(detail_url)

        if not raw_id and detail_url:
            raw_id = self._extract_raw_id(detail_url) or ""

        if not detail_url and raw_id:
            detail_url = DETAIL_URL_TEMPLATE.format(raw_id=raw_id)

        if detail_url and detail_url.startswith("/"):
            detail_url = urljoin(BASE_URL, detail_url)

        detail_url = self._clean_detail_url(detail_url)

        procedure = self._clean_text(
            self._get_first(
                row,
                [
                    "procedura_tipologia",
                    "tipologia",
                    "tipo_procedura",
                    "procedura_tipo",
                    "procedure",
                ],
            )
        )

        proponent = self._clean_text(
            self._get_first(
                row,
                [
                    "proponente_progetto",
                    "proponente",
                    "soggetto_proponente",
                    "ditta",
                    "richiedente",
                    "societa",
                    "societa_proponente",
                ],
            )
        )

        status_raw = self._extract_status_raw(row)
        municipalities = self._extract_municipalities(title)
        province = self._extract_province(title, municipalities)
        power = self._extract_power_text(title)

        return {
            "codice": codice,
            "raw_id": raw_id,
            "title": title,
            "detail_url": detail_url or LIST_URL,
            "procedure": procedure,
            "proponent": proponent,
            "status_raw": status_raw,
            "municipalities": municipalities,
            "province": province,
            "power": power,
        }

    def _extract_status_raw(self, row: dict) -> str | None:
        preferred_keys = [
            "procedura_stato",
            "stato_procedura",
            "procedura_stato_procedura",
            "stato",
            "status",
            "procedura_status",
            "fase",
        ]

        for key in preferred_keys:
            value = self._clean_text(row.get(key) or "")
            if value:
                return value

        for key, value in row.items():
            key_norm = self._normalize_for_match(str(key))
            if "stato" in key_norm or "status" in key_norm:
                cleaned = self._clean_text(value or "")
                if cleaned:
                    return cleaned

        return None

    def _is_commercial_pv_project(self, title: str) -> bool:
        lowered = f" {self._normalize_for_match(title)} "

        has_pv = any(keyword in lowered for keyword in COMMERCIAL_PV_KEYWORDS)
        has_storage = any(keyword in lowered for keyword in BESS_KEYWORDS)
        has_solar_context = (
            " solare " in lowered
            or " fonte solare " in lowered
            or " energia solare " in lowered
            or " fotovolta" in lowered
            or " agrivolta" in lowered
            or " agrovolta" in lowered
        )

        if not has_pv and not (has_storage and has_solar_context):
            return False

        if any(keyword in lowered for keyword in NON_PV_EXCLUDE):
            return False

        if any(keyword in lowered for keyword in SECONDARY_PV_EXCLUDE):
            strong_new_plant_signal = any(
                keyword in lowered
                for keyword in [
                    "realizzazione impianto fotovoltaico",
                    "realizzazione di un impianto fotovoltaico",
                    "costruzione impianto fotovoltaico",
                    "parco fotovoltaico",
                    "centrale fotovoltaica",
                    "impianto agrivoltaico",
                    "impianto agro fotovoltaico",
                ]
            )
            if not strong_new_plant_signal:
                return False

        return True

    def _extract_municipalities(self, title: str) -> list[str]:
        """
        Estrae i comuni dal titolo.

        Prima strategia: cerca segmenti espliciti tipo "nel Comune di...",
        "nei Comuni di..." e li valida contro l'elenco ufficiale dei comuni
        siciliani. Questo evita falsi positivi tipo "Provincia di Ragusa",
        dove Ragusa è provincia citata nel testo ma non necessariamente comune
        di impianto.

        Seconda strategia: se il testo non ha formule esplicite, fa fallback
        sulla scansione dell'intero titolo.
        """
        title = self._clean_text(title)
        if not title:
            return []

        contextual = self._extract_municipalities_by_context(title)
        if contextual:
            return contextual

        title_norm = f" {self._normalize_for_match(title)} "
        municipalities_data = self._load_sicily_municipalities()

        found: list[tuple[int, int, str]] = []
        seen_norms: set[str] = set()

        for item in municipalities_data:
            name = item["name"]
            name_norm = item["norm"]

            if not name_norm or name_norm in seen_norms:
                continue

            pattern = r"(?<![a-z0-9])" + re.escape(name_norm) + r"(?![a-z0-9])"
            match = re.search(pattern, title_norm)

            if match:
                found.append((match.start(), -len(name_norm), name))
                seen_norms.add(name_norm)

        found.sort()
        return self._dedupe_municipalities([item[2] for item in found])

    def _extract_municipalities_by_context(self, title: str) -> list[str]:
        municipalities_data = self._load_sicily_municipalities()

        context_patterns = [
            r"\b(?:nel|nella|nei|nelle)\s+comuni?\s+di\s+(.{2,280})",
            r"\bcomuni?\s+di\s+(.{2,280})",
            r"\bterritori(?:o)?\s+comunale\s+di\s+(.{2,220})",
            r"\bterritorio\s+dei\s+comuni\s+di\s+(.{2,280})",
        ]

        found: list[str] = []

        for pattern in context_patterns:
            for match in re.finditer(pattern, title, flags=re.IGNORECASE):
                segment = self._trim_municipality_segment(match.group(1))
                matched = self._match_municipalities_in_text(segment, municipalities_data)

                for municipality in matched:
                    municipality_key = self._normalize_for_match(municipality)
                    existing_keys = {self._normalize_for_match(item) for item in found}
                    if municipality_key and municipality_key not in existing_keys:
                        found.append(municipality)

        return self._dedupe_municipalities(found)

    def _trim_municipality_segment(self, segment: str) -> str:
        segment = self._clean_text(segment)
        if not segment:
            return ""

        norm_segment = self._normalize_for_match(segment)

        stop_patterns = [
            r"\bprovincia\s+di\b",
            r"\bprov\s+di\b",
            r"\blocalit[aà]\b",
            r"\bc\s*da\b",
            r"\bcontrada\b",
            r"\bdistinto\b",
            r"\bcatasto\b",
            r"\bfoglio\b",
            r"\bparticell",
            r"\be\s+(?:delle|relative)\s+opere\b",
            r"\bcon\s+relative\s+opere\b",
            r"\bcomprensiv",
            r"\bcollegat",
            r"\bper\s+la\s+connessione\b",
            r"\bpotenza\b",
            r"\bdenominat",
        ]

        cut = len(norm_segment)
        for stop in stop_patterns:
            match = re.search(stop, norm_segment, flags=re.IGNORECASE)
            if match:
                cut = min(cut, match.start())

        norm_segment = norm_segment[:cut].strip()
        return norm_segment[:280]

    def _match_municipalities_in_text(self, text: str, municipalities_data: list[dict]) -> list[str]:
        text_norm = f" {self._normalize_for_match(text)} "
        if not text_norm.strip():
            return []

        found: list[tuple[int, int, str]] = []
        seen_norms: set[str] = set()

        for item in municipalities_data:
            name = item["name"]
            name_norm = item["norm"]

            if not name_norm or name_norm in seen_norms:
                continue

            pattern = r"(?<![a-z0-9])" + re.escape(name_norm) + r"(?![a-z0-9])"
            match = re.search(pattern, text_norm)

            if match:
                found.append((match.start(), -len(name_norm), name))
                seen_norms.add(name_norm)

        found.sort()
        return [item[2] for item in found]

    def _dedupe_municipalities(self, municipalities: list[str]) -> list[str]:
        cleaned: list[str] = []
        seen: set[str] = set()

        for municipality in municipalities:
            municipality = self._clean_text(municipality)
            if not municipality:
                continue

            key = self._normalize_for_match(municipality)

            # Spazzatura prodotta da vecchie regex: "DI AUGUSTA", "DI GELA", ecc.
            if key.startswith("di "):
                key = key[3:].strip()
                municipality = municipality[3:].strip()

            display_name = self._display_municipality_from_norm(key) or municipality

            if key and key not in seen:
                cleaned.append(display_name)
                seen.add(key)

        return cleaned

    def _display_municipality_from_norm(self, norm_name: str) -> str | None:
        for item in self._load_sicily_municipalities():
            if item.get("norm") == norm_name:
                return item.get("name")
        return None

    def _extract_province(self, title: str, municipalities: list[str] | None = None) -> str | None:
        municipalities = municipalities or []
        municipalities_data = self._load_sicily_municipalities()

        province_by_name = {
            item["name"]: item["province"]
            for item in municipalities_data
            if item.get("name") and item.get("province")
        }

        provinces: list[str] = []
        for municipality in municipalities:
            province = province_by_name.get(municipality)
            if province and province not in provinces:
                provinces.append(province)

        if provinces:
            return ", ".join(provinces[:3])

        title_upper = title.upper()
        for match in re.findall(r"\(([A-Z]{2})\)", title_upper):
            if match in SICILY_PROVINCES:
                return match

        loose_matches = re.findall(r"\b(AG|CL|CT|EN|ME|PA|RG|SR|TP)\b", title_upper)
        if loose_matches:
            return loose_matches[0]

        return None

    def _extract_power_text(self, title: str | None) -> str | None:
        if not title:
            return None

        text = self._clean_text(title).upper()

        number = r"(?:\d{1,3}(?:[.\s'’]\d{3})+(?:[,.]\d+)?|\d+(?:[,.]\d+)?)"
        unit = r"(?:MWP|MW|KWP|KW|MVA)"

        patterns = [
            rf"\bPOTENZA(?:\s+\w+){{0,10}}\s+(?:PARI\s+A|DI|DA)?\s*({number})\s*({unit})\b",
            rf"\bDA\s+({number})\s*({unit})\b",
            rf"\b({number})\s*({unit})\b",
        ]

        for pattern in patterns:
            match = re.search(pattern, text, flags=re.IGNORECASE)
            if match:
                value = self._clean_text(match.group(1))
                measure = self._clean_text(match.group(2)).upper()
                return f"{value} {measure}"

        return None

    def _clean_detail_url(self, value: str | None) -> str:
        """Scarta valori che non sono URL o path validi."""
        value = self._clean_text(value or "")
        if not value:
            return ""
        if value.startswith("/"):
            return urljoin(BASE_URL, value)
        if value.startswith("http://") or value.startswith("https://"):
            return value
        return ""

    def _source_key(self, normalized: dict) -> str:
        raw_id = self._clean_text(normalized.get("raw_id") or "")
        if raw_id:
            return f"raw:{raw_id}"

        detail_url = self._clean_text(normalized.get("detail_url") or "")
        if detail_url and detail_url != LIST_URL:
            raw_id_from_url = self._extract_raw_id(detail_url)
            if raw_id_from_url:
                return f"raw:{raw_id_from_url}"
            return f"url:{self._normalize_url_for_key(detail_url)}"

        basis = "|".join(
            [
                self._normalize_for_match(normalized.get("title") or ""),
                self._normalize_for_match(normalized.get("proponent") or ""),
                self._normalize_for_match(normalized.get("procedure") or ""),
                self._normalize_for_match(normalized.get("power") or ""),
            ]
        )
        digest = hashlib.sha1(basis.encode("utf-8")).hexdigest()[:24]
        return f"hash:{digest}"

    def _row_quality_score(self, normalized: dict) -> int:
        score = 0
        if normalized.get("power"):
            score += 40
        if normalized.get("province"):
            score += 25
        if normalized.get("municipalities"):
            score += 25
        if normalized.get("status_raw"):
            score += 5
        if normalized.get("procedure"):
            score += 3
        if normalized.get("proponent"):
            score += 2

        municipalities_text = ", ".join(normalized.get("municipalities") or [])
        municipalities_norm = self._normalize_for_match(municipalities_text)
        if " di " in f" {municipalities_norm} ":
            score -= 3

        return score

    def _build_external_id(self, normalized: dict) -> str:
        """
        ID stabile del record sorgente.

        Prima usavamo titolo/proponente nell'external_id: appena migliorava
        l'estrazione di comune/provincia o cambiava la pulizia del testo, il
        sistema creava un nuovo raw_item invece di aggiornare quello esistente.
        Qui usiamo l'identificativo del portale Sicilia quando disponibile.
        """
        raw_id = self._clean_text(normalized.get("raw_id") or "")
        detail_url = self._clean_text(normalized.get("detail_url") or "")

        if not raw_id and detail_url:
            raw_id = self._extract_raw_id(detail_url) or ""

        if raw_id:
            return f"sicilia:{raw_id}"[:250]

        if detail_url and detail_url != LIST_URL:
            stable_url = self._normalize_url_for_key(detail_url)
            digest = hashlib.sha1(stable_url.encode("utf-8")).hexdigest()[:24]
            return f"sicilia-url:{digest}"

        basis = "|".join(
            [
                self._normalize_for_match(normalized.get("title") or ""),
                self._normalize_for_match(normalized.get("proponent") or ""),
                self._normalize_for_match(normalized.get("procedure") or ""),
                self._normalize_for_match(normalized.get("power") or ""),
            ]
        )
        digest = hashlib.sha1(basis.encode("utf-8")).hexdigest()[:24]
        return f"sicilia-hash:{digest}"

    def _normalize_url_for_key(self, value: str) -> str:
        parsed = urlparse(value)
        query = parse_qs(parsed.query)
        keep_keys = [
            "procedura___oggetto_raw",
            "oggetto_raw",
            "procedura_oggetto_raw",
            "Itemid",
        ]

        parts: list[str] = []
        for key in keep_keys:
            values = query.get(key)
            if values and values[0]:
                parts.append(f"{key}={values[0]}")

        if parts:
            return f"{parsed.scheme}://{parsed.netloc}{parsed.path}?" + "&".join(parts)

        return self._clean_text(value)

    def _extract_raw_id(self, value: str) -> str | None:
        if not value:
            return None

        parsed = urlparse(value)
        query = parse_qs(parsed.query)
        for key in ["procedura___oggetto_raw", "oggetto_raw", "procedura_oggetto_raw"]:
            values = query.get(key)
            if values and values[0]:
                return str(values[0])

        match = re.search(r"procedura___oggetto_raw=([0-9]+)", value)
        if match:
            return match.group(1)

        match = re.search(r"oggetto_raw=([0-9]+)", value)
        if match:
            return match.group(1)

        return None

    def _load_sicily_municipalities(self) -> list[dict]:
        if hasattr(self, "_sicily_municipalities_cache"):
            return self._sicily_municipalities_cache

        data_path = Path(__file__).resolve().parents[1] / "data" / "comuni_italiani.csv"
        municipalities: list[dict] = []

        if data_path.exists():
            with data_path.open("r", encoding="utf-8-sig", newline="") as handle:
                reader = csv.reader(handle)

                for row in reader:
                    if len(row) < 4:
                        continue

                    name = self._clean_text(row[0])
                    province = self._clean_text(row[2]).upper()
                    region = self._clean_text(row[3])

                    if not name or name.lower() == "comune":
                        continue

                    if self._normalize_for_match(region) != "sicilia":
                        continue

                    municipalities.append(
                        {
                            "name": name,
                            "norm": self._normalize_for_match(name),
                            "province": province,
                        }
                    )

        municipalities.sort(key=lambda item: len(item["norm"]), reverse=True)
        self._sicily_municipalities_cache = municipalities
        return municipalities

    def _get_first(self, row: dict, keys: list[str]) -> str:
        for key in keys:
            value = row.get(key)
            if value:
                return str(value)
        return ""

    def _get_first_by_contains(self, row: dict, needles: list[str]) -> str:
        for key, value in row.items():
            key_norm = self._normalize_header(str(key))
            for needle in needles:
                needle_norm = self._normalize_header(needle)
                if needle_norm and needle_norm in key_norm and value:
                    return str(value)
        return ""

    def _contains_any(self, value: str, keywords: list[str]) -> bool:
        normalized = f" {self._normalize_for_match(value)} "
        return any(keyword in normalized for keyword in keywords)

    def _normalize_header(self, value: str) -> str:
        value = self._clean_text(value)
        value = self._strip_accents(value).lower()
        value = re.sub(r"[^a-z0-9_]+", "_", value)
        value = value.strip("_")
        return value

    def _normalize_for_match(self, value: str) -> str:
        value = self._clean_text(value)
        value = self._strip_accents(value).lower()
        value = re.sub(r"[^a-z0-9]+", " ", value)
        value = re.sub(r"\s+", " ", value).strip()
        return value

    def _slug(self, value: str) -> str:
        value = self._normalize_for_match(value)
        value = re.sub(r"\s+", "-", value).strip("-")
        return value

    def _strip_accents(self, value: str) -> str:
        return "".join(
            char
            for char in unicodedata.normalize("NFKD", value)
            if not unicodedata.combining(char)
        )

    def _clean_text(self, value) -> str:
        if value is None:
            return ""

        value = str(value)
        value = html.unescape(value)
        value = value.replace("\ufeff", "")
        value = value.replace("\xa0", " ")
        value = value.replace('\\"', '"')
        value = value.replace("\\'", "'")
        value = value.replace("\\", "")

        if "<" in value and ">" in value:
            value = re.sub(r"<[^>]+>", " ", value)

        value = re.sub(r"\s+", " ", value).strip()
        return value

    def _write_text(self, path: Path, content: str) -> None:
        try:
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text(content, encoding="utf-8")
        except Exception:
            pass

    def _write_json(self, path: Path, payload) -> None:
        try:
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text(
                json.dumps(payload, ensure_ascii=False, indent=2, default=str),
                encoding="utf-8",
            )
        except Exception:
            pass


if __name__ == "__main__":
    collector = SiciliaCollector()
    items = collector.fetch()

    print("items:", len(items))

    for item in items[:50]:
        print(
            f"{item.external_id[:45]} | "
            f"{item.title[:120]} | "
            f"{item.payload.get('province')} | "
            f"{item.payload.get('municipalities')} | "
            f"{item.payload.get('power')} | "
            f"{item.payload.get('status_raw')}"
        )
