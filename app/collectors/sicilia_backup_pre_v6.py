from __future__ import annotations

import csv
import hashlib
import io
import json
import re
import unicodedata
from pathlib import Path
from urllib.parse import urlparse, parse_qs

import requests
from bs4 import BeautifulSoup

from app.collectors.base import BaseCollector, CollectorResult


CSV_URL = (
    "https://dati.regione.sicilia.it/download/dataset/"
    "progetti-sottoposti-valutazione-ambientale/filesystem/"
    "progetti-sottoposti-valutazione-ambientale_csv.csv"
)

SOURCE_URL = "https://si-vvi.regione.sicilia.it/viavas/"

COMMERCIAL_PV_KEYWORDS = [
    "fotovoltaico",
    "agro-fotovoltaico",
    "agrofotovoltaico",
    "agrivoltaico",
    "agrovoltaico",
    "impianto fv",
    "parco fv",
    "fv ",
    " fv",
    "solare fotovoltaico",
]

BESS_KEYWORDS = [
    "accumulo",
    "storage",
    "bess",
]

EXCLUDE_KEYWORDS = [
    "pensilina",
    "pensiline",
    "tettoia",
    "copertura",
    "coperture",
    "fabbricato",
    "edificio",
    "capannone",
    "scuola",
    "ospedale",
    "ripristino dell'impianto fotovoltaico esistente",
    "ripristino dell’impianto fotovoltaico esistente",
]

PROVINCE_CODES = {
    "AG",
    "CL",
    "CT",
    "EN",
    "ME",
    "PA",
    "RG",
    "SR",
    "TP",
}


# Mappa prudente: serve solo come fallback quando il testo espone il comune senza sigla provincia.
# Non deve sostituire l'estrazione esplicita dal dettaglio del portale.
SICILIA_MUNICIPALITY_TO_PROVINCE = {
    # AG
    "agrigento": "AG",
    "bivona": "AG",
    "canicatti": "AG",
    "canicattini bagni": "SR",
    "castrofilippo": "AG",
    "licata": "AG",
    "menfi": "AG",
    "naro": "AG",
    "palma di montechiaro": "AG",
    "racalmuto": "AG",
    "raffadali": "AG",
    "ravanusa": "AG",
    "ribera": "AG",
    "sambuca di sicilia": "AG",
    "sciacca": "AG",

    # CL
    "acquaviva platani": "CL",
    "butera": "CL",
    "caltanissetta": "CL",
    "delia": "CL",
    "gela": "CL",
    "mazzarino": "CL",
    "mussomeli": "CL",
    "niscemi": "CL",
    "riesi": "CL",
    "san cataldo": "CL",
    "sommatino": "CL",
    "villalba": "CL",

    # CT
    "adrano": "CT",
    "belpasso": "CT",
    "biancavilla": "CT",
    "caltagirone": "CT",
    "catania": "CT",
    "licodia eubea": "CT",
    "mazzarrone": "CT",
    "mineo": "CT",
    "misterbianco": "CT",
    "motta sant anastasia": "CT",
    "motta santanastasia": "CT",
    "paterno": "CT",
    "paternò": "CT",
    "ramacca": "CT",
    "raddusa": "CT",
    "randazzo": "CT",
    "scordia": "CT",

    # EN
    "agira": "EN",
    "aidone": "EN",
    "assoro": "EN",
    "bararrafranca": "EN",
    "barotta": "EN",
    "barrafranca": "EN",
    "calascibetta": "EN",
    "centuripe": "EN",
    "enna": "EN",
    "leonforte": "EN",
    "nicosia": "EN",
    "piazza armerina": "EN",
    "regalbuto": "EN",
    "troina": "EN",
    "valguarnera caropepe": "EN",

    # ME
    "barcellona pozzo di gotto": "ME",
    "milazzo": "ME",
    "messina": "ME",
    "patti": "ME",
    "san filippo del mela": "ME",

    # PA
    "bagheria": "PA",
    "bisacquino": "PA",
    "caccamo": "PA",
    "castellana sicula": "PA",
    "cefalu": "PA",
    "corleone": "PA",
    "monreale": "PA",
    "palermo": "PA",
    "petralia sottana": "PA",
    "termini imerese": "PA",
    "ventimiglia di sicilia": "PA",

    # RG
    "acate": "RG",
    "chiaramonte gulfi": "RG",
    "comiso": "RG",
    "ispica": "RG",
    "modica": "RG",
    "pozzallo": "RG",
    "ragusa": "RG",
    "scicli": "RG",
    "vittoria": "RG",

    # SR
    "augusta": "SR",
    "avola": "SR",
    "carlentini": "SR",
    "floridia": "SR",
    "francofonte": "SR",
    "lentini": "SR",
    "melilli": "SR",
    "noto": "SR",
    "pachino": "SR",
    "priolo gargallo": "SR",
    "rosolini": "SR",
    "siracusa": "SR",
    "solarino": "SR",
    "sortino": "SR",

    # TP
    "alcamo": "TP",
    "calatafimi segesta": "TP",
    "campobello di mazara": "TP",
    "campofelice di fitalia": "PA",
    "ciminna": "PA",
    "mezzojuso": "PA",
    "castelvetrano": "TP",
    "castel di iudica": "CT",
    "marsala": "TP",
    "mazara del vallo": "TP",
    "partanna": "TP",
    "salemi": "TP",
    "trapani": "TP",
}


class SiciliaCollector(BaseCollector):
    source_name = "sicilia"
    base_url = SOURCE_URL

    def fetch(self) -> list[CollectorResult]:
        debug_base = Path("/app/reports/debug_sicilia")
        debug_base.mkdir(parents=True, exist_ok=True)

        try:
            response = self.session.get(
                CSV_URL,
                timeout=120,
                headers={"User-Agent": "Mozilla/5.0 pv-agent"},
            )
            response.raise_for_status()
            text = response.content.decode("utf-8-sig", errors="replace")
        except Exception as exc:
            self._write_text(debug_base / "download_error.txt", str(exc))
            return []

        self._write_text(debug_base / "sicilia_raw.csv", text[:800000])

        rows = self._read_csv(text, debug_base)
        if not rows:
            self._write_json(
                debug_base / "rows_empty.json",
                {"note": "Nessuna riga letta dal CSV Sicilia"},
            )
            return []

        self._write_json(debug_base / "sample_rows.json", rows[:20])
        self._write_json(
            debug_base / "columns.json",
            {"columns": list(rows[0].keys()) if rows else []},
        )

        results: list[CollectorResult] = []
        matched_rows: list[dict] = []
        excluded_rows: list[dict] = []
        seen_keys: set[str] = set()

        for row in rows:
            normalized = self._normalize_row(row)
            if not normalized:
                continue

            title = normalized["title"]

            if not self._is_commercial_pv_project(title):
                if self._contains_any(title, COMMERCIAL_PV_KEYWORDS + BESS_KEYWORDS):
                    excluded_rows.append(row)
                continue

            detail_url = normalized.get("detail_url") or CSV_URL
            detail_info = self._fetch_detail_info(detail_url, debug_base)
            normalized = self._merge_detail_info(normalized, detail_info)

            # Dopo l'arricchimento dal dettaglio, il titolo può essere più completo.
            title = normalized["title"]

            status_raw = (
                detail_info.get("status_raw")
                or normalized.get("status_raw")
                or "Conclusa"
            )

            external_id = self._build_external_id(normalized)
            if external_id in seen_keys:
                continue
            seen_keys.add(external_id)

            matched_rows.append(row)

            results.append(
                CollectorResult(
                    external_id=external_id,
                    source_url=detail_url,
                    title=title[:250],
                    payload={
                        "title": title[:900],
                        "project_name": title[:900],
                        "proponent": normalized.get("proponent"),
                        "status_raw": status_raw,
                        "region": "Sicilia",
                        "province": normalized.get("province"),
                        "municipalities": normalized.get("municipalities") or [],
                        "power": normalized.get("power"),
                        "project_type_hint": normalized.get("procedure") or "Sicilia VIA/VAS",
                        "procedure": normalized.get("procedure"),
                        "latitudine": normalized.get("latitudine"),
                        "longitudine": normalized.get("longitudine"),
                    },
                )
            )

        self._write_json(debug_base / "matched_rows_sample.json", matched_rows[:100])
        self._write_json(debug_base / "excluded_rows_sample.json", excluded_rows[:100])
        self._write_json(
            debug_base / "summary.json",
            {
                "used_url": CSV_URL,
                "rows_total": len(rows),
                "matched_rows": len(matched_rows),
                "excluded_pv_like_rows": len(excluded_rows),
                "results": len(results),
            },
        )

        return results

    def _read_csv(self, text: str, debug_base: Path) -> list[dict]:
        """
        Il CSV Sicilia contiene almeno una riga formalmente sporca:
        BARRAFRANCA\\"
        Senza escapechar='\\', Python sposta le colonne e manda l'URL nel titolo.
        """
        try:
            reader = csv.DictReader(
                io.StringIO(text),
                delimiter=";",
                quotechar='"',
                escapechar="\\",
                doublequote=True,
            )

            rows: list[dict] = []

            for row in reader:
                clean_row = {}
                for key, value in row.items():
                    if key is None:
                        continue

                    clean_key = self._normalize_column_name(key)
                    clean_value = self._clean_text(value)

                    clean_row[clean_key] = clean_value

                if clean_row:
                    rows.append(clean_row)

            return rows

        except Exception as exc:
            self._write_text(debug_base / "csv_parse_error.txt", str(exc))
            return []

    def _normalize_row(self, row: dict) -> dict | None:
        title = self._clean_text(
            row.get("procedura_progetto_oggetto")
            or row.get("oggetto")
            or row.get("titolo")
            or ""
        )

        if not title:
            return None

        title = self._repair_title(title)

        codice = self._clean_text(
            row.get("procedura_codice")
            or row.get("codice")
            or ""
        )

        detail_url = self._clean_text(
            row.get("procedura_url")
            or row.get("url")
            or ""
        )

        detail_url = self._repair_url(detail_url, title)

        procedure = self._clean_text(
            row.get("procedura_tipologia")
            or row.get("tipologia")
            or row.get("procedura")
            or ""
        )

        proponent = self._clean_text(
            row.get("proponente_progetto")
            or row.get("proponente")
            or ""
        )

        province = self._extract_province(title)
        municipalities = self._extract_municipalities(title)
        power = self._extract_power_text(title)

        return {
            "codice": codice,
            "title": title,
            "detail_url": detail_url,
            "procedure": procedure,
            "proponent": proponent,
            "municipalities": municipalities,
            "province": province,
            "power": power,
            "latitudine": row.get("latitudine"),
            "longitudine": row.get("longitudine"),
            "status_raw": row.get("stato") or row.get("status"),
        }

    def _repair_title(self, title: str) -> str:
        title = self._clean_text(title)

        # Caso CSV sporco: URL finito dentro il titolo.
        title = re.sub(r"https?://\S+", "", title)

        # Se resta un separatore finale sporco.
        title = title.strip(" ;")

        # Quote sporche residue.
        title = title.replace('\\"', '"')
        title = title.replace('""', '"')

        return self._clean_text(title)

    def _repair_url(self, url: str, title: str) -> str:
        url = self._clean_text(url)

        if self._is_valid_url(url):
            return url

        # Caso CSV sporco: URL finito dentro il titolo.
        match = re.search(r"https?://[^\s;\"']+", title or "")
        if match:
            candidate = match.group(0).strip()
            if self._is_valid_url(candidate):
                return candidate

        return CSV_URL

    def _fetch_detail_status(self, url: str, debug_base: Path) -> str | None:
        """
        Compatibilità con vecchie chiamate: ora lo stato viene letto da _fetch_detail_info.
        """
        return self._fetch_detail_info(url, debug_base).get("status_raw")

    def _fetch_detail_info(self, url: str, debug_base: Path) -> dict:
        """
        Legge la pagina di dettaglio Sicilia.
        La vecchia versione usava il dettaglio quasi solo per lo stato; qui lo usiamo anche
        per recuperare titolo completo, comune, provincia, potenza e proponente quando il CSV
        è troncato o povero.
        """
        if not self._is_valid_url(url) or url == CSV_URL:
            return {}

        try:
            response = self.session.get(
                url,
                timeout=45,
                headers={"User-Agent": "Mozilla/5.0 pv-agent"},
            )

            if response.status_code != 200:
                return {}

            html = response.text or ""
            soup = BeautifulSoup(html, "html.parser")
            plain = self._clean_text(soup.get_text(" ", strip=True))
            line_text = soup.get_text("\n", strip=True)
            lines = [self._clean_text(x) for x in line_text.splitlines() if self._clean_text(x)]
            relevant_text = self._extract_relevant_detail_text(lines, plain)

            title = self._extract_detail_title(soup, relevant_text or plain)
            status_raw = self._extract_status_from_lines(lines, plain)
            proponent = self._extract_proponent_from_text(relevant_text or plain)
            province = self._extract_province(relevant_text or plain)
            municipalities = self._extract_municipalities(relevant_text or plain)
            power = self._extract_power_text(relevant_text or plain)

            return {
                "title": title,
                "plain_text_sample": (relevant_text or plain)[:5000],
                "status_raw": status_raw,
                "proponent": proponent,
                "province": province,
                "municipalities": municipalities,
                "power": power,
            }

        except Exception as exc:
            safe_name = self._safe_filename(url)
            self._write_text(debug_base / f"detail_error_{safe_name}.txt", str(exc))
            return {}

    def _merge_detail_info(self, normalized: dict, detail_info: dict) -> dict:
        if not detail_info:
            return normalized

        merged = dict(normalized)

        current_title = self._clean_text(merged.get("title"))
        detail_title = self._clean_text(detail_info.get("title"))
        detail_plain = self._clean_text(detail_info.get("plain_text_sample"))

        # Usa il titolo dettaglio solo se è davvero più informativo, non è solo una ripetizione
        # del titolo CSV e non contiene code tecniche del portale.
        if (
            detail_title
            and len(detail_title) > len(current_title) + 25
            and self._contains_any(detail_title, COMMERCIAL_PV_KEYWORDS + BESS_KEYWORDS)
            and not self._looks_like_page_chrome(detail_title)
            and not self._same_title_core(current_title, detail_title)
        ):
            merged["title"] = detail_title

        combined = self._clean_text(" ".join([
            current_title,
            detail_title,
            detail_plain[:4000],
        ]))

        municipalities = self._merge_lists(
            merged.get("municipalities") or [],
            detail_info.get("municipalities") or [],
            self._extract_municipalities(combined),
        )

        municipalities = self._finalize_municipalities(municipalities)

        merged["municipalities"] = municipalities
        merged["province"] = (
            merged.get("province")
            or detail_info.get("province")
            or self._extract_province(combined)
            or self._infer_province_from_municipalities(municipalities)
        )

        merged["power"] = merged.get("power") or detail_info.get("power") or self._extract_power_text(combined)
        merged["proponent"] = merged.get("proponent") or detail_info.get("proponent")
        merged["status_raw"] = detail_info.get("status_raw") or merged.get("status_raw")

        return merged

    def _extract_relevant_detail_text(self, lines: list[str], plain: str) -> str:
        """
        Riduce il testo della pagina ai soli blocchi utili.
        Evita che footer/header del portale finiscano nei comuni.
        """
        useful: list[str] = []
        seen: set[str] = set()

        keywords = [
            "oggetto",
            "progetto",
            "impianto",
            "fotovoltaico",
            "agrivoltaico",
            "agrovoltaico",
            "comune",
            "comuni",
            "territorio",
            "localizzazione",
            "ubicazione",
            "località",
            "localita",
            "contrada",
            "c.da",
            "proponente",
            "potenza",
            "provincia",
        ]

        for idx, line in enumerate(lines):
            norm = self._normalize_for_match(line)
            if not norm:
                continue

            if any(k in norm for k in [self._normalize_for_match(x) for x in keywords]):
                # Prende anche la riga successiva, utile quando il portale usa label/valore su righe separate.
                chunk = " ".join(lines[idx:idx + 2])
                chunk = self._clean_text(chunk)

                if self._looks_like_page_chrome(chunk):
                    continue

                key = self._normalize_for_match(chunk)
                if key not in seen:
                    seen.add(key)
                    useful.append(chunk)

        if useful:
            return self._clean_text(" ".join(useful[:30]))

        return plain[:3000]

    def _extract_detail_title(self, soup: BeautifulSoup, plain: str) -> str | None:
        candidates: list[str] = []

        for selector in ["h1", "h2", "h3", ".title", ".titolo", ".page-title"]:
            for node in soup.select(selector):
                text = self._clean_text(node.get_text(" ", strip=True))
                if text:
                    candidates.append(text)

        # Fallback: cerca frasi lunghe contenenti fotovoltaico/agrivoltaico.
        for match in re.finditer(
            r"((?:progetto|realizzazione|impianto|parco|centrale)[^.]{40,900}(?:fotovoltaic|agrivoltaic|agrovoltaic)[^.]{0,500})",
            plain,
            flags=re.IGNORECASE,
        ):
            candidates.append(self._clean_text(match.group(1)))

        for candidate in candidates:
            if self._looks_like_page_chrome(candidate):
                continue
            if len(candidate) >= 35 and self._contains_any(candidate, COMMERCIAL_PV_KEYWORDS + BESS_KEYWORDS):
                return candidate

        return None

    def _extract_status_from_lines(self, lines: list[str], plain: str) -> str | None:
        for line in lines:
            normalized = self._normalize_for_match(line)

            if normalized in {"conclusa", "concluso"}:
                return "Conclusa"

            if "conclusa |" in normalized or "concluso |" in normalized:
                return "Conclusa"

            if normalized in {"in corso", "avviata", "avviato"}:
                return "In corso"

            if "archiviata" in normalized or "archiviato" in normalized:
                return "Archiviata"

        if "Conclusa |" in plain or "Concluso |" in plain:
            return "Conclusa"

        return None

    def _extract_proponent_from_text(self, text: str) -> str | None:
        if not text:
            return None

        patterns = [
            r"\bProponente\s*[:\-]\s*(.+?)(?:\s+(?:Oggetto|Procedura|Localizzazione|Comune|Data|Stato)\b|$)",
            r"\bDitta\s+proponente\s*[:\-]\s*(.+?)(?:\s+(?:Oggetto|Procedura|Localizzazione|Comune|Data|Stato)\b|$)",
            r"\bSociet[aà]\s+proponente\s*[:\-]\s*(.+?)(?:\s+(?:Oggetto|Procedura|Localizzazione|Comune|Data|Stato)\b|$)",
        ]

        for pattern in patterns:
            match = re.search(pattern, text, flags=re.IGNORECASE)
            if match:
                value = self._clean_proponent(match.group(1))
                if value:
                    return value

        return None

    def _clean_proponent(self, value: str) -> str | None:
        value = self._clean_text(value)
        value = re.split(
            r"\s+(?:Oggetto|Procedura|Localizzazione|Comune|Data|Stato|Documentazione)\b",
            value,
            maxsplit=1,
            flags=re.IGNORECASE,
        )[0]
        value = value.strip(" .,:;-")

        if not value or len(value) > 180:
            return None

        bad = ["sistema", "regione siciliana", "valutazione ambientale", "procedura"]
        norm = self._normalize_for_match(value)
        if any(x in norm for x in bad):
            return None

        return value

    def _looks_like_page_chrome(self, value: str) -> bool:
        norm = self._normalize_for_match(value)
        bad = [
            "regione siciliana",
            "valutazione ambientale",
            "assessorato",
            "dipartimento",
            "procedura elenco",
            "homepage",
            "accesso",
            "privacy",
        ]
        return any(x in norm for x in bad)

    def _merge_lists(self, *lists: list[str]) -> list[str]:
        found: list[str] = []
        seen: set[str] = set()

        for values in lists:
            for value in values or []:
                clean = self._clean_municipality(value)
                if not clean:
                    continue
                key = self._normalize_for_match(clean)
                if key in seen:
                    continue
                seen.add(key)
                found.append(clean)

        return found[:10]

    def _infer_province_from_municipalities(self, municipalities: list[str]) -> str | None:
        for municipality in municipalities or []:
            key = self._normalize_for_match(municipality)
            if key in SICILIA_MUNICIPALITY_TO_PROVINCE:
                return SICILIA_MUNICIPALITY_TO_PROVINCE[key]
        return None

    def _is_commercial_pv_project(self, title: str) -> bool:
        lowered = f" {self._normalize_for_match(title)} "

        has_core_pv = any(k in lowered for k in COMMERCIAL_PV_KEYWORDS)
        has_bess = any(k in lowered for k in BESS_KEYWORDS)

        if not has_core_pv and not has_bess:
            return False

        if any(k in lowered for k in EXCLUDE_KEYWORDS):
            return False

        # Tiene solo impianti/parchi/progetti energetici, evita citazioni marginali.
        strong_terms = [
            "impianto",
            "parco",
            "centrale",
            "produzione di energia",
            "agro",
            "agrivoltaico",
            "agrovoltaico",
            "revamping",
        ]

        return any(term in lowered for term in strong_terms)

    def _extract_power_text(self, text: str) -> str | None:
        if not text:
            return None

        value = self._clean_text(text)

        patterns = [
            r"potenza\s+(?:complessiva\s+)?(?:nominale\s+)?(?:di\s+picco\s+)?(?:pari\s+a\s+|di\s+)?([0-9][0-9\.\,]*)\s*(mw[p]?|kw[p]?)",
            r"da\s+([0-9][0-9\.\,]*)\s*(mw[p]?|kw[p]?)",
            r"([0-9][0-9\.\,]*)\s*(mw[p]?|kw[p]?)",
        ]

        for pattern in patterns:
            match = re.search(pattern, value, flags=re.IGNORECASE)
            if match:
                number = match.group(1)
                unit = match.group(2).upper()
                return f"{number} {unit}"

        return None

    def _extract_province(self, text: str) -> str | None:
        if not text:
            return None

        matches = re.findall(r"\(([A-Z]{2})\)", text.upper())
        for match in matches:
            if match in PROVINCE_CODES:
                return match

        matches = re.findall(r"\b(AG|CL|CT|EN|ME|PA|RG|SR|TP)\b", text.upper())
        for match in matches:
            if match in PROVINCE_CODES:
                return match

        return None

    def _same_title_core(self, a: str, b: str) -> bool:
        na = self._normalize_for_match(a)
        nb = self._normalize_for_match(b)
        if not na or not nb:
            return False

        # Se uno contiene l'altro, il dettaglio spesso ha solo duplicato il titolo CSV.
        return na in nb or nb in na

    def _finalize_municipalities(self, values: list[str]) -> list[str]:
        cleaned: list[str] = []
        seen: set[str] = set()

        # Primo passaggio: pulizia forte e deduplica.
        for value in values or []:
            comune = self._clean_municipality(value)
            if not comune:
                continue

            key = self._normalize_for_match(comune)
            if key in seen:
                continue

            seen.add(key)
            cleaned.append(comune)

        # Secondo passaggio: se esiste "Augusta" e "Melilli", elimina "Augusta E Melilli".
        single_keys = {self._normalize_for_match(x) for x in cleaned}
        final: list[str] = []

        for comune in cleaned:
            norm = self._normalize_for_match(comune)

            if " e " in f" {norm} ":
                parts = [p.strip() for p in norm.split(" e ") if p.strip()]
                if parts and all(part in single_keys for part in parts):
                    continue

            # Scarta località/contrade sfuggite alla prima pulizia.
            if norm.startswith(("da ", "c da ", "contrada ", "localita ")):
                continue

            final.append(comune)

        return final[:10]

    def _scan_known_municipalities(self, text: str) -> list[str]:
        """
        Fallback prudente: cerca comuni siciliani noti solo quando nel testo ci sono
        marcatori territoriali. Evita di dedurre il comune da semplici nomi commerciali.
        """
        norm = self._normalize_for_match(text)
        if not norm:
            return []

        location_markers = [
            "comune",
            "comuni",
            "territorio",
            "territori",
            "sito in",
            "sita in",
            "ubicato",
            "localizzato",
            "da realizzarsi",
            "da realizzare",
        ]

        if not any(marker in norm for marker in location_markers):
            return []

        found: list[str] = []
        for municipality_norm in sorted(SICILIA_MUNICIPALITY_TO_PROVINCE, key=len, reverse=True):
            if re.search(rf"\b{re.escape(municipality_norm)}\b", norm):
                title = self._title_case_location(municipality_norm)
                if title not in found:
                    found.append(title)

        return found[:10]

    def _extract_municipalities(self, text: str) -> list[str]:
        if not text:
            return []

        value = self._clean_text(text)
        found: list[str] = []

        def add_exact(value: str) -> None:
            comune = self._clean_municipality(value)
            if comune and comune not in found:
                found.append(comune)

        # Casi espliciti ad alta affidabilità: "Comune di X (SR)", "in comune di X (SR)".
        # Vanno trattati prima dei pattern generici, per non catturare località successive.
        explicit_patterns = [
            r"\b(?:nel\s+|in\s+|sito\s+nel\s+|sito\s+in\s+|sita\s+nel\s+|sita\s+in\s+)?comune\s+di\s+([A-ZÀ-Ý][A-Za-zÀ-ÿ'’\-\s]{2,55}?)\s*\((AG|CL|CT|EN|ME|PA|RG|SR|TP)\)",
            r"\b(?:nei\s+|in\s+|siti\s+nei\s+|siti\s+in\s+)?comuni\s+di\s+(.{3,180}?)\s*\((AG|CL|CT|EN|ME|PA|RG|SR|TP)\)",
        ]

        for pattern in explicit_patterns:
            for match in re.findall(pattern, value, flags=re.IGNORECASE):
                chunk = match[0] if isinstance(match, tuple) else match
                for part in self._split_municipality_chunk(chunk):
                    add_exact(part)

        def add_chunk(chunk: str) -> None:
            for part in self._split_municipality_chunk(chunk):
                comune = self._clean_municipality(part)
                if comune and comune not in found:
                    found.append(comune)

        # Pattern esplicito: "Comune di X (XX)" / "in comune di X (XX)".
        # Va processato prima del generico "Nome (XX)" per evitare località dopo il comune.
        for match in re.finditer(
            r"\b(?:comune\s+di|comune|in\s+comune\s+di|nel\s+comune\s+di)\s+([A-ZÀ-Ý][A-Za-zÀ-ÿ'’\-\s]{2,55}?)\s*\((AG|CL|CT|EN|ME|PA|RG|SR|TP)\)",
            value,
            flags=re.IGNORECASE,
        ):
            add_chunk(match.group(1))

        # Pattern abbastanza affidabile: "Nome Comune (XX)".
        # Prima puliamo la parte prima della provincia per evitare "KWP da realizzare nel Comune di Augusta".
        for match in re.finditer(
            r"\b([A-ZÀ-Ý][A-Za-zÀ-ÿ'’\-\s]{2,90}?)\s*\((AG|CL|CT|EN|ME|PA|RG|SR|TP)\)",
            value,
            flags=re.IGNORECASE,
        ):
            add_chunk(match.group(1))

        patterns = [
            r"\bnei\s+territori\s+(?:dei\s+)?comuni\s+di\s+(.+?)(?:\s+(?:provincia|e\s+relative|e\s+delle\s+relative|con\s+opere|opere\s+di|c\.da|contrada|localit[aà]|foglio|particella|particelle|potenza|denominato|denominata|codice|societ[aà]\s+proponente)|[.;]|$)",
            r"\bnel\s+territorio\s+(?:del\s+)?comune\s+di\s+(.+?)(?:\s+(?:provincia|e\s+relative|e\s+delle\s+relative|con\s+opere|opere\s+di|c\.da|contrada|localit[aà]|foglio|particella|particelle|potenza|denominato|denominata|codice|societ[aà]\s+proponente)|[.;]|$)",
            r"\bda\s+realizzarsi\s+nei\s+comuni\s+di\s+(.+?)(?:\s+(?:provincia|e\s+relative|e\s+delle\s+relative|con\s+opere|opere\s+di|c\.da|contrada|localit[aà]|foglio|particella|particelle|potenza|denominato|denominata|codice|societ[aà]\s+proponente)|[.;]|$)",
            r"\bda\s+realizzarsi\s+nel\s+comune\s+di\s+(.+?)(?:\s+(?:provincia|e\s+relative|e\s+delle\s+relative|con\s+opere|opere\s+di|c\.da|contrada|localit[aà]|foglio|particella|particelle|potenza|denominato|denominata|codice|societ[aà]\s+proponente)|[.;]|$)",
            r"\bda\s+realizzare\s+nel\s+comune\s+di\s+(.+?)(?:\s+(?:provincia|e\s+relative|e\s+delle\s+relative|con\s+opere|opere\s+di|c\.da|contrada|localit[aà]|foglio|particella|particelle|potenza|denominato|denominata|codice|societ[aà]\s+proponente)|[.;]|$)",
            r"\bsito\s+in\s+comune\s+di\s+(.+?)(?:\s+(?:provincia|e\s+relative|e\s+delle\s+relative|con\s+opere|opere\s+di|c\.da|contrada|localit[aà]|foglio|particella|particelle|potenza|denominato|denominata|codice|societ[aà]\s+proponente)|[.;]|$)",
            r"\bsito\s+nel\s+comune\s+di\s+(.+?)(?:\s+(?:provincia|e\s+relative|e\s+delle\s+relative|con\s+opere|opere\s+di|c\.da|contrada|localit[aà]|foglio|particella|particelle|potenza|denominato|denominata|codice|societ[aà]\s+proponente)|[.;]|$)",
            r"\bin\s+comune\s+di\s+(.+?)(?:\s+(?:provincia|e\s+relative|e\s+delle\s+relative|con\s+opere|opere\s+di|c\.da|contrada|localit[aà]|foglio|particella|particelle|potenza|denominato|denominata|codice|societ[aà]\s+proponente)|[.;]|$)",
            r"\bnei\s+comuni\s+di\s+(.+?)(?:\s+(?:provincia|e\s+relative|e\s+delle\s+relative|con\s+opere|opere\s+di|c\.da|contrada|localit[aà]|foglio|particella|particelle|potenza|denominato|denominata|codice|societ[aà]\s+proponente)|[.;]|$)",
            r"\bnel\s+comune\s+di\s+(.+?)(?:\s+(?:provincia|e\s+relative|e\s+delle\s+relative|con\s+opere|opere\s+di|c\.da|contrada|localit[aà]|foglio|particella|particelle|potenza|denominato|denominata|codice|societ[aà]\s+proponente)|[.;]|$)",
            r"\bcomuni\s+di\s+(.+?)(?:\s+(?:provincia|e\s+relative|e\s+delle\s+relative|con\s+opere|opere\s+di|c\.da|contrada|localit[aà]|foglio|particella|particelle|potenza|denominato|denominata|codice|societ[aà]\s+proponente)|[.;]|$)",
            r"\bcomune\s+di\s+(.+?)(?:\s+(?:provincia|e\s+relative|e\s+delle\s+relative|con\s+opere|opere\s+di|c\.da|contrada|localit[aà]|foglio|particella|particelle|potenza|denominato|denominata|codice|societ[aà]\s+proponente)|[.;]|$)",
        ]

        for pattern in patterns:
            for match in re.findall(pattern, value, flags=re.IGNORECASE):
                add_chunk(match)

        # Fallback finale: se il testo contiene marcatori territoriali, prova a riconoscere
        # comuni siciliani noti rimasti nel titolo/dettaglio.
        for comune in self._scan_known_municipalities(value):
            if comune not in found:
                found.append(comune)

        return found[:10]

    def _split_municipality_chunk(self, chunk: str) -> list[str]:
        chunk = self._clean_text(chunk)

        # Elimina sigle provincia e code del portale.
        chunk = re.sub(r"\((AG|CL|CT|EN|ME|PA|RG|SR|TP)\)", " ", chunk, flags=re.IGNORECASE)
        chunk = re.split(
            r"\b(?:cod\.?|codice|regione\s+siciliana|portale\s+valutazioni|urbanistiche|societ[aà]\s+proponente|proponente|c\.da|contrada|localit[aà]|distinto|distin|foglio|particella|particelle|snc|elettrodotto|cavidotto|rtn|stazione|cabina)\b",
            chunk,
            maxsplit=1,
            flags=re.IGNORECASE,
        )[0]
        chunk = self._strip_location_prefixes(chunk)

        parts = re.split(r",|;|/|\s+-\s+|\s+ e\s+|\s+ ed\s+", chunk, flags=re.IGNORECASE)

        # Secondo giro: se una parte contiene ancora "Comune di X", tiene solo X.
        cleaned_parts = []
        for part in parts:
            part = self._strip_location_prefixes(part)

            # Caso residuo: "Mezzojuso E Ciminna" o "Siracusa E Noto" non splittato al primo giro.
            subparts = re.split(r"\s+e\s+|\s+ed\s+", part, flags=re.IGNORECASE)
            for subpart in subparts:
                subpart = self._strip_location_prefixes(subpart)
                if subpart:
                    cleaned_parts.append(subpart)

        return cleaned_parts

    def _strip_location_prefixes(self, value: str) -> str:
        value = self._clean_text(value)

        patterns = [
            r"^.*?\bterritori\s+(?:dei\s+)?comuni\s+di\s+",
            r"^.*?\bterritorio\s+(?:del\s+)?comune\s+di\s+",
            r"^.*?\bda\s+realizzarsi\s+nei\s+comuni\s+di\s+",
            r"^.*?\bda\s+realizzarsi\s+nel\s+comune\s+di\s+",
            r"^.*?\bda\s+realizzare\s+nel\s+comune\s+di\s+",
            r"^.*?\bsito\s+in\s+comune\s+di\s+",
            r"^.*?\bsito\s+nel\s+comune\s+di\s+",
            r"^.*?\bsito\s+in\s+",
            r"^.*?\bsita\s+in\s+",
            r"^.*?\bin\s+comune\s+di\s+",
            r"^.*?\blocalizzato\s+nel\s+comune\s+di\s+",
            r"^.*?\bubicato\s+nel\s+comune\s+di\s+",
            r"^.*?\bricadente\s+nel\s+comune\s+di\s+",
            r"^.*?\bnei\s+comuni\s+di\s+",
            r"^.*?\bnel\s+comune\s+di\s+",
            r"^.*?\bcomuni\s+di\s+",
            r"^.*?\bcomune\s+di\s+",
        ]

        for pattern in patterns:
            value = re.sub(pattern, "", value, flags=re.IGNORECASE).strip()

        return value

    def _clean_municipality(self, value: str) -> str | None:
        value = self._clean_text(value)
        value = self._strip_location_prefixes(value)

        if not value:
            return None

        value = re.sub(r"\((AG|CL|CT|EN|ME|PA|RG|SR|TP)\)", "", value, flags=re.IGNORECASE)
        value = re.sub(r"^(?:e|ed|di|del|della|dello|dei|degli|in|nel|nella|da|c\.?\s*da|contrada|localit[aà])\s+", "", value, flags=re.IGNORECASE)
        value = re.sub(r"\b(?:di|del|della|dello|dei|degli|in|provincia)\b$", "", value, flags=re.IGNORECASE)
        value = value.strip(" ,.;:-()[]\"'")
        value = re.sub(r"\s+['’]?$", "", value).strip()
        value = re.sub(r"\s+\b[cC]\b$", "", value).strip()

        # Evita località e codici rimasti agganciati al comune.
        if re.search(r"\bin\s+via\b", value, flags=re.IGNORECASE):
            return None
        if re.search(r"-[a-z]$", value, flags=re.IGNORECASE):
            return None

        if not value:
            return None

        if len(value) < 3 or len(value) > 55:
            return None

        if re.search(r"\d", value):
            return None

        bad_fragments = [
            "potenza",
            "impianto",
            "fotovoltaico",
            "agrivoltaico",
            "agrovoltaico",
            "opere",
            "connessione",
            "rete",
            "rtn",
            "catasto",
            "foglio",
            "particelle",
            "particella",
            "progetto",
            "realizzazione",
            "produzione",
            "energia",
            "denominato",
            "denominata",
            "contrada",
            "localita",
            "località",
            "cavidotto",
            "elettrodotto",
            "stazione",
            "cabina",
            "procedura",
            "valutazione",
            "ambientale",
            "portale",
            "urbanistiche",
            "kwp",
            "mwp",
            "mw",
            "centrale",
            "internamente",
            "esterna",
            "nco",
            "sottostazione",
            "cod",
            "siciliana",
            "passaneto",
            "contado",
            "settefarine",
            "camemi",
            "pozzocamino",
            "pozzo camino",
        ]

        normalized = self._normalize_for_match(value)

        # Se è esattamente un comune noto, accettalo prima dei filtri anti-frammento.
        if normalized in SICILIA_MUNICIPALITY_TO_PROVINCE:
            return self._title_case_location(normalized)

        if any(fragment in normalized for fragment in bad_fragments):
            return None

        if normalized in {"sicilia", "provincia", "comune", "comuni", "sito", "siti", "maz"}:
            return None

        if " in " in f" {normalized} " and normalized not in SICILIA_MUNICIPALITY_TO_PROVINCE:
            return None

        # Se contiene ancora frasi troppo amministrative, meglio non inventare.
        if len(normalized.split()) > 5:
            return None

        return self._title_case_location(value)

    def _title_case_location(self, value: str) -> str:
        minor = {"di", "del", "della", "dello", "dei", "degli", "delle", "da", "de", "la", "lo", "il", "l"}
        words = []
        for word in self._clean_text(value).split():
            lower = word.lower()
            if lower in minor:
                words.append(lower)
            else:
                words.append(word[:1].upper() + word[1:].lower())
        return " ".join(words)

    def _build_external_id(self, normalized: dict) -> str:
        codice = normalized.get("codice") or ""
        url = normalized.get("detail_url") or ""
        title = normalized.get("title") or ""
        proponent = normalized.get("proponent") or ""

        raw_id = self._extract_raw_id_from_url(url)

        stable = "|".join(
            [
                str(codice).strip(),
                str(raw_id).strip(),
                self._slugify(title)[:120],
                self._slugify(proponent)[:80],
            ]
        )

        if stable.strip("|"):
            return stable[:240]

        digest = hashlib.sha1(f"{title}|{proponent}|{url}".encode("utf-8")).hexdigest()
        return f"sicilia-{digest}"

    def _extract_raw_id_from_url(self, url: str) -> str:
        if not url:
            return ""

        try:
            parsed = urlparse(url)
            query = parse_qs(parsed.query)
            values = query.get("procedura___oggetto_raw")
            if values:
                return values[0]
        except Exception:
            return ""

        match = re.search(r"procedura___oggetto_raw=([0-9]+)", url)
        if match:
            return match.group(1)

        return ""

    def _contains_any(self, text: str, needles: list[str]) -> bool:
        value = self._normalize_for_match(text)
        return any(self._normalize_for_match(needle) in value for needle in needles)

    def _is_valid_url(self, value: str | None) -> bool:
        if not value:
            return False

        value = str(value).strip()
        return value.startswith("http://") or value.startswith("https://")

    def _normalize_column_name(self, value: str) -> str:
        value = self._clean_text(value)
        value = value.replace("\ufeff", "")
        value = value.strip().lower()

        replacements = {
            "aoo_nome": "aoo_nome",
            "aoo_codiceipa": "aoo_codiceipa",
            "aoo_codiceipa": "aoo_codiceipa",
        }

        return replacements.get(value, value)

    def _normalize_for_match(self, text: str) -> str:
        text = self._clean_text(text).lower()
        text = unicodedata.normalize("NFKD", text)
        text = "".join(ch for ch in text if not unicodedata.combining(ch))
        text = text.replace("’", "'")
        text = re.sub(r"[^a-z0-9àèéìòù'\s\.-]", " ", text)
        text = re.sub(r"\s+", " ", text)
        return text.strip()

    def _slugify(self, text: str) -> str:
        text = self._normalize_for_match(text)
        text = re.sub(r"[^a-z0-9]+", "-", text)
        text = re.sub(r"-+", "-", text)
        return text.strip("-")

    def _clean_text(self, value) -> str:
        if value is None:
            return ""

        value = str(value)
        value = value.replace("\ufeff", "")
        value = value.replace("\xa0", " ")
        value = value.replace("\r", " ")
        value = value.replace("\n", " ")
        value = value.replace("\\u2019", "’")
        value = value.strip()

        value = re.sub(r"\s+", " ", value)

        return value.strip()

    def _safe_filename(self, value: str) -> str:
        digest = hashlib.sha1(value.encode("utf-8", errors="ignore")).hexdigest()
        return digest[:16]

    def _write_json(self, path: Path, data) -> None:
        try:
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text(
                json.dumps(data, ensure_ascii=False, indent=2),
                encoding="utf-8",
            )
        except Exception:
            pass

    def _write_text(self, path: Path, text: str) -> None:
        try:
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text(str(text), encoding="utf-8")
        except Exception:
            pass


if __name__ == "__main__":
    collector = SiciliaCollector()
    items = collector.fetch()
    print("items:", len(items))
    missing_province = sum(1 for item in items if not item.payload.get("province"))
    missing_municipalities = sum(1 for item in items if not item.payload.get("municipalities"))
    print("missing_province:", missing_province)
    print("missing_municipalities:", missing_municipalities)

    for item in items[:80]:
        print(
            str(item.external_id)[:80],
            "|",
            str(item.title)[:120],
            "|",
            item.payload.get("province"),
            "|",
            item.payload.get("municipalities"),
            "|",
            item.payload.get("power"),
            "|",
            item.payload.get("status_raw"),
        )