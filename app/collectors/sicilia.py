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
            detail_status = self._fetch_detail_status(detail_url, debug_base)

            status_raw = (
                detail_status
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
                        "title": title[:500],
                        "project_name": title[:500],
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
        if not self._is_valid_url(url):
            return None

        try:
            response = self.session.get(
                url,
                timeout=45,
                headers={"User-Agent": "Mozilla/5.0 pv-agent"},
            )

            if response.status_code != 200:
                return None

            html = response.text or ""
            soup = BeautifulSoup(html, "html.parser")
            text = soup.get_text("\n", strip=True)
            lines = [self._clean_text(x) for x in text.splitlines() if self._clean_text(x)]

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

            if "Conclusa |" in text or "Concluso |" in text:
                return "Conclusa"

            return None

        except Exception as exc:
            safe_name = self._safe_filename(url)
            self._write_text(debug_base / f"detail_error_{safe_name}.txt", str(exc))
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

    def _extract_municipalities(self, text: str) -> list[str]:
        if not text:
            return []

        value = self._clean_text(text)

        patterns = [
            r"nel comune di\s+([^,\.;\(\)]+)",
            r"nel comune\s+di\s+([^,\.;\(\)]+)",
            r"nei comuni di\s+([^\.]+)",
            r"nei territori comunali di\s+([^\.]+)",
            r"sito nel comune di\s+([^,\.;\(\)]+)",
            r"siti nel comune di\s+([^,\.;\(\)]+)",
            r"da realizzarsi nel comune di\s+([^,\.;\(\)]+)",
            r"da realizzare nel comune di\s+([^,\.;\(\)]+)",
            r"ubicato nel comune di\s+([^,\.;\(\)]+)",
            r"localizzato nel comune di\s+([^,\.;\(\)]+)",
        ]

        found: list[str] = []

        for pattern in patterns:
            for match in re.findall(pattern, value, flags=re.IGNORECASE):
                chunk = self._clean_text(match)

                # Taglia eventuali code tecniche.
                chunk = re.split(
                    r"\b(?:c\.da|contrada|localit[aà]|provincia|distinto|foglio|particella|particelle|snc|con potenza)\b",
                    chunk,
                    flags=re.IGNORECASE,
                )[0]

                parts = re.split(r",|\se\s|\s-\s|/", chunk)

                for part in parts:
                    comune = self._clean_municipality(part)
                    if comune and comune not in found:
                        found.append(comune)

        return found[:8]

    def _clean_municipality(self, value: str) -> str | None:
        value = self._clean_text(value)

        if not value:
            return None

        value = re.sub(r"\([A-Z]{2}\)", "", value)
        value = re.sub(r"\b(?:di|del|della|dello|dei|degli|in|provincia)\b$", "", value, flags=re.IGNORECASE)
        value = value.strip(" ,.;:-")

        if not value:
            return None

        if len(value) < 3:
            return None

        bad_fragments = [
            "potenza",
            "impianto",
            "fotovoltaico",
            "opere",
            "connessione",
            "rete",
            "rtn",
            "catasto",
            "foglio",
            "particelle",
        ]

        normalized = self._normalize_for_match(value)
        if any(fragment in normalized for fragment in bad_fragments):
            return None

        return value.title()

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
        return any(needle in value for needle in needles)

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

    for item in items[:20]:
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