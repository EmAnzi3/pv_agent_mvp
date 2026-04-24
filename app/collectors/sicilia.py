from __future__ import annotations

import csv
import html
import io
import json
import re
from pathlib import Path

from app.collectors.base import BaseCollector, CollectorResult


COMMERCIAL_PV_KEYWORDS = [
    "impianto fotovoltaico",
    "impianti fotovoltaici",
    "parco fotovoltaico",
    "centrale fotovoltaica",
    "centrale fotovoltaico",
    "agrivoltaico",
    "agro-fotovoltaico",
    "agrofotovoltaico",
    "agrovoltaico",
    "fonte solare fotovoltaica",
    "moduli fotovoltaici",
    "impianto fv",
    "impianti fv",
    "fv-",
    "fv ",
    " f.v.",
]

SECONDARY_PV_EXCLUDE = [
    "efficientamento energetico",
    "revamping depuratore",
    "depuratore",
    "impianto di depurazione",
    "trattamento rifiuti",
    "rifiuti",
    "discarica",
    "biogas",
    "biometano",
    "allevamento",
    "cartiera",
    "carta e cartone",
    "raffineria",
    "impianto peaker",
    "ripristino dell'impianto fotovoltaico esistente",
    "ripristino impianto fotovoltaico esistente",
    "potenziamento dell'impianto fotovoltaico esistente",
    "ammodernamento dell'impianto fotovoltaico esistente",
    "revamping dell'impianto fotovoltaico esistente",
]

NON_PV_EXCLUDE = [
    "eolica",
    "eolico",
    "parco eolico",
    "impianto eolico",
    "imboschimento",
    "miniera",
    "acque minerali",
    "allevamento avicolo",
]

BESS_KEYWORDS = [
    "bess",
    "sistema di accumulo",
    "accumulo integrato",
    "accumulo elettrochimico",
    "storage",
]


CSV_URL = (
    "https://dati.regione.sicilia.it/download/dataset/"
    "progetti-sottoposti-valutazione-ambientale/filesystem/"
    "progetti-sottoposti-valutazione-ambientale_csv.csv"
)


class SiciliaCollector(BaseCollector):
    source_name = "sicilia"
    base_url = "https://si-vvi.regione.sicilia.it/viavas/"

    def fetch(self) -> list[CollectorResult]:
        debug_base = Path("/app/reports/debug_sicilia")
        debug_base.mkdir(parents=True, exist_ok=True)

        try:
            response = self.session.get(CSV_URL, timeout=90)
            response.raise_for_status()
            text = response.content.decode("utf-8-sig", errors="replace")
        except Exception as exc:
            self._write_text(debug_base / "download_error.txt", str(exc))
            return []

        self._write_text(debug_base / "sicilia_raw.csv", text[:500000])

        rows = self._read_csv(text, debug_base)
        if not rows:
            self._write_json(debug_base / "rows_empty.json", {"note": "Nessuna riga letta dal CSV"})
            return []

        self._write_json(debug_base / "columns.json", list(rows[0].keys()))
        self._write_json(debug_base / "sample_rows.json", rows[:20])

        results: list[CollectorResult] = []
        matched_rows: list[dict] = []
        excluded_rows: list[dict] = []
        seen_ids: set[str] = set()

        for row in rows:
            title = self._clean_text(row.get("procedura_progetto_oggetto") or "")

            if not self._is_commercial_pv_project(title):
                if self._contains_any(title, COMMERCIAL_PV_KEYWORDS + BESS_KEYWORDS):
                    excluded_rows.append(row)
                continue

            matched_rows.append(row)

            normalized = self._normalize_row(row)
            if not normalized:
                continue

            external_id = self._build_external_id(
                normalized["codice"],
                normalized["title"],
                normalized.get("proponent"),
                normalized.get("detail_url"),
            )

            if external_id in seen_ids:
                continue

            seen_ids.add(external_id)

            results.append(
                CollectorResult(
                    external_id=external_id,
                    source_url=normalized.get("detail_url") or CSV_URL,
                    title=normalized["title"][:250],
                    payload={
                        "title": normalized["title"][:500],
                        "proponent": normalized.get("proponent"),
                        "status_raw": None,
                        "region": "Sicilia",
                        "province": normalized.get("province"),
                        "municipalities": normalized.get("municipalities") or [],
                        "power": normalized.get("power"),
                        "project_type_hint": normalized.get("procedure") or "Sicilia VIA/VAS",
                    },
                )
            )

        self._write_json(debug_base / "matched_rows_sample.json", matched_rows[:80])
        self._write_json(debug_base / "excluded_rows_sample.json", excluded_rows[:80])
        self._write_json(
            debug_base / "summary.json",
            {
                "rows_total": len(rows),
                "matched_rows": len(matched_rows),
                "excluded_pv_like_rows": len(excluded_rows),
                "results": len(results),
            },
        )

        return results

    def _read_csv(self, text: str, debug_base: Path) -> list[dict]:
        try:
            reader = csv.DictReader(io.StringIO(text), delimiter=";")
            rows = [dict(r) for r in reader if r]

            self._write_json(
                debug_base / "csv_dialect.json",
                {
                    "delimiter": ";",
                    "columns": list(rows[0].keys()) if rows else [],
                },
            )

            return rows

        except Exception as exc:
            self._write_text(debug_base / "csv_parse_error.txt", str(exc))
            return []

    def _normalize_row(self, row: dict) -> dict | None:
        title = self._clean_text(row.get("procedura_progetto_oggetto") or "")
        if not title:
            return None

        codice = self._clean_text(row.get("procedura_codice") or "")
        detail_url = self._clean_text(row.get("procedura_url") or "")
        procedure = self._clean_text(row.get("procedura_tipologia") or "")
        proponent = self._clean_text(row.get("proponente_progetto") or "")

        municipalities = self._extract_municipalities(title)
        province = self._extract_province(title)

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
        }

    def _is_commercial_pv_project(self, title: str) -> bool:
        lowered = f" {self._normalize_for_match(title)} "

        has_core_pv = any(k in lowered for k in COMMERCIAL_PV_KEYWORDS)
        has_bess = any(k in lowered for k in BESS_KEYWORDS)

        # BESS da solo entra solo se collegato esplicitamente a FV/fotovoltaico/solare.
        if has_bess and not any(k in lowered for k in ["fotovolta", "solare", "agrivolta", "agro-fotovolta", "fv"]):
            return False

        if not has_core_pv and not has_bess:
            return False

        # Esclude progetti chiaramente non FV.
        if any(k in lowered for k in NON_PV_EXCLUDE):
            return False

        # Esclude FV accessorio dentro progetti industriali/ambientali,
        # salvo che sia chiaramente un nuovo impianto/parco/centrale FV.
        has_strong_utility_signal = any(
            k in lowered
            for k in [
                "realizzazione",
                "costruzione",
                "esercizio",
                "parco fotovoltaico",
                "centrale fotovoltaica",
                "impianto fotovoltaico",
                "impianto agrivoltaico",
                "agro-fotovoltaico",
                "agrofotovoltaico",
                "agrivoltaico",
            ]
        )

        if any(k in lowered for k in SECONDARY_PV_EXCLUDE) and not has_strong_utility_signal:
            return False

        return True

    def _contains_any(self, text: str, keywords: list[str]) -> bool:
        lowered = f" {self._normalize_for_match(text)} "
        return any(k in lowered for k in keywords)

    def _normalize_for_match(self, value: str) -> str:
        value = self._clean_text(value).lower()
        value = value.replace("à", "a")
        value = value.replace("è", "e")
        value = value.replace("é", "e")
        value = value.replace("ì", "i")
        value = value.replace("ò", "o")
        value = value.replace("ù", "u")
        value = value.replace("â", "")
        return value

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

    def _extract_municipalities(self, title: str) -> list[str]:
        patterns = [
            r"nel comune di\s+([A-ZÀ-Ú][A-Za-zÀ-Úà-ú'`\- ]+)",
            r"nel comune\s+di\s+([A-ZÀ-Ú][A-Za-zÀ-Úà-ú'`\- ]+)",
            r"comune di\s+([A-ZÀ-Ú][A-Za-zÀ-Úà-ú'`\- ]+)",
            r"comune\s+([A-ZÀ-Ú][A-Za-zÀ-Úà-ú'`\- ]+)",
            r"nei comuni di\s+([A-ZÀ-Ú][A-Za-zÀ-Úà-ú'`\- ,]+)",
            r"comuni di\s+([A-ZÀ-Ú][A-Za-zÀ-Úà-ú'`\- ,]+)",
            r"da realizzarsi nel comune di\s+([A-ZÀ-Ú][A-Za-zÀ-Úà-ú'`\- ]+)",
            r"da realizzarsi nei comuni di\s+([A-ZÀ-Ú][A-Za-zÀ-Úà-ú'`\- ,]+)",
            r"ricadente nel territorio del comune di\s+([A-ZÀ-Ú][A-Za-zÀ-Úà-ú'`\- ]+)",
            r"ricadenti nei comuni di\s+([A-ZÀ-Ú][A-Za-zÀ-Úà-ú'`\- ,]+)",
        ]

        out: list[str] = []

        for pattern in patterns:
            for match in re.finditer(pattern, title, flags=re.IGNORECASE):
                raw = match.group(1).strip()
                out.extend(self._split_municipalities(raw))

        deduped: list[str] = []
        for item in out:
            item = self._clean_municipality(item)
            if item and item not in deduped:
                deduped.append(item)

        return deduped

    def _split_municipalities(self, raw: str) -> list[str]:
        raw = self._clean_text(raw)
        if not raw:
            return []

        raw = re.sub(r"\([A-Z]{2}\)", "", raw)
        raw = re.sub(r"\bprovincia di\b.*$", "", raw, flags=re.IGNORECASE)
        raw = re.sub(r"\bc/da\b.*$", "", raw, flags=re.IGNORECASE)
        raw = re.sub(r"\bcontrada\b.*$", "", raw, flags=re.IGNORECASE)
        raw = re.sub(r"\blocalit[aà]\b.*$", "", raw, flags=re.IGNORECASE)
        raw = re.sub(r"\bfoglio\b.*$", "", raw, flags=re.IGNORECASE)

        parts = re.split(r",|;|\se\s|\s-\s", raw)

        out: list[str] = []
        for p in parts:
            p = self._clean_municipality(p)
            if p:
                out.append(p)

        return out

    def _clean_municipality(self, value: str) -> str | None:
        value = self._clean_text(value)
        value = value.strip(" .:-,;()")

        if not value:
            return None

        if len(value) > 60:
            return None

        bad_words = [
            "potenza",
            "impianto",
            "opere",
            "connessione",
            "rete",
            "catasto",
            "particelle",
            "terreni",
            "foglio",
            "provincia",
            "localita",
            "località",
            "contrada",
            "c/da",
        ]

        lowered = value.lower()
        if any(w in lowered for w in bad_words):
            return None

        return value

    def _extract_province(self, title: str) -> str | None:
        m = re.search(r"\(([A-Z]{2})\)", title)
        if m:
            return m.group(1)
        return None

    def _build_external_id(
        self,
        codice: str,
        title: str,
        proponent: str | None,
        detail_url: str | None,
    ) -> str:
        base = f"{codice}|{title}|{proponent or ''}|{detail_url or ''}".lower()
        base = re.sub(r"\s+", "-", base)
        base = re.sub(r"[^a-z0-9:/._|-]", "", base)
        return base[:250]

    def _clean_text(self, value: str) -> str:
        value = html.unescape(value or "")
        return " ".join(value.replace("\xa0", " ").split()).strip()

    def _write_text(self, path: Path, content: str) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(content, encoding="utf-8")

    def _write_json(self, path: Path, obj) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(obj, ensure_ascii=False, indent=2), encoding="utf-8")