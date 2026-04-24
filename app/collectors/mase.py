from __future__ import annotations

import csv
import io
import re
from datetime import datetime

from app.collectors.base import BaseCollector, CollectorResult
from app.config import settings


STATUS_VALUES = {
    "Positivo",
    "Negativo",
    "Archiviata",
    "Ottemperata",
    "In istruttoria",
    "In corso",
    "Improcedibile",
    "Sospesa",
    "Verifica amministrativa",
}

SECTOR_VALUES = {
    "Fotovoltaici",
    "Fotovoltaico",
    "Agrivoltaico",
    "Agrivoltaici",
    "BESS",
    "Accumulo",
}


class MASECollector(BaseCollector):
    source_name = "mase"
    report_base = "https://va.mite.gov.it/it-IT/Procedure/Report"

    def fetch(self) -> list[CollectorResult]:
        current_year = datetime.now().year

        # tipo=1 e tipo=3 compaiono nei report pubblici del portale MASE
        # per proceduraID=27, e dalle risultanze pubbliche includono record FV.
        query_sets = [
            {"anno": current_year, "mto": 1, "proceduraID": 27, "tipo": 1},
            {"anno": current_year - 1, "mto": 1, "proceduraID": 27, "tipo": 1},
            {"anno": current_year, "mto": 1, "proceduraID": 27, "tipo": 3},
            {"anno": current_year - 1, "mto": 1, "proceduraID": 27, "tipo": 3},
        ]

        results: list[CollectorResult] = []
        seen_ids: set[str] = set()

        for params in query_sets:
            try:
                response = self.session.get(
                    self.report_base,
                    params=params,
                    timeout=settings.request_timeout,
                )
                response.raise_for_status()
            except Exception:
                continue

            rows = self._parse_report_rows(response.text)

            for row_idx, fields in enumerate(rows):
                normalized = [self._clean_field(f) for f in fields if self._clean_field(f)]
                if not normalized:
                    continue

                joined = " | ".join(normalized)
                lowered = joined.lower()

                if not self._looks_like_pv_record(normalized, lowered):
                    continue

                title = self._extract_title(normalized)
                if not title:
                    continue

                status_raw = self._extract_status(normalized)
                proponent = self._extract_proponent(normalized, title)
                sector = self._extract_sector(normalized)
                power = self._extract_power(joined)
                municipalities = self._extract_municipalities(title)

                source_url = self._build_source_url(params)
                external_id = self._build_external_id(title, proponent, params, row_idx)

                if external_id in seen_ids:
                    continue
                seen_ids.add(external_id)

                results.append(
                    CollectorResult(
                        external_id=external_id,
                        source_url=source_url,
                        title=title[:250],
                        payload={
                            "title": title[:500],
                            "proponent": proponent,
                            "status_raw": status_raw,
                            "region": None,
                            "province": None,
                            "municipalities": municipalities,
                            "power": power,
                            "project_type_hint": sector or joined[:500],
                        },
                    )
                )

        return results

    def _parse_report_rows(self, text: str) -> list[list[str]]:
        """
        Il report MASE non è sempre un CSV canonico.
        Prima provo a separarlo per righe, poi per ';', scartando il rumore.
        """
        raw = text.replace("\ufeff", "").strip()
        if not raw:
            return []

        rows: list[list[str]] = []

        for line in raw.splitlines():
            line = line.strip()
            if not line:
                continue

            # Scarta righe html o rumore evidente
            lowered = line.lower()
            if lowered.startswith("<!doctype") or lowered.startswith("<html") or lowered.startswith("<head"):
                continue

            # split semplice; va meglio del csv.reader su questo endpoint sporco
            parts = [self._clean_field(p.strip().strip('"')) for p in line.split(";")]
            parts = [p for p in parts if p]

            if parts:
                rows.append(parts)

        return rows

    def _looks_like_pv_record(self, fields: list[str], lowered_joined: str) -> bool:
        if len(fields) < 3:
            return False

        if any(k in lowered_joined for k in ["home", "ricerca", "tipologia", "procedura", "documenti", "menu"]):
            return False

        has_keyword = any(
            k in lowered_joined
            for k in ["fotovolta", "agrivolta", "agrovolta", "bess", "accumulo", "impianto fotovoltaico"]
        )

        has_sector = any(f in SECTOR_VALUES for f in fields)

        return has_keyword or has_sector

    def _extract_title(self, fields: list[str]) -> str | None:
        candidates = []
        for field in fields:
            lowered = field.lower()
            if any(k in lowered for k in ["fotovolta", "agrivolta", "agrovolta", "bess"]):
                candidates.append(field)

        if not candidates:
            return None

        # Preferisco il candidato più lungo e descrittivo
        candidates.sort(key=len, reverse=True)
        return candidates[0]

    def _extract_status(self, fields: list[str]) -> str | None:
        for field in fields:
            if field in STATUS_VALUES:
                return field
        return None

    def _extract_sector(self, fields: list[str]) -> str | None:
        for field in fields:
            if field in SECTOR_VALUES:
                return field
        return None

    def _extract_proponent(self, fields: list[str], title: str) -> str | None:
        title_found = False
        for field in fields:
            if field == title:
                title_found = True
                continue

            if not title_found:
                continue

            if self._looks_like_date(field):
                continue
            if field in STATUS_VALUES or field in SECTOR_VALUES:
                continue
            if field.isdigit():
                continue
            if len(field) < 2:
                continue

            return field

        # fallback: prendi un campo “azienda-like”
        for field in fields:
            if self._looks_like_company(field):
                return field

        return None

    def _extract_power(self, text: str) -> str | None:
        m = re.search(
            r"(\d{1,3}(?:[.\s]\d{3})*(?:,\d+)?|\d+(?:,\d+)?)\s*(MWp|MW|Kwp|kWp)",
            text,
            flags=re.IGNORECASE,
        )
        if m:
            return f"{m.group(1)} {m.group(2)}"
        return None

    def _extract_municipalities(self, title: str) -> list[str]:
        patterns = [
            r"nel Comune di\s+([A-ZÀ-Ú][A-Za-zÀ-Úà-ú'`\- ]+)",
            r"nei Comuni di\s+([A-ZÀ-Ú][A-Za-zÀ-Úà-ú'`\- ,]+)",
            r"in località\s+([A-ZÀ-Ú][A-Za-zÀ-Úà-ú'`\- ]+)",
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

    def _build_source_url(self, params: dict) -> str:
        return (
            f"{self.report_base}?anno={params['anno']}&mto={params['mto']}"
            f"&proceduraID={params['proceduraID']}&tipo={params['tipo']}"
        )

    def _build_external_id(self, title: str, proponent: str | None, params: dict, row_idx: int) -> str:
        base = f"{params['anno']}|{params['tipo']}|{title}|{proponent or ''}|{row_idx}".lower()
        base = re.sub(r"\s+", "-", base)
        base = re.sub(r"[^a-z0-9:/._|-]", "", base)
        return base[:250]

    def _clean_field(self, value: str) -> str:
        return " ".join((value or "").replace("\xa0", " ").split()).strip()

    def _looks_like_date(self, value: str) -> bool:
        return bool(re.fullmatch(r"\d{2}/\d{2}/\d{4}", value))

    def _looks_like_company(self, value: str) -> bool:
        lowered = value.lower()
        company_markers = ["srl", "spa", "s.p.a", "s.r.l", "energia", "solar", "renewables", "power"]
        return any(marker in lowered for marker in company_markers)