from __future__ import annotations

import json
import re
from pathlib import Path
from urllib.parse import urljoin

from app.collectors.base import BaseCollector, CollectorResult
from app.config import settings


PV_KEYWORDS = [
    "fotovolta",
    "agrivolta",
    "agrovolta",
    "bess",
    "accumulo",
    "solare",
    "moduli fotovoltaici",
    "fonte solare",
]

# Da consts.js:
# SCO = 1
# VER = 2
# VIA = 3
# VAL_PRE = 5
# PAUR = 15
TIPO_PROCEDURA_LIST = "1,2,3,5,15"

# Da getAllSettori.html:
# 2 = IND. ENERGETICA ED ESTRATTIVA
# 8 = ALTRI PROGETTI
LOMBARDIA_SETTORI_TARGET = {"2", "8"}


class LombardiaCollector(BaseCollector):
    source_name = "lombardia"
    base_url = "https://www.silvia.servizirl.it/silviaweb/"

    def fetch(self) -> list[CollectorResult]:
        debug_base = Path("/app/reports/debug_lombardia")
        debug_base.mkdir(parents=True, exist_ok=True)

        settori = self._load_settori(debug_base)
        if not settori:
            self._write_json(
                debug_base / "settori_empty.json",
                {"note": "Nessun settore restituito da getAllSettori.html"},
            )
            return []

        self._write_json(debug_base / "settori_raw.json", settori)

        pv_settori = []
        for s in settori:
            settore_id = s.get("idSettore") or s.get("id_settore") or s.get("id")

            descr = self._clean_text(
                str(
                    s.get("descrSettore")
                    or s.get("siglaSettore")
                    or s.get("descSettore")
                    or s.get("descrizione")
                    or s.get("settore")
                    or s.get("descr")
                    or s.get("label")
                    or ""
                )
            )

            if str(settore_id) in LOMBARDIA_SETTORI_TARGET:
                pv_settori.append(
                    {
                        "id": str(settore_id),
                        "descr": descr,
                        "raw": s,
                    }
                )

        self._write_json(debug_base / "settori_pv.json", pv_settori)

        results: list[CollectorResult] = []
        seen_ids: set[str] = set()

        for settore in pv_settori:
            settore_id = settore["id"]
            rows = self._search_by_settore(str(settore_id), debug_base)

            if not rows:
                continue

            self._write_json(
                debug_base / f"normalized_source_rows_{settore_id}.json",
                rows[:50],
            )

            for row in rows:
                normalized = self._normalize_row(row)
                if not normalized:
                    continue

                title = normalized["title"]
                lowered_title = title.lower()

                if not any(k in lowered_title for k in PV_KEYWORDS):
                    continue

                external_id = self._build_external_id(
                    title,
                    normalized.get("proponent"),
                    normalized.get("detail_url"),
                )

                if external_id in seen_ids:
                    continue

                seen_ids.add(external_id)

                results.append(
                    CollectorResult(
                        external_id=external_id,
                        source_url=normalized.get("detail_url") or self.base_url,
                        title=title[:250],
                        payload={
                            "title": title[:500],
                            "proponent": normalized.get("proponent"),
                            "status_raw": normalized.get("status"),
                            "region": "Lombardia",
                            "province": None,
                            "municipalities": normalized.get("municipalities") or [],
                            "power": self._extract_power(title),
                            "project_type_hint": normalized.get("procedure") or "Lombardia SILVIA",
                        },
                    )
                )

        return results

    def _load_settori(self, debug_base: Path) -> list[dict]:
        url = urljoin(self.base_url, "getAllSettori.html")

        try:
            response = self.session.get(url, timeout=settings.request_timeout)
            response.raise_for_status()

            self._write_text(debug_base / "getAllSettori_response.txt", response.text)

            data = response.json()
            if isinstance(data, list):
                return data

            self._write_json(
                debug_base / "getAllSettori_unexpected_json.json",
                data,
            )
            return []

        except Exception as exc:
            self._write_text(debug_base / "getAllSettori_error.txt", str(exc))
            return []

    def _search_by_settore(self, settore_id: str, debug_base: Path) -> list[dict]:
        """
        SILVIA va in timeout se si chiede tutto insieme.
        Qui interroghiamo per anno, così la query resta più leggera.
        """
        all_rows: list[dict] = []

        years = ["2026", "2025", "2024"]

        for year in years:
            params = {
                "tipoProcedura": TIPO_PROCEDURA_LIST,
                "rgroupAutorita": "",
                "codiceProcedura": "",
                "descrProcedura": "",
                "idMacroStato": "",
                "interessati": "",
                "strFiltroEnte": "",
                "optionSettore": settore_id,
                "dataAvvioDa": "",
                "dataAvvioA": "",
                "dataDepositoDa": "",
                "dataDepositoA": "",
                "checkedAutorita": "",
                "checkedTipologiaProg": "",
                "tipoProponente": "",
                "idReferenteSelect": "",
                "descrProponente": "",
                "idTipoEnte": "",
                "idEnteACSelected": "",
                "accTipoEnte": "",
                "accTipoProc": "",
                "annoAvvio": year,
                "idSett": settore_id,
            }

            try:
                response = self.session.get(
                    urljoin(self.base_url, "avviaRicercaProcedura.html"),
                    params=params,
                    timeout=90,
                )
                response.raise_for_status()

                self._write_text(
                    debug_base / f"avviaRicercaProcedura_{settore_id}_{year}_response.txt",
                    response.text,
                )

                data = response.json()

                if isinstance(data, list):
                    all_rows.extend(data)
                else:
                    self._write_json(
                        debug_base / f"avviaRicercaProcedura_{settore_id}_{year}_unexpected_json.json",
                        data,
                    )

            except Exception as exc:
                self._write_text(
                    debug_base / f"avviaRicercaProcedura_{settore_id}_{year}_error.txt",
                    str(exc),
                )

        return all_rows

    def _normalize_row(self, row: dict) -> dict | None:
        if not isinstance(row, dict):
            return None

        title = self._first_non_empty(
            row,
            [
                "descrProgetto",
                "descrProcedura",
                "titolo",
                "oggetto",
                "descrizione",
                "descProcedura",
                "nomeProcedura",
                "procedura",
            ],
        )

        if not title:
            return None

        proponent = self._clean_proponenti(
            self._first_non_empty(
                row,
                [
                    "proponenti",
                    "proponente",
                    "descrProponente",
                    "descrEnteAzienda",
                    "enteProponente",
                    "referente",
                    "richiedente",
                ],
            )
        )

        macro_stato = row.get("macroStato") or {}
        status = None
        if isinstance(macro_stato, dict):
            status = self._clean_text(str(macro_stato.get("descrMacroStato") or "")) or None

        if not status:
            status = self._first_non_empty(
                row,
                [
                    "descrMacroStato",
                    "stato",
                    "macroStato",
                    "descrStato",
                    "descStato",
                ],
            )

        procedure = self._first_non_empty(
            row,
            [
                "group",
                "descrTipoProcedura",
                "tipoProcedura",
                "descTipoProcedura",
                "proceduraTipo",
            ],
        )

        proc_id = (
            row.get("idProgetto")
            or row.get("idProcedura")
            or row.get("id_procedura")
            or row.get("id")
            or row.get("idStudio")
        )

        detail_url = None
        if proc_id:
            detail_url = urljoin(self.base_url, f"#/scheda-sintesi/{proc_id}")

        municipalities = self._extract_municipalities(title)

        return {
            "title": title,
            "proponent": proponent,
            "status": status,
            "procedure": procedure,
            "detail_url": detail_url,
            "municipalities": municipalities,
        }

    def _first_non_empty(self, row: dict, keys: list[str]) -> str | None:
        for key in keys:
            value = row.get(key)
            if value is None:
                continue

            cleaned = self._clean_text(str(value))
            if cleaned and cleaned.lower() != "none":
                return cleaned

        return None

    def _clean_proponenti(self, value: str | None) -> str | None:
        if not value:
            return None

        text = self._clean_text(value)

        text = text.replace("(Azienda:", "")
        text = text.replace("(Ente:", "")
        text = text.replace("(Persona Fisica):", "")
        text = text.replace(");", ";")
        text = text.replace(")", "")

        text = self._clean_text(text.strip(" ;"))

        return text or None

    def _extract_power(self, text: str) -> str | None:
        m = re.search(
            r"(\d{1,3}(?:[.\s]\d{3})*(?:,\d+)?|\d+(?:,\d+)?)\s*(MWP|MW|KWP|KW)",
            text,
            flags=re.IGNORECASE,
        )

        if m:
            return f"{m.group(1)} {m.group(2)}"

        return None

    def _extract_municipalities(self, title: str) -> list[str]:
        patterns = [
            r"nel comune di\s+([A-ZÀ-Ú][A-Za-zÀ-Úà-ú'`\- ]+)",
            r"nei comuni di\s+([A-ZÀ-Ú][A-Za-zÀ-Úà-ú'`\- ,]+)",
            r"localizzato nel comune di\s+([A-ZÀ-Ú][A-Za-zÀ-Úà-ú'`\- ]+)",
            r"localizzato nei comuni di\s+([A-ZÀ-Ú][A-Za-zÀ-Úà-ú'`\- ,]+)",
            r"ubicato nel comune di\s+([A-ZÀ-Ú][A-Za-zÀ-Úà-ú'`\- ]+)",
            r"ubicato nei comuni di\s+([A-ZÀ-Ú][A-Za-zÀ-Úà-ú'`\- ,]+)",
            r"sito nei comuni di\s+([A-ZÀ-Ú][A-Za-zÀ-Úà-ú'`\- ,]+)",
            r"sito nel comune di\s+([A-ZÀ-Ú][A-Za-zÀ-Úà-ú'`\- ]+)",
            r"territori comunali di\s+([A-ZÀ-Ú][A-Za-zÀ-Úà-ú'`\- ,]+)",
            r"comuni di\s+([A-ZÀ-Ú][A-Za-zÀ-Úà-ú'`\- ,]+)",
            r"comune di\s+([A-ZÀ-Ú][A-Za-zÀ-Úà-ú'`\- ]+)",
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

    def _build_external_id(
        self,
        title: str,
        proponent: str | None,
        detail_url: str | None,
    ) -> str:
        base = f"{title}|{proponent or ''}|{detail_url or ''}".lower()
        base = re.sub(r"\s+", "-", base)
        base = re.sub(r"[^a-z0-9:/._|-]", "", base)
        return base[:250]

    def _clean_text(self, value: str) -> str:
        return " ".join((value or "").replace("\xa0", " ").split()).strip()

    def _write_text(self, path: Path, content: str) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(content, encoding="utf-8")

    def _write_json(self, path: Path, obj) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(
            json.dumps(obj, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )