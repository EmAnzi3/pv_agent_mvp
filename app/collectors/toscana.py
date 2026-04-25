from __future__ import annotations

import html
import json
import re
from datetime import datetime
from pathlib import Path
from typing import Any

from app.collectors.base import BaseCollector, CollectorResult


API_BASE_URL = "https://api.regione.toscana.it"
API_URL_TEMPLATE = (
    API_BASE_URL
    + "/C01/suap-dt/v1/avvisi/eventiPubblici/{page_index}/{page_size}"
)

PUBLIC_URL = "https://servizi.patti.regione.toscana.it/star-info/avvisiPubblici"

PAGE_SIZE = 100
MAX_PAGES = 30

# Richiesta: considerare solo da gennaio 2025 in poi.
MIN_YEAR = 2025

PV_KEYWORDS = [
    "fotovoltaico",
    "fotovoltaica",
    "agrivoltaico",
    "agrovoltaico",
    "agro-fotovoltaico",
    "agrofotovoltaico",
    "impianto fv",
    "parco fotovoltaico",
    "centrale fotovoltaica",
    "solare fotovoltaico",
    "bess",
    "accumulo",
]

NON_PV_EXCLUDE = [
    "eolico",
    "eolica",
    "rifiuti",
    "discarica",
    "depuratore",
    "trattamento reflui",
    "biometano",
    "biogas",
    "allevamento",
    "cava",
    "miniera",
    "attività estrattiva",
]


class ToscanaCollector(BaseCollector):
    source_name = "toscana"
    base_url = PUBLIC_URL

    def fetch(self) -> list[CollectorResult]:
        debug_base = Path("/app/reports/debug_toscana")
        debug_base.mkdir(parents=True, exist_ok=True)

        results: list[CollectorResult] = []
        seen_ids: set[str] = set()
        matched_items: list[dict] = []
        raw_items_sample: list[dict] = []

        total_raw_items = 0
        pages_visited = 0

        for page_index in range(MAX_PAGES):
            try:
                data = self._fetch_page(page_index=page_index, page_size=PAGE_SIZE)
            except Exception as exc:
                self._write_text(debug_base / f"page_{page_index}_error.txt", str(exc))
                break

            self._write_json(debug_base / f"page_{page_index}.json", data)

            items = self._extract_items_from_response(data)
            pages_visited += 1
            total_raw_items += len(items)

            if page_index == 0:
                self._write_json(
                    debug_base / "page_0_debug.json",
                    {
                        "response_type": type(data).__name__,
                        "top_level_keys": list(data.keys()) if isinstance(data, dict) else None,
                        "items_on_page": len(items),
                        "sample_keys": list(items[0].keys()) if items and isinstance(items[0], dict) else None,
                    },
                )

            if not items:
                break

            for item in items:
                if len(raw_items_sample) < 50:
                    raw_items_sample.append(item)

                normalized = self._normalize_api_item(item)
                if not normalized:
                    continue

                raw_text = normalized.get("raw_text") or ""

                if not self._is_recent_enough(normalized):
                    continue

                if not self._is_pv_related(raw_text):
                    continue

                external_id = self._build_external_id(
                    normalized.get("date"),
                    normalized["title"],
                    normalized.get("proponent"),
                    normalized.get("municipality"),
                    normalized.get("url"),
                )

                if external_id in seen_ids:
                    continue

                seen_ids.add(external_id)
                matched_items.append(normalized)

                results.append(
                    CollectorResult(
                        external_id=external_id,
                        source_url=normalized.get("url") or PUBLIC_URL,
                        title=normalized["title"][:250],
                        payload={
                            "title": normalized["title"][:500],
                            "proponent": normalized.get("proponent"),
                            "status_raw": normalized.get("status"),
                            "region": "Toscana",
                            "province": normalized.get("province"),
                            "municipalities": (
                                [normalized["municipality"]]
                                if normalized.get("municipality")
                                else []
                            ),
                            "power": normalized.get("power"),
                            "project_type_hint": normalized.get("procedure") or "Toscana GeA",
                        },
                    )
                )

            # Stop euristico: se la pagina è più corta del page size siamo arrivati alla fine.
            if len(items) < PAGE_SIZE:
                break

        self._write_json(debug_base / "raw_items_sample.json", raw_items_sample)
        self._write_json(debug_base / "matched_items_sample.json", matched_items[:200])
        self._write_json(
            debug_base / "summary.json",
            {
                "pages_visited": pages_visited,
                "raw_items_seen": total_raw_items,
                "results": len(results),
                "matched_items": len(matched_items),
                "min_year": MIN_YEAR,
                "api_base": API_BASE_URL,
            },
        )

        return results

    def _fetch_page(self, page_index: int, page_size: int) -> Any:
        url = API_URL_TEMPLATE.format(page_index=page_index, page_size=page_size)

        headers = {
            "Accept": "application/json, text/plain, */*",
            "Content-Type": "application/json",
            "Origin": "https://servizi.patti.regione.toscana.it",
            "Referer": "https://servizi.patti.regione.toscana.it/",
            "User-Agent": "Mozilla/5.0 PV-Agent-MVP/0.1",
            "X-Domain": "GEA",
            "X-Ente": "GEA",
            "Cache-Control": "no-cache",
            "Pragma": "no-cache",
            "Expires": "Sat, 01 Jan 2000 00:00:00 GMT",
            "If-Modified-Since": "Sat, 01 Jan 2000 00:00:00 GMT",
        }

        params = {
            "dominio": "GEA",
            "codiceTerritorio": "GEA",
        }

        body = {
            "sortField": "stato",
            "order": "desc",
            "sezione": "GEA",
        }

        response = self.session.post(
            url,
            params=params,
            json=body,
            headers=headers,
            timeout=90,
        )
        response.raise_for_status()
        return response.json()

    def _extract_items_from_response(self, data: Any) -> list[dict]:
        if isinstance(data, list):
            return [x for x in data if isinstance(x, dict)]

        if not isinstance(data, dict):
            return []

        candidate_keys = [
            "elements",
            "content",
            "data",
            "items",
            "result",
            "results",
            "eventi",
            "avvisi",
        ]

        for key in candidate_keys:
            value = data.get(key)
            if isinstance(value, list):
                return [x for x in value if isinstance(x, dict)]

        # Fallback: cerca la prima lista di dizionari.
        for value in data.values():
            if isinstance(value, list) and all(isinstance(x, dict) for x in value):
                return value

        return []

    def _normalize_api_item(self, item: dict) -> dict | None:
        expanded = self._expand_item(item)

        raw_text = self._clean_text(
            " | ".join(
                str(v)
                for v in expanded.values()
                if v is not None and not isinstance(v, (dict, list))
            )
        )

        if not raw_text:
            return None

        title = self._first_non_empty(
            expanded,
            [
                "descrizione",
                "oggetto",
                "titolo",
                "denominazione",
                "intervento",
                "nome",
                "procedimento",
            ],
        )

        if not title:
            title = raw_text[:700]

        date = self._first_non_empty(
            expanded,
            [
                "dataInizioPubblicazione",
                "dataPubblicazione",
                "dataUltimoAggiornamento",
                "dataFinePubblicazione",
                "dataProtocollo",
                "createdAt",
                "updatedAt",
            ],
        ) or self._extract_date(raw_text)

        procedure = self._first_non_empty(
            expanded,
            [
                "tipoProcedimento",
                "procedimento",
                "procedura",
                "tipologia",
                "tipo",
            ],
        ) or self._extract_procedure(raw_text)

        status = self._first_non_empty(
            expanded,
            [
                "stato",
                "status",
                "statoProcedimento",
                "fase",
            ],
        ) or self._extract_status(raw_text)

        proponent = self._first_non_empty(
            expanded,
            [
                "proponente",
                "richiedente",
                "soggettoProponente",
                "intestatario",
                "societa",
                "società",
                "azienda",
            ],
        ) or self._extract_proponent(raw_text)

        municipality = self._first_non_empty(
            expanded,
            [
                "comune",
                "comuni",
                "localizzazione",
                "ubicazione",
                "territorio",
            ],
        ) or self._extract_municipality(raw_text)

        municipality = self._clean_municipality(municipality) if municipality else None

        province = self._first_non_empty(
            expanded,
            [
                "provincia",
                "siglaProvincia",
            ],
        ) or self._extract_province(raw_text)

        power = self._extract_power_text(title) or self._extract_power_text(raw_text)

        detail_url = self._extract_url_from_item(expanded)

        return {
            "date": self._clean_text(str(date)) if date else None,
            "title": self._clean_text(str(title))[:700],
            "procedure": self._clean_text(str(procedure)) if procedure else None,
            "status": self._clean_text(str(status)) if status else None,
            "proponent": self._clean_text(str(proponent)) if proponent else None,
            "municipality": municipality,
            "province": self._clean_text(str(province)) if province else None,
            "power": power,
            "url": detail_url or PUBLIC_URL,
            "raw_text": raw_text,
            "expanded": expanded,
        }

    def _expand_item(self, item: dict) -> dict:
        """
        Il JS GeA sembra leggere item.contenuto come JSON.
        Qui fondiamo i campi top-level con quelli interni a contenuto.
        """
        expanded: dict = {}

        for key, value in item.items():
            expanded[key] = value

        contenuto = item.get("contenuto")
        if isinstance(contenuto, str) and contenuto.strip():
            parsed = self._try_parse_json(contenuto)
            if isinstance(parsed, dict):
                for key, value in parsed.items():
                    expanded.setdefault(key, value)

        # Se ci sono dict annidati semplici, appiattiamo di un livello.
        for key, value in list(expanded.items()):
            if isinstance(value, dict):
                for sub_key, sub_value in value.items():
                    expanded.setdefault(sub_key, sub_value)
                    expanded.setdefault(f"{key}_{sub_key}", sub_value)

        return expanded

    def _try_parse_json(self, value: str) -> Any:
        try:
            return json.loads(value)
        except Exception:
            pass

        try:
            return json.loads(html.unescape(value))
        except Exception:
            return None

    def _is_recent_enough(self, normalized: dict) -> bool:
        date_text = normalized.get("date")
        raw_text = normalized.get("raw_text") or ""

        year = self._extract_year(str(date_text or "")) or self._extract_year(raw_text)

        if year is None:
            # Se non capiamo la data, teniamo il record: meglio valutarlo dal debug.
            return True

        return year >= MIN_YEAR

    def _extract_year(self, text: str) -> int | None:
        if not text:
            return None

        # ISO / date comuni
        m = re.search(r"\b(20[0-9]{2})[-/\.][0-9]{1,2}[-/\.][0-9]{1,2}\b", text)
        if m:
            return int(m.group(1))

        m = re.search(r"\b[0-9]{1,2}[-/\.][0-9]{1,2}[-/\.](20[0-9]{2})\b", text)
        if m:
            return int(m.group(1))

        m = re.search(r"\b[0-9]{1,2}\s+[a-zàéèìòù]+\s+(20[0-9]{2})\b", text, flags=re.IGNORECASE)
        if m:
            return int(m.group(1))

        # formato breve tipo 09-03-26
        m = re.search(r"\b[0-9]{1,2}[-/\.][0-9]{1,2}[-/\.]([0-9]{2})\b", text)
        if m:
            yy = int(m.group(1))
            return 2000 + yy if yy < 80 else 1900 + yy

        return None

    def _first_non_empty(self, data: dict, keys: list[str]) -> str | None:
        normalized_map = {self._normalize_key(k): v for k, v in data.items()}

        for key in keys:
            value = data.get(key)
            if value is None:
                value = normalized_map.get(self._normalize_key(key))

            if value is None:
                continue

            if isinstance(value, (dict, list)):
                continue

            text = self._clean_text(str(value))
            if text:
                return text

        return None

    def _extract_url_from_item(self, data: dict) -> str | None:
        for key in [
            "url",
            "link",
            "href",
            "dettaglio",
            "urlDettaglio",
            "linkDettaglio",
            "documentazione",
        ]:
            value = data.get(key)
            if not value:
                continue

            text = self._clean_text(str(value))
            if text.startswith("http"):
                return text

        # fallback: se esiste un id, link pubblico generico
        item_id = self._first_non_empty(data, ["id", "idEvento", "idAvviso", "codice", "identificativo"])
        if item_id:
            return f"{PUBLIC_URL}?id={item_id}"

        return PUBLIC_URL

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
                "agrovoltaico",
                "agro-fotovoltaico",
                "agrofotovoltaico",
                "solare fotovoltaico",
            ]
        )

        if not has_strong_pv and any(k in lowered for k in NON_PV_EXCLUDE):
            return False

        return True

    def _extract_date(self, text: str) -> str | None:
        patterns = [
            r"\b([0-9]{2}/[0-9]{2}/[0-9]{4})\b",
            r"\b([0-9]{2}\.[0-9]{2}\.[0-9]{4})\b",
            r"\b([0-9]{4}-[0-9]{2}-[0-9]{2})\b",
            r"\b([0-9]{1,2}\s+[a-zàéèìòù]+\s+[0-9]{4})\b",
            r"\b([0-9]{1,2}[-/\.][0-9]{1,2}[-/\.][0-9]{2})\b",
        ]

        for pattern in patterns:
            m = re.search(pattern, text, flags=re.IGNORECASE)
            if m:
                return self._clean_text(m.group(1))

        return None

    def _extract_procedure(self, text: str) -> str | None:
        lowered = self._normalize_for_match(text)

        if "p.a.u.r" in lowered or "paur" in lowered or "procedimento autorizzatorio unico" in lowered:
            return "PAUR"

        if "verifica" in lowered or "assoggettabilita" in lowered:
            return "VERIFICA"

        if "valutazione di impatto ambientale" in lowered or " via " in f" {lowered} ":
            return "VIA"

        return None

    def _extract_status(self, text: str) -> str | None:
        lowered = self._normalize_for_match(text)

        if "conclus" in lowered:
            return "Concluso"

        if "archiviat" in lowered:
            return "Archiviato"

        if "favorevole con prescrizioni" in lowered:
            return "Favorevole con prescrizioni"

        if "favorevole" in lowered:
            return "Favorevole"

        if "osservazioni" in lowered:
            return "Osservazioni"

        if "integrazioni" in lowered:
            return "Integrazioni"

        if "in corso" in lowered:
            return "In corso"

        return None

    def _extract_proponent(self, text: str) -> str | None:
        patterns = [
            r"Proponente\s*:?\s*(.+?)(?:\s+Comune|\s+Provincia|\s+Procedimento|\s+Oggetto|,|;|\||$)",
            r"Società\s+(.+?)(?:\s+ha\s+presentato|\s+ha\s+depositato|\s+richiede|,|;|$)",
            r"Societa\s+(.+?)(?:\s+ha\s+presentato|\s+ha\s+depositato|\s+richiede|,|;|$)",
        ]

        for pattern in patterns:
            m = re.search(pattern, text, flags=re.IGNORECASE)
            if m:
                value = self._clean_text(m.group(1)).strip(" .,:;")
                if 2 <= len(value) <= 180:
                    return value

        return None

    def _extract_municipality(self, text: str) -> str | None:
        patterns = [
            r"Comune\s*:?\s*([A-ZÀ-Ú][A-Za-zÀ-Úà-ú'`\- ]+?)(?:\s+Provincia|\s*\([A-Z]{2}\)|,|;|\||$)",
            r"Comune di\s+([A-ZÀ-Ú][A-Za-zÀ-Úà-ú'`\- ]+?)(?:\s*\([A-Z]{2}\)|,|;|\.|\s+e\s+|$)",
            r"in comune di\s+([A-ZÀ-Ú][A-Za-zÀ-Úà-ú'`\- ]+?)(?:\s*\([A-Z]{2}\)|,|;|\.|$)",
            r"nel comune di\s+([A-ZÀ-Ú][A-Za-zÀ-Úà-ú'`\- ]+?)(?:\s*\([A-Z]{2}\)|,|;|\.|$)",
        ]

        for pattern in patterns:
            m = re.search(pattern, text, flags=re.IGNORECASE)
            if m:
                value = self._clean_municipality(m.group(1))
                if value:
                    return value

        return None

    def _extract_province(self, text: str) -> str | None:
        m = re.search(r"\(([A-Z]{2})\)", text)
        if m:
            return m.group(1)

        provinces = [
            "AREZZO",
            "FIRENZE",
            "GROSSETO",
            "LIVORNO",
            "LUCCA",
            "MASSA-CARRARA",
            "PISA",
            "PISTOIA",
            "PRATO",
            "SIENA",
        ]

        lowered = text.lower()
        for province in provinces:
            if province.lower() in lowered:
                return province

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
            r"(MWp|MW|MVA|MWh|kWp|KWp|kW|KW|kw)"
            r"\b",
            text,
            flags=re.IGNORECASE,
        )

        if not m:
            return None

        return f"{m.group(1)} {m.group(2)}"

    def _build_external_id(
        self,
        date: str | None,
        title: str,
        proponent: str | None,
        municipality: str | None,
        url: str | None,
    ) -> str:
        base = f"{date or ''}|{title}|{proponent or ''}|{municipality or ''}|{url or ''}".lower()
        base = re.sub(r"\s+", "-", base)
        base = re.sub(r"[^a-z0-9:/._|-]", "", base)
        return base[:250]

    def _clean_municipality(self, value: str) -> str | None:
        value = self._clean_text(str(value))
        value = value.strip(" .:-,;()")

        if not value:
            return None

        if len(value) > 100:
            return None

        bad_words = [
            "impianto",
            "potenza",
            "opere",
            "connessione",
            "rete",
            "provincia",
            "localita",
            "località",
            "proponente",
            "procedimento",
            "verifica",
            "paur",
            "via",
        ]

        lowered = value.lower()
        if any(w in lowered for w in bad_words):
            return None

        return value

    def _normalize_key(self, value: str) -> str:
        value = self._normalize_for_match(value)
        value = re.sub(r"[^a-z0-9]+", "_", value)
        value = value.strip("_")
        return value

    def _normalize_for_match(self, value: str) -> str:
        value = self._clean_text(str(value)).lower()
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