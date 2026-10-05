from __future__ import annotations

import html
import json
import re
from pathlib import Path
from urllib.parse import urljoin

from bs4 import BeautifulSoup

from app.collectors.base import BaseCollector, CollectorResult


START_URL = "http://www.sistemapiemonte.it/skvia/HomePage.do?ricerca=ArchivioProgetti"

SEARCH_CLICK_ACTION = "cpRicercaArchivioProgetti!handleBtRicercaArchivioProgetti_CLICKED.do"

PV_KEYWORDS = [
    "fotovoltaico",
    "fotovoltaica",
    "agrivoltaico",
    "agrovoltaico",
    "agrofotovoltaico",
]

YEARS = [
    "2026",
    "2025",
    "2024",
    "2023",
]

MIN_YEAR = 2023

NEGATIVE_OUTCOME_MARKERS = [
    "esito negativo",
    "parere negativo",
    "valutazione negativa",
    "conclusione negativa",
    "conclusa negativamente",
    "concluso negativamente",
    "giudizio negativo",
    "provvedimento negativo",
    "non favorevole",
    "diniego",
]


class PiemonteCollector(BaseCollector):
    source_name = "piemonte"
    base_url = START_URL

    def fetch(self) -> list[CollectorResult]:
        debug_base = Path("/app/reports/debug_piemonte")
        debug_base.mkdir(parents=True, exist_ok=True)

        results: list[CollectorResult] = []
        seen_ids: set[str] = set()
        matched_rows: list[dict] = []
        excluded_negative_rows: list[dict] = []
        detail_outcome_cache: dict[str, tuple[bool, str | None]] = {}

        session = self.session
        session.headers.update(
            {
                "User-Agent": "Mozilla/5.0 PV-Agent-MVP/0.1",
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            }
        )

        home = session.get(
            START_URL,
            timeout=90,
            allow_redirects=True,
        )
        home.raise_for_status()

        self._write_text(
            debug_base / "00_home.html",
            home.content.decode("utf-8", errors="replace")[:2_000_000],
        )

        home_soup = self._parse_soup(home)
        search_action, base_data = self._get_form_action_and_data(home_soup, home.url)
        base_search_data = self._build_search_base_payload(base_data)

        self._write_json(
            debug_base / "base_search.json",
            {
                "form_action": search_action,
                "button_action": urljoin(search_action, SEARCH_CLICK_ACTION),
                "data_keys": list(base_search_data.keys()),
                "selects": self._extract_select_options(home_soup),
            },
        )

        debug_searches: list[dict] = []

        for keyword in PV_KEYWORDS:
            for year in YEARS:
                data_search = dict(base_search_data)
                data_search["appDataRicercaArchivioProgetti.denominazioneProgetto"] = keyword
                data_search["appDataRicercaArchivioProgetti.annoRegistro"] = year

                try:
                    response, submit_mode = self._submit_search(
                        session=session,
                        form_action=search_action,
                        data=data_search,
                        referer=home.url,
                    )
                    response.raise_for_status()

                    html_text = response.content.decode("utf-8", errors="replace")
                    safe_keyword = self._safe_filename(keyword)
                    self._write_text(
                        debug_base / f"search_{safe_keyword}_{year}.html",
                        html_text[:2_000_000],
                    )

                    soup = BeautifulSoup(html_text, "html.parser")
                    rows = self._extract_result_rows(soup, response.url)

                    debug_searches.append(
                        {
                            "keyword": keyword,
                            "year": year,
                            "submit_mode": submit_mode,
                            "contains_fotovoltaico": "fotovoltaico" in html_text.lower(),
                            "contains_agrivoltaico": (
                                "agrivoltaico" in html_text.lower()
                                or "agrovoltaico" in html_text.lower()
                            ),
                            "rows": len(rows),
                        }
                    )

                    for row in rows:
                        if not self._is_pv_related(row.get("raw_text") or ""):
                            continue

                        row_year = self._extract_year(row.get("code") or row.get("raw_text") or "")
                        if row_year is not None and row_year < MIN_YEAR:
                            continue

                        is_negative, negative_evidence = self._is_negative_concluded(
                            row=row,
                            session=session,
                            cache=detail_outcome_cache,
                        )
                        if is_negative:
                            excluded = dict(row)
                            excluded["negative_evidence"] = negative_evidence
                            excluded_negative_rows.append(excluded)
                            continue

                        external_id = self._build_external_id(row)

                        if external_id in seen_ids:
                            continue

                        seen_ids.add(external_id)
                        matched_rows.append(row)

                        results.append(
                            CollectorResult(
                                external_id=external_id,
                                source_url=row.get("url") or START_URL,
                                title=row["title"][:250],
                                payload={
                                    "title": row["title"][:500],
                                    "proponent": row.get("proponent"),
                                    "status_raw": row.get("status"),
                                    "region": "Piemonte",
                                    "province": row.get("province"),
                                    "municipalities": (
                                        [row["municipality"]]
                                        if row.get("municipality")
                                        else []
                                    ),
                                    "power": row.get("power"),
                                    "project_type_hint": row.get("procedure") or "Piemonte SKVIA",
                                    "source_code": row.get("code"),
                                    "authority": row.get("authority"),
                                    "_project_key_strategy": "external_id",
                                },
                            )
                        )

                except Exception as exc:
                    debug_searches.append(
                        {
                            "keyword": keyword,
                            "year": year,
                            "error": str(exc),
                        }
                    )

        self._write_json(debug_base / "matched_rows_sample.json", matched_rows[:200])
        self._write_json(
            debug_base / "excluded_negative_rows.json",
            excluded_negative_rows[:200],
        )
        self._write_json(debug_base / "searches_debug.json", debug_searches)
        self._write_json(
            debug_base / "summary.json",
            {
                "results": len(results),
                "matched_rows": len(matched_rows),
                "excluded_negative": len(excluded_negative_rows),
                "min_year": MIN_YEAR,
                "keywords": PV_KEYWORDS,
                "years": YEARS,
            },
        )

        return results

    # ------------------------------------------------------------------
    # SKVIA FLOW
    # ------------------------------------------------------------------

    def _build_search_base_payload(self, base_data: dict) -> dict:
        data = dict(base_data)

        # Ricerca volutamente senza filtro di autorità competente: il sito
        # restituisce nello stesso archivio sia REGIONE PIEMONTE sia
        # SOGGETTO GESTORE RN2000. Limitarsi alla Regione crea falsi negativi.
        data["appDataRicercaArchivioProgetti.competenza"] = ""
        data["appDataRicercaArchivioProgetti.tipologia"] = ""
        data["appDataRicercaArchivioProgetti.annoRegistro"] = ""
        data["appDataRicercaArchivioProgetti.codice"] = ""
        data["appDataRicercaArchivioProgetti.denominazioneProgetto"] = ""
        data["__checkbox_appDataRicercaArchivioProgetti.flagLeggeObiettivo"] = ""
        data["__checkbox_appDataRicercaArchivioProgetti.incidenza"] = ""
        data["appDataRicercaArchivioProgetti.cat"] = ""
        data["appDataRicercaArchivioProgetti.codIstatProvincia"] = ""
        data["appDataRicercaArchivioProgetti.istatComune"] = ""
        data["appDataRicercaArchivioProgetti.flagStato"] = ""
        data["appDataCodiceSitoReteNaturaSelezionato"] = ""
        data["appDataRicercaArchivioProgetti.idParco"] = ""

        return data

    def _submit_search(
        self,
        session,
        form_action: str,
        data: dict,
        referer: str,
    ):
        headers = {
            "Referer": referer,
            "Origin": "http://www.sistemapiemonte.it",
            "Content-Type": "application/x-www-form-urlencoded",
        }

        # Sul portale il tasto Ricerca scatena un evento dedicato. L'invio
        # della form (equivalente al tasto Invio da tastiera) non è affidabile.
        button_action = urljoin(form_action, SEARCH_CLICK_ACTION)

        try:
            response = session.post(
                button_action,
                data=data,
                timeout=90,
                allow_redirects=True,
                headers=headers,
            )
            response.raise_for_status()
            return response, "button_endpoint"
        except Exception:
            # Fallback compatibile con il comportamento storico del collector.
            fallback_data = dict(data)
            fallback_data["method:handleBtRicercaArchivioProgetti_CLICKED"] = "Ricerca"

            response = session.post(
                form_action,
                data=fallback_data,
                timeout=90,
                allow_redirects=True,
                headers=headers,
            )
            response.raise_for_status()
            return response, "form_method_fallback"

    def _is_negative_concluded(
        self,
        row: dict,
        session,
        cache: dict[str, tuple[bool, str | None]],
    ) -> tuple[bool, str | None]:
        status = self._normalize_for_match(row.get("status") or "")

        if "conclus" not in status:
            return False, None

        raw_text = row.get("raw_text") or ""
        marker = self._negative_outcome_marker(raw_text)
        if marker:
            return True, marker

        detail_url = row.get("url") or ""
        if not detail_url or detail_url == START_URL:
            return False, None

        if detail_url in cache:
            return cache[detail_url]

        try:
            response = session.get(
                detail_url,
                timeout=60,
                allow_redirects=True,
                headers={"Referer": START_URL},
            )
            response.raise_for_status()

            soup = self._parse_soup(response)
            detail_text = self._clean_text(soup.get_text(" ", strip=True))
            marker = self._negative_outcome_marker(detail_text)
            result = (bool(marker), marker)
        except Exception:
            # Non scartiamo una pratica conclusa solo perché il dettaglio non
            # è raggiungibile: si esclude esclusivamente su evidenza negativa.
            result = (False, None)

        cache[detail_url] = result
        return result

    def _negative_outcome_marker(self, text: str | None) -> str | None:
        normalized = self._normalize_for_match(text or "")

        for marker in NEGATIVE_OUTCOME_MARKERS:
            if self._normalize_for_match(marker) in normalized:
                return marker

        return None

    def _parse_soup(self, response) -> BeautifulSoup:
        text = response.content.decode("utf-8", errors="replace")
        return BeautifulSoup(text, "html.parser")

    def _get_form_action_and_data(self, soup: BeautifulSoup, current_url: str) -> tuple[str, dict]:
        form = soup.find("form", {"id": "cpRicercaArchivioProgetti"})
        if form is None:
            form = soup.find("form")

        if form is None:
            raise RuntimeError("Form SKVIA non trovato")

        action = urljoin(current_url, form.get("action") or "")

        data: dict[str, str] = {}

        for field in form.find_all(["input", "select", "textarea"]):
            name = field.get("name")
            if not name:
                continue

            if field.name == "select":
                selected = field.find("option", selected=True)
                data[name] = selected.get("value") if selected else ""
                continue

            field_type = (field.get("type") or "").lower()

            if field_type in ["submit", "button", "image"]:
                continue

            if field_type == "checkbox":
                continue

            data[name] = field.get("value") or ""

        return action, data

    # ------------------------------------------------------------------
    # RESULT PARSING
    # ------------------------------------------------------------------

    def _extract_result_rows(self, soup: BeautifulSoup, page_url: str) -> list[dict]:
        table = soup.find("table", {"id": "row_tElencoProgetti"})
        if table is None:
            table = soup.find("table", {"id": "wpRisultatiRicercaArchivioProgetti"})

        if table is None:
            return []

        rows: list[dict] = []

        for tr in table.find_all("tr"):
            cells = [
                self._clean_text(td.get_text(" ", strip=True))
                for td in tr.find_all(["td", "th"])
            ]

            if not cells:
                continue

            joined = self._clean_text(" | ".join(cells))

            if not joined:
                continue

            lowered = joined.lower()

            if "autorità competente" in lowered and "codice pratica" in lowered:
                continue

            if "risultati trovati" in lowered or "scarica in excel" in lowered or "scarica in pdf" in lowered:
                continue

            parsed = self._parse_result_cells(cells, tr, page_url)

            if parsed:
                rows.append(parsed)

        return rows

    def _parse_result_cells(self, cells: list[str], tr, page_url: str) -> dict | None:
        clean_cells = [c for c in cells if c]

        if len(clean_cells) < 5:
            return None

        # Struttura attesa:
        # Autorità competente | Codice pratica | Denominazione | Localizzazione | Scadenza Osservazioni | Stato
        authority = clean_cells[0]

        code = clean_cells[1] if len(clean_cells) > 1 else None
        title = clean_cells[2] if len(clean_cells) > 2 else None
        municipality = clean_cells[3] if len(clean_cells) > 3 else None
        status = clean_cells[-1] if clean_cells else None

        if not code or not title:
            return None

        raw_text = self._clean_text(" | ".join(clean_cells))
        detail_url = self._extract_first_url(tr, page_url)

        proponent = self._extract_proponent(title)
        province = self._extract_province(title) or self._extract_province(municipality or raw_text)
        power = self._extract_power_text(title) or self._extract_power_text(raw_text)
        procedure = self._extract_procedure(code)

        municipality_clean = self._clean_municipality(municipality or "")

        return {
            "authority": authority,
            "code": code,
            "title": title,
            "municipality": municipality_clean,
            "province": province,
            "status": status,
            "proponent": proponent,
            "power": power,
            "procedure": procedure,
            "url": detail_url or START_URL,
            "raw_text": raw_text,
        }

    def _extract_first_url(self, tr, page_url: str) -> str | None:
        for a in tr.find_all("a", href=True):
            href = a.get("href")
            if not href:
                continue

            absolute = urljoin(page_url, href)

            if absolute.startswith("mailto:"):
                continue

            return absolute

        return None

    # ------------------------------------------------------------------
    # EXTRACTION HELPERS
    # ------------------------------------------------------------------

    def _extract_proponent(self, text: str) -> str | None:
        if not text:
            return None

        parts = [self._clean_text(p) for p in text.split(",") if self._clean_text(p)]

        company_markers = [
            "s.r.l",
            "srl",
            "s.p.a",
            "spa",
            "soc agr",
            "soc. agr",
            "società agricola",
            "societa agricola",
            "green energy",
        ]

        # Priorità assoluta: ragioni sociali esplicite.
        for part in parts:
            lowered = part.lower()
            if any(marker in lowered for marker in company_markers):
                return part.strip(" .,-;:")

        # Fallback solo per testo plausibilmente societario. Evita di
        # interpretare decimali/potenze spezzate dalla virgola come proponente.
        for part in parts[1:]:
            lowered = part.lower()

            if re.search(r"\d", part):
                continue

            if any(
                bad in lowered
                for bad in [
                    "impianto",
                    "fotovoltaico",
                    "fotovoltaica",
                    "agrivoltaico",
                    "agrovoltaico",
                    "potenza",
                    "mwp",
                    "mw",
                    "kwp",
                    "kw",
                    "immissione",
                    "nominale",
                    "dc",
                    "ac",
                    "zsc",
                    "zps",
                    "comune",
                    "parco",
                    "fraz",
                    "località",
                    "localita",
                    "cn",
                    "al",
                    "at",
                    "bi",
                    "no",
                    "to",
                    "vb",
                    "vc",
                ]
            ):
                continue

            if 3 <= len(part) <= 120:
                return part.strip(" .,-;:")

        return None

    def _extract_province(self, text: str | None) -> str | None:
        if not text:
            return None

        piemonte_codes = {"AL", "AT", "BI", "CN", "NO", "TO", "VB", "VC"}

        # Nei titoli tecnici compaiono anche sigle come (DC) e (AC):
        # non sono province. Accettiamo tra parentesi solo codici provinciali
        # piemontesi effettivi.
        for m in re.finditer(r"\(([A-Z]{2})\)", text):
            code = m.group(1)
            if code in piemonte_codes:
                return code

        province_names = {
            "ALESSANDRIA": "AL",
            "ASTI": "AT",
            "BIELLA": "BI",
            "CUNEO": "CN",
            "NOVARA": "NO",
            "TORINO": "TO",
            "VERBANIA": "VB",
            "VERBANO-CUSIO-OSSOLA": "VB",
            "VERCELLI": "VC",
        }

        lowered = text.lower()
        for name, sigla in province_names.items():
            if name.lower() in lowered:
                return sigla

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

    def _extract_procedure(self, code: str | None) -> str | None:
        if not code:
            return None

        upper = code.upper()

        if "/VI" in upper:
            return "Valutazione di incidenza"

        if "/VAL" in upper:
            return "VIA"

        if "/VER" in upper:
            return "Verifica"

        if "/SPE" in upper:
            return "Specificazione"

        return "Piemonte SKVIA"

    def _extract_year(self, text: str | None) -> int | None:
        if not text:
            return None

        m = re.search(r"\b(20[0-9]{2})\b", text)
        if m:
            return int(m.group(1))

        return None

    def _clean_municipality(self, value: str | None) -> str | None:
        value = self._clean_text(value or "")
        value = value.strip(" .:-,;()")

        if not value:
            return None

        if len(value) > 100:
            return None

        bad_words = [
            "autorità competente",
            "codice pratica",
            "denominazione",
            "localizzazione",
            "scadenza",
            "osservazioni",
            "stato",
            "scarica",
        ]

        lowered = value.lower()

        if any(word in lowered for word in bad_words):
            return None

        return value

    # ------------------------------------------------------------------
    # FILTERS / DEBUG / UTILS
    # ------------------------------------------------------------------

    def _is_pv_related(self, text: str) -> bool:
        lowered = self._normalize_for_match(text)

        return any(
            keyword in lowered
            for keyword in [
                "fotovoltaico",
                "fotovoltaica",
                "agrivoltaico",
                "agrovoltaico",
                "agrofotovoltaico",
            ]
        )

    def _build_external_id(self, row: dict) -> str:
        # Il codice pratica è l'identificatore stabile della procedura.
        # L'autorità evita collisioni teoriche fra registri diversi. Titolo,
        # comune e stato NON devono entrare nella chiave perché possono cambiare
        # nel tempo e trasformare un aggiornamento in un falso nuovo progetto.
        code = self._clean_text(row.get("code") or "")
        authority = self._clean_text(row.get("authority") or "")

        if code:
            base = f"{authority}|{code}".lower()
        else:
            base = "|".join(
                [
                    row.get("title") or "",
                    row.get("municipality") or "",
                ]
            ).lower()

        base = re.sub(r"\s+", "-", base)
        base = re.sub(r"[^a-z0-9:/._|-]", "", base)

        return base[:250]

    def _extract_select_options(self, soup: BeautifulSoup) -> dict:
        out = {}

        for select in soup.find_all("select"):
            name = select.get("name") or select.get("id") or "unknown"

            options = []
            for option in select.find_all("option"):
                options.append(
                    {
                        "value": option.get("value") or "",
                        "text": self._clean_text(option.get_text(" ", strip=True)),
                        "selected": option.has_attr("selected"),
                    }
                )

            out[name] = {
                "id": select.get("id"),
                "options_count": len(options),
                "options_sample": options[:160],
            }

        return out

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

    def _clean_text(self, value: str) -> str:
        value = html.unescape(value or "")
        return " ".join(value.replace("\xa0", " ").split()).strip()

    def _safe_filename(self, value: str) -> str:
        value = re.sub(r"[^a-zA-Z0-9_-]+", "_", value)
        return value.strip("_")[:120] or "file"

    def _write_text(self, path: Path, content: str) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(content, encoding="utf-8")

    def _write_json(self, path: Path, obj) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(obj, ensure_ascii=False, indent=2), encoding="utf-8")