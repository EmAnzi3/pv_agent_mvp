from __future__ import annotations

import html
import json
import re
from pathlib import Path
from urllib.parse import urljoin

from bs4 import BeautifulSoup

from app.collectors.base import BaseCollector, CollectorResult


BASE_URL = "https://portal.sardegnasira.it"
NEWS_URL = "https://portal.sardegnasira.it/impatto-ambientale"
SEARCH_URL = "https://portal.sardegnasira.it/ricerca-dei-progetti"

PORTLET = "_ViaProgetto_WAR_RegioneSardegnaportlet_"
FORM = "_ViaProgetto_WAR_RegioneSardegnaportlet_:form"

# Limite richiesto: solo pratiche da gennaio 2025 in poi.
SEARCH_YEARS = ["2026", "2025"]

# 560 = Verifica assoggettabilità a VIA
# 566 = VIA Regionale e PAUR
SEARCH_PROCEDURES = {
    "560": "VERIFICA",
    "566": "VIA/PAUR",
}

SEARCH_KEYWORDS = [
    "fotovoltaico",
    "agrivoltaico",
    "agrovoltaico",
]

PV_KEYWORDS = [
    "fotovoltaico",
    "fotovoltaica",
    "imp. fotovoltaico",
    "impianto fotovoltaico",
    "impianto fv",
    "fv ",
    "agrivoltaico",
    "agrovoltaico",
    "agro-fotovoltaico",
    "agrofotovoltaico",
    "solare fotovoltaico",
    "solare fotovoltaica",
    "parco fotovoltaico",
    "centrale fotovoltaica",
    "bess",
    "accumulo",
]

NON_PV_EXCLUDE = [
    "eolico",
    "eolica",
    "parco eolico",
    "impianto eolico",
    "rifiuti",
    "discarica",
    "depuratore",
    "trattamento reflui",
    "biometano",
    "biogas",
    "allevamento",
    "miniera",
    "cava",
    "attività estrattiva",
]


class SardegnaCollector(BaseCollector):
    source_name = "sardegna"
    base_url = NEWS_URL

    def fetch(self) -> list[CollectorResult]:
        debug_base = Path("/app/reports/debug_sardegna")
        debug_base.mkdir(parents=True, exist_ok=True)

        all_results: list[CollectorResult] = []
        seen_external_ids: set[str] = set()

        news_results = self._fetch_news(debug_base)
        search_results = self._fetch_search(debug_base)

        for item in news_results + search_results:
            if item.external_id in seen_external_ids:
                continue

            seen_external_ids.add(item.external_id)
            all_results.append(item)

        self._write_json(
            debug_base / "summary_total.json",
            {
                "news_results": len(news_results),
                "search_results": len(search_results),
                "total_results": len(all_results),
            },
        )

        return all_results

    # ---------------------------------------------------------------------
    # 1) HOME "ULTIME NOTIZIE"
    # ---------------------------------------------------------------------

    def _fetch_news(self, debug_base: Path) -> list[CollectorResult]:
        results: list[CollectorResult] = []
        matched_blocks: list[dict] = []
        raw_blocks_sample: list[str] = []

        try:
            response = self.session.get(NEWS_URL, timeout=90)
            response.raise_for_status()
            text = response.content.decode("utf-8", errors="replace")
        except Exception as exc:
            self._write_text(debug_base / "news_error.txt", str(exc))
            return results

        self._write_text(debug_base / "news_page.html", text[:500000])

        soup = BeautifulSoup(text, "html.parser")
        blocks = self._extract_news_blocks(soup)

        self._write_json(
            debug_base / "news_debug.json",
            {
                "url": NEWS_URL,
                "blocks": len(blocks),
            },
        )

        seen_ids: set[str] = set()

        for block in blocks:
            raw_text = self._clean_text(block.get_text(" ", strip=True))

            if len(raw_blocks_sample) < 100:
                raw_blocks_sample.append(raw_text)

            if not self._is_pv_related(raw_text):
                continue

            normalized = self._normalize_news_block(block, NEWS_URL)
            if not normalized:
                continue

            external_id = self._build_external_id(
                "news",
                normalized.get("date"),
                normalized["title"],
                normalized.get("proponent"),
                normalized.get("municipality"),
            )

            if external_id in seen_ids:
                continue

            seen_ids.add(external_id)
            matched_blocks.append(normalized)

            results.append(
                CollectorResult(
                    external_id=external_id,
                    source_url=normalized.get("url") or NEWS_URL,
                    title=normalized["title"][:250],
                    payload={
                        "title": normalized["title"][:500],
                        "proponent": normalized.get("proponent"),
                        "status_raw": normalized.get("status"),
                        "region": "Sardegna",
                        "province": normalized.get("province"),
                        "municipalities": (
                            [normalized["municipality"]]
                            if normalized.get("municipality")
                            else []
                        ),
                        "power": normalized.get("power"),
                        "project_type_hint": normalized.get("procedure") or "Sardegna VIA",
                    },
                )
            )

        self._write_json(debug_base / "raw_blocks_sample.json", raw_blocks_sample)
        self._write_json(debug_base / "matched_blocks_sample.json", matched_blocks[:120])
        self._write_json(
            debug_base / "summary.json",
            {
                "pages_visited": 1,
                "results": len(results),
                "matched_blocks": len(matched_blocks),
                "source": "news",
            },
        )

        return results

    def _extract_news_blocks(self, soup: BeautifulSoup) -> list:
        blocks = []

        for node in soup.select("div.news-sardegna"):
            text = self._clean_text(node.get_text(" ", strip=True))

            if len(text) < 40:
                continue

            if "news-sardegna-title" not in str(node) and "news-sardegna-text" not in str(node):
                continue

            blocks.append(node)

        if not blocks:
            for node in soup.select(".news-list .row-fluid"):
                text = self._clean_text(node.get_text(" ", strip=True))
                if len(text) >= 40:
                    blocks.append(node)

        return blocks

    def _normalize_news_block(self, block, page_url: str) -> dict | None:
        text = self._clean_text(block.get_text(" ", strip=True))
        if not text:
            return None

        title = self._extract_news_title(block, text)
        if not title:
            return None

        date = self._extract_news_date(block, text)
        procedure = self._extract_procedure(title + " " + text)
        status = self._extract_status(text)
        proponent = self._extract_proponent(text)
        municipality = self._extract_municipality(text)
        province = self._extract_province(text)
        power = self._extract_power_text(title) or self._extract_power_text(text)
        detail_url = self._extract_first_url(block, page_url)

        return {
            "date": date,
            "title": title,
            "procedure": procedure,
            "proponent": proponent,
            "municipality": municipality,
            "province": province,
            "status": status,
            "power": power,
            "url": detail_url or page_url,
        }

    def _extract_news_title(self, block, text: str) -> str | None:
        title_node = block.select_one(".news-sardegna-title h4")
        if title_node:
            title = self._clean_text(title_node.get_text(" ", strip=True))
            if title:
                return title[:700]

        title_link = block.select_one(".news-sardegna-title a")
        if title_link:
            title = self._clean_text(title_link.get_text(" ", strip=True))
            if title:
                return title[:700]

        cleaned = re.sub(
            r"\b\d{1,2}\s+[a-zàéèìòù]+\s+\d{4}\b",
            "",
            text,
            flags=re.IGNORECASE,
        )
        cleaned = self._clean_text(cleaned).strip(" -–—:;")

        if not cleaned:
            return None

        return cleaned[:700]

    def _extract_news_date(self, block, text: str) -> str | None:
        date_node = block.select_one(".news-sardegna-date")
        if date_node:
            value = self._clean_text(date_node.get_text(" ", strip=True))
            if value:
                return value

        return self._extract_date_from_text(text)

    # ---------------------------------------------------------------------
    # 2) RICERCA STORICA PROGETTI - LIMITATA A 2025/2026
    # ---------------------------------------------------------------------

    def _fetch_search(self, debug_base: Path) -> list[CollectorResult]:
        results: list[CollectorResult] = []
        matched_rows: list[dict] = []
        seen_ids: set[str] = set()

        combo_counter = 0

        for year in SEARCH_YEARS:
            for procedure_code, procedure_label in SEARCH_PROCEDURES.items():
                for keyword in SEARCH_KEYWORDS:
                    combo_counter += 1

                    try:
                        html_page, action, encoded_url, viewstate = self._get_search_form()
                    except Exception as exc:
                        self._write_text(
                            debug_base / f"form_error_{combo_counter}.txt",
                            str(exc),
                        )
                        continue

                    safe_keyword = re.sub(r"[^a-zA-Z0-9_]+", "_", keyword)

                    self._write_text(
                        debug_base / f"get_form_{combo_counter}_{year}_{procedure_code}_{safe_keyword}.html",
                        html_page[:500000],
                    )

                    try:
                        response_text = self._post_search(
                            action=action,
                            encoded_url=encoded_url,
                            viewstate=viewstate,
                            year=year,
                            procedure_code=procedure_code,
                            keyword=keyword,
                        )
                    except Exception as exc:
                        self._write_text(
                            debug_base / f"post_error_{combo_counter}_{year}_{procedure_code}_{safe_keyword}.txt",
                            str(exc),
                        )
                        continue

                    self._write_text(
                        debug_base / f"post_{combo_counter}_{year}_{procedure_code}_{safe_keyword}.xml",
                        response_text[:1000000],
                    )

                    rows = self._parse_search_response(
                        response_text=response_text,
                        page_url=SEARCH_URL,
                        year=year,
                        procedure_label=procedure_label,
                        keyword=keyword,
                    )

                    self._write_json(
                        debug_base / f"rows_{combo_counter}_{year}_{procedure_code}_{safe_keyword}.json",
                        rows[:200],
                    )

                    for row in rows:
                        raw_text = row.get("raw_text") or ""

                        if not self._is_pv_related(raw_text):
                            continue

                        normalized = self._normalize_search_row(row)
                        if not normalized:
                            continue

                        external_id = self._build_external_id(
                            "search",
                            normalized.get("date"),
                            normalized["title"],
                            normalized.get("proponent"),
                            normalized.get("municipality"),
                        )

                        if external_id in seen_ids:
                            continue

                        seen_ids.add(external_id)
                        matched_rows.append(normalized)

                        results.append(
                            CollectorResult(
                                external_id=external_id,
                                source_url=normalized.get("url") or SEARCH_URL,
                                title=normalized["title"][:250],
                                payload={
                                    "title": normalized["title"][:500],
                                    "proponent": normalized.get("proponent"),
                                    "status_raw": normalized.get("status"),
                                    "region": "Sardegna",
                                    "province": normalized.get("province"),
                                    "municipalities": (
                                        [normalized["municipality"]]
                                        if normalized.get("municipality")
                                        else []
                                    ),
                                    "power": normalized.get("power"),
                                    "project_type_hint": normalized.get("procedure") or "Sardegna VIA",
                                },
                            )
                        )

        self._write_json(debug_base / "matched_rows_sample.json", matched_rows[:200])
        self._write_json(
            debug_base / "summary_search.json",
            {
                "years": SEARCH_YEARS,
                "procedures": SEARCH_PROCEDURES,
                "keywords": SEARCH_KEYWORDS,
                "results": len(results),
                "matched_rows": len(matched_rows),
            },
        )

        return results

    def _get_search_form(self) -> tuple[str, str, str, str]:
        response = self.session.get(
            SEARCH_URL,
            timeout=90,
            headers={
                "User-Agent": "PV-Agent-MVP/0.1",
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            },
        )
        response.raise_for_status()

        html_page = response.text
        soup = BeautifulSoup(html_page, "html.parser")

        form = soup.find("form", id=FORM)
        if not form:
            raise RuntimeError("Form JSF non trovato")

        action = form.get("action")
        if not action:
            raise RuntimeError("Action form non trovata")
        action = urljoin(SEARCH_URL, html.unescape(action))

        viewstate_el = soup.find("input", {"name": "javax.faces.ViewState"})
        if not viewstate_el:
            raise RuntimeError("javax.faces.ViewState non trovato")
        viewstate = viewstate_el.get("value")
        if not viewstate:
            raise RuntimeError("javax.faces.ViewState vuoto")

        encoded_el = soup.find("input", {"name": "javax.faces.encodedURL"})
        if encoded_el and encoded_el.get("value"):
            encoded_url = html.unescape(encoded_el.get("value"))
        else:
            encoded_url = action.replace(
                "_facesViewIdResource=",
                "_jsfBridgeAjax=true&_facesViewIdResource=",
            )

        return html_page, action, encoded_url, viewstate

    def _post_search(
        self,
        action: str,
        encoded_url: str,
        viewstate: str,
        year: str,
        procedure_code: str,
        keyword: str,
    ) -> str:
        payload = {
            FORM: FORM,
            "javax.faces.encodedURL": encoded_url,
            f"{FORM}:a_focus": "",
            f"{FORM}:a_input": procedure_code,
            f"{FORM}:b": "",
            f"{FORM}:c": "",
            f"{FORM}:d_focus": "",
            f"{FORM}:d_input": year,
            f"{FORM}:e_focus": "",
            f"{FORM}:e_input": "_",
            f"{FORM}:f_focus": "",
            f"{FORM}:f_input": "_",
            f"{FORM}:g": keyword,
            f"{FORM}:toggleable_collapsed": "false",
            f"{FORM}:confirmForm": "xx",
            "javax.faces.ViewState": viewstate,
            "javax.faces.partial.ajax": "true",
            "javax.faces.source": f"{FORM}:cerca",
            "javax.faces.partial.execute": "@all",
            "javax.faces.partial.render": FORM,
            f"{FORM}:cerca": f"{FORM}:cerca",
        }

        headers = {
            "User-Agent": "PV-Agent-MVP/0.1",
            "Faces-Request": "partial/ajax",
            "X-Requested-With": "XMLHttpRequest",
            "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
            "Origin": BASE_URL,
            "Referer": SEARCH_URL,
            "Accept": "application/xml, text/xml, */*; q=0.01",
        }

        response = self.session.post(
            action,
            data=payload,
            headers=headers,
            timeout=90,
        )
        response.raise_for_status()
        return response.text

    def _parse_search_response(
        self,
        response_text: str,
        page_url: str,
        year: str,
        procedure_label: str,
        keyword: str,
    ) -> list[dict]:
        snippets = self._extract_jsf_update_snippets(response_text)

        if not snippets:
            snippets = [response_text]

        rows: list[dict] = []

        for snippet in snippets:
            soup = BeautifulSoup(snippet, "html.parser")

            parsed_rows = self._parse_table_rows(
                soup=soup,
                page_url=page_url,
                year=year,
                procedure_label=procedure_label,
                keyword=keyword,
            )
            rows.extend(parsed_rows)

        # Niente fallback su plain text:
        # il form contiene select, comuni e keyword e genera falsi positivi.
        return rows

    def _extract_jsf_update_snippets(self, response_text: str) -> list[str]:
        snippets: list[str] = []

        for match in re.finditer(
            r"<update[^>]*>(.*?)</update>",
            response_text,
            flags=re.IGNORECASE | re.DOTALL,
        ):
            content = match.group(1)
            content = re.sub(r"^\s*<!\[CDATA\[", "", content)
            content = re.sub(r"\]\]>\s*$", "", content)
            content = html.unescape(content)
            content = content.strip()

            if content:
                snippets.append(content)

        return snippets

    def _parse_table_rows(
        self,
        soup: BeautifulSoup,
        page_url: str,
        year: str,
        procedure_label: str,
        keyword: str,
    ) -> list[dict]:
        rows: list[dict] = []

        tables = soup.find_all("table")
        if not tables:
            return rows

        for table in tables:
            table_id = table.get("id") or ""
            table_text_norm = self._normalize_for_match(table.get_text(" ", strip=True))

            # Prende solo la tabella risultati vera.
            # Evita tabelle/select/form della maschera di ricerca.
            looks_like_result_table = (
                "tblresult" in table_id.lower()
                or (
                    "proponente" in table_text_norm
                    and (
                        "titolo" in table_text_norm
                        or "progetto" in table_text_norm
                        or "comune" in table_text_norm
                        or "procedimento" in table_text_norm
                    )
                )
            )

            if not looks_like_result_table:
                continue

            headers = [
                self._clean_text(th.get_text(" ", strip=True))
                for th in table.find_all("th")
            ]

            for tr in table.find_all("tr"):
                cells = tr.find_all("td")
                if not cells:
                    continue

                values = [
                    self._clean_text(td.get_text(" ", strip=True))
                    for td in cells
                ]

                raw_text = self._clean_text(" | ".join(values))
                if not raw_text:
                    continue

                raw_norm = self._normalize_for_match(raw_text)

                # Scarta righe che sono chiaramente il form di ricerca,
                # non risultati.
                if (
                    "selezionare un comune" in raw_norm
                    or "abbasanta aggius aglientu" in raw_norm
                    or "tipo procedimento" in raw_norm
                    or "anno protocollo" in raw_norm
                    or "categoria progettuale" in raw_norm
                ):
                    continue

                # Deve sembrare una riga progetto, non una select.
                if not any(
                    marker in raw_norm
                    for marker in [
                        "fotovoltaico",
                        "agrivoltaico",
                        "agrovoltaico",
                        "impianto",
                        "proponente",
                        "comune",
                        "via",
                        "paur",
                        "verifica",
                    ]
                ):
                    continue

                record = {
                    "raw_text": raw_text,
                    "year": year,
                    "procedure": procedure_label,
                    "keyword": keyword,
                    "url": self._extract_first_url(tr, page_url) or page_url,
                }

                if headers and len(headers) == len(values):
                    for header, value in zip(headers, values):
                        if header:
                            record[self._normalize_header(header)] = value

                rows.append(record)

        return rows

    def _normalize_search_row(self, row: dict) -> dict | None:
        raw_text = self._clean_text(row.get("raw_text") or "")
        if not raw_text:
            return None

        raw_norm = self._normalize_for_match(raw_text)

        # Anti-falsi positivi: evita di importare la tendina comuni.
        if (
            "selezionare un comune" in raw_norm
            or "abbasanta aggius aglientu" in raw_norm
            or raw_norm.startswith("comune | selezionare un comune")
        ):
            return None

        title = self._extract_title_from_search_row(row, raw_text)
        if not title:
            return None

        title_norm = self._normalize_for_match(title)

        if (
            "selezionare un comune" in title_norm
            or "abbasanta aggius aglientu" in title_norm
            or title_norm.startswith("comune | selezionare un comune")
        ):
            return None

        # Deve contenere almeno una parola forte FV/agri-FV.
        if not self._is_pv_related(raw_text + " " + title):
            return None

        date = self._extract_date_from_text(raw_text) or row.get("year")
        procedure = row.get("procedure") or self._extract_procedure(raw_text)
        status = self._extract_status(raw_text)
        proponent = self._extract_proponent_from_search_row(row, raw_text)
        municipality = self._extract_municipality_from_search_row(row, raw_text)
        province = self._extract_province(raw_text)
        power = self._extract_power_text(title) or self._extract_power_text(raw_text)
        url = row.get("url") or SEARCH_URL

        return {
            "date": date,
            "title": title,
            "procedure": procedure,
            "proponent": proponent,
            "municipality": municipality,
            "province": province,
            "status": status,
            "power": power,
            "url": url,
        }

    def _extract_title_from_search_row(self, row: dict, raw_text: str) -> str | None:
        preferred_keys = [
            "progetto",
            "titolo",
            "oggetto",
            "intervento",
            "descrizione",
            "denominazione",
            "categoria_progettuale",
            "categoria",
        ]

        for key in preferred_keys:
            value = row.get(key)
            if value and len(value) >= 10:
                return self._clean_text(value)[:700]

        return raw_text[:700]

    def _extract_proponent_from_search_row(self, row: dict, raw_text: str) -> str | None:
        for key in ["proponente", "societa", "società", "richiedente"]:
            value = row.get(key)
            if value and 2 <= len(value) <= 180:
                return self._clean_text(value)

        return self._extract_proponent(raw_text)

    def _extract_municipality_from_search_row(self, row: dict, raw_text: str) -> str | None:
        for key in ["comune", "comuni", "localizzazione"]:
            value = row.get(key)
            if value:
                cleaned = self._clean_municipality(value)
                if cleaned:
                    return cleaned

        return self._extract_municipality(raw_text)

    # ---------------------------------------------------------------------
    # ESTRAZIONI COMUNI
    # ---------------------------------------------------------------------

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
                "agro-fotovoltaico",
                "agrofotovoltaico",
                "agrovoltaico",
                "solare fotovoltaico",
                "solare fotovoltaica",
            ]
        )

        if not has_strong_pv and any(k in lowered for k in NON_PV_EXCLUDE):
            return False

        return True

    def _extract_date_from_text(self, text: str) -> str | None:
        patterns = [
            r"\b([0-9]{2}/[0-9]{2}/[0-9]{4})\b",
            r"\b([0-9]{2}\.[0-9]{2}\.[0-9]{4})\b",
            r"\b([0-9]{1,2}\s+[a-zàéèìòù]+\s+[0-9]{4})\b",
        ]

        for pattern in patterns:
            m = re.search(pattern, text, flags=re.IGNORECASE)
            if m:
                return self._clean_text(m.group(1))

        return None

    def _extract_procedure(self, text: str) -> str | None:
        lowered = self._normalize_for_match(text)

        if "p.a.u.r" in lowered or "paur" in lowered:
            return "PAUR"

        if "verifica" in lowered:
            return "VERIFICA"

        if "valutazione preliminare" in lowered:
            return "VALUTAZIONE PRELIMINARE"

        if "v.i.a" in lowered or " via " in f" {lowered} ":
            return "VIA"

        return None

    def _extract_status(self, text: str) -> str | None:
        lowered = self._normalize_for_match(text)

        if "riavvio" in lowered or "riavvio l'iter" in lowered:
            return "Riavvio procedimento"

        if "favorevole con prescrizioni" in lowered:
            return "Favorevole con prescrizioni"

        if "favorevole" in lowered:
            return "Favorevole"

        if "negativo" in lowered or "non compatibile" in lowered:
            return "Negativo"

        if "improcedibil" in lowered:
            return "Improcedibile"

        if "archiviat" in lowered:
            return "Archiviato"

        if "osservazioni pervenute" in lowered:
            return "Osservazioni pervenute"

        if "integrazioni" in lowered:
            return "Integrazioni"

        if "ha depositato l'istanza" in lowered or "depositato l’istanza" in lowered:
            return "Istanza depositata"

        if "avvio del procedimento" in lowered:
            return "Avvio procedimento"

        return None

    def _extract_proponent(self, text: str) -> str | None:
        patterns = [
            r"Società Proponente\s*:?\s*(.+?)(?:,\s+per\s+la\s+quale|;\s+per\s+la\s+quale|\s+per\s+la\s+quale|$)",
            r"Societa Proponente\s*:?\s*(.+?)(?:,\s+per\s+la\s+quale|;\s+per\s+la\s+quale|\s+per\s+la\s+quale|$)",
            r"La società\s+(.+?)(?:,\s+in data|\s+in data|\s+ha\s+depositato|\s+ha\s+trasmesso|$)",
            r"La Società\s+(.+?)(?:,\s+in data|\s+in data|\s+ha\s+depositato|\s+ha\s+trasmesso|$)",
            r"dalla Società\s+(.+?)(?:,\s+per|\s+per\s+la\s+quale|$)",
            r"dal Proponente\s+(.+?)(?:,\s+per|\s+per\s+la\s+quale|$)",
            r"Proponente\s*:?\s*(.+?)(?:\s*\||,|;|$)",
        ]

        for pattern in patterns:
            m = re.search(pattern, text, flags=re.IGNORECASE)
            if m:
                value = self._clean_text(m.group(1))
                value = value.strip(" .,:;")

                value = re.sub(r"\s+relativo\s+all.*$", "", value, flags=re.IGNORECASE)
                value = re.sub(r"\s+per\s+la\s+quale.*$", "", value, flags=re.IGNORECASE)
                value = self._clean_text(value).strip(" .,:;")

                if 2 <= len(value) <= 180:
                    return value

        return None

    def _extract_municipality(self, text: str) -> str | None:
        patterns = [
            r"Comune di\s+([A-ZÀ-Ú][A-Za-zÀ-Úà-ú'`\- ]+?)(?:\s*\([A-Z]{2}\)|\.|,|;|\s+e\s+|$)",
            r"Comune\s*:?\s*([A-ZÀ-Ú][A-Za-zÀ-Úà-ú'`\- ]+?)(?:\s*-|\s+Provincia|\.|,|;|\||$)",
            r"in Comune di\s+([A-ZÀ-Ú][A-Za-zÀ-Úà-ú'`\- ]+?)(?:\s*\([A-Z]{2}\)|\.|,|;|$)",
            r"nel Comune di\s+([A-ZÀ-Ú][A-Za-zÀ-Úà-ú'`\- ]+?)(?:\s*\([A-Z]{2}\)|\.|,|;|$)",
            r"comune di\s+([A-ZÀ-Ú][A-Za-zÀ-Úà-ú'`\- ]+?)(?:\s*\([A-Z]{2}\)|\.|,|;|$)",
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

        for province in [
            "CITTÀ METROPOLITANA DI CAGLIARI",
            "CITTA METROPOLITANA DI CAGLIARI",
            "NUORO",
            "ORISTANO",
            "SASSARI",
            "SUD SARDEGNA",
        ]:
            if province.lower() in text.lower():
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

    def _extract_first_url(self, block, page_url: str) -> str | None:
        preferred: list[str] = []
        fallback: list[str] = []

        for a in block.find_all("a", href=True):
            href = a.get("href")
            label = self._clean_text(a.get_text(" ", strip=True)).lower()

            if not href:
                continue

            absolute_url = urljoin(page_url, href)

            if absolute_url.startswith("mailto:"):
                continue

            if any(x in label for x in ["leggi", "continua", "dettaglio", "procedimento"]):
                preferred.append(absolute_url)
            else:
                fallback.append(absolute_url)

        if preferred:
            return preferred[0]

        if fallback:
            return fallback[0]

        return None

    def _build_external_id(
        self,
        source_type: str,
        date: str | None,
        title: str,
        proponent: str | None,
        municipality: str | None,
    ) -> str:
        base = f"{source_type}|{date or ''}|{title}|{proponent or ''}|{municipality or ''}".lower()
        base = re.sub(r"\s+", "-", base)
        base = re.sub(r"[^a-z0-9:/._|-]", "", base)
        return base[:250]

    def _clean_municipality(self, value: str) -> str | None:
        value = self._clean_text(value)
        value = value.strip(" .:-,;()")

        if not value:
            return None

        if len(value) > 80:
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
            "proponente",
            "procedimento",
        ]

        lowered = value.lower()
        if any(w in lowered for w in bad_words):
            return None

        return value

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

    def _clean_text(self, value: str) -> str:
        value = html.unescape(value or "")
        return " ".join(value.replace("\xa0", " ").split()).strip()

    def _write_text(self, path: Path, content: str) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(content, encoding="utf-8")

    def _write_json(self, path: Path, obj) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(obj, ensure_ascii=False, indent=2), encoding="utf-8")