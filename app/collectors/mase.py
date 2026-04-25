from __future__ import annotations

import html
import re
import time
from urllib.parse import parse_qs, urlencode, urljoin, urlparse

from bs4 import BeautifulSoup

from app.collectors.base import BaseCollector, CollectorResult


BASE_URL = "https://va.mite.gov.it"
SEARCH_URL = "https://va.mite.gov.it/it-IT/Ricerca/Via"
VIA_TIPOLOGIA_URL = "https://va.mite.gov.it/it-IT/Ricerca/ViaTipologia"

# Test controllato. Quando validiamo bene i dati, puoi alzare o mettere None.
MAX_PAGES_PER_CATEGORY: int | None = 5

REQUEST_SLEEP_SECONDS = 0.25

TIPOLOGIE = [
    {
        "id": "38",
        "label": "Fotovoltaici",
        "project_type_hint": "Fotovoltaico",
    },
    {
        "id": "41",
        "label": "Agrivoltaici",
        "project_type_hint": "Agrivoltaico",
    },
]

PV_KEYWORDS = [
    "fotovoltaico",
    "fotovoltaica",
    "agrivoltaico",
    "agrivoltaica",
    "agrovoltaico",
    "agrovoltaica",
    "agrofotovoltaico",
    "agro-fotovoltaico",
    "impianto fotovoltaico",
    "impianto agrivoltaico",
    "impianto agrovoltaico",
]

NON_PV_EXCLUDE = [
    "eolico",
    "eolica",
    "offshore",
    "rifiuti",
    "discarica",
    "depuratore",
    "depurazione",
    "metanodotto",
    "gasdotto",
    "idroelettrico",
    "geotermico",
    "stradale",
    "ferroviaria",
    "aeroporto",
    "porto",
    "raffineria",
]

VALID_PROCEDURE_KEYWORDS = [
    "via",
    "valutazione di impatto ambientale",
    "valutazione impatto ambientale",
    "verifica",
    "verifica di assoggettabilità",
    "verifica di assoggettabilita",
    "pniec",
    "pnrr",
    "ottemperanza",
    "provvedimento",
    "scoping",
    "consultazione",
    "in corso",
    "conclusa",
    "concluso",
    "archiviata",
    "archiviato",
]

INVALID_PROCEDURE_VALUES = [
    "codice istanza online",
    "id",
    "codice",
    "localizzazione",
    "proponente",
    "progetto",
    "documentazione",
    "scheda",
    "info",
]


PROVINCE_TO_REGION = {
    # Abruzzo
    "AQ": "Abruzzo",
    "CH": "Abruzzo",
    "PE": "Abruzzo",
    "TE": "Abruzzo",
    # Basilicata
    "MT": "Basilicata",
    "PZ": "Basilicata",
    # Calabria
    "CZ": "Calabria",
    "CS": "Calabria",
    "KR": "Calabria",
    "RC": "Calabria",
    "VV": "Calabria",
    # Campania
    "AV": "Campania",
    "BN": "Campania",
    "CE": "Campania",
    "NA": "Campania",
    "SA": "Campania",
    # Emilia-Romagna
    "BO": "Emilia-Romagna",
    "FC": "Emilia-Romagna",
    "FE": "Emilia-Romagna",
    "MO": "Emilia-Romagna",
    "PC": "Emilia-Romagna",
    "PR": "Emilia-Romagna",
    "RA": "Emilia-Romagna",
    "RE": "Emilia-Romagna",
    "RN": "Emilia-Romagna",
    # Friuli-Venezia Giulia
    "GO": "Friuli-Venezia Giulia",
    "PN": "Friuli-Venezia Giulia",
    "TS": "Friuli-Venezia Giulia",
    "UD": "Friuli-Venezia Giulia",
    # Lazio
    "FR": "Lazio",
    "LT": "Lazio",
    "RI": "Lazio",
    "RM": "Lazio",
    "VT": "Lazio",
    # Liguria
    "GE": "Liguria",
    "IM": "Liguria",
    "SP": "Liguria",
    "SV": "Liguria",
    # Lombardia
    "BG": "Lombardia",
    "BS": "Lombardia",
    "CO": "Lombardia",
    "CR": "Lombardia",
    "LC": "Lombardia",
    "LO": "Lombardia",
    "MB": "Lombardia",
    "MI": "Lombardia",
    "MN": "Lombardia",
    "PV": "Lombardia",
    "SO": "Lombardia",
    "VA": "Lombardia",
    # Marche
    "AN": "Marche",
    "AP": "Marche",
    "FM": "Marche",
    "MC": "Marche",
    "PU": "Marche",
    # Molise
    "CB": "Molise",
    "IS": "Molise",
    # Piemonte
    "AL": "Piemonte",
    "AT": "Piemonte",
    "BI": "Piemonte",
    "CN": "Piemonte",
    "NO": "Piemonte",
    "TO": "Piemonte",
    "VB": "Piemonte",
    "VC": "Piemonte",
    # Puglia
    "BA": "Puglia",
    "BR": "Puglia",
    "BT": "Puglia",
    "FG": "Puglia",
    "LE": "Puglia",
    "TA": "Puglia",
    # Sardegna
    "CA": "Sardegna",
    "CI": "Sardegna",
    "NU": "Sardegna",
    "OR": "Sardegna",
    "SS": "Sardegna",
    "SU": "Sardegna",
    # Sicilia
    "AG": "Sicilia",
    "CL": "Sicilia",
    "CT": "Sicilia",
    "EN": "Sicilia",
    "ME": "Sicilia",
    "PA": "Sicilia",
    "RG": "Sicilia",
    "SR": "Sicilia",
    "TP": "Sicilia",
    # Toscana
    "AR": "Toscana",
    "FI": "Toscana",
    "GR": "Toscana",
    "LI": "Toscana",
    "LU": "Toscana",
    "MS": "Toscana",
    "PI": "Toscana",
    "PO": "Toscana",
    "PT": "Toscana",
    "SI": "Toscana",
    # Trentino-Alto Adige
    "BZ": "Trentino-Alto Adige",
    "TN": "Trentino-Alto Adige",
    # Umbria
    "PG": "Umbria",
    "TR": "Umbria",
    # Valle d'Aosta
    "AO": "Valle d'Aosta",
    # Veneto
    "BL": "Veneto",
    "PD": "Veneto",
    "RO": "Veneto",
    "TV": "Veneto",
    "VE": "Veneto",
    "VI": "Veneto",
    "VR": "Veneto",
}


# Mappa mirata per correggere i casi in cui la pagina MASE contiene testo territoriale sporco.
# La usiamo con priorità sul titolo, poi sul testo arricchito.
COMUNE_TO_PROVINCE = {
    "venezia": "VE",
    "taranto": "TA",
    "gonnesa": "SU",
    "giarratana": "RG",
    "nardo": "LE",
    "nardò": "LE",
    "potenza picena": "MC",
    "riano": "RM",
    "bientina": "PI",
    "montalto di castro": "VT",
    "argenta": "FE",
    "erchie": "BR",
    "monreale": "PA",
    "santa maria la fossa": "CE",
    "buonabitacolo": "SA",
    "bisaccia": "AV",
    "morcone": "BN",
    "scampitella": "AV",
    "montecalvo irpino": "AV",
    "giugliano in campania": "NA",
    "orta nova": "FG",
    "cerignola": "FG",
    "brindisi": "BR",
    "foggia": "FG",
    "mazara del vallo": "TP",
    "trapani": "TP",
    "salemi": "TP",
    "marsala": "TP",
    "bronte": "CT",
    "vizzini": "CT",
    "catania": "CT",
    "ragusa": "RG",
    "modica": "RG",
    "siracusa": "SR",
    "lentini": "SR",
    "carlentini": "SR",
    "gela": "CL",
    "licata": "AG",
    "agrigento": "AG",
    "viterbo": "VT",
    "tuscania": "VT",
    "canino": "VT",
    "montalto": "VT",
    "tarquinia": "VT",
    "roma": "RM",
    "guidonia montecelio": "RM",
    "aprilia": "LT",
    "latina": "LT",
    "pisa": "PI",
    "grosseto": "GR",
    "manciano": "GR",
    "scansano": "GR",
    "orbetello": "GR",
    "sinalunga": "SI",
    "ferrara": "FE",
    "ravenna": "RA",
    "faenza": "RA",
    "forli": "FC",
    "forlì": "FC",
    "cesena": "FC",
    "bologna": "BO",
    "modena": "MO",
    "reggio emilia": "RE",
    "mantova": "MN",
    "brescia": "BS",
    "bergamo": "BG",
    "pavia": "PV",
    "alessandria": "AL",
    "asti": "AT",
    "cuneo": "CN",
    "novara": "NO",
    "torino": "TO",
    "vercelli": "VC",
    "rovigo": "RO",
    "padova": "PD",
    "verona": "VR",
    "vicenza": "VI",
    "treviso": "TV",
    "udine": "UD",
}


class MaseCollector(BaseCollector):
    source_name = "mase"
    base_url = BASE_URL

    def fetch(self) -> list[CollectorResult]:
        results: list[CollectorResult] = []
        seen: set[str] = set()

        token = self._get_request_verification_token()

        for tipologia in TIPOLOGIE:
            page = 1
            last_page = 1

            while True:
                if MAX_PAGES_PER_CATEGORY is not None and page > MAX_PAGES_PER_CATEGORY:
                    break

                html_page = self._fetch_search_page(
                    token=token,
                    tipologia_id=tipologia["id"],
                    testo="",
                    page=page,
                )

                page_url = self._build_search_url(
                    token=token,
                    tipologia_id=tipologia["id"],
                    testo="",
                    page=page,
                )

                rows = self._parse_result_rows(
                    html_page=html_page,
                    page_url=page_url,
                    category_label=tipologia["label"],
                    project_type_hint=tipologia["project_type_hint"],
                )

                if not rows:
                    break

                last_page = max(last_page, self._extract_last_page(html_page))

                for row in rows:
                    normalized = self._normalize_row(row)

                    if normalized is None:
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
                                "title": normalized["title"][:700],
                                "proponent": normalized.get("proponent"),
                                "status_raw": normalized.get("status_raw"),
                                "region": normalized.get("region"),
                                "province": normalized.get("province"),
                                "municipalities": normalized.get("municipalities") or [],
                                "power": normalized.get("power"),
                                "project_type_hint": normalized.get("project_type_hint"),
                                "procedure": normalized.get("procedure"),
                                "category": normalized.get("category"),
                                "object_id": normalized.get("object_id"),
                                "document_url": normalized.get("document_url"),
                                "date_presented": normalized.get("date_presented"),
                                "date_last_update": normalized.get("date_last_update"),
                            },
                        )
                    )

                if page >= last_page:
                    break

                page += 1
                time.sleep(REQUEST_SLEEP_SECONDS)

        return results

    # ------------------------------------------------------------------
    # HTTP
    # ------------------------------------------------------------------

    def _headers(self) -> dict[str, str]:
        return {
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/147.0.0.0 Safari/537.36"
            ),
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "it-IT,it;q=0.9,en-US;q=0.8,en;q=0.7",
            "Referer": SEARCH_URL,
        }

    def _get_request_verification_token(self) -> str:
        response = self.session.get(
            SEARCH_URL,
            headers=self._headers(),
            timeout=90,
            allow_redirects=True,
        )
        response.raise_for_status()

        soup = BeautifulSoup(response.text, "html.parser")

        for form in soup.find_all("form"):
            action = urljoin(SEARCH_URL, form.get("action") or "")

            if "/Ricerca/ViaTipologia" not in action:
                continue

            token_input = form.find("input", attrs={"name": "__RequestVerificationToken"})
            if token_input and token_input.get("value"):
                return token_input.get("value") or ""

        token_input = soup.find("input", attrs={"name": "__RequestVerificationToken"})
        if token_input and token_input.get("value"):
            return token_input.get("value") or ""

        return ""

    def _build_search_url(
        self,
        token: str,
        tipologia_id: str,
        testo: str = "",
        page: int = 1,
    ) -> str:
        params = {
            "__RequestVerificationToken": token,
            "tipologiaID": tipologia_id,
            "testo": testo,
        }

        if page > 1:
            params["pagina"] = str(page)

        return f"{VIA_TIPOLOGIA_URL}?{urlencode(params)}"

    def _fetch_search_page(
        self,
        token: str,
        tipologia_id: str,
        testo: str = "",
        page: int = 1,
    ) -> str:
        url = self._build_search_url(
            token=token,
            tipologia_id=tipologia_id,
            testo=testo,
            page=page,
        )

        response = self.session.get(
            url,
            headers=self._headers(),
            timeout=90,
            allow_redirects=True,
        )
        response.raise_for_status()

        return response.content.decode("utf-8", errors="replace")

    def _fetch_info_page(self, url: str) -> str | None:
        try:
            response = self.session.get(
                url,
                headers=self._headers(),
                timeout=90,
                allow_redirects=True,
            )
            response.raise_for_status()
            return response.content.decode("utf-8", errors="replace")
        except Exception:
            return None

    # ------------------------------------------------------------------
    # SEARCH RESULT PARSING
    # ------------------------------------------------------------------

    def _parse_result_rows(
        self,
        html_page: str,
        page_url: str,
        category_label: str,
        project_type_hint: str,
    ) -> list[dict]:
        soup = BeautifulSoup(html_page, "html.parser")
        rows: list[dict] = []

        for table in soup.find_all("table"):
            table_text = self._clean_text(table.get_text(" ", strip=True))
            table_norm = self._normalize_for_match(table_text)

            if "progetto" not in table_norm or "proponente" not in table_norm:
                continue

            headers: list[str] = []

            for tr in table.find_all("tr"):
                cells = tr.find_all(["th", "td"])
                values = [self._clean_text(cell.get_text(" ", strip=True)) for cell in cells]

                if not values:
                    continue

                norm_values = [self._normalize_header(value) for value in values]

                if "progetto" in norm_values and "proponente" in norm_values:
                    headers = norm_values
                    continue

                if not headers:
                    continue

                if len(values) < 2:
                    continue

                record: dict = {
                    "category": category_label,
                    "project_type_hint": project_type_hint,
                    "page_url": page_url,
                    "raw_text": self._clean_text(" | ".join(values)),
                }

                for idx, header in enumerate(headers):
                    record[header] = values[idx] if idx < len(values) else ""

                links = self._extract_row_links(tr, page_url)
                record.update(links)

                rows.append(record)

        if rows:
            return rows

        # Fallback se il markup cambia.
        for a in soup.find_all("a", href=True):
            href = a.get("href") or ""
            absolute = urljoin(page_url, href)

            object_id = self._extract_object_id(absolute)
            if not object_id:
                continue

            container = a.find_parent(["tr", "li", "div", "article"]) or a
            text = self._clean_text(container.get_text(" ", strip=True))

            if not self._is_pv_related(text):
                continue

            links = self._extract_row_links(container, page_url)

            rows.append(
                {
                    "category": category_label,
                    "project_type_hint": project_type_hint,
                    "page_url": page_url,
                    "raw_text": text,
                    "progetto": text,
                    "proponente": None,
                    **links,
                }
            )

        return rows

    def _extract_row_links(self, node, page_url: str) -> dict:
        info_url = None
        doc_url = None
        object_id = None

        for a in node.find_all("a", href=True):
            href = a.get("href") or ""
            absolute = urljoin(page_url, href)

            if "/Oggetti/Info/" in absolute:
                info_url = absolute
                object_id = self._extract_object_id(absolute)

            if "/Oggetti/Documentazione/" in absolute:
                doc_url = absolute

        return {
            "source_url": info_url or page_url,
            "document_url": doc_url,
            "object_id": object_id,
        }

    def _extract_last_page(self, html_page: str) -> int:
        soup = BeautifulSoup(html_page, "html.parser")
        last_page = 1

        for a in soup.find_all("a", href=True):
            label = self._clean_text(a.get_text(" ", strip=True)).lower()
            href = a.get("href") or ""
            url = urljoin(VIA_TIPOLOGIA_URL, href)

            parsed = urlparse(url)
            params = parse_qs(parsed.query)

            if "pagina" not in params:
                continue

            try:
                page = int(params["pagina"][0])
            except Exception:
                continue

            if label in {"ultima", "ultimo", "last"}:
                return max(last_page, page)

            last_page = max(last_page, page)

        return last_page

    # ------------------------------------------------------------------
    # ROW NORMALIZATION
    # ------------------------------------------------------------------

    def _normalize_row(self, row: dict) -> dict | None:
        title = (
            row.get("progetto")
            or row.get("titolo")
            or row.get("oggetto")
            or row.get("raw_text")
            or ""
        )
        title = self._clean_text(title)

        proponent = row.get("proponente") or row.get("proponenti") or None
        proponent = self._clean_proponent(proponent)

        raw_procedure = (
            row.get("ultima_procedura")
            or row.get("procedura")
            or row.get("ultimo_procedimento")
            or row.get("tipologia_procedura")
            or None
        )
        procedure = self._clean_procedure(raw_procedure)
        status_raw = procedure

        source_url = row.get("source_url") or row.get("page_url") or SEARCH_URL
        document_url = row.get("document_url")
        object_id = row.get("object_id") or self._extract_object_id(source_url)

        raw_text = self._clean_text(
            " ".join(
                [
                    title,
                    proponent or "",
                    procedure or "",
                    row.get("raw_text") or "",
                ]
            )
        )

        if not title:
            return None

        if not self._is_pv_related(raw_text):
            return None

        info_data = {}
        if source_url and object_id:
            info_html = self._fetch_info_page(source_url)
            if info_html:
                info_data = self._parse_info_page(info_html, source_url)
                time.sleep(REQUEST_SLEEP_SECONDS)

        if info_data.get("title"):
            title = info_data["title"]

        if info_data.get("proponent"):
            proponent = info_data["proponent"]

        info_procedure = self._clean_procedure(info_data.get("procedure"))
        if info_procedure:
            procedure = info_procedure
            status_raw = info_procedure

        enriched_text = self._clean_text(
            " ".join(
                [
                    title,
                    raw_text,
                    info_data.get("plain_text_sample") or "",
                ]
            )
        )

        province = self._resolve_province(
            title=title,
            raw_text=raw_text,
            enriched_text=enriched_text,
            info_data=info_data,
        )
        region = self._region_from_province(province)

        municipalities = self._resolve_municipalities(
            title=title,
            enriched_text=enriched_text,
            info_data=info_data,
        )

        power = info_data.get("power") or self._extract_power_text(enriched_text)
        date_presented = info_data.get("date_presented")
        date_last_update = info_data.get("date_last_update")

        project_type_hint = row.get("project_type_hint")
        if project_type_hint not in {"Fotovoltaico", "Agrivoltaico"}:
            project_type_hint = self._infer_project_type(enriched_text) or project_type_hint

        external_id = f"mase_{object_id}" if object_id else self._build_fallback_external_id(title, proponent)

        return {
            "external_id": external_id,
            "source_url": source_url,
            "title": title,
            "proponent": proponent,
            "status_raw": status_raw,
            "region": region,
            "province": province,
            "municipalities": municipalities,
            "power": power,
            "project_type_hint": project_type_hint,
            "procedure": procedure,
            "category": row.get("category"),
            "object_id": object_id,
            "document_url": document_url,
            "date_presented": date_presented,
            "date_last_update": date_last_update,
        }

    def _resolve_province(
        self,
        title: str,
        raw_text: str,
        enriched_text: str,
        info_data: dict,
    ) -> str | None:
        """
        Ordine volutamente conservativo:
        1. titolo progetto;
        2. comuni noti nel titolo;
        3. testo riga risultato;
        4. comuni noti nel testo riga;
        5. dati scheda Info;
        6. testo pagina completo, solo come fallback.

        Questo evita di prendere province spurie da footer, menu, riferimenti accessori o documenti allegati.
        """
        return (
            self._extract_province(title)
            or self._province_from_known_municipality(title)
            or self._extract_province(raw_text)
            or self._province_from_known_municipality(raw_text)
            or info_data.get("province")
            or self._province_from_known_municipality(enriched_text)
            or self._extract_province(enriched_text)
        )

    def _resolve_municipalities(
        self,
        title: str,
        enriched_text: str,
        info_data: dict,
    ) -> list[str]:
        municipalities = self._extract_municipalities(title)

        if municipalities:
            return municipalities

        municipalities = self._municipalities_from_known_map(title)

        if municipalities:
            return municipalities

        municipalities = info_data.get("municipalities") or []

        if municipalities:
            return municipalities

        municipalities = self._municipalities_from_known_map(enriched_text)

        if municipalities:
            return municipalities

        return self._extract_municipalities(enriched_text)

    # ------------------------------------------------------------------
    # INFO PAGE PARSING
    # ------------------------------------------------------------------

    def _parse_info_page(self, html_page: str, page_url: str) -> dict:
        soup = BeautifulSoup(html_page, "html.parser")
        plain = self._clean_text(soup.get_text(" ", strip=True))

        title = self._extract_info_title(soup)
        info_table = self._extract_key_value_data(soup)

        proponent = self._find_value_by_keys(
            info_table,
            [
                "proponente",
                "proponenti",
                "societa proponente",
                "società proponente",
                "soggetto proponente",
            ],
        )

        raw_procedure = self._find_value_by_keys(
            info_table,
            [
                "procedura",
                "ultima procedura",
                "tipo procedura",
                "tipologia procedura",
                "procedimento",
                "procedura in corso",
            ],
        )
        procedure = self._clean_procedure(raw_procedure)

        date_presented = self._find_value_by_keys(
            info_table,
            [
                "data presentazione",
                "data avvio",
                "data deposito",
                "data pubblicazione",
            ],
        )

        date_last_update = self._find_value_by_keys(
            info_table,
            [
                "ultimo aggiornamento",
                "data aggiornamento",
                "data provvedimento",
            ],
        )

        province = (
            self._extract_province(title or "")
            or self._province_from_known_municipality(title or "")
            or self._province_from_known_municipality(plain)
            or self._extract_province(plain)
        )

        region = self._region_from_province(province)

        municipalities = (
            self._extract_municipalities(title or "")
            or self._municipalities_from_known_map(title or "")
            or self._municipalities_from_known_map(plain)
            or self._extract_municipalities(plain)
        )

        return {
            "title": title,
            "proponent": self._clean_proponent(proponent),
            "procedure": procedure,
            "region": region,
            "province": province,
            "municipalities": municipalities,
            "power": self._extract_power_text(plain),
            "date_presented": self._clean_text(date_presented or "") or None,
            "date_last_update": self._clean_text(date_last_update or "") or None,
            "plain_text_sample": plain[:3000],
            "page_url": page_url,
        }

    def _extract_info_title(self, soup: BeautifulSoup) -> str | None:
        candidates = []

        for selector in ["h1", "h2", ".titolo", ".title", ".oggetto"]:
            for node in soup.select(selector):
                text = self._clean_text(node.get_text(" ", strip=True))
                if text:
                    candidates.append(text)

        for candidate in candidates:
            norm = self._normalize_for_match(candidate)

            if "valutazioni e autorizzazioni" in norm:
                continue

            if "progetti - via" in norm:
                continue

            if "ricerca progetti" in norm:
                continue

            if len(candidate) > 20:
                return candidate

        title = soup.title.get_text(" ", strip=True) if soup.title else ""
        title = self._clean_text(title)

        if title:
            return title

        return None

    def _extract_key_value_data(self, soup: BeautifulSoup) -> dict[str, str]:
        data: dict[str, str] = {}

        for tr in soup.find_all("tr"):
            cells = [
                self._clean_text(cell.get_text(" ", strip=True))
                for cell in tr.find_all(["th", "td"])
            ]

            if len(cells) < 2:
                continue

            key = self._normalize_for_match(cells[0])
            value = cells[1]

            if key and value:
                data[key] = value

        for dt in soup.find_all("dt"):
            dd = dt.find_next_sibling("dd")
            if not dd:
                continue

            key = self._normalize_for_match(dt.get_text(" ", strip=True))
            value = self._clean_text(dd.get_text(" ", strip=True))

            if key and value:
                data[key] = value

        return data

    def _find_value_by_keys(self, data: dict[str, str], keys: list[str]) -> str | None:
        if not data:
            return None

        normalized_keys = [self._normalize_for_match(key) for key in keys]

        for key, value in data.items():
            key_norm = self._normalize_for_match(key)

            if any(target in key_norm for target in normalized_keys):
                return value

        return None

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
                "agrivoltaica",
                "agrovoltaico",
                "agrovoltaica",
            ]
        )

        if not has_strong_pv and any(exclude in norm for exclude in NON_PV_EXCLUDE):
            return False

        return True

    def _infer_project_type(self, text: str | None) -> str | None:
        norm = self._normalize_for_match(text or "")

        if "agrivoltaic" in norm or "agrovoltaic" in norm:
            return "Agrivoltaico"

        if "fotovoltaic" in norm:
            return "Fotovoltaico"

        return None

    def _clean_procedure(self, value: str | None) -> str | None:
        value = self._clean_text(value or "")

        if not value:
            return None

        norm = self._normalize_for_match(value)

        if any(invalid == norm for invalid in INVALID_PROCEDURE_VALUES):
            return None

        if any(invalid in norm for invalid in INVALID_PROCEDURE_VALUES):
            if not any(valid in norm for valid in VALID_PROCEDURE_KEYWORDS):
                return None

        if not any(keyword in norm for keyword in VALID_PROCEDURE_KEYWORDS):
            return None

        return value

    def _extract_object_id(self, url: str | None) -> str | None:
        if not url:
            return None

        match = re.search(r"/Oggetti/Info/(\d+)", url)
        if match:
            return match.group(1)

        match = re.search(r"/Oggetti/Documentazione/(\d+)", url)
        if match:
            return match.group(1)

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

    def _extract_province(self, text: str | None) -> str | None:
        if not text:
            return None

        # Prima cerca sigle in contesto territoriale chiaro.
        context_patterns = [
            r"\b(?:provincia|prov\.|comune|comuni|località|localita)\b[^.;,\n]{0,120}\(([A-Z]{2})\)",
            r"\(([A-Z]{2})\)[^.;,\n]{0,120}\b(?:provincia|prov\.|comune|comuni|località|localita)\b",
        ]

        for pattern in context_patterns:
            matches = re.findall(pattern, text, flags=re.IGNORECASE)
            for match in matches:
                code = match.upper()
                if code in PROVINCE_TO_REGION:
                    return code

        # Poi le sigle tra parentesi, ma solo se sono province vere.
        matches = re.findall(r"\(([A-Z]{2})\)", text)
        for match in matches:
            code = match.upper()
            if code in PROVINCE_TO_REGION:
                return code

        province_names = {
            "Agrigento": "AG",
            "Alessandria": "AL",
            "Ancona": "AN",
            "Aosta": "AO",
            "Arezzo": "AR",
            "Ascoli Piceno": "AP",
            "Asti": "AT",
            "Avellino": "AV",
            "Bari": "BA",
            "Barletta-Andria-Trani": "BT",
            "Belluno": "BL",
            "Benevento": "BN",
            "Bergamo": "BG",
            "Biella": "BI",
            "Bologna": "BO",
            "Bolzano": "BZ",
            "Brescia": "BS",
            "Brindisi": "BR",
            "Cagliari": "CA",
            "Caltanissetta": "CL",
            "Campobasso": "CB",
            "Caserta": "CE",
            "Catania": "CT",
            "Catanzaro": "CZ",
            "Chieti": "CH",
            "Como": "CO",
            "Cosenza": "CS",
            "Cremona": "CR",
            "Crotone": "KR",
            "Cuneo": "CN",
            "Enna": "EN",
            "Fermo": "FM",
            "Ferrara": "FE",
            "Firenze": "FI",
            "Foggia": "FG",
            "Forlì-Cesena": "FC",
            "Frosinone": "FR",
            "Genova": "GE",
            "Gorizia": "GO",
            "Grosseto": "GR",
            "Imperia": "IM",
            "Isernia": "IS",
            "La Spezia": "SP",
            "L'Aquila": "AQ",
            "Latina": "LT",
            "Lecce": "LE",
            "Lecco": "LC",
            "Livorno": "LI",
            "Lodi": "LO",
            "Lucca": "LU",
            "Macerata": "MC",
            "Mantova": "MN",
            "Massa-Carrara": "MS",
            "Matera": "MT",
            "Messina": "ME",
            "Milano": "MI",
            "Modena": "MO",
            "Monza e Brianza": "MB",
            "Napoli": "NA",
            "Novara": "NO",
            "Nuoro": "NU",
            "Oristano": "OR",
            "Padova": "PD",
            "Palermo": "PA",
            "Parma": "PR",
            "Pavia": "PV",
            "Perugia": "PG",
            "Pesaro e Urbino": "PU",
            "Pescara": "PE",
            "Piacenza": "PC",
            "Pisa": "PI",
            "Pistoia": "PT",
            "Pordenone": "PN",
            # "Potenza": "PZ",  # NON INSERIRE: crea falsi positivi con "potenza impianto".
            "Prato": "PO",
            "Ragusa": "RG",
            "Ravenna": "RA",
            "Reggio Calabria": "RC",
            "Reggio Emilia": "RE",
            "Rieti": "RI",
            "Rimini": "RN",
            "Roma": "RM",
            "Rovigo": "RO",
            "Salerno": "SA",
            "Sassari": "SS",
            "Savona": "SV",
            "Siena": "SI",
            "Siracusa": "SR",
            "Sondrio": "SO",
            "Sud Sardegna": "SU",
            "Carbonia-Iglesias": "CI",
            "Taranto": "TA",
            "Teramo": "TE",
            "Terni": "TR",
            "Torino": "TO",
            "Trapani": "TP",
            "Trento": "TN",
            "Treviso": "TV",
            "Trieste": "TS",
            "Udine": "UD",
            "Varese": "VA",
            "Venezia": "VE",
            "Verbano-Cusio-Ossola": "VB",
            "Vercelli": "VC",
            "Verona": "VR",
            "Vibo Valentia": "VV",
            "Vicenza": "VI",
            "Viterbo": "VT",
        }

        norm_text = self._normalize_for_match(text)

        for name, code in sorted(province_names.items(), key=lambda item: len(item[0]), reverse=True):
            name_norm = self._normalize_for_match(name)
            if re.search(rf"\b{re.escape(name_norm)}\b", norm_text):
                return code

        return None

    def _province_from_known_municipality(self, text: str | None) -> str | None:
        if not text:
            return None

        norm_text = self._normalize_for_match(text)

        for municipality, province in sorted(COMUNE_TO_PROVINCE.items(), key=lambda item: len(item[0]), reverse=True):
            municipality_norm = self._normalize_for_match(municipality)

            if re.search(rf"\b{re.escape(municipality_norm)}\b", norm_text):
                return province

        return None

    def _municipalities_from_known_map(self, text: str | None) -> list[str]:
        if not text:
            return []

        norm_text = self._normalize_for_match(text)
        municipalities: list[str] = []

        for municipality in sorted(COMUNE_TO_PROVINCE.keys(), key=len, reverse=True):
            municipality_norm = self._normalize_for_match(municipality)

            if re.search(rf"\b{re.escape(municipality_norm)}\b", norm_text):
                formatted = municipality.upper()
                if formatted not in municipalities:
                    municipalities.append(formatted)

        return municipalities[:10]

    def _region_from_province(self, province: str | None) -> str | None:
        if not province:
            return None

        return PROVINCE_TO_REGION.get(province.upper())

    def _extract_municipalities(self, text: str | None) -> list[str]:
        if not text:
            return []

        values: list[str] = []

        patterns = [
            r"Comune di\s+([A-ZÀ-Ú][A-Za-zÀ-Úà-ú'`\- ]+?)(?:\s*\([A-Z]{2}\)|,|\.|;|$)",
            r"Comuni di\s+([A-ZÀ-Ú][A-Za-zÀ-Úà-ú'`\- ,]+?)(?:\s*\([A-Z]{2}\)|\.|;|$)",
            r"nel Comune di\s+([A-ZÀ-Ú][A-Za-zÀ-Úà-ú'`\- ]+?)(?:\s*\([A-Z]{2}\)|,|\.|;|$)",
            r"nei Comuni di\s+([A-ZÀ-Ú][A-Za-zÀ-Úà-ú'`\- ,]+?)(?:\s*\([A-Z]{2}\)|\.|;|$)",
            r"in Comune di\s+([A-ZÀ-Ú][A-Za-zÀ-Úà-ú'`\- ]+?)(?:\s*\([A-Z]{2}\)|,|\.|;|$)",
            r"in località\s+[A-Za-zÀ-Úà-ú'`\- ]+?\s+nel Comune di\s+([A-ZÀ-Ú][A-Za-zÀ-Úà-ú'`\- ]+?)(?:\s*\([A-Z]{2}\)|,|\.|;|$)",
        ]

        for pattern in patterns:
            for match in re.finditer(pattern, text, flags=re.IGNORECASE):
                raw = match.group(1)

                for part in re.split(r",|\s+e\s+", raw, flags=re.IGNORECASE):
                    cleaned = self._clean_municipality(part)
                    if cleaned and cleaned not in values:
                        values.append(cleaned)

        return values[:10]

    def _clean_municipality(self, value: str | None) -> str | None:
        value = self._clean_text(value or "")
        value = value.strip(" .:-,;()")

        if not value:
            return None

        if len(value) > 70:
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
            "catasto",
            "foglio",
            "particella",
            "mwp",
            "mw",
            "kwp",
            "kvac",
            "autorizzazione",
            "valutazione",
            "verifica",
        ]

        lowered = value.lower()

        if any(word in lowered for word in bad_words):
            return None

        return value.upper()

    def _clean_proponent(self, value: str | None) -> str | None:
        value = self._clean_text(value or "")

        if not value:
            return None

        value = re.sub(r"^\(?\s*Azienda\s*:\s*", "", value, flags=re.IGNORECASE)
        value = value.strip(" ();")

        return value or None

    def _build_fallback_external_id(self, title: str, proponent: str | None) -> str:
        base = f"mase|{title}|{proponent or ''}"
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

    def _normalize_for_match(self, value: str | None) -> str:
        value = html.unescape(value or "")
        value = self._clean_text(value).lower()
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