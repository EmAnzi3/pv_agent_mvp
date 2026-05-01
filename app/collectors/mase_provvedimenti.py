from __future__ import annotations

import re
import time
from urllib.parse import urljoin

from bs4 import BeautifulSoup

from app.collectors.base import BaseCollector, CollectorResult
from app.collectors.mase import MaseCollector
from app.geo_enrichment import enrich_geo_from_text


BASE_URL = "https://va.mite.gov.it"
ULTIMI_PROVVEDIMENTI_URL = "https://va.mite.gov.it/it-IT/Comunicazione/UltimiProvvedimenti"

REQUEST_TIMEOUT = 90
REQUEST_SLEEP_SECONDS = 0.25

# Pagine recenti dei provvedimenti MASE.
MAX_PAGES = 20

TARGET_KEYWORDS = [
    "fotovoltaico",
    "agrivoltaico",
    "agrovoltaico",
    "agrofotovoltaico",
    "agro-fotovoltaico",
    "agro fotovoltaico",
    "solare",
    "impianto di produzione di energia elettrica da fonte rinnovabile",
]

POSITIVE_KEYWORDS = [
    "esito positivo",
    "giudizio positivo",
    "conclusa con esito positivo",
    "concluso con esito positivo",
    "compatibilità ambientale positiva",
    "compatibilita ambientale positiva",
    "parere positivo",
]

NEGATIVE_KEYWORDS = [
    "esito negativo",
    "giudizio negativo",
    "conclusa con esito negativo",
    "concluso con esito negativo",
    "parere negativo",
]

DECREE_PATTERNS = [
    r"\bD\.M\.\s*[^,\.;\n]+",
    r"\bDM[_\-\s]?\d{4}[-_]\d+",
    r"\bDecreto\s+(?:Direttoriale|Ministeriale)?\s*[^,\.;\n]+",
    r"\bn\.\s*\d+\s+del\s+\d{1,2}/\d{1,2}/\d{4}",
]

DATE_PATTERNS = [
    r"\b\d{1,2}/\d{1,2}/\d{4}\b",
    r"\b\d{4}-\d{2}-\d{2}\b",
]


class MaseProvvedimentiCollector(BaseCollector):
    source_name = "mase_provvedimenti"
    base_url = BASE_URL

    def fetch(self) -> list[CollectorResult]:
        detail_items = self._collect_detail_items()

        results: list[CollectorResult] = []
        seen: set[str] = set()

        for detail_item in detail_items:
            detail = self._fetch_and_parse_detail(detail_item)

            if not detail:
                continue

            if not self._is_relevant(detail):
                continue

            external_id = detail["external_id"]

            if external_id in seen:
                continue

            seen.add(external_id)

            results.append(
                CollectorResult(
                    external_id=external_id,
                    source_url=detail["source_url"],
                    title=detail["title"],
                    payload={
                        "title": detail["title"],
                        "proponent": detail["proponent"],
                        "status_raw": detail["status_raw"],
                        "region": detail["region"],
                        "province": detail["province"],
                        "municipalities": detail["municipalities"],
                        "power": detail["power"],
                        "power_mw": detail["power_mw"],
                        "project_type_hint": detail["project_type_hint"],
                        "procedure": detail["procedure"],
                        "category": "MASE – Provvedimenti",
                        "decree_number": detail["decree_number"],
                        "decree_date": detail["decree_date"],
                        "outcome": detail["outcome"],
                        "detail_url": detail["source_url"],
                        "project_url": detail["project_url"],
                        "document_url": detail["document_url"],
                        "list_title": detail["list_title"],
                        "detail_title": detail["detail_title"],
                        "project_info_title": detail["project_info_title"],
                        "project_info_proponent": detail["project_info_proponent"],
                        "plain_text_sample": detail["plain_text_sample"],
                    },
                )
            )

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
            "Referer": BASE_URL,
        }

    def _get_html(self, url: str) -> str | None:
        try:
            response = self.session.get(
                url,
                headers=self._headers(),
                timeout=REQUEST_TIMEOUT,
                allow_redirects=True,
            )
            response.raise_for_status()
            return response.content.decode("utf-8", errors="replace")
        except Exception:
            return None

    # ------------------------------------------------------------------
    # LIST PAGES
    # ------------------------------------------------------------------

    def _collect_detail_items(self) -> list[dict]:
        items: list[dict] = []

        for page in range(1, MAX_PAGES + 1):
            if page == 1:
                page_url = ULTIMI_PROVVEDIMENTI_URL
            else:
                page_url = f"{ULTIMI_PROVVEDIMENTI_URL}?pagina={page}"

            html_page = self._get_html(page_url)

            if not html_page:
                continue

            page_items = self._extract_detail_items_from_list_page(html_page, page_url)

            for item in page_items:
                if not any(existing["detail_url"] == item["detail_url"] for existing in items):
                    items.append(item)

            time.sleep(REQUEST_SLEEP_SECONDS)

        return items

    def _extract_detail_items_from_list_page(self, html_page: str, page_url: str) -> list[dict]:
        soup = BeautifulSoup(html_page, "html.parser")
        items: list[dict] = []

        for a in soup.find_all("a", href=True):
            href = a.get("href") or ""
            absolute = urljoin(page_url, href)

            if "/Comunicazione/DettaglioUltimiProvvedimenti/" not in absolute:
                continue

            list_title = self._clean_list_title(a.get_text(" ", strip=True))

            if not list_title:
                parent = a.find_parent(["li", "tr", "div", "article", "section"])
                if parent:
                    list_title = self._clean_list_title(parent.get_text(" ", strip=True))

            if not list_title:
                continue

            items.append(
                {
                    "detail_url": absolute,
                    "list_title": list_title,
                    "list_page_url": page_url,
                }
            )

        return items

    def _clean_list_title(self, value: str | None) -> str | None:
        text = self._clean_text(value)

        if not text:
            return None

        text = re.sub(r"\s+", " ", text).strip()
        text = text.replace("Dettaglio", "").strip(" -–—:")

        if self._is_generic_title(text):
            return None

        return text[:800]

    # ------------------------------------------------------------------
    # DETAIL PARSING
    # ------------------------------------------------------------------

    def _fetch_and_parse_detail(self, detail_item: dict) -> dict | None:
        detail_url = detail_item["detail_url"]
        list_title = detail_item.get("list_title") or ""

        html_page = self._get_html(detail_url)

        if not html_page:
            return None

        soup = BeautifulSoup(html_page, "html.parser")
        plain = self._clean_text(soup.get_text(" ", strip=True))

        if not plain:
            return None

        detail_id = self._extract_detail_id(detail_url)
        detail_title = self._extract_detail_title(soup, plain)

        if list_title and not self._is_generic_title(list_title):
            title = list_title
        elif detail_title and not self._is_generic_title(detail_title):
            title = detail_title
        else:
            title = self._fallback_title(plain)

        enriched_text = self._clean_text(
            " ".join(
                [
                    list_title or "",
                    detail_title or "",
                    title or "",
                    plain or "",
                ]
            )
        )

        # Non estrarre il proponente dalla pagina DettaglioUltimiProvvedimenti:
        # contiene menu e testo generico del portale. Il proponente affidabile
        # va preso dalla scheda "Vai al progetto" /Oggetti/Info/<id>.
        proponent = None

        procedure = self._extract_procedure(enriched_text)
        outcome = self._extract_outcome(enriched_text)
        decree_number = self._extract_decree_number(enriched_text)
        decree_date = self._extract_decree_date(enriched_text)

        project_url = self._extract_project_url(soup, detail_url)
        document_url = self._extract_document_url(soup, detail_url)

        project_info: dict = {}

        if project_url:
            project_html = self._get_html(project_url)
            if project_html:
                project_info = self._parse_project_info_page(project_html)
                time.sleep(REQUEST_SLEEP_SECONDS)

        project_info_proponent = self._clean_proponent(project_info.get("proponent"))
        proponent = project_info_proponent

        status_raw = self._build_status_raw(
            procedure=procedure,
            outcome=outcome,
            decree_number=decree_number,
        )

        power_text = self._extract_power_text(enriched_text)

        if not power_text and project_info.get("power"):
            power_text = project_info.get("power")

        if not power_text and project_info.get("plain_text"):
            power_text = self._extract_power_text(project_info["plain_text"])

        power_mw = self._power_text_to_mw(power_text)

        project_type_hint = self._infer_project_type(enriched_text)

        # Geografia: prima usa i dati strutturati della scheda progetto.
        # Solo se mancano, ricade sul titolo/list title del provvedimento.
        geo_text = self._clean_text(
            " ".join(
                [
                    list_title or "",
                    detail_title or "",
                    title or "",
                ]
            )
        )

        geo = enrich_geo_from_text(
            geo_text,
            existing_region=project_info.get("region"),
            existing_province=project_info.get("province"),
            existing_municipalities=project_info.get("municipalities"),
        )

        external_id = (
            f"mase_provvedimenti_{detail_id}"
            if detail_id
            else f"mase_provvedimenti_{self._normalize_key(detail_url)}"
        )

        return {
            "external_id": external_id,
            "source_url": detail_url,
            "title": title[:500],
            "proponent": proponent,
            "status_raw": status_raw,
            "region": geo.region,
            "province": geo.province,
            "municipalities": geo.municipalities or [],
            "power": power_text,
            "power_mw": power_mw,
            "project_type_hint": project_type_hint,
            "procedure": procedure,
            "decree_number": decree_number,
            "decree_date": decree_date,
            "outcome": outcome,
            "project_url": project_url,
            "document_url": document_url,
            "list_title": list_title,
            "detail_title": detail_title,
            "project_info_title": project_info.get("title"),
            "project_info_proponent": project_info_proponent,
            "plain_text_sample": enriched_text[:5000],
        }

    def _extract_detail_id(self, url: str) -> str | None:
        match = re.search(r"/DettaglioUltimiProvvedimenti/(\d+)", url)

        if match:
            return match.group(1)

        return None

    def _extract_detail_title(self, soup: BeautifulSoup, plain: str) -> str | None:
        candidates: list[str] = []

        for selector in ["h1", "h2", "h3", ".titolo", ".title", ".page-title"]:
            for node in soup.select(selector):
                text = self._clean_text(node.get_text(" ", strip=True))

                if text:
                    candidates.append(text)

        for candidate in candidates:
            if self._is_generic_title(candidate):
                continue

            if len(candidate) < 15:
                continue

            return candidate

        match = re.search(
            r"(Valutazione Impatto Ambientale|Valutazione di Impatto Ambientale|Verifica di Assoggettabilità a VIA|Provvedimento Unico in materia Ambientale|Verifica di Ottemperanza)\s*:\s*(.+?)(?:\s+Data pubblicazione|\s+Esito|\s+D\.M\.|\s+DM_|$)",
            plain,
            flags=re.IGNORECASE,
        )

        if match:
            value = self._clean_text(match.group(2))
            if value and not self._is_generic_title(value):
                return value

        return None

    def _fallback_title(self, plain: str) -> str:
        text = self._clean_text(plain) or "MASE Provvedimento"

        match = re.search(
            r"(Valutazione Impatto Ambientale|Valutazione di Impatto Ambientale|Verifica di Assoggettabilità a VIA|Provvedimento Unico in materia Ambientale|Verifica di Ottemperanza)\s*:?\s*(.+)",
            text,
            flags=re.IGNORECASE,
        )

        if match:
            text = match.group(0)

        if len(text) > 350:
            text = text[:350].rsplit(" ", 1)[0]

        return text

    def _is_generic_title(self, value: str | None) -> bool:
        text = self._normalize_for_match(value or "")

        generic_fragments = [
            "valutazioni e autorizzazioni ambientali vas via aia",
            "provvedimenti 2026 valutazioni ambientali",
            "provvedimenti 2026",
            "valutazioni ambientali",
            "vas via aia",
            "dettaglio valutazioni",
            "home comunicazione",
        ]

        if not text:
            return True

        if any(fragment in text for fragment in generic_fragments):
            return True

        if len(text) < 10:
            return True

        return False

    # ------------------------------------------------------------------
    # PROJECT INFO PAGE
    # ------------------------------------------------------------------

    def _parse_project_info_page(self, html_page: str) -> dict:
        """
        Riusa il parser MASE già sviluppato per /Oggetti/Info/<id>.
        È più affidabile della pagina DettaglioUltimiProvvedimenti.
        """
        try:
            parser = MaseCollector()
            info = parser._parse_info_page(html_page, "")
        except Exception:
            info = {}

        soup = BeautifulSoup(html_page, "html.parser")
        plain = self._clean_text(soup.get_text(" ", strip=True))
        key_values = self._extract_key_value_data(soup)

        proponent = (
            info.get("proponent")
            or self._find_value_by_keys(
                key_values,
                [
                    "proponente",
                    "proponenti",
                    "società proponente",
                    "societa proponente",
                    "soggetto proponente",
                    "richiedente",
                ],
            )
            or self._extract_project_info_proponent_from_plain(plain)
        )

        return {
            "title": info.get("title"),
            "proponent": self._clean_proponent(proponent),
            "region": info.get("region"),
            "province": info.get("province"),
            "municipalities": info.get("municipalities") or [],
            "power": info.get("power"),
            "plain_text": plain,
            "plain_text_sample": info.get("plain_text_sample") or plain[:5000],
            "key_values": key_values,
        }

    def _extract_key_value_data(self, soup: BeautifulSoup) -> dict[str, str]:
        data: dict[str, str] = {}

        # Tabelle classiche th/td o td/td.
        for row in soup.find_all("tr"):
            cells = row.find_all(["th", "td"])

            if len(cells) < 2:
                continue

            key = self._clean_text(cells[0].get_text(" ", strip=True))
            value = self._clean_text(cells[1].get_text(" ", strip=True))

            if key and value:
                data[key] = value

        # Liste descrittive dt/dd.
        for dt in soup.find_all("dt"):
            dd = dt.find_next_sibling("dd")

            if not dd:
                continue

            key = self._clean_text(dt.get_text(" ", strip=True))
            value = self._clean_text(dd.get_text(" ", strip=True))

            if key and value:
                data[key] = value

        # Fallback testuale.
        plain = self._clean_text(soup.get_text(" ", strip=True))

        if plain:
            proponent = self._extract_project_info_proponent_from_plain(plain)
            if proponent:
                data["proponente"] = proponent

        return data

    def _find_value_by_keys(self, data: dict[str, str], keys: list[str]) -> str | None:
        normalized_targets = [self._normalize_for_match(key) for key in keys]

        for key, value in data.items():
            key_norm = self._normalize_for_match(key)

            if any(target == key_norm or target in key_norm for target in normalized_targets):
                value = self._clean_proponent(value)

                if value:
                    return value

        return None

    def _clean_proponent(self, value: str | None) -> str | None:
        value = self._clean_text(value)

        if not value:
            return None

        # Evita date o numeri nel campo proponente.
        if re.fullmatch(r"\d{1,2}/\d{1,2}/\d{4}", value):
            return None

        if re.fullmatch(r"\d{4}-\d{2}-\d{2}", value):
            return None

        if re.fullmatch(r"\d+", value):
            return None

        bad_fragments = [
            "/gestore",
            "gestore eventi",
            "eventi e notizie",
            "provvedimenti 2026",
            "la direzione informa",
            "attività delle commissioni tecniche",
            "attivita delle commissioni tecniche",
            "domande frequenti",
            "procedure in corso",
            "avvisi al pubblico",
            "invio osservazioni",
            "consultazioni transfrontaliere",
            "procedure integrate",
            "valutazioni e autorizzazioni ambientali",
            "vas via aia",
            "ministero dell'ambiente",
            "mase",
        ]

        norm = self._normalize_for_match(value)
        bad_norms = [self._normalize_for_match(fragment) for fragment in bad_fragments]

        if any(fragment and fragment in norm for fragment in bad_norms):
            return None

        if len(value) > 300:
            return None

        return value

    def _extract_project_info_proponent_from_plain(self, plain: str) -> str | None:
        """
        Estrae il proponente dalla pagina /Oggetti/Info/<id>.

        Il portale MASE contiene anche voci di menu tipo:
        "Spazio per il proponente/gestore..."
        Quindi non basta cercare la prima parola 'Proponente':
        serve una label vera con ':' e bisogna scorrere tutti i match.
        """
        patterns = [
            r"\bProponente\s*:\s*(.+?)(?:\s+Tipologia\s+di\s+opera\s*:|\s+Scadenza\s+presentazione|\s+Territori\s+ed\s+aree|\s+Scegli\s+la\s+procedura|\s+Procedura\s+Codice|\s+Data\s+presentazione|\s+Oggetto\s*:|$)",
            r"\bSocietà\s+proponente\s*:\s*(.+?)(?:\s+Tipologia\s+di\s+opera\s*:|\s+Scadenza\s+presentazione|\s+Territori\s+ed\s+aree|\s+Scegli\s+la\s+procedura|\s+Procedura\s+Codice|\s+Data\s+presentazione|\s+Oggetto\s*:|$)",
            r"\bSocieta\s+proponente\s*:\s*(.+?)(?:\s+Tipologia\s+di\s+opera\s*:|\s+Scadenza\s+presentazione|\s+Territori\s+ed\s+aree|\s+Scegli\s+la\s+procedura|\s+Procedura\s+Codice|\s+Data\s+presentazione|\s+Oggetto\s*:|$)",
            r"\bSoggetto\s+proponente\s*:\s*(.+?)(?:\s+Tipologia\s+di\s+opera\s*:|\s+Scadenza\s+presentazione|\s+Territori\s+ed\s+aree|\s+Scegli\s+la\s+procedura|\s+Procedura\s+Codice|\s+Data\s+presentazione|\s+Oggetto\s*:|$)",
        ]

        for pattern in patterns:
            for match in re.finditer(pattern, plain, flags=re.IGNORECASE):
                value = self._clean_text(match.group(1))
                value = self._clean_proponent(value)

                if value:
                    return value

        return None

    # ------------------------------------------------------------------
    # FIELD EXTRACTION
    # ------------------------------------------------------------------

    def _extract_proponent(self, plain: str) -> str | None:
        """
        Tenuta solo come fallback/compatibilità.
        Non va usata sulla pagina DettaglioUltimiProvvedimenti.
        """
        patterns = [
            r"\bProponente\s*:?\s*(.+?)(?:\s+Procedura|\s+Data|\s+Esito|\s+D\.M\.|\s+DM_|\s+Vai al progetto|\s+Scarica|$)",
            r"\bSocietà proponente\s*:?\s*(.+?)(?:\s+Procedura|\s+Data|\s+Esito|\s+D\.M\.|\s+DM_|\s+Vai al progetto|\s+Scarica|$)",
            r"\bSocieta proponente\s*:?\s*(.+?)(?:\s+Procedura|\s+Data|\s+Esito|\s+D\.M\.|\s+DM_|\s+Vai al progetto|\s+Scarica|$)",
            r"\bSoggetto proponente\s*:?\s*(.+?)(?:\s+Procedura|\s+Data|\s+Esito|\s+D\.M\.|\s+DM_|\s+Vai al progetto|\s+Scarica|$)",
        ]

        for pattern in patterns:
            match = re.search(pattern, plain, flags=re.IGNORECASE)

            if match:
                value = self._clean_text(match.group(1))
                value = self._clean_proponent(value)

                if value:
                    return value

        return None

    def _extract_procedure(self, plain: str) -> str | None:
        procedures = [
            "Provvedimento Unico in materia Ambientale (PNIEC-PNRR)",
            "Valutazione Impatto Ambientale (PNIEC-PNRR)",
            "Verifica di Assoggettabilità a VIA (PNIEC-PNRR)",
            "Verifica di Ottemperanza (PNIEC-PNRR)",
            "Provvedimento Unico in materia Ambientale",
            "Valutazione di Impatto Ambientale",
            "Valutazione Impatto Ambientale",
            "Verifica di Assoggettabilità a VIA",
            "Verifica di Assoggettabilita a VIA",
            "Verifica di Ottemperanza",
            "Valutazione preliminare",
            "Proroga validità temporale provvedimento di VIA",
        ]

        plain_norm = self._normalize_for_match(plain)

        for procedure in procedures:
            if self._normalize_for_match(procedure) in plain_norm:
                return procedure

        return None

    def _extract_outcome(self, plain: str) -> str | None:
        norm = self._normalize_for_match(plain)

        if any(self._normalize_for_match(item) in norm for item in POSITIVE_KEYWORDS):
            return "positivo"

        if any(self._normalize_for_match(item) in norm for item in NEGATIVE_KEYWORDS):
            return "negativo"

        if "conclusa" in norm and "positivo" in norm:
            return "positivo"

        if "concluso" in norm and "positivo" in norm:
            return "positivo"

        if "conclusa" in norm and "negativo" in norm:
            return "negativo"

        if "concluso" in norm and "negativo" in norm:
            return "negativo"

        return None

    def _build_status_raw(
        self,
        procedure: str | None,
        outcome: str | None,
        decree_number: str | None,
    ) -> str:
        pieces = []

        if procedure:
            pieces.append(procedure)

        if outcome:
            pieces.append(f"Esito {outcome}")

        if decree_number:
            pieces.append(decree_number)

        return " - ".join(pieces) or "Provvedimento MASE"

    def _extract_decree_number(self, plain: str) -> str | None:
        for pattern in DECREE_PATTERNS:
            match = re.search(pattern, plain, flags=re.IGNORECASE)

            if match:
                value = self._clean_text(match.group(0))
                if value:
                    return value[:250]

        return None

    def _extract_decree_date(self, plain: str) -> str | None:
        match = re.search(
            r"(?:D\.M\.|DM[_\-\s]?\d{4}[-_]\d+|Decreto[^,\.;\n]{0,80})\s+del\s+(\d{1,2}/\d{1,2}/\d{4})",
            plain,
            flags=re.IGNORECASE,
        )
        if match:
            return match.group(1)

        match = re.search(r"\bdel\s+(\d{1,2}/\d{1,2}/\d{4})\b", plain, flags=re.IGNORECASE)
        if match:
            return match.group(1)

        for pattern in DATE_PATTERNS:
            match = re.search(pattern, plain)
            if match:
                return match.group(0)

        return None

    def _extract_project_url(self, soup: BeautifulSoup, page_url: str) -> str | None:
        for a in soup.find_all("a", href=True):
            label = self._clean_text(a.get_text(" ", strip=True)).lower()
            href = a.get("href") or ""
            absolute = urljoin(page_url, href)

            if "vai al progetto" in label:
                return absolute

            if "scheda progetto" in label:
                return absolute

            if "progetto" in label and "/Oggetti/Info/" in absolute:
                return absolute

            if "/Oggetti/Info/" in absolute:
                return absolute

        html = str(soup)

        patterns = [
            r'["\']([^"\']*/Oggetti/Info/\d+[^"\']*)["\']',
            r'["\']([^"\']*/it-IT/Oggetti/Info/\d+[^"\']*)["\']',
            r'href=["\']([^"\']+)["\'][^>]*>\s*Vai al progetto',
        ]

        for pattern in patterns:
            match = re.search(pattern, html, flags=re.IGNORECASE)

            if match:
                return urljoin(page_url, match.group(1))

        return None

    def _extract_document_url(self, soup: BeautifulSoup, page_url: str) -> str | None:
        for a in soup.find_all("a", href=True):
            label = self._clean_text(a.get_text(" ", strip=True)).lower()
            href = a.get("href") or ""
            absolute = urljoin(page_url, href)

            if "scarica il provvedimento" in label:
                return absolute

            if "/File/Provvedimento/" in absolute:
                return absolute

        return None

    # ------------------------------------------------------------------
    # FILTERS
    # ------------------------------------------------------------------

    def _is_relevant(self, detail: dict) -> bool:
        text = " ".join(
            [
                detail.get("title") or "",
                detail.get("list_title") or "",
                detail.get("detail_title") or "",
                detail.get("plain_text_sample") or "",
                detail.get("procedure") or "",
                detail.get("status_raw") or "",
            ]
        )

        norm = self._normalize_for_match(text)

        if any(self._normalize_for_match(keyword) in norm for keyword in TARGET_KEYWORDS):
            return True

        if "fonte rinnovabile" in norm and "energia elettrica" in norm:
            return True

        return False

    # ------------------------------------------------------------------
    # POWER / TYPE
    # ------------------------------------------------------------------

    def _extract_power_text(self, text: str) -> str | None:
        patterns = [
            r"potenza\s+(?:nominale|complessiva|elettrica)?\s*(?:pari\s+a|di)?\s*([0-9]+(?:[.,][0-9]+)?)\s*(MWp|MW|MWe|kWp|kW)",
            r"([0-9]+(?:[.,][0-9]+)?)\s*(MWp|MW|MWe|kWp|kW)",
        ]

        for pattern in patterns:
            match = re.search(pattern, text, flags=re.IGNORECASE)

            if match:
                value = match.group(1)
                unit = match.group(2)
                return f"{value} {unit}"

        return None

    def _power_text_to_mw(self, power_text: str | None) -> float | None:
        if not power_text:
            return None

        match = re.search(
            r"([0-9]+(?:[.,][0-9]+)?)\s*(MWp|MW|MWe|kWp|kW)",
            power_text,
            flags=re.IGNORECASE,
        )

        if not match:
            return None

        value = match.group(1).replace(",", ".")
        unit = match.group(2).lower()

        try:
            number = float(value)
        except ValueError:
            return None

        if unit in {"kw", "kwp"}:
            return number / 1000

        return number

    def _infer_project_type(self, text: str) -> str | None:
        norm = self._normalize_for_match(text)

        if (
            "agrivoltaico" in norm
            or "agrovoltaico" in norm
            or "agrofotovoltaico" in norm
            or "agro fotovoltaico" in norm
            or "agro fotovoltaici" in norm
        ):
            return "Agrivoltaico"

        if "fotovoltaico" in norm or "solare" in norm:
            return "Fotovoltaico"

        if "fonte rinnovabile" in norm and "energia elettrica" in norm:
            return "FER"

        return None

    # ------------------------------------------------------------------
    # TEXT HELPERS
    # ------------------------------------------------------------------

    def _clean_text(self, value: str | None) -> str:
        return " ".join((value or "").replace("\xa0", " ").split()).strip()

    def _normalize_key(self, value: str | None) -> str:
        value = self._clean_text(value or "")
        value = value.lower()
        value = value.replace("à", "a")
        value = value.replace("è", "e")
        value = value.replace("é", "e")
        value = value.replace("ì", "i")
        value = value.replace("ò", "o")
        value = value.replace("ù", "u")
        value = re.sub(r"[^a-z0-9]+", "_", value)
        return value.strip("_") or "nd"

    def _normalize_for_match(self, value: str | None) -> str:
        value = self._clean_text(value or "")
        value = value.lower()
        value = value.replace("à", "a")
        value = value.replace("è", "e")
        value = value.replace("é", "e")
        value = value.replace("ì", "i")
        value = value.replace("ò", "o")
        value = value.replace("ù", "u")
        value = re.sub(r"[^a-z0-9]+", " ", value)
        return " ".join(value.split())


if __name__ == "__main__":
    collector = MaseProvvedimentiCollector()
    items = collector.fetch()
    print(f"items: {len(items)}")
    for item in items[:80]:
        print(
            item.external_id,
            "|",
            item.title,
            "| proponente:",
            item.payload.get("proponent"),
            "|",
            item.payload.get("status_raw"),
            "|",
            item.payload.get("power_mw"),
            "|",
            item.payload.get("province"),
            "|",
            item.payload.get("municipalities"),
        )