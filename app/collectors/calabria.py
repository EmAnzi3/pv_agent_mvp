from __future__ import annotations

import hashlib
import re
import time
from datetime import date, datetime
from urllib.parse import urljoin, urlparse

from bs4 import BeautifulSoup

from app.collectors.base import BaseCollector, CollectorResult


BASE_URL = "https://www.regione.calabria.it"
SOURCE_URL = "https://www.regione.calabria.it/dipartimento-per-la-sostenibilita-ambientale/avvisi-via-e-vas/"

CUTOFF_DATE = date(2025, 1, 1)
MIN_POWER_MW = 5.0
REQUIRE_PROPONENT = True

MANUAL_EXCLUDE_URL_PATTERNS = [
    "annullamento-decreto-n-2871-del-27-02-2026",
    # Marcato in rosso nel file di revisione: diniego / non utilizzabile
    "provvedimento-autorizzatorio-unico-regionale-paur-ai-sensi-dellart-27bis-del-d-lgs-152-2006-e-s-m-i-relativo-al-progetto-di-riqualifica-del-sito-industriale-di-saline-joniche",

    # Marcato in giallo: duplicato Montebello Jonico / Riqualifica Saline Joniche
    "impianto-fotovoltaico-di-produzione-di-energia-elettrica-da-fonte-fotovoltaica-denominato-riqualifica-saline-joniche",
]

MANUAL_PROPONENT_OVERRIDES = {
    # Crotone / Scandale - Cargo
    "progetto-di-costruzione-ed-esercizio-di-impianto-fotovoltaico-della-potenza-complessiva-pari-a-189865": "Cargo S.r.l.",
    "impianto-fotovoltaico-variante-cargosrl": "Cargo S.r.l.",

    # Fotovoltaico flottante Monte Mamone / Enerflo
    "fotflott06": "Enerflo S.r.l.",
    "enerflo": "Enerflo S.r.l.",

    # DIT040 Calusia / Caccuri
    "dit040-calusia": "RESOL 1 S.r.l.",

    # Badolato
    "pari-a997920mwp": "ENERSPV2 S.r.l.",
    "badolato": "ENERSPV2 S.r.l.",

    # Castrovillari
    "pratica-n-749-cs": "AGRI-PV CASTROVILLARI - Soc. di progetto S.r.l.",
    "agri-pv-castrovillari": "AGRI-PV CASTROVILLARI - Soc. di progetto S.r.l.",

    # Riqualifica Saline Joniche
    "riqualifica-saline-joniche": "SOLUX S.r.l.",
    "riqualifica-del-sito-industriale-di-saline-joniche": "SOLUX S.r.l.",
}
REQUEST_TIMEOUT = 60
REQUEST_SLEEP_SECONDS = 0.12

SEARCH_TERMS = [
    "fotovoltaico",
    "fotovoltaica",
    "agrivoltaico",
    "agrivoltaica",
    "agrovoltaico",
    "agro-voltaico",
    "impianto fotovoltaico",
    "parco fotovoltaico",
    "PAUR fotovoltaico",
    "VIA fotovoltaico",
    "verifica assoggettabilita fotovoltaico",
]

MAX_SEARCH_PAGES_PER_TERM = 15

PV_KEYWORDS = [
    "fotovoltaico",
    "fotovoltaica",
    "agrivoltaico",
    "agrivoltaica",
    "agrovoltaico",
    "agro-voltaico",
    "agrofotovoltaico",
    "agro fotovoltaico",
    "impianto fv",
    "parco fv",
    "solare fotovoltaico",
    "fonte solare",
]

EXCLUDE_KEYWORDS = [
    "collocamento mirato",
    "preselezione",
    "legge 68 99",
    "legge 68/99",
    "installatore",
    "manutentore",
    "offerta di lavoro",
    "lavoro",
    "rifiuti",
    "depuratore",
    "bonifica",
    "aia",
    "autorizzazione integrata ambientale",
    "vas",
    "vinca",
    "incidenza ambientale",
]

PROVINCE_CODES = {"CZ", "CS", "KR", "RC", "VV"}

PROVINCE_NAME_TO_CODE = {
    "catanzaro": "CZ",
    "cosenza": "CS",
    "crotone": "KR",
    "reggio calabria": "RC",
    "vibo valentia": "VV",
}

MONTHS = {
    "gennaio": 1,
    "febbraio": 2,
    "marzo": 3,
    "aprile": 4,
    "maggio": 5,
    "giugno": 6,
    "luglio": 7,
    "agosto": 8,
    "settembre": 9,
    "ottobre": 10,
    "novembre": 11,
    "dicembre": 12,
}


# V11: collector selettivo >= 5 MW con override manuali e esclusioni revisione
class CalabriaCollector(BaseCollector):
    source_name = "calabria"
    base_url = SOURCE_URL

    def fetch(self) -> list[CollectorResult]:
        results: list[CollectorResult] = []
        seen_urls: set[str] = set()
        candidate_urls = self._discover_candidate_urls()

        for url in candidate_urls:
            if url in seen_urls:
                continue
            seen_urls.add(url)

            parsed = self._parse_project_page(url)
            if not parsed:
                continue

            external_id = self._build_external_id(parsed)

            results.append(
                CollectorResult(
                    external_id=external_id,
                    source_url=url,
                    title=parsed["title"][:250],
                    payload={
                        "title": parsed["title"][:900],
                        "project_name": parsed["title"][:900],
                        "proponent": parsed["proponent"],
                        "status_raw": parsed["status_raw"],
                        "region": "Calabria",
                        "province": parsed["province"],
                        "municipalities": parsed["municipalities"],
                        "power": parsed["power"],
                        "power_mw": parsed["power_mw"],
                        "project_type_hint": parsed["project_type_hint"],
                        "procedure": parsed["procedure"],
                        "publication_date": parsed["publication_date"],
                        "plain_text_sample": parsed["plain_text_sample"],
                    },
                )
            )

            time.sleep(REQUEST_SLEEP_SECONDS)

        return results

    # ------------------------------------------------------------------
    # Discovery
    # ------------------------------------------------------------------

    def _discover_candidate_urls(self) -> list[str]:
        urls: list[str] = []
        seen: set[str] = set()

        for term in SEARCH_TERMS:
            term_slug = term.replace(" ", "+")
            search_urls = [f"{BASE_URL}/?s={term_slug}"]

            for page in range(1, MAX_SEARCH_PAGES_PER_TERM + 1):
                search_urls.append(f"{BASE_URL}/page/{page}/?s={term_slug}")

            for search_url in search_urls:
                html_page = self._get_html(search_url)
                if not html_page:
                    continue

                soup = BeautifulSoup(html_page, "html.parser")
                page_text = self._clean_text(soup.get_text(" ", strip=True))

                # Se la pagina ricerca non contiene più nulla di utile, non insistere sulle pagine successive.
                if search_url != search_urls[0] and not self._contains_any(page_text, PV_KEYWORDS):
                    continue

                for link_url, label, context in self._extract_links(soup, search_url):
                    if link_url in seen:
                        continue
                    if not self._is_project_url(link_url):
                        continue

                    combined = self._clean_text(f"{label} {context} {link_url}")
                    if not self._contains_any(combined, PV_KEYWORDS):
                        continue

                    if self._is_navigation_noise(combined, link_url):
                        continue

                    seen.add(link_url)
                    urls.append(link_url)

                time.sleep(REQUEST_SLEEP_SECONDS)

        return urls

    def _extract_links(self, soup: BeautifulSoup, base_url: str) -> list[tuple[str, str, str]]:
        out: list[tuple[str, str, str]] = []

        for a in soup.find_all("a", href=True):
            href = a.get("href") or ""
            url = urljoin(base_url, href).split("#", 1)[0]
            label = self._clean_text(a.get_text(" ", strip=True))

            if not self._is_allowed_url(url):
                continue

            parent_text = ""
            parent = a.find_parent(["article", "li", "div", "p", "h2", "h3", "section"])
            if parent:
                parent_text = self._clean_text(parent.get_text(" ", strip=True))

            out.append((url, label, parent_text))

        return out

    def _is_allowed_url(self, url: str) -> bool:
        try:
            parsed = urlparse(url)
        except Exception:
            return False

        return parsed.scheme in {"http", "https"} and parsed.netloc.lower() in {
            "www.regione.calabria.it",
            "regione.calabria.it",
        }

    def _is_project_url(self, url: str) -> bool:
        path = urlparse(url).path.lower()
        query = urlparse(url).query.lower()

        if ".pdf" in path:
            return True

        if "/page/" in path and "s=" in query:
            return False

        if path in {"", "/"}:
            return False

        bad = [
            "wp-json",
            "feed",
            "privacy",
            "cookie",
            "accessibilita",
            "trasparenza",
            "uffici",
            "contatti",
            "eventi",
            "collocamento",
            "preselezione",
        ]
        if any(x in path for x in bad):
            return False

        good = [
            "fotovoltaic",
            "agrivoltaic",
            "agrovoltaic",
            "valutazione",
            "impatto",
            "assoggettabil",
            "paur",
            "via",
            "avvisi-via-e-vas",
            "provvedimenti-regionali",
            "bandi",
            "dipartimento-per-la-sostenibilita-ambientale",
        ]

        return any(x in path for x in good)

    def _is_navigation_noise(self, text: str, url: str) -> bool:
        n = self._normalize_for_match(text)

        if n in {"vai al contenuto principale", "leggi tutto", "continua a leggere"}:
            return True

        if url.endswith("/?s=fotovoltaico") or "/page/" in urlparse(url).path:
            return True

        return False

    # ------------------------------------------------------------------
    # Project page parsing
    # ------------------------------------------------------------------

    def _parse_project_page(self, url: str) -> dict | None:
        html_page = self._get_html(url)
        if not html_page:
            return None

        soup = BeautifulSoup(html_page, "html.parser")
        plain = self._clean_text(soup.get_text(" ", strip=True))
        title = self._extract_title(soup, plain, url)

        if not title:
            return None

        combined = self._clean_text(f"{title} {plain[:6000]}")

        if not self._contains_any(combined, PV_KEYWORDS):
            return None

        if self._manual_exclude(url, combined):
            return None

        manual_proponent = self._manual_proponent(url, combined)

        # Le override manuali sono casi verificati: non li scartiamo per filtri generici
        # tipo concessioni/demanio, ma li scarteremo comunque se il procedimento è negativo.
        if not manual_proponent and self._is_excluded(combined):
            return None

        publication_date = self._extract_publication_date(soup, combined, url)
        if publication_date:
            try:
                parsed_date = datetime.strptime(publication_date, "%Y-%m-%d").date()
                if parsed_date < CUTOFF_DATE:
                    return None
            except ValueError:
                pass
        else:
            # Richiesta utente: gennaio 2025 in poi. Senza data, meglio non includere.
            return None

        extracted_proponent = self._extract_proponent(combined)
        proponent = manual_proponent or extracted_proponent

        power = self._extract_power_text(combined)
        power_mw = self._power_text_to_mw(power, combined)

        # Pipeline commerciale: teniamo solo taglie utility/commercialmente rilevanti.
        if power_mw is None or power_mw < MIN_POWER_MW:
            return None

        # Senza soggetto/titolare/proponente il record non è lavorabile.
        if REQUIRE_PROPONENT and not proponent:
            return None

        province = self._extract_province(combined)
        municipalities = self._extract_municipalities(combined)
        procedure = self._extract_procedure(combined)
        status_raw = self._extract_status(combined, procedure)

        # Dinieghi/rigetti/non favorevoli: non sono lead operativi.
        if self._is_negative_outcome(combined, status_raw):
            return None

        return {
            "title": title,
            "proponent": proponent,
            "status_raw": status_raw,
            "region": "Calabria",
            "province": province,
            "municipalities": municipalities,
            "power": power,
            "power_mw": power_mw,
            "project_type_hint": self._infer_project_type(combined),
            "procedure": procedure,
            "publication_date": publication_date,
            "plain_text_sample": combined[:5000],
            "url": url,
        }

    def _extract_title(self, soup: BeautifulSoup, plain: str, url: str) -> str | None:
        candidates: list[str] = []

        for selector in ["h1", "h2", ".entry-title", ".page-title", ".wp-block-post-title", "title"]:
            for node in soup.select(selector):
                text = self._clean_text(node.get_text(" ", strip=True))
                if text:
                    candidates.append(text)

        for candidate in candidates:
            if self._looks_like_title(candidate):
                return self._clean_title(candidate)

        slug = urlparse(url).path.strip("/").split("/")[-1]
        slug = slug.replace("-", " ")
        title = self._clean_text(slug)
        if len(title) > 20:
            return self._clean_title(title)

        return None

    def _clean_title(self, title: str) -> str:
        title = self._clean_text(title)
        title = re.sub(r"\s*-\s*Regione Calabria\s*$", "", title, flags=re.IGNORECASE)
        title = re.sub(r"\s*\|\s*Regione Calabria\s*$", "", title, flags=re.IGNORECASE)
        return title.strip(" .:-")

    def _looks_like_title(self, title: str) -> bool:
        n = self._normalize_for_match(title)
        if len(n) < 20:
            return False
        bad = [
            "regione calabria",
            "dipartimento",
            "home",
            "cookie",
            "privacy",
            "risultati della ricerca",
            "vai al contenuto",
        ]
        if any(n == b or n.startswith(b + " ") for b in bad):
            return False
        return self._contains_any(title, PV_KEYWORDS)

    def _extract_publication_date(self, soup: BeautifulSoup, text: str, url: str) -> str | None:
        for time_node in soup.find_all("time"):
            dt = time_node.get("datetime") or ""
            parsed = self._parse_date(dt)
            if parsed:
                return parsed.isoformat()

            parsed = self._parse_date(time_node.get_text(" ", strip=True))
            if parsed:
                return parsed.isoformat()

        for meta_name in ["article:published_time", "date", "pubdate", "publish_date"]:
            node = soup.find("meta", attrs={"property": meta_name}) or soup.find("meta", attrs={"name": meta_name})
            if node and node.get("content"):
                parsed = self._parse_date(node.get("content"))
                if parsed:
                    return parsed.isoformat()

        parsed = self._parse_date(text)
        if parsed:
            return parsed.isoformat()

        parsed = self._parse_date(url)
        if parsed:
            return parsed.isoformat()

        return None

    def _parse_date(self, raw: str | None) -> date | None:
        raw = raw or ""

        m = re.search(r"\b(20\d{2})[-_./](\d{1,2})[-_./](\d{1,2})\b", raw)
        if m:
            y, mo, d = map(int, m.groups())
            try:
                return date(y, mo, d)
            except ValueError:
                pass

        m = re.search(r"\b(\d{1,2})[-_/\.](\d{1,2})[-_/\.](20\d{2})\b", raw)
        if m:
            d, mo, y = map(int, m.groups())
            try:
                return date(y, mo, d)
            except ValueError:
                pass

        month_regex = "|".join(MONTHS)
        m = re.search(rf"\b(\d{{1,2}})\s+({month_regex})\s+(20\d{{2}})\b", raw, flags=re.IGNORECASE)
        if m:
            d = int(m.group(1))
            mo = MONTHS[m.group(2).lower()]
            y = int(m.group(3))
            try:
                return date(y, mo, d)
            except ValueError:
                pass

        m = re.search(r"\b(20\d{2})\b", raw)
        if m:
            return date(int(m.group(1)), 1, 1)

        return None

    def _manual_exclude(self, url: str, text: str) -> bool:
        haystack = self._normalize_for_match(f"{url} {text}")

        for needle in MANUAL_EXCLUDE_URL_PATTERNS:
            if self._normalize_for_match(needle) in haystack:
                return True

        return False

    def _manual_proponent(self, url: str, text: str) -> str | None:
        haystack = self._normalize_for_match(f"{url} {text}")

        for needle, proponent in MANUAL_PROPONENT_OVERRIDES.items():
            if self._normalize_for_match(needle) in haystack:
                return proponent

        return None

    def _extract_proponent(self, text: str) -> str | None:
        patterns = [
            # Campo più affidabile su Calabria: "Titolare: ..."
            r"\bTitolare\s*:\s*(.+?)(?=\s+(?:Conclusione|Parere|Regione Calabria|Pratica|Sistema Regionale|Calabria SUAP|Sportello|Pubblicato|Data|Codice|Comune|Comuni|Oggetto|Procedura)\b|\s+[–-]\s+|$)",
            r"\bTitolare\s+([A-Z0-9][A-Za-z0-9À-ÿ\.\s&'’\-]{2,180}?)(?=\s+(?:Conclusione|Parere|Regione Calabria|Pratica|Sistema Regionale|Calabria SUAP|Sportello|Pubblicato|Data|Codice|Comune|Comuni|Oggetto|Procedura)\b|\s+[–-]\s+|$)",

            # Varianti standard.
            r"\b(?:Proponente|Richiedente)\s*:\s*(.+?)(?=\s+(?:Conclusione|Parere|Regione Calabria|Pratica|Sistema Regionale|Calabria SUAP|Sportello|Pubblicato|Data|Codice|Comune|Comuni|Oggetto|Procedura)\b|\s+[–-]\s+|$)",
            r"\b(?:Proponente|Richiedente)\s+([A-Z0-9][A-Za-z0-9À-ÿ\.\s&'’\-]{2,180}?)(?=\s+(?:Conclusione|Parere|Regione Calabria|Pratica|Sistema Regionale|Calabria SUAP|Sportello|Pubblicato|Data|Codice|Comune|Comuni|Oggetto|Procedura)\b|\s+[–-]\s+|$)",
            r"\bSociet[aà]\s+proponente\s*:\s*(.+?)(?=\s+(?:Conclusione|Parere|Regione Calabria|Pratica|Sistema Regionale|Calabria SUAP|Sportello|Pubblicato|Data|Codice|Comune|Comuni|Oggetto|Procedura)\b|\s+[–-]\s+|$)",
        ]

        for pattern in patterns:
            match = re.search(pattern, text, flags=re.IGNORECASE)
            if match:
                value = self._clean_proponent(match.group(1))
                if value:
                    return value

        # Fallback dal titolo: "AGRIPV CASTROVILLARI – Soc. di progetto s.r.l."
        fallback_patterns = [
            r"\b([A-Z0-9][A-Za-z0-9À-ÿ'’\-\s&]+?)\s+[–-]\s+Soc\.?\s+di\s+progetto\s+s\.?\s*r\.?\s*l\.?",
            r"\b([A-Z0-9][A-Za-z0-9À-ÿ'’\-\s&]+?)\s+[–-]\s+S\.?\s*R\.?\s*L\.?",
        ]

        for pattern in fallback_patterns:
            match = re.search(pattern, text, flags=re.IGNORECASE)
            if match:
                value = self._clean_proponent(match.group(0))
                if value:
                    return value

        return None

    def _clean_proponent(self, value: str) -> str | None:
        value = self._clean_text(value)
        value = re.sub(r"^ditta\s+", "", value, flags=re.IGNORECASE)
        value = re.sub(r"\s+Avviso\s*$", "", value, flags=re.IGNORECASE)
        value = re.sub(r"Fri\s*-\s*El", "FRI-EL", value, flags=re.IGNORECASE)
        value = value.replace("S.r.L.", "S.r.l.").replace("S.r.L", "S.r.l.")
        value = value.strip(" .,:;-–—")

        # Taglia code residue.
        value = re.split(
            r"\s+(?:Avviso|PAUR|Conclusione|Parere|Regione Calabria|Pratica|Sistema Regionale|Calabria SUAP|Sportello|Pubblicato|Data|Codice|Comuni|Comune|Realizzazione|della\s+potenza|da\s+realizzarsi|Oggetto|Procedura)\b",
            value,
            maxsplit=1,
            flags=re.IGNORECASE,
        )[0].strip(" .,:;-–—")

        # Normalizza società di progetto se il testo è "AGRI-PV CASTROVILLARI – Soc. di progetto s.r.l."
        value = re.sub(
            r"\s+[–-]\s+Soc\.?\s+di\s+progetto\s+s\.?\s*r\.?\s*l\.?\s*$",
            " Soc. di progetto S.r.l.",
            value,
            flags=re.IGNORECASE,
        )

        # Se è un troncone con " S" ma nel titolo/pagina appare chiaramente "S.r.l.", qui non inventiamo.
        if not value or len(value) > 140:
            return None

        bad = [
            "regione calabria",
            "dipartimento",
            "procedimento",
            "valutazione",
            "impatto ambientale",
            "parere",
            "conclusione",
        ]
        n = self._normalize_for_match(value)
        if any(x in n for x in bad):
            return None

        if re.fullmatch(r"\d+", value):
            return None

        # Evita tronconi troppo sospetti.
        if re.fullmatch(r"[A-Z0-9]{6,}", value) and not any(x in n for x in ["srl", "spa", "sas", "societa"]):
            return None
        if value.endswith(" S") and not any(x in n for x in ["srl", "spa", "sas", "societa"]):
            return None

        return value

    def _extract_power_text(self, text: str) -> str | None:
        patterns = [
            # Gestisce anche "pari a9,97920MWp" senza spazi.
            r"potenza\s+(?:complessiva\s+)?(?:nominale\s+)?(?:pari\s*a\s*|di\s*)?([0-9][0-9\.\,]*)\s*(MWp|MW|MWe|MWdc|MWac|kWp|kW)",
            r"\bda\s+([0-9][0-9\.\,]*)\s*(MWp|MW|MWe|MWdc|MWac|kWp|kW)\b",
            r"\b([0-9][0-9\.\,]*)\s*(MWp|MW|MWe|MWdc|MWac|kWp|kW)\b",
        ]

        for pattern in patterns:
            match = re.search(pattern, text, flags=re.IGNORECASE)
            if match:
                return f"{match.group(1)} {match.group(2)}"

        return None

    def _power_text_to_mw(self, power_text: str | None, context: str = "") -> float | None:
        if not power_text:
            return None

        match = re.search(r"([0-9][0-9\.\,]*)\s*(MWp|MW|MWe|MWdc|MWac|kWp|kW)", power_text, flags=re.IGNORECASE)
        if not match:
            return None

        number_text = match.group(1)
        unit = match.group(2).lower()

        if "," in number_text and "." in number_text:
            if number_text.rfind(",") > number_text.rfind("."):
                number_text = number_text.replace(".", "").replace(",", ".")
            else:
                number_text = number_text.replace(",", "")
        elif "," in number_text:
            number_text = number_text.replace(",", ".")

        try:
            number = float(number_text)
        except ValueError:
            return None

        # Correzione per slug/testi che perdono la virgola: 997920 MWp -> 9.97920 MWp.
        # Applica solo a valori palesemente irrealistici dentro contesto con "9,..." o URL "997920".
        if unit in {"mw", "mwp", "mwe", "mwdc", "mwac"} and number > 10000:
            if "997920" in self._normalize_for_match(context) or "9 97920" in self._normalize_for_match(context):
                number = 9.97920

        if unit in {"kw", "kwp"}:
            return number / 1000

        return number

    def _extract_province(self, text: str) -> str | None:
        matches = re.findall(r"\((CZ|CS|KR|RC|VV)\)", text.upper())
        if matches:
            return matches[0]

        n = self._normalize_for_match(text)
        for name, code in PROVINCE_NAME_TO_CODE.items():
            if re.search(rf"\b{re.escape(name)}\b", n):
                return code

        return None

    def _extract_municipalities(self, text: str) -> list[str]:
        found: list[str] = []

        def add(value: str) -> None:
            for part in self._split_municipality_part(value):
                cleaned = self._clean_municipality(part)
                if cleaned and cleaned not in found:
                    found.append(cleaned)

        # Cattura tutti i comuni espliciti con provincia tra parentesi.
        for match in re.finditer(
            r"\b([A-ZÀ-Ý][A-Za-zÀ-ÿ'’\-\s]{2,90}?)\s*\((CZ|CS|KR|RC|VV)\)",
            text,
            flags=re.IGNORECASE,
        ):
            add(match.group(1))

        patterns = [
            r"\bComune\s+di\s+(.+?)(?:\s*\((CZ|CS|KR|RC|VV)\)|,|\.|;|\s+loc\.|\s+localit[aà]|\s+e\s+relative|\s+-\s+|$)",
            r"\bComuni\s+di\s+(.+?)(?:\.|;|\s+loc\.|\s+localit[aà]|\s+e\s+relative|\s+-\s+|$)",
            r"\bterritorio\s+comunale\s+di\s+(.+?)(?:\s*\((CZ|CS|KR|RC|VV)\)|,|\.|;|\s+loc\.|\s+localit[aà]|\s+e\s+relative|\s+-\s+|$)",
            r"\bloc\.\s+[^,\.]{1,80}\s+Comune\s+di\s+(.+?)(?:\s*\((CZ|CS|KR|RC|VV)\)|,|\.|;|\s+-\s+|$)",
            r"\bda\s+realizzarsi\s+nel\s+Comune\s+di\s+(.+?)(?:\s*\((CZ|CS|KR|RC|VV)\)|,|\.|;|\s+loc\.|\s+localit[aà]|\s+e\s+relative|\s+-\s+|$)",
            r"\bdaubicare\s+nel\s+Comune\s+di\s+(.+?)(?:\s*\((CZ|CS|KR|RC|VV)\)|,|\.|;|\s+loc\.|\s+localit[aà]|\s+e\s+relative|\s+-\s+|$)",
        ]

        for pattern in patterns:
            for match in re.finditer(pattern, text, flags=re.IGNORECASE):
                add(match.group(1))

        return self._finalize_municipalities(found)[:8]

    def _split_municipality_part(self, value: str) -> list[str]:
        value = self._clean_text(value)

        # Taglia code tecniche/amministrative.
        value = re.split(
            r"\b(?:localit[aà]|loc\.|strada|provinciale|e\s+relative|opere|connessione|pratica|sistema|sportello|proponente|avviso|paur|comprensivo|infrastrutture|capacità|sistema\s+di\s+accumulo)\b",
            value,
            maxsplit=1,
            flags=re.IGNORECASE,
        )[0]

        # Rimuove prefissi lunghi.
        for _ in range(3):
            value = re.sub(
                r"^(?:intervento:\s*)?(?:e\s+)?(?:che\s+interessano\s+anche\s+il\s+)?(?:mwp\s+)?(?:da\s+realizzarsi\s+nel\s+comune\s+di|da\s+realizzarsi\s+nei\s+comuni\s+di|daubicare\s+nel\s+comune\s+di|ubicato\s+nel\s+territorio\s+comunale\s+di|territorio\s+comunale\s+di|comune\s+di|comuni\s+di|e\s+comune\s+di|in\s+comune\s+di)\s+",
                "",
                value,
                flags=re.IGNORECASE,
            )

        parts = re.split(r",|;|/|\s+e\s+|\s+ed\s+", value, flags=re.IGNORECASE)
        return [p for p in parts if self._clean_text(p)]

    def _finalize_municipalities(self, values: list[str]) -> list[str]:
        cleaned: list[str] = []
        seen: set[str] = set()

        for value in values:
            item = self._clean_municipality(value)
            if not item:
                continue
            key = self._normalize_for_match(item)
            if key in seen:
                continue
            seen.add(key)
            cleaned.append(item)

        # Rimuove combinazioni tipo "Scandale E Cutro" se i singoli sono presenti.
        single_keys = {self._normalize_for_match(x) for x in cleaned}
        final: list[str] = []
        for item in cleaned:
            key = self._normalize_for_match(item)
            if " e " in f" {key} ":
                parts = [p.strip() for p in key.split(" e ") if p.strip()]
                if parts and all(part in single_keys for part in parts):
                    continue
            final.append(item)

        return final

    def _clean_municipality(self, value: str) -> str | None:
        value = self._clean_text(value)
        value = re.sub(r"\((CZ|CS|KR|RC|VV)\)", "", value, flags=re.IGNORECASE)

        # Se dentro il frammento c'è una forma esplicita, tieni solo il comune.
        # Esempi:
        # - "Da Sant’Elia nel Comune di Montebello Jonico" -> "Montebello Jonico"
        # - "Ed ubicato nel territorio comunale di Caccuri" -> "Caccuri"
        explicit = re.search(
            r"\b(?:nel|in|del|della|ubicato\s+nel|ed\s+ubicato\s+nel)?\s*(?:Comune\s+di|territorio\s+comunale\s+di)\s+([A-ZÀ-Ý][A-Za-zÀ-ÿ'’\-\s]{2,70})",
            value,
            flags=re.IGNORECASE,
        )
        if explicit:
            value = explicit.group(1)

        # Rimuove prefissi e residui ripetuti.
        for _ in range(4):
            value = re.sub(
                r"^(?:intervento:\s*)?(?:e\s+)?(?:ed\s+)?(?:che\s+interessano\s+anche\s+il\s+)?(?:mwp\s+)?(?:comune\s+di|comuni\s+di|comune|territorio\s+dei\s+comuni\s+di|territorio\s+del\s+comune\s+di|territorio\s+comunale\s+di|da\s+realizzarsi\s+nel\s+comune\s+di|da\s+realizzarsi\s+nei\s+comuni\s+di|daubicare\s+nel\s+comune\s+di|ubicato\s+nel\s+territorio\s+comunale\s+di|ed\s+ubicato\s+nel\s+territorio\s+comunale\s+di|e\s+comune\s+di|di|del|della|in|nel|nella)\s+",
                "",
                value,
                flags=re.IGNORECASE,
            )

        value = re.sub(r"\s+\bin\b$", "", value, flags=re.IGNORECASE)
        value = value.strip(" ,.;:-()[]\"'")

        if not value or len(value) < 3 or len(value) > 70:
            return None

        if re.search(r"\d", value):
            return None

        n = self._normalize_for_match(value)

        # Località/contrade/residui amministrativi, non comuni.
        exact_bad = {
            "scalano",
            "sant elia",
            "santelia",
            "delle relative",
        }
        if n in exact_bad:
            return None

        bad = [
            "potenza",
            "impianto",
            "fotovoltaico",
            "agrivoltaico",
            "agrovoltaico",
            "opere",
            "connessione",
            "cabina",
            "primaria",
            "regione calabria",
            "procedimento",
            "parere",
            "costruzione",
            "esercizio",
            "comprensivo",
            "infrastrutture",
            "indispensabili",
            "sistema",
            "accumulo",
            "realizzarsi",
            "daubicare",
            "mwp",
            "mw",
            "kwp",
            "kw",
            "proponente",
            "colli crotonesi",
            "canalicchi",
            "ed ubicato",
            "territorio comunale",
            "ubicato",
            "relativi",
            "strettamente",
            "connesse",
        ]
        if any(b in n for b in bad):
            return None

        return value.title()

    def _extract_procedure(self, text: str) -> str | None:
        n = self._normalize_for_match(text)
        if "paur" in n or "p a u r" in n or "provvedimento autorizzatorio unico" in n:
            return "PAUR"
        if "verifica di assoggettabilita" in n or "art 19" in n:
            return "Verifica di Assoggettabilità a VIA"
        if "valutazione di impatto ambientale" in n or " via " in f" {n} ":
            return "VIA"
        if "autorizzazione unica" in n:
            return "Autorizzazione Unica"
        return "Atto ambientale Regione Calabria"

    def _extract_status(self, text: str, procedure: str | None) -> str:
        n = self._normalize_for_match(text)
        if "conclusione del procedimento" in n or "concluso" in n or "conclusa" in n:
            return f"{procedure or 'Procedura'} - Conclusa"
        if "parere di esclusione" in n:
            return f"{procedure or 'Procedura'} - Esclusione da VIA"
        if "archiviato" in n or "archiviata" in n:
            return f"{procedure or 'Procedura'} - Archiviata"
        return procedure or "Regione Calabria"

    def _infer_project_type(self, text: str) -> str:
        n = self._normalize_for_match(text)
        if "agrivoltaico" in n or "agrivoltaica" in n or "agrovoltaico" in n:
            return "Agrivoltaico"
        if "fotovoltaico" in n or "fotovoltaica" in n:
            return "Fotovoltaico"
        return "FER"

    def _is_excluded(self, text: str) -> bool:
        n = self._normalize_for_match(text)

        # Esclusioni dure per atti non progettuali o non rilevanti per pipeline cantieri.
        if "concessione" in n and ("demanio" in n or "demaniale" in n or "area appartenente" in n):
            return True
        if "area demaniale" in n or "demanio idrico" in n:
            return True
        if "annullamento decreto" in n:
            return True
        if "deposito indennita" in n or "indennita di espropriazione" in n:
            return True

        # Esclusioni dure.
        hard = [
            "collocamento mirato",
            "preselezione",
            "legge 68 99",
            "installatore",
            "manutentore",
            "offerta di lavoro",
            "deposito indennita",
            "deposito indennità",
            "indennita di espropriazione",
            "indennità di espropriazione",
            "concessione area demaniale",
            "concessione di un area",
            "tettoia",
            "copertura di fabbricato",
            "su edificio",
            "su copertura",
            "fabbricato produttivo",
        ]
        if any(x in n for x in hard):
            return True

        # Se contiene FV è comunque potenzialmente rilevante, salvo hard exclusions.
        if self._contains_any(text, PV_KEYWORDS):
            return False

        return any(self._normalize_for_match(x) in n for x in EXCLUDE_KEYWORDS)

    def _is_negative_outcome(self, text: str, status_raw: str | None = None) -> bool:
        n = self._normalize_for_match(f"{text} {status_raw or ''}")

        negative_terms = [
            "diniego",
            "parere negativo",
            "non favorevole",
            "rigetto",
            "rigettata",
            "rigettato",
            "istanza respinta",
            "esito negativo",
            "non accoglibile",
            "improcedibile",
            "archiviazione dell istanza",
        ]

        # Attenzione: "parere di esclusione dalla VIA" NON è un diniego.
        if "parere di esclusione" in n or "esclusione dalla procedura di via" in n:
            return False

        return any(term in n for term in negative_terms)

    def _contains_any(self, text: str, keywords: list[str]) -> bool:
        n = self._normalize_for_match(text)
        return any(self._normalize_for_match(k) in n for k in keywords)

    def _build_external_id(self, parsed: dict) -> str:
        url = parsed.get("url") or ""
        title = parsed.get("title") or ""
        proponent = parsed.get("proponent") or ""
        raw = f"{url}|{title}|{proponent}"
        digest = hashlib.sha1(raw.encode("utf-8", errors="ignore")).hexdigest()
        return f"calabria_{digest}"

    def _get_html(self, url: str) -> str | None:
        try:
            response = self.session.get(
                url,
                timeout=REQUEST_TIMEOUT,
                headers={"User-Agent": "Mozilla/5.0 pv-agent"},
                allow_redirects=True,
            )
            if response.status_code != 200:
                return None
            response.encoding = response.apparent_encoding or response.encoding
            return response.text
        except Exception:
            return None

    def _normalize_for_match(self, text: str | None) -> str:
        text = self._clean_text(text).lower()
        for src, dst in {
            "à": "a", "è": "e", "é": "e", "ì": "i", "ò": "o", "ù": "u",
            "’": "'", "‘": "'", "“": '"', "”": '"',
        }.items():
            text = text.replace(src, dst)
        text = re.sub(r"[^a-z0-9\s'\.-]", " ", text)
        text = re.sub(r"\s+", " ", text)
        return text.strip()

    def _clean_text(self, value) -> str:
        if value is None:
            return ""
        value = str(value)
        value = value.replace("\ufeff", "")
        value = value.replace("\xa0", " ")
        value = value.replace("\r", " ")
        value = value.replace("\n", " ")
        value = value.strip()
        value = re.sub(r"\s+", " ", value)
        return value.strip()


if __name__ == "__main__":
    collector = CalabriaCollector()
    items = collector.fetch()
    print("items:", len(items))
    for item in items[:80]:
        print(
            item.external_id,
            "|",
            item.title,
            "|",
            item.payload.get("proponent"),
            "|",
            item.payload.get("power_mw"),
            "|",
            item.payload.get("province"),
            "|",
            item.payload.get("municipalities"),
            "|",
            item.source_url,
        )
