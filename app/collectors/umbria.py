from __future__ import annotations

import hashlib
import html
import re
import time
from urllib.parse import urljoin, urlparse

from bs4 import BeautifulSoup

from app.collectors.base import BaseCollector, CollectorResult
from app.config import settings


BASE_URL = "https://www.va.regione.umbria.it"

SOURCE_PAGES = [
    {
        "url": "https://www.va.regione.umbria.it/via/elenco-dei-procedimenti-di-valutazione-di-impatto-ambientale",
        "procedure": "VIA/PAUR",
    },
    {
        "url": "https://www.va.regione.umbria.it/via/elenco-dei-procedimenti-di-verifica-di-assoggettabilita-a-via",
        "procedure": "Verifica di assoggettabilità a VIA",
    },
    {
        "url": "https://www.va.regione.umbria.it/via/valutazione-preliminare",
        "procedure": "Valutazione preliminare",
    },
]

REQUEST_TIMEOUT = 60
REQUEST_SLEEP_SECONDS = 0.12
MIN_POWER_MW = 5.0

PV_KEYWORDS = [
    "fotovoltaico",
    "fotovoltaica",
    "agrivoltaico",
    "agrivoltaica",
    "agrovoltaico",
    "agrovoltaica",
    "agrofotovoltaico",
    "agro-fotovoltaico",
    "impianto fv",
]

NON_PV_EXCLUDE = [
    "eolico",
    "eolica",
    "impianto eolico",
    "parco eolico",
    "rifiuti",
    "discarica",
    "cava",
    "estrazione",
    "acquedotto",
    "idroelettrico",
    "biometano",
    "biogas",
    "metanodotto",
    "gasdotto",
    "stradale",
    "ferroviaria",
]

PROVINCE_CODES = {"PG", "TR"}

PROVINCE_NAME_TO_CODE = {
    "perugia": "PG",
    "terni": "TR",
}

# I casi sotto sono correzioni conservative su pratiche già viste.
# Servono a evitare rumore da testo libero/HTML Liferay.
KNOWN_OVERRIDES = {
    "94-2025-012": {
        "province": "PG",
        "municipalities": ["Città della Pieve"],
        "power_mw": 34.776,
        "proponent": "Società Greencells Italia S.r.l.",
    },
    "94-2025-011": {
        "province": "PG",
        "municipalities": ["Piegaro"],
        "power_mw": 9.2,
        "proponent": "Società Alpicapital Development S.r.l.",
    },
    "94-2025-007": {
        "province": "PG",
        "municipalities": ["Castiglione del Lago"],
        "power_mw": 16.97468,
        "proponent": "Società Agrovolt 01 S.r.l.",
    },
    "93-2025-011": {
        "province": "TR",
        "municipalities": ["Orvieto", "Castel Giorgio"],
        "power_mw": 34.6008,
        "proponent": "Società ContourGlobal Samas S.r.l.",
    },
    "93-2025-008": {
        "province": "TR",
        "municipalities": ["Orvieto", "Castel Giorgio"],
        "power_mw": 33.97248,
        "proponent": "Società Ecoener Alfina S.r.l.",
    },
}


class UmbriaCollector(BaseCollector):
    source_name = "umbria"
    base_url = BASE_URL

    def fetch(self) -> list[CollectorResult]:
        results: list[CollectorResult] = []
        seen_ids: set[str] = set()

        for source in SOURCE_PAGES:
            html_page = self._get_html(source["url"])
            if not html_page:
                continue

            rows = self._extract_candidate_rows(html_page, source["url"], source["procedure"])

            for row in rows:
                parsed = self._normalize_row(row)

                if not parsed:
                    continue

                external_id = self._build_external_id(parsed["source_url"], parsed["title"])

                if external_id in seen_ids:
                    continue

                seen_ids.add(external_id)

                results.append(
                    CollectorResult(
                        external_id=external_id,
                        source_url=parsed["source_url"],
                        title=parsed["title"][:250],
                        payload={
                            "title": parsed["title"][:900],
                            "project_name": parsed["title"][:900],
                            "proponent": parsed["proponent"],
                            "status_raw": parsed["status_raw"],
                            "region": "Umbria",
                            "province": parsed["province"],
                            "municipalities": parsed["municipalities"],
                            "power": parsed["power"],
                            "power_mw": parsed["power_mw"],
                            "project_type_hint": parsed["project_type_hint"],
                            "procedure": parsed["procedure"],
                            "authority": parsed["authority"],
                            "plain_text_sample": parsed["plain_text_sample"],
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
                timeout=getattr(settings, "request_timeout", REQUEST_TIMEOUT),
                allow_redirects=True,
            )
            response.raise_for_status()
            return response.content.decode("utf-8", errors="replace")
        except Exception:
            return None

    # ------------------------------------------------------------------
    # LIST PARSING
    # ------------------------------------------------------------------

    def _extract_candidate_rows(self, html_page: str, page_url: str, procedure: str) -> list[dict]:
        soup = BeautifulSoup(html_page, "html.parser")
        rows: list[dict] = []

        for table in soup.find_all("table"):
            text = self._clean_text(table.get_text(" ", strip=True))

            if not text:
                continue

            if "Soggetto proponente" not in text:
                continue

            if not self._is_pv_related(text):
                continue

            first_project_link = self._extract_first_project_link(table, page_url)

            rows.append(
                {
                    "text": text,
                    "source_url": first_project_link or page_url,
                    "procedure": procedure,
                }
            )

        return rows

    def _extract_first_project_link(self, table, page_url: str) -> str | None:
        for a in table.find_all("a", href=True):
            label = self._clean_text(a.get_text(" ", strip=True))
            href = a.get("href") or ""

            if not label or not href:
                continue

            label_norm = self._normalize_for_match(label)
            if not any(keyword in label_norm for keyword in PV_KEYWORDS):
                continue

            return urljoin(page_url, href).split("#", 1)[0]

        return None

    def _normalize_row(self, row: dict) -> dict | None:
        text = self._clean_text(row.get("text") or "")
        source_url = row.get("source_url") or ""
        procedure = row.get("procedure") or "Umbria VIA"

        if not text or not source_url:
            return None

        if not self._is_pv_related(text):
            return None

        if self._is_excluded(text):
            return None

        authority = self._extract_authority(text)

        # Evita procedimenti interregionali/statali fuori Umbria.
        # Esempio noto: Cortona (AR), autorità Regione Toscana.
        if authority and "Regione Umbria" not in authority and "Umbria" not in authority:
            return None

        title = self._extract_title(text)
        proponent = self._extract_proponent(text)
        province = self._extract_province(text)
        municipalities = self._extract_municipalities(text)
        power = self._extract_power_text(text)
        power_mw = self._power_text_to_mw(power)
        status_raw = self._extract_status(text)

        if not title:
            return None

        if not proponent:
            return None

        if power_mw is None or power_mw < MIN_POWER_MW:
            return None

        if province not in PROVINCE_CODES:
            return None

        if not municipalities:
            return None

        parsed = {
            "title": title,
            "proponent": proponent,
            "status_raw": status_raw,
            "region": "Umbria",
            "province": province,
            "municipalities": municipalities,
            "power": power,
            "power_mw": power_mw,
            "project_type_hint": self._infer_project_type(title),
            "procedure": procedure,
            "authority": authority,
            "source_url": source_url,
            "plain_text_sample": text[:1200],
        }

        self._apply_known_overrides(parsed)

        return parsed

    # ------------------------------------------------------------------
    # FIELD EXTRACTION
    # ------------------------------------------------------------------

    def _extract_title(self, text: str) -> str | None:
        if not text:
            return None

        before_proponent = re.split(
            r"\bSoggetto\s+proponente\s*:",
            text,
            maxsplit=1,
            flags=re.IGNORECASE,
        )[0]

        title = before_proponent.strip(" -–—")

        # Rimuove eventuali prefissi di pagina/tabella.
        markers = [
            "Oggetto Procedimento Autorità procedente Stato del procedimento",
            "Oggetto Procedimento Autorita procedente Stato del procedimento",
        ]
        for marker in markers:
            if marker in title:
                title = title.split(marker, 1)[-1].strip()

        title = re.sub(r"^\s*P\.A\.U\.R\._\s*", "P.A.U.R. - ", title, flags=re.IGNORECASE)
        title = re.sub(r"\s+-\s+\([0-9]{1,3}-[0-9]{1,3}-[0-9]{4}\)\s*$", "", title)
        title = self._clean_text(title)

        return title[:900] if title else None

    def _extract_proponent(self, text: str) -> str | None:
        match = re.search(
            r"\bSoggetto\s+proponente\s*:\s*(.+?)(?=\s+Termine\s+per\s+la\s+presentazione|\s+Data\s+di\s+pubblicazione|\s+Verifica\s+di|\s+Provvedimento\s+autorizzatorio|\s+VIA\s+|\s+P\.A\.U\.R\.|\s*$)",
            text,
            flags=re.IGNORECASE,
        )

        if not match:
            return None

        value = self._clean_text(match.group(1))
        value = value.strip(" .,:;–—-")

        if value and len(value) >= 3:
            return value[:250]

        return None

    def _extract_power_text(self, text: str) -> str | None:
        if not text:
            return None

        patterns = [
            # 34.776,00 kWp / 24,77 MWp / 50,4 MW
            r"(?<![\d.,])(\d{1,3}(?:[.\s'’]\d{3})+(?:[,.]\d+)?|\d+[,.]\d+|\d+)\s*(MWp|MW|MVA|MWh|kWp|KWp|kW|KW)\b",
            # MWp 9,2
            r"\b(MWp|MW|MVA|MWh|kWp|KWp|kW|KW)\s*(\d{1,3}(?:[.\s'’]\d{3})+(?:[,.]\d+)?|\d+[,.]\d+|\d+)",
        ]

        for pattern in patterns:
            match = re.search(pattern, text, flags=re.IGNORECASE)
            if not match:
                continue

            if match.group(1).replace(".", "", 1).replace(",", "", 1).isdigit():
                return f"{match.group(1)} {match.group(2)}"

            return f"{match.group(2)} {match.group(1)}"

        return None

    def _power_text_to_mw(self, power: str | None) -> float | None:
        if not power:
            return None

        match = re.search(
            r"(\d{1,3}(?:[.\s'’]\d{3})+(?:[,.]\d+)?|\d+[,.]\d+|\d+)\s*(MWp|MW|MVA|MWh|kWp|KWp|kW|KW)",
            power,
            flags=re.IGNORECASE,
        )

        if not match:
            return None

        raw_number = match.group(1)
        unit = match.group(2).lower()

        number = raw_number.replace(" ", "").replace("'", "").replace("’", "")

        if "," in number:
            number = number.replace(".", "").replace(",", ".")
        elif number.count(".") > 1:
            number = number.replace(".", "")

        try:
            value = float(number)
        except ValueError:
            return None

        if unit.lower().startswith("kw"):
            return round(value / 1000.0, 6)

        return round(value, 6)

    def _extract_province(self, text: str) -> str | None:
        matches = re.findall(r"\(([A-Z]{2})\)", text or "")

        for code in matches:
            if code in PROVINCE_CODES:
                return code

        norm = self._normalize_for_match(text)
        for name, code in PROVINCE_NAME_TO_CODE.items():
            if name in norm:
                return code

        return None

    def _extract_municipalities(self, text: str) -> list[str]:
        municipalities: list[str] = []

        def add(name: str | None) -> None:
            if not name:
                return

            cleaned = self._clean_text(name)
            cleaned = cleaned.strip(" .,:;–—-()[]\"'")

            cleaned = re.sub(
                r"^(?:nel|nei|nella|nelle|del|della|di|comune|comuni)\s+",
                "",
                cleaned,
                flags=re.IGNORECASE,
            ).strip(" .,:;–—-()[]\"'")

            if not cleaned:
                return

            if len(cleaned) < 2 or len(cleaned) > 80:
                return

            # Evita frammenti non-comune.
            bad = ["potenza", "impianto", "progetto", "opere", "connessione", "rtN"]
            if any(x.lower() in cleaned.lower() for x in bad):
                return

            if cleaned not in municipalities:
                municipalities.append(cleaned)

        # nel Comune di X (PG) / nei Comuni di X e Y (TR)
        for match in re.finditer(
            r"\b(?:nel|nei|nella|nelle)?\s*Comuni?\s+di\s+(.+?)\s*\((PG|TR)\)",
            text,
            flags=re.IGNORECASE,
        ):
            chunk = match.group(1)
            chunk = re.split(r"\s+Soggetto\s+proponente\s*:", chunk, maxsplit=1, flags=re.IGNORECASE)[0]
            for part in re.split(r",|\s+e\s+|\s+ed\s+", chunk):
                add(part)

        # sito nel Comune di X (PG)
        for match in re.finditer(
            r"\bComune\s+di\s+(.+?)\s*\((PG|TR)\)",
            text,
            flags=re.IGNORECASE,
        ):
            add(match.group(1))

        return municipalities

    def _extract_status(self, text: str) -> str | None:
        norm = self._normalize_for_match(text)

        if " in corso" in f" {norm} ":
            return "In corso"

        if " concluso" in f" {norm} " or " conclusa" in f" {norm} ":
            return "Concluso"

        return None

    def _extract_authority(self, text: str) -> str | None:
        # Nei testi delle liste la sequenza finale è spesso:
        # ... [procedura] Regione Umbria In corso / Concluso
        if "Regione Umbria" in text:
            return "Regione Umbria"

        if "Ministero Ambiente" in text:
            return "Ministero Ambiente e della Sicurezza Energetica"

        if "Regione Toscana" in text:
            return "Regione Toscana"

        if "Regione Marche" in text:
            return "Regione Marche"

        return None

    def _infer_project_type(self, text: str | None) -> str:
        norm = self._normalize_for_match(text or "")

        if "agrivolta" in norm or "agrovolta" in norm:
            return "Agrivoltaico"

        return "Fotovoltaico"

    def _apply_known_overrides(self, parsed: dict) -> None:
        url = (parsed.get("source_url") or "").lower()

        for needle, patch in KNOWN_OVERRIDES.items():
            if needle not in url:
                continue

            parsed.update(patch)

            if "power_mw" in patch:
                parsed["power"] = f"{str(patch['power_mw']).replace('.', ',')} MW"

            return

    # ------------------------------------------------------------------
    # FILTERS / HELPERS
    # ------------------------------------------------------------------

    def _is_pv_related(self, text: str | None) -> bool:
        norm = self._normalize_for_match(text or "")
        return any(keyword in norm for keyword in PV_KEYWORDS)

    def _is_excluded(self, text: str | None) -> bool:
        norm = self._normalize_for_match(text or "")

        if any(keyword in norm for keyword in NON_PV_EXCLUDE):
            # Però non escludiamo un PV solo perché contiene "opere di connessione".
            if any(keyword in norm for keyword in PV_KEYWORDS):
                # Se contiene eolico, è quasi sempre non-PV nelle pagine Umbria.
                if "eolico" in norm or "eolica" in norm:
                    return True

                return False

            return True

        return False

    def _build_external_id(self, source_url: str, title: str) -> str:
        parsed = urlparse(source_url)
        stable = parsed.path.strip("/") or source_url

        if not stable or stable == "via":
            stable = title

        digest = hashlib.sha1(self._normalize_for_match(stable).encode("utf-8")).hexdigest()[:16]
        return f"umbria_{digest}"

    def _normalize_for_match(self, value: str | None) -> str:
        value = self._clean_text(value).lower()
        value = html.unescape(value)
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
