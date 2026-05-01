from __future__ import annotations

import html
import re
import time
import unicodedata
from urllib.parse import urljoin

from bs4 import BeautifulSoup

from app.collectors.base import BaseCollector, CollectorResult


BASE_URL = "https://www.sistema.puglia.it"
DETAIL_URL_TEMPLATE = "https://www.sistema.puglia.it/portal/page/portal/SistemaPuglia/DettaglioInfo?id={id}"

REQUEST_TIMEOUT = 60
REQUEST_SLEEP_SECONDS = 0.12

# Range prudente: copre grosso modo atti recenti 2025-2026.
# Se funziona bene, si può allargare.
DETAIL_ID_MIN = 62750
DETAIL_ID_MAX = 64250

PV_KEYWORDS = [
    "fotovoltaico",
    "fotovoltaica",
    "agrivoltaico",
    "agrivoltaica",
    "agrovoltaico",
    "agro-voltaico",
    "agro fotovoltaico",
    "agrofotovoltaico",
    "fonte solare",
]

EXCLUDE_KEYWORDS = [
    "eolico",
    "eolica",
    "offshore",
    "efficientamento energetico",
    "edilizia ospedaliera",
    "cabina primaria",
    "linea elettrica",
    "elettrodotto",
]

PROVINCE_TO_REGION = {
    "BA": "Puglia",
    "BT": "Puglia",
    "BR": "Puglia",
    "FG": "Puglia",
    "LE": "Puglia",
    "TA": "Puglia",
}


class SistemaPugliaEnergiaCollector(BaseCollector):
    source_name = "sistema_puglia_energia"
    base_url = BASE_URL

    def fetch(self) -> list[CollectorResult]:
        results: list[CollectorResult] = []
        seen: set[str] = set()

        for detail_id in range(DETAIL_ID_MAX, DETAIL_ID_MIN - 1, -1):
            url = DETAIL_URL_TEMPLATE.format(id=detail_id)
            html_page = self._get_html(url)

            if not html_page:
                continue

            parsed = self._parse_detail_page(
                html_page=html_page,
                url=url,
                detail_id=detail_id,
            )

            if not parsed:
                continue

            if not self._is_relevant(parsed):
                continue

            external_id = f"sistema_puglia_energia_{detail_id}"

            if external_id in seen:
                continue

            seen.add(external_id)

            results.append(
                CollectorResult(
                    external_id=external_id,
                    source_url=url,
                    title=parsed["title"][:250],
                    payload={
                        "title": parsed["title"][:700],
                        "administrative_title": parsed["administrative_title"][:700] if parsed["administrative_title"] else None,
                        "proponent": parsed["proponent"],
                        "status_raw": parsed["status_raw"],
                        "region": parsed["region"],
                        "province": parsed["province"],
                        "municipalities": parsed["municipalities"],
                        "power": parsed["power"],
                        "power_mw": parsed["power_mw"],
                        "project_type_hint": parsed["project_type_hint"],
                        "procedure": parsed["procedure"],
                        "category": "Sistema Puglia – Energia",
                        "publication_date": parsed["publication_date"],
                        "detail_id": detail_id,
                        "pdf_url": parsed["pdf_url"],
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
                timeout=REQUEST_TIMEOUT,
                allow_redirects=True,
            )

            if response.status_code != 200:
                return None

            # Sistema Puglia in diversi casi è pubblicato/letto meglio come Windows-1252.
            # Evita il classico mojibake sulle lettere accentate.
            text = response.content.decode("windows-1252", errors="replace")
            text = self._repair_mojibake(text)

            # Alcuni ID esistono ma sono pagine vuote/non informative.
            if "DettaglioInfo" not in url:
                return text

            if "Data Pubblicazione" not in text and "Determinazione" not in text:
                return None

            return text
        except Exception:
            return None

    # ------------------------------------------------------------------
    # PARSING
    # ------------------------------------------------------------------

    def _parse_detail_page(self, html_page: str, url: str, detail_id: int) -> dict | None:
        soup = BeautifulSoup(html_page, "html.parser")
        plain = self._clean_text(soup.get_text(" ", strip=True))

        if not plain:
            return None

        administrative_title = self._extract_administrative_title(soup, plain)

        if administrative_title and self._is_weekly_roundup(administrative_title):
            return None

        combined = self._clean_text(f"{administrative_title or ''} {plain}")

        # Esclude pagine riepilogative settimanali: mischiano più atti e creano record sporchi.
        if self._is_weekly_roundup(combined):
            return None

        proponent = self._extract_proponent(combined)
        power = self._extract_power_text(combined)
        power_mw = self._power_text_to_mw(power)

        province = self._extract_province(combined)
        municipalities = self._extract_municipalities(combined)

        procedure = self._extract_procedure(combined)
        publication_date = self._extract_publication_date(combined)
        pdf_url = self._extract_pdf_url(soup, url)

        project_type_hint = self._infer_project_type(combined)
        status_raw = self._build_status_raw(procedure, publication_date)

        title = self._build_operational_title(
            proponent=proponent,
            municipalities=municipalities,
            province=province,
            power_mw=power_mw,
            project_type_hint=project_type_hint,
            procedure=procedure,
            administrative_title=administrative_title,
        )

        return {
            "title": title,
            "administrative_title": administrative_title,
            "proponent": proponent,
            "status_raw": status_raw,
            "region": PROVINCE_TO_REGION.get(province) if province else "Puglia",
            "province": province,
            "municipalities": municipalities,
            "power": power,
            "power_mw": power_mw,
            "project_type_hint": project_type_hint,
            "procedure": procedure,
            "publication_date": publication_date,
            "pdf_url": pdf_url,
            "plain_text_sample": combined[:5000],
        }

    def _extract_administrative_title(self, soup: BeautifulSoup, plain: str) -> str | None:
        candidates: list[str] = []

        for selector in ["h1", "h2", "h3", ".title", ".titolo"]:
            for node in soup.select(selector):
                text = self._clean_text(node.get_text(" ", strip=True))
                if text:
                    candidates.append(text)

        for candidate in candidates:
            norm = self._normalize_for_match(candidate)

            if "sistema puglia" in norm:
                continue

            if len(candidate) > 20:
                return candidate

        match = re.search(
            r"(Determinazione\s+del\s+Dirigente\s+Sezione\s+Transizione\s+Energetica\s+n\.?\s*\d+\s+del\s+[^.]+)",
            plain,
            flags=re.IGNORECASE,
        )

        if match:
            return self._clean_text(match.group(1))

        title = soup.title.get_text(" ", strip=True) if soup.title else ""
        title = self._clean_text(title)

        if title and len(title) > 20 and "sistema puglia" not in self._normalize_for_match(title):
            return title

        return None

    def _build_operational_title(
        self,
        proponent: str | None,
        municipalities: list[str],
        province: str | None,
        power_mw: float | None,
        project_type_hint: str | None,
        procedure: str | None,
        administrative_title: str | None,
    ) -> str:
        location = ""
        if municipalities:
            location = ", ".join(municipalities[:4])
            if len(municipalities) > 4:
                location += "…"
        elif province:
            location = province

        power = self._format_mw(power_mw)

        pieces: list[str] = []

        if proponent:
            pieces.append(proponent)
        elif project_type_hint:
            pieces.append(project_type_hint)
        elif procedure:
            pieces.append(procedure)

        if location:
            pieces.append(location)

        if power:
            pieces.append(power)

        if pieces:
            return self._clean_text(" - ".join(pieces))[:700]

        # Fallback: meglio un titolo amministrativo che un titolo vuoto.
        if administrative_title:
            return administrative_title[:700]

        return "Sistema Puglia Energia"

    def _format_mw(self, value: float | None) -> str | None:
        if value is None:
            return None

        rounded = round(float(value), 6)
        text = f"{rounded:.6f}".rstrip("0").rstrip(".")
        return f"{text} MW"

    def _is_weekly_roundup(self, title: str | None) -> bool:
        norm = self._normalize_for_match(title or "")
        return "gli atti della settimana" in norm

    def _extract_proponent(self, text: str) -> str | None:
        text = self._repair_mojibake(text)
        normalized_text = self._normalize_legal_text(text)

        patterns = [
            r"\bsocieta\s+proponente\s*:\s*(.+?)(?:\s+-\s+partita\s+iva|\s+-\s+p\.?\s*iva|\s+c\.?\s*fisc|\s+c\.?\s*f\.|\s+cod\.?\s*fis|\s+sede\s+legale|\s+data\s+pubblicazione|\s+\[scarica|\s+pubblicato|\s*$)",
            r"\bproponente\s*:\s*(.+?)(?:\s+-\s+partita\s+iva|\s+-\s+p\.?\s*iva|\s+c\.?\s*fisc|\s+c\.?\s*f\.|\s+cod\.?\s*fis|\s+sede\s+legale|\s+data\s+pubblicazione|\s+\[scarica|\s+pubblicato|\s*$)",
            r"\bvoltura\s+alla\s+societa\s+(.+?)(?:\s+con\s+sede|\s+-\s+p\.?\s*iva|\s+c\.?\s*fisc|\s+data\s+pubblicazione|\s+\[scarica|\s*$)",
            r"\bvoltura\s+a\s+favore\s+di\s+(.+?)(?:\s+con\s+sede|\s+-\s+p\.?\s*iva|\s+c\.?\s*fisc|\s+data\s+pubblicazione|\s+\[scarica|\s*$)",
            r"\bsocieta\s*:\s*(.+?)(?:\s+con\s+sede|\s+-\s+p\.?\s*iva|\s+c\.?\s*fisc|\s+data\s+pubblicazione|\s+\[scarica|\s*$)",
            r"\bsubentro\s+della\s+societa\s+(.+?)(?:\s+con\s+sede|\s+-\s+p\.?\s*iva|\s+c\.?\s*fisc|\s+data\s+pubblicazione|\s+\[scarica|\s*$)",
            r"\bsubentrata\s+la\s+societa\s+(.+?)(?:\s+con\s+sede|\s+-\s+p\.?\s*iva|\s+c\.?\s*fisc|\s+data\s+pubblicazione|\s+\[scarica|\s*$)",
        ]

        for pattern in patterns:
            for match in re.finditer(pattern, normalized_text, flags=re.IGNORECASE):
                value = self._clean_proponent(match.group(1))
                if value:
                    return value

        # Fallback sul testo originale, nel caso il portale esponga accenti corretti.
        original_patterns = [
            r"\bSocietà\s+proponente\s*:\s*(.+?)(?:\s+-\s+Partita\s+IVA|\s+-\s+P\.?\s*IVA|\s+C\.?\s*Fisc|\s+C\.?\s*F\.|\s+Cod\.?\s*Fis|\s+Sede\s+Legale|\s+Data\s+Pubblicazione|\s+\[Scarica|\s+Pubblicato|\s*$)",
            r"\bProponente\s*:\s*(.+?)(?:\s+-\s+Partita\s+IVA|\s+-\s+P\.?\s*IVA|\s+C\.?\s*Fisc|\s+C\.?\s*F\.|\s+Cod\.?\s*Fis|\s+Sede\s+Legale|\s+Data\s+Pubblicazione|\s+\[Scarica|\s+Pubblicato|\s*$)",
            r"\bVoltura\s+alla\s+società\s+(.+?)(?:\s+con\s+sede|\s+-\s+P\.?\s*IVA|\s+C\.?\s*Fisc|\s+Data\s+Pubblicazione|\s+\[Scarica|\s*$)",
        ]

        for pattern in original_patterns:
            for match in re.finditer(pattern, text, flags=re.IGNORECASE):
                value = self._clean_proponent(match.group(1))
                if value:
                    return value

        return None

    def _clean_proponent(self, value: str | None) -> str | None:
        value = self._repair_mojibake(value or "")
        value = self._clean_text(value)

        if not value:
            return None

        value = value.strip(" .;:-,()")

        # Rimuove parentesi societarie/accessorie non utili.
        value = re.sub(
            r"\s*\((?:ex|già|gia|giŕ|subentrata|subentro)[^)]*(?:\)|$)",
            "",
            value,
            flags=re.IGNORECASE,
        )

        value = re.sub(
            r"\s*\((?:P\.?\s*IVA|P\.?\s*Iva|C\.?\s*F\.?|Codice\s+Fiscale)[^)]+\)",
            "",
            value,
            flags=re.IGNORECASE,
        )
        value = re.sub(
            r"\s*\((?:P\.?\s*IVA|P\.?\s*Iva|C\.?\s*F\.?|Codice\s+Fiscale)[^)]*$",
            "",
            value,
            flags=re.IGNORECASE,
        )

        # Rimuove indirizzi e dati fiscali.
        value = re.sub(
            r"\s*[-,]\s*(?:P\.?\s*IVA|P\.?\s*Iva|Partita\s+IVA|C\.?\s*F\.?|Codice\s+Fiscale).*$",
            "",
            value,
            flags=re.IGNORECASE,
        )
        value = re.sub(
            r"\s*[-,]\s*(?:Via|Viale|Piazza|Corso|Largo|Rotonda|Sede\s+Legale)\b.*$",
            "",
            value,
            flags=re.IGNORECASE,
        )
        value = re.sub(r"\s+con\s+sede\b.*$", "", value, flags=re.IGNORECASE)
        value = re.sub(r"\s+avente\s+sede\b.*$", "", value, flags=re.IGNORECASE)

        # Taglia descrizioni amministrative finite dentro al nome società.
        value = re.sub(
            r"\.\s*(?:Presa\s+d['’]atto|Voltura|Autorizzazione|Aggiornamento|Modifica|Rettifica|Cambio\s+compagine|Proroga).*$",
            "",
            value,
            flags=re.IGNORECASE,
        )

        # Taglia residui finali frequenti.
        value = re.sub(r"\s+con\s*$", "", value, flags=re.IGNORECASE)
        value = re.sub(r"\s+codice\s+fiscale\s+e\s*$", "", value, flags=re.IGNORECASE)
        value = re.sub(r"\s+P\.?\s*IVA\s*$", "", value, flags=re.IGNORECASE)

        value = value.strip(" .;:-,()")

        if not value:
            return None

        if re.fullmatch(r"\d{1,2}/\d{1,2}/\d{4}", value):
            return None

        if re.fullmatch(r"\d+", value):
            return None

        if len(value) > 140:
            return None

        bad = [
            "sistema puglia",
            "regione puglia",
            "determinazione",
            "autorizzazione unica",
            "scarica",
            "data pubblicazione",
            "transizione energetica",
            "bollettino ufficiale",
            "gli atti della settimana",
        ]

        norm = self._normalize_for_match(value)

        if any(item in norm for item in bad):
            return None

        return value

    def _extract_procedure(self, text: str) -> str | None:
        norm = self._normalize_for_match(text)

        if "autorizzazione unica" in norm:
            return "Autorizzazione Unica"

        if "p a u r" in norm or "paur" in norm:
            return "PAUR"

        if "voltura" in norm:
            return "Voltura"

        if "proroga" in norm:
            return "Proroga"

        return "Atto energia Regione Puglia"

    def _build_status_raw(self, procedure: str | None, publication_date: str | None) -> str:
        pieces = []

        if procedure:
            pieces.append(procedure)

        if publication_date:
            pieces.append(f"Pubblicato {publication_date}")

        return " - ".join(pieces) or "Sistema Puglia Energia"

    def _extract_publication_date(self, text: str) -> str | None:
        match = re.search(
            r"\bData\s+Pubblicazione\s*:\s*(\d{1,2}\s+[A-Za-zÀ-ÿ]+\s+\d{4}|\d{1,2}/\d{1,2}/\d{4})",
            text,
            flags=re.IGNORECASE,
        )

        if match:
            return self._clean_text(match.group(1))

        return None

    def _extract_power_text(self, text: str) -> str | None:
        patterns = [
            r"potenza\s+(?:nominale|complessiva|pari\s+a|di|prevista\s+pari\s+a|elettrica|installata)?\s*(?:totale\s+pari\s+a|pari\s+a)?\s*([0-9]+(?:[.,][0-9]+)?)\s*(MWp|MW|MWe|MWdc|MWac|kWp|kW)",
            r"([0-9]+(?:[.,][0-9]+)?)\s*(MWp|MW|MWe|MWdc|MWac|kWp|kW)",
        ]

        for pattern in patterns:
            match = re.search(pattern, text, flags=re.IGNORECASE)

            if match:
                return f"{match.group(1)} {match.group(2)}"

        return None

    def _power_text_to_mw(self, power_text: str | None) -> float | None:
        if not power_text:
            return None

        match = re.search(
            r"([0-9]+(?:[.,][0-9]+)?)\s*(MWp|MW|MWe|MWdc|MWac|kWp|kW)",
            power_text,
            flags=re.IGNORECASE,
        )

        if not match:
            return None

        number_text = match.group(1).replace(",", ".")

        try:
            number = float(number_text)
        except ValueError:
            return None

        unit = match.group(2).lower()

        if unit in {"kw", "kwp"}:
            return number / 1000

        return number

    def _extract_province(self, text: str) -> str | None:
        matches = re.findall(r"\(([A-Z]{2})\)", text)

        for match in matches:
            code = match.upper()
            if code in PROVINCE_TO_REGION:
                return code

        province_names = {
            "bari": "BA",
            "barletta": "BT",
            "andria": "BT",
            "trani": "BT",
            "brindisi": "BR",
            "foggia": "FG",
            "lecce": "LE",
            "taranto": "TA",
        }

        norm = self._normalize_for_match(text)

        for name, code in province_names.items():
            if re.search(rf"\b{re.escape(name)}\b", norm):
                return code

        return None

    def _extract_municipalities(self, text: str) -> list[str]:
        values: list[str] = []

        # I pattern sono volutamente ampi: il portale alterna molte formule redazionali.
        patterns = [
            r"\b(?:nel|nella|nei|nelle|del|dei|delle)?\s*(?:territorio\s+del|territori\s+dei|territorio\s+dei)?\s*Comune\s+di\s+(.+?)(?:\s+(?:e\s+relative|e\s+delle\s+relative|nonché|nonche|località|localita|alla\s+contrada|in\s+contrada|con\s+cavidotto|per\s+la\s+realizzazione|data\s+pubblicazione|societa\s+proponente)|[.;]|$)",
            r"\b(?:nel|nella|nei|nelle|del|dei|delle)?\s*(?:territorio\s+del|territori\s+dei|territorio\s+dei)?\s*Comuni\s+di\s+(.+?)(?:\s+(?:e\s+relative|e\s+delle\s+relative|nonché|nonche|località|localita|alla\s+contrada|in\s+contrada|con\s+cavidotto|per\s+la\s+realizzazione|data\s+pubblicazione|societa\s+proponente)|[.;]|$)",
            r"\bagro\s+di\s+(.+?)(?:\s+(?:e\s+relative|e\s+delle\s+relative|nonché|nonche|località|localita|alla\s+contrada|in\s+contrada|con\s+cavidotto|per\s+la\s+realizzazione|data\s+pubblicazione|societa\s+proponente)|[.;]|$)",
            r"\bubicat[oa]\s+(?:nel|nei)\s+Comune[i]?\s+di\s+(.+?)(?:\s+(?:e\s+relative|e\s+delle\s+relative|nonché|nonche|località|localita|alla\s+contrada|in\s+contrada|con\s+cavidotto|per\s+la\s+realizzazione|data\s+pubblicazione|societa\s+proponente)|[.;]|$)",
            r"\bda\s+realizzarsi\s+(?:nel|nei)\s+Comune[i]?\s+di\s+(.+?)(?:\s+(?:e\s+relative|e\s+delle\s+relative|nonché|nonche|località|localita|alla\s+contrada|in\s+contrada|con\s+cavidotto|per\s+la\s+realizzazione|data\s+pubblicazione|societa\s+proponente)|[.;]|$)",
        ]

        searchable_texts = [
            self._repair_mojibake(text),
            self._normalize_legal_text(text),
        ]

        for search_text in searchable_texts:
            for pattern in patterns:
                for match in re.finditer(pattern, search_text, flags=re.IGNORECASE):
                    raw = match.group(1)
                    for cleaned in self._split_and_clean_municipalities(raw):
                        if cleaned and cleaned not in values:
                            values.append(cleaned)

        return values[:10]

    def _split_and_clean_municipalities(self, raw: str) -> list[str]:
        raw = self._repair_mojibake(raw or "")
        raw = self._clean_text(raw)

        # Toglie contenuti tra parentesi, quasi sempre sigle provincia/codici.
        raw = re.sub(r"\([^)]*\)", " ", raw)

        # Taglia code amministrative o tecniche che il portale appende spesso ai comuni.
        raw = re.split(
            r"\s+(?:per\s+la\s+realizzazione|e\s+relative|e\s+delle\s+relative|nonché|nonche|località|localita|alla\s+contrada|in\s+contrada|con\s+cavidotto|cavidotto|stazione|cabina|data\s+pubblicazione|societa\s+proponente|proponente|codice\s+di\s+rintracciabilita|gestore\s+di\s+rete)\b",
            raw,
            maxsplit=1,
            flags=re.IGNORECASE,
        )[0]

        parts = re.split(r",|;|/|•|\s+e\s+|\s+ed\s+", raw, flags=re.IGNORECASE)

        values: list[str] = []
        seen: set[str] = set()

        for part in parts:
            cleaned = self._clean_municipality(part)
            if not cleaned:
                continue

            key = self._municipality_key(cleaned)
            if key in seen:
                continue

            seen.add(key)
            values.append(cleaned)

        return values

    def _municipality_key(self, value: str) -> str:
        return self._normalize_for_match(value)

    def _clean_municipality(self, value: str | None) -> str | None:
        value = self._repair_mojibake(value or "")
        value = self._clean_text(value)
        value = value.strip(" .:-,;()\"'")

        # Se arriva un frammento con parentesi non chiusa, quasi sempre è un codice/coda tecnica:
        # "Avetrana (cod", "Serracapriola (fg", ecc. Teniamo solo la parte prima.
        if "(" in value:
            value = value.split("(", 1)[0].strip(" .:-,;()\"'")

        # Rimuove residui di provincia rimasti da parentesi tagliate male.
        value = re.sub(
            r"\s*\b(?:ba|bt|br|fg|le|ta|pz)\b\s*$",
            "",
            value,
            flags=re.IGNORECASE,
        ).strip(" .:-,;()\"'")

        # Rimuove prefissi non comunali: "di Torre Susanna" -> "Torre Susanna".
        value = re.sub(
            r"^(?:di|del|della|dei|degli|delle|nel|nella|nei|nelle|in)\s+",
            "",
            value,
            flags=re.IGNORECASE,
        ).strip(" .:-,;()\"'")

        # Taglia subito porzioni tecniche o amministrative finite nel match.
        value = re.split(
            r"\s+(?:alla\s+contrada|alla|contrada|località|localita|consistenti|con\s+cavidotto|cavidotto|stazione|cabina|provincia|foglio|particella|società\s+proponente|societa\s+proponente|proponente|codice|id_vip|id_mattm)\b",
            value,
            maxsplit=1,
            flags=re.IGNORECASE,
        )[0].strip(" .:-,;()\"'")

        if not value:
            return None

        norm = self._normalize_for_match(value)

        # Singole parole o frammenti non-comune.
        exact_bad = {
            "in",
            "nel",
            "nella",
            "nei",
            "nelle",
            "con",
            "per",
            "fg",
            "ba",
            "bt",
            "br",
            "le",
            "ta",
            "pz",
        }
        if norm in exact_bad:
            return None

        # Un comune non deve contenere numeri: qui intercettiamo potenze, kV, date, codici.
        if re.search(r"\d", value):
            return None

        if len(value) > 55:
            return None

        bad_terms = [
            "potenza",
            "impianto",
            "opere",
            "opera",
            "infrastrutture",
            "infrastruttura",
            "connessione",
            "localita",
            "denominato",
            "denominata",
            "sito",
            "sita",
            "fonte",
            "rinnovabile",
            "cavidotto",
            "consistenti",
            "stazione",
            "cabina",
            "sottostazione",
            "utente",
            "mt",
            "at",
            "rtn",
            "mw",
            "mwp",
            "mwe",
            "mwac",
            "mw dc",
            "kw",
            "kwp",
            "kv",
            "mwh",
            "capacita",
            "nominale",
            "rintracciabilita",
            "gestore",
            "rete",
            "id vip",
            "id mattm",
            "proponente",
            "societa",
            "presidio",
            "caserma",
            "aree limitrofe",
            "zona industriale",
            "presso",
            "masseria",
            "integrato",
            "sistema",
            "accumulo",
            "capacità",
        ]

        if any(re.search(rf"\b{re.escape(item)}\b", norm) for item in bad_terms):
            return None

        # Taglia residui finali tipo "Brindisi In".
        value = re.sub(
            r"\s+(?:in|nel|nella|nei|nelle|nonché|nonche|con|per)$",
            "",
            value,
            flags=re.IGNORECASE,
        ).strip(" .:-,;()\"'")

        if not value:
            return None

        if re.fullmatch(r"[A-Z]{2}", value):
            return None

        return self._title_case_location(value)

    def _title_case_location(self, value: str) -> str:
        minor = {"di", "del", "della", "dei", "degli", "delle", "da", "de", "la", "lo", "il", "l"}
        words = []
        for word in value.split():
            lower = word.lower()
            if lower in minor:
                words.append(lower)
            else:
                words.append(word[:1].upper() + word[1:].lower())
        return " ".join(words)

    def _extract_pdf_url(self, soup: BeautifulSoup, page_url: str) -> str | None:
        for a in soup.find_all("a", href=True):
            href = a.get("href") or ""
            label = self._clean_text(a.get_text(" ", strip=True)).lower()
            absolute = urljoin(page_url, href)

            if ".pdf" in absolute.lower():
                return absolute

            if "scarica" in label and "pdf" in label:
                return absolute

        return None

    def _infer_project_type(self, text: str) -> str | None:
        norm = self._normalize_for_match(text)

        if (
            "agrivoltaico" in norm
            or "agrivoltaico" in norm
            or "agrovoltaico" in norm
            or "agro voltaico" in norm
            or "agro fotovoltaico" in norm
        ):
            return "Agrivoltaico"

        if "fotovoltaico" in norm or "fotovoltaica" in norm or "fonte solare" in norm:
            return "Fotovoltaico"

        return "FER"

    def _is_relevant(self, parsed: dict) -> bool:
        text = self._clean_text(
            " ".join(
                [
                    parsed.get("title") or "",
                    parsed.get("administrative_title") or "",
                    parsed.get("plain_text_sample") or "",
                    parsed.get("project_type_hint") or "",
                ]
            )
        )

        norm = self._normalize_for_match(text)

        has_pv = any(keyword in norm for keyword in PV_KEYWORDS)

        if not has_pv:
            return False

        # Esclude gli eolici puri.
        if any(keyword in norm for keyword in EXCLUDE_KEYWORDS):
            if "fotovoltaic" not in norm and "agrivoltaic" not in norm and "agrovoltaic" not in norm:
                return False

        return True

    # ------------------------------------------------------------------
    # TEXT
    # ------------------------------------------------------------------

    def _clean_text(self, value: str | None) -> str:
        value = html.unescape(value or "")
        value = self._repair_mojibake(value)
        return " ".join(value.replace("\xa0", " ").split()).strip()

    def _repair_mojibake(self, value: str | None) -> str:
        value = value or ""
        replacements = {
            "SocietÃ\xa0": "Società",
            "societÃ\xa0": "società",
            "SocietÃ ": "Società ",
            "societÃ ": "società ",
            "giÃ\xa0": "già",
            "giÃ ": "già ",
            "giŕ": "già",
            "Ã¨": "è",
            "Ã©": "é",
            "Ã ": "à ",
            "Ã¬": "ì",
            "Ã²": "ò",
            "Ã¹": "ù",
            "Â": "",
        }

        for bad, good in replacements.items():
            value = value.replace(bad, good)

        return value

    def _normalize_legal_text(self, value: str | None) -> str:
        value = self._repair_mojibake(value or "")
        value = unicodedata.normalize("NFKD", value)
        value = "".join(ch for ch in value if not unicodedata.combining(ch))
        value = value.lower()
        value = value.replace("\xa0", " ")
        value = re.sub(r"\s+", " ", value).strip()
        return value

    def _normalize_for_match(self, value: str | None) -> str:
        value = self._normalize_legal_text(value)
        value = re.sub(r"[^a-z0-9]+", " ", value)
        return " ".join(value.split())


if __name__ == "__main__":
    collector = SistemaPugliaEnergiaCollector()
    items = collector.fetch()
    print(f"items: {len(items)}")
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
