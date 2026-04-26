from __future__ import annotations

import html
import re
import time
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
            # Evita il classico "giŕ" al posto di "già".
            text = response.content.decode("windows-1252", errors="replace")

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

        title = self._extract_title(soup, plain)

        if not title:
            return None

        # Esclude pagine riepilogative settimanali: mischiano più atti e creano record sporchi.
        if self._is_weekly_roundup(title):
            return None

        combined = self._clean_text(f"{title} {plain}")

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

        return {
            "title": title,
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

    def _extract_title(self, soup: BeautifulSoup, plain: str) -> str | None:
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
            r"(Determinazione\s+del\s+Dirigente\s+Sezione\s+Transizione\s+Energetica\s+n\.\s*\d+\s+del\s+[^.]+)",
            plain,
            flags=re.IGNORECASE,
        )

        if match:
            return self._clean_text(match.group(1))

        title = soup.title.get_text(" ", strip=True) if soup.title else ""
        title = self._clean_text(title)

        if title and len(title) > 20:
            return title

        return None

    def _is_weekly_roundup(self, title: str | None) -> bool:
        norm = self._normalize_for_match(title or "")
        return "gli atti della settimana" in norm

    def _extract_proponent(self, text: str) -> str | None:
        patterns = [
            r"\bSocietà\s+proponente\s*:\s*(.+?)(?:\s+-\s+Partita\s+IVA|\s+-\s+P\.?\s*IVA|\s+C\.?\s*Fisc|\s+C\.?\s*F\.|\s+Cod\.?\s*Fis|\s+Sede\s+Legale|\s+Data\s+Pubblicazione|\s+\[Scarica|\s+Pubblicato|\s*$)",
            r"\bSocieta\s+proponente\s*:\s*(.+?)(?:\s+-\s+Partita\s+IVA|\s+-\s+P\.?\s*IVA|\s+C\.?\s*Fisc|\s+C\.?\s*F\.|\s+Cod\.?\s*Fis|\s+Sede\s+Legale|\s+Data\s+Pubblicazione|\s+\[Scarica|\s+Pubblicato|\s*$)",
            r"\bSocietà\s+Proponente\s*:\s*(.+?)(?:\s+-\s+Partita\s+IVA|\s+-\s+P\.?\s*IVA|\s+C\.?\s*Fisc|\s+C\.?\s*F\.|\s+Cod\.?\s*Fis|\s+Sede\s+Legale|\s+Data\s+Pubblicazione|\s+\[Scarica|\s+Pubblicato|\s*$)",
            r"\bProponente\s*:\s*(.+?)(?:\s+-\s+Partita\s+IVA|\s+-\s+P\.?\s*IVA|\s+C\.?\s*Fisc|\s+C\.?\s*F\.|\s+Cod\.?\s*Fis|\s+Sede\s+Legale|\s+Data\s+Pubblicazione|\s+\[Scarica|\s+Pubblicato|\s*$)",
            r"\bVoltura\s+alla\s+società\s+(.+?)(?:\s+con\s+sede|\s+-\s+P\.?\s*IVA|\s+C\.?\s*Fisc|\s+Data\s+Pubblicazione|\s+\[Scarica|\s*$)",
            r"\bVoltura\s+a\s+favore\s+di\s+(.+?)(?:\s+con\s+sede|\s+-\s+P\.?\s*IVA|\s+C\.?\s*Fisc|\s+Data\s+Pubblicazione|\s+\[Scarica|\s*$)",
            r"\bSocietà\s*:\s*(.+?)(?:\s+con\s+sede|\s+-\s+P\.?\s*IVA|\s+C\.?\s*Fisc|\s+Data\s+Pubblicazione|\s+\[Scarica|\s*$)",
        ]

        for pattern in patterns:
            for match in re.finditer(pattern, text, flags=re.IGNORECASE):
                value = self._clean_proponent(match.group(1))
                if value:
                    return value

        return None

    def _clean_proponent(self, value: str | None) -> str | None:
        value = self._clean_text(value or "")

        if not value:
            return None

        value = (
            value.replace("gi ", "già")
            .replace("giŕ", "già")
            .replace("giÃ ", "già")
            .replace("giÃ\xa0", "già")
            .replace("Ã¨", "è")
            .replace("Ã©", "é")
            .replace("Ã ", "à")
            .replace("Â", "")
        )

        value = value.strip(" .;:-,()")

        # Rimuove parentesi societarie/accessorie non utili.
        value = re.sub(
            r"\s*\((?:ex|già|gia|giŕ|subentrata)[^)]*(?:\)|$)",
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
            r"\.\s*(?:Presa\s+d['’]atto|Voltura|Autorizzazione|Aggiornamento|Modifica|Rettifica|Cambio\s+compagine).*$",
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
            r"potenza\s+(?:nominale|complessiva|pari\s+a|di|prevista\s+pari\s+a|elettrica)?\s*(?:totale\s+pari\s+a)?\s*([0-9]+(?:[.,][0-9]+)?)\s*(MWp|MW|MWe|MWdc|MWac|kWp|kW)",
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

        patterns = [
            r"\bComune\s+di\s+([A-ZÀ-Ú][A-Za-zÀ-Úà-ú'`\- ]+?)(?:\s*\([A-Z]{2}\)|,|\.|;|\s+località|\s+localita|\s+nonché|\s+e\s+relative|$)",
            r"\bComuni\s+di\s+([A-ZÀ-Ú][A-Za-zÀ-Úà-ú'`\- ,]+?)(?:\s*\([A-Z]{2}\)|\.|;|\s+località|\s+localita|\s+nonché|\s+e\s+relative|$)",
            r"\bnei\s+Comuni\s+di\s+([A-ZÀ-Ú][A-Za-zÀ-Úà-ú'`\- ,]+?)(?:\s*\([A-Z]{2}\)|\.|;|\s+località|\s+localita|\s+nonché|\s+e\s+relative|$)",
            r"\bnel\s+Comune\s+di\s+([A-ZÀ-Ú][A-Za-zÀ-Úà-ú'`\- ]+?)(?:\s*\([A-Z]{2}\)|,|\.|;|\s+località|\s+localita|\s+nonché|\s+e\s+relative|$)",
        ]

        for pattern in patterns:
            for match in re.finditer(pattern, text, flags=re.IGNORECASE):
                raw = match.group(1)
                raw = re.sub(r"\s+e\s+delle\s+relative.*$", "", raw, flags=re.IGNORECASE)

                for part in re.split(r",|\s+e\s+", raw, flags=re.IGNORECASE):
                    cleaned = self._clean_municipality(part)
                    if cleaned and cleaned not in values:
                        values.append(cleaned)

        return values[:10]

    def _clean_municipality(self, value: str | None) -> str | None:
        value = self._clean_text(value or "")
        value = value.strip(" .:-,;()")

        value = re.split(
            r"\s+(?:alla\s+contrada|alla|contrada|località|localita|consistenti|con\s+cavidotto|cavidotto|stazione|cabina)\b",
            value,
            flags=re.IGNORECASE,
        )[0]

        value = value.strip(" .:-,;()")

        if not value:
            return None

        if len(value) > 70:
            return None

        bad = [
            "potenza",
            "impianto",
            "opere",
            "infrastrutture",
            "connessione",
            "località",
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
            "mt",
            "at",
            "rtn",
        ]

        lowered = value.lower()

        if any(item in lowered for item in bad):
            return None

        # Taglia residui finali tipo "Brindisi In".
        value = re.sub(
            r"\s+(?:in|nel|nella|nei|nelle|nonché|nonche)$",
            "",
            value,
            flags=re.IGNORECASE,
        ).strip(" .:-,;()")

        if not value:
            return None

        return value.title()

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
        return " ".join(value.replace("\xa0", " ").split()).strip()

    def _normalize_for_match(self, value: str | None) -> str:
        value = self._clean_text(value or "").lower()
        value = value.replace("à", "a")
        value = value.replace("è", "e")
        value = value.replace("é", "e")
        value = value.replace("ì", "i")
        value = value.replace("ò", "o")
        value = value.replace("ù", "u")
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