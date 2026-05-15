from __future__ import annotations

import re
import time
from urllib.parse import urljoin, urlparse, parse_qs

from bs4 import BeautifulSoup

from app.collectors.base import BaseCollector, CollectorResult
from app.config import settings


BASE_URL = "http://valutazioneambientale.regione.basilicata.it/valutazioneambie/"

# Sezioni principali + anni recenti emersi dal menu del portale.
START_URLS = [
    # Screening
    {
        "url": urljoin(BASE_URL, "section.jsp?sec=100002"),
        "procedure": "Screening",
    },
    {
        "url": urljoin(BASE_URL, "section.jsp?sec=145352"),
        "procedure": "Screening - Anno 2025",
    },
    {
        "url": urljoin(BASE_URL, "section.jsp?sec=150868"),
        "procedure": "Screening - Anno 2026",
    },
    # VIA regionali
    {
        "url": urljoin(BASE_URL, "section.jsp?sec=100003"),
        "procedure": "VIA regionale",
    },
    {
        "url": urljoin(BASE_URL, "section.jsp?sec=145351"),
        "procedure": "VIA regionale - Anno 2025",
    },
    {
        "url": urljoin(BASE_URL, "section.jsp?sec=150867"),
        "procedure": "VIA regionale - Anno 2026",
    },
]

REQUEST_SLEEP_SECONDS = 0.10
MIN_POWER_MW = 5.0

PV_KEYWORDS = [
    "fotovoltaico",
    "fotovoltaica",
    "agrivoltaico",
    "agrivoltaica",
    "agrifotovoltaico",
    "agro-fotovoltaico",
    "agro fotovoltaico",
    "agro  voltaico",
    "agro voltaico",
    "fotovo",
    "fonte solare",
    "impianto fv",
]

EXCLUDE_KEYWORDS = [
    "eolico",
    "eolica",
    "rifiuti",
    "r.a.e.e",
    "raee",
    "discarica",
    "cava",
    "estrattiv",
    "idroelettric",
    "metanodotto",
    "gasdotto",
    "bonifica",
    "depuratore",
    "acquedotto",
    "amianto",
    "i.p.p.c",
    "ippc",
    "a.i.a",
    "aia",
]

MUNICIPALITY_TO_PROVINCE = {
    # MT
    "Bernalda": "MT",
    "Colobraro": "MT",
    "Ferrandina": "MT",
    "Grottole": "MT",
    "Montescaglioso": "MT",
    "Pomarico": "MT",
    # PZ
    "Banzi": "PZ",
    "Genzano di Lucania": "PZ",
    "Maschito": "PZ",
    "Melfi": "PZ",
    "Montemilone": "PZ",
    "Oppido Lucano": "PZ",
    "Palazzo San Gervasio": "PZ",
    "Tito": "PZ",
    "Tolve": "PZ",
    "Venosa": "PZ",
}

PROTECTED_MUNICIPALITIES = sorted(MUNICIPALITY_TO_PROVINCE, key=len, reverse=True)


class BasilicataCollector(BaseCollector):
    source_name = "basilicata"
    base_url = BASE_URL

    def fetch(self) -> list[CollectorResult]:
        results: list[CollectorResult] = []
        seen_ids: set[str] = set()

        for source in START_URLS:
            html_page = self._get_html(source["url"])

            if not html_page:
                continue

            rows = self._parse_list_page(
                html_page=html_page,
                page_url=source["url"],
                procedure=source["procedure"],
            )

            for row in rows:
                normalized = self._normalize_row(row)

                if not normalized:
                    continue

                if not self._is_relevant(normalized):
                    continue

                external_id = self._build_external_id(normalized["source_url"])

                if external_id in seen_ids:
                    continue

                seen_ids.add(external_id)

                title = normalized["title"]

                results.append(
                    CollectorResult(
                        external_id=external_id,
                        source_url=normalized["source_url"],
                        title=title[:250],
                        payload={
                            "title": title[:900],
                            "proponent": normalized.get("proponent"),
                            "status_raw": normalized.get("status_raw"),
                            "region": "Basilicata",
                            "province": normalized.get("province"),
                            "municipalities": normalized.get("municipalities") or [],
                            "power": normalized.get("power"),
                            "power_mw": normalized.get("power_mw"),
                            "project_type_hint": normalized.get("project_type_hint"),
                            "procedure": normalized.get("procedure"),
                            "category": "Basilicata VIA/Screening",
                            "detail_url": normalized["source_url"],
                            "plain_text_sample": normalized.get("plain_text_sample"),
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
                "Chrome/120 Safari/537.36"
            ),
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "it-IT,it;q=0.9,en;q=0.8",
            "Referer": BASE_URL,
            "Connection": "close",
        }

    def _get_html(self, url: str) -> str | None:
        try:
            response = self.session.get(
                url,
                headers=self._headers(),
                timeout=settings.request_timeout,
                allow_redirects=True,
            )
            response.raise_for_status()
            return response.content.decode("utf-8", errors="replace")
        except Exception:
            return None

    # ------------------------------------------------------------------
    # LIST PARSING
    # ------------------------------------------------------------------

    def _parse_list_page(self, html_page: str, page_url: str, procedure: str) -> list[dict]:
        soup = BeautifulSoup(html_page, "html.parser")
        rows: list[dict] = []

        for h2 in soup.find_all("h2"):
            a = h2.find("a", href=True)
            if not a:
                continue

            title = self._clean_text(a.get_text(" ", strip=True))
            detail_url = urljoin(page_url, a.get("href") or "")

            subtitle = ""
            p = h2.find_next_sibling("p")
            if p and "subtitle" in (p.get("class") or []):
                subtitle = self._clean_text(p.get_text(" ", strip=True))

            combined = self._clean_text(" ".join([title, subtitle]))

            if not combined:
                continue

            rows.append(
                {
                    "title": title,
                    "subtitle": subtitle,
                    "combined": combined,
                    "source_url": detail_url,
                    "procedure": procedure,
                }
            )

        return rows

    def _normalize_row(self, row: dict) -> dict | None:
        title = self._clean_text(row.get("title"))
        subtitle = self._clean_text(row.get("subtitle"))
        combined = self._clean_text(row.get("combined"))

        if not title:
            return None

        power = self._extract_power_text(combined)
        power_mw = self._power_text_to_mw(power)

        municipalities = self._extract_municipalities(combined)
        province = self._extract_province(combined, municipalities)
        proponent = self._extract_proponent(title=title, subtitle=subtitle)

        return {
            "title": title,
            "proponent": self._manual_proponent_override(row.get("source_url"), title, proponent),
            "status_raw": row.get("procedure"),
            "province": province,
            "municipalities": municipalities,
            "power": power,
            "power_mw": power_mw,
            "project_type_hint": self._infer_project_type(combined),
            "procedure": row.get("procedure"),
            "source_url": row.get("source_url"),
            "plain_text_sample": combined[:900],
        }

    def _is_relevant(self, item: dict) -> bool:
        text = self._normalize_for_match(
            " ".join(
                str(item.get(k) or "")
                for k in ["title", "proponent", "plain_text_sample", "procedure"]
            )
        )

        if not any(self._normalize_for_match(k) in text for k in PV_KEYWORDS):
            return False

        # Se compare eolico/rifiuti/cava ecc. e non c'è un segnale PV forte, scarta.
        if any(self._normalize_for_match(k) in text for k in EXCLUDE_KEYWORDS):
            return False

        power_mw = item.get("power_mw")
        if power_mw is None or power_mw < MIN_POWER_MW:
            return False

        if not item.get("proponent"):
            return False

        if not item.get("municipalities"):
            return False

        if item.get("province") not in {"PZ", "MT"}:
            return False

        return True

    # ------------------------------------------------------------------
    # EXTRACTORS
    # ------------------------------------------------------------------

    def _extract_power_text(self, text: str | None) -> str | None:
        if not text:
            return None

        number = r"([0-9]+(?:[.\s][0-9]{3})*(?:[,\.][0-9]+)?|[0-9]+(?:[,\.][0-9]+)?)"
        unit = r"(MWp|MW|MWe|kWp|kW|KWp|KW)"

        preferred = [
            rf"potenza\s+(?:complessiva|nominale|pari)?\s*(?:di|pari\s+a)?\s*{number}\s*{unit}",
            rf"di\s+potenza\s+(?:pari\s+a)?\s*{number}\s*{unit}",
            rf"pot\.?\s*pari\s*a?\s*{number}\s*{unit}",
            rf"pot\.\s*{number}\s*{unit}",
            rf"{number}\s*{unit}",
        ]

        for pattern in preferred:
            match = re.search(pattern, text, flags=re.IGNORECASE)
            if match:
                # Negli ultimi due gruppi ci sono numero e unità.
                groups = match.groups()
                return f"{groups[-2]} {groups[-1]}"

        return None

    def _power_text_to_mw(self, power_text: str | None) -> float | None:
        if not power_text:
            return None

        match = re.search(
            r"([0-9]+(?:[.\s][0-9]{3})*(?:[,\.][0-9]+)?|[0-9]+(?:[,\.][0-9]+)?)\s*(MWp|MW|MWe|kWp|kW|KWp|KW)",
            power_text,
            flags=re.IGNORECASE,
        )

        if not match:
            return None

        raw_value = match.group(1).strip()
        unit = match.group(2).lower()

        # Formati italiani:
        # 19.992,00 kWp -> 19992.00 kW -> 19.992 MW
        # 16.522,94 kWp -> 16522.94 kW -> 16.52294 MW
        # 19,97 MW -> 19.97 MW
        if "," in raw_value:
            raw_value = raw_value.replace(".", "").replace(" ", "").replace(",", ".")
        else:
            raw_value = raw_value.replace(" ", "")

        try:
            value = float(raw_value)
        except ValueError:
            return None

        if unit in {"kw", "kwp"}:
            return round(value / 1000.0, 6)

        return round(value, 6)

    def _extract_proponent(self, title: str, subtitle: str | None) -> str | None:
        subtitle = self._clean_text(subtitle)

        if subtitle:
            value = re.sub(r"^Proponente:\s*", "", subtitle, flags=re.IGNORECASE).strip()
            # Evita sottotitoli descrittivi troppo lunghi: in quel caso prova dal titolo.
            if len(value) <= 120 and not self._looks_like_project_description(value):
                return self._clean_proponent(value)

        # Fallback: proponente spesso in coda al titolo dopo punto o virgolette.
        candidates = []

        for sep in [". ", "”. ", " - "]:
            if sep in title:
                candidates.append(title.split(sep)[-1])

        # fallback specifici su parole finali note; non serve essere eleganti, serve robustezza.
        tail = title[-120:]
        candidates.append(tail)

        for candidate in candidates:
            cleaned = self._clean_proponent(candidate)
            if cleaned and not self._looks_like_project_description(cleaned):
                return cleaned

        return None


    def _manual_proponent_override(
        self,
        source_url: str | None,
        title: str | None,
        proponent: str | None,
    ) -> str | None:
        url = (source_url or "").lower()
        title_norm = self._normalize_for_match(title or "")
        prop = self._clean_text(proponent or "").strip(" .,:;??-")

        # Override puntuali per casi noti Basilicata.
        if "id=150208" in url or "citrino new energy" in title_norm or "miro" in title_norm:
            return "CITRINO NEW ENERGY S.R.L."

        if "id=147887" in url or "cat energy" in title_norm:
            return "CAT ENERGY S.r.l."

        if "id=148284" in url or "erp 1" in title_norm:
            return "ERP 1 S.r.l."

        # Normalizza forme legali senza punto finale.
        if prop:
            prop = re.sub(r"\bS\.r\.l$", "S.r.l.", prop, flags=re.IGNORECASE)
            prop = re.sub(r"\bS\.p\.A$", "S.p.A.", prop, flags=re.IGNORECASE)
            prop = re.sub(r"\bS\.p\.a$", "S.p.A.", prop, flags=re.IGNORECASE)

        canon = {
            "no new energy s.r.l": "CITRINO NEW ENERGY S.R.L.",
            "cat energy s.r.l": "CAT ENERGY S.r.l.",
            "erp 1 s.r.l": "ERP 1 S.r.l.",
        }

        key = prop.lower().strip(" .,:;??-")
        return canon.get(key, prop or None)


    def _looks_like_project_description(self, value: str | None) -> bool:
        norm = self._normalize_for_match(value or "")
        bad = [
            "progetto",
            "impianto",
            "potenza",
            "comune",
            "localita",
            "località",
            "opere",
            "connessione",
            "realizzazione",
            "costruzione",
            "esercizio",
        ]
        return any(x in norm for x in bad)

    def _clean_proponent(self, value: str | None) -> str | None:
        value = self._clean_text(value)
        value = value.strip(" .,:;–—-\"'“”")

        if not value:
            return None

        value = re.sub(r"^Proponente:\s*", "", value, flags=re.IGNORECASE).strip()
        value = re.sub(r"\s+", " ", value)

        # Taglia dopo forma legale quando c'è altro testo.
        legal = r"(S\.r\.l\.?|SRL|Srl|S\.p\.A\.?|S\.p\.a\.?|SPA|SpA|SRLS|S\.R\.L\.S\.?)"
        m = re.search(rf"\b(.+?\b{legal})\b", value, flags=re.IGNORECASE)
        if m:
            value = m.group(1).strip(" .,:;–—-")

        canon = {
            "fimenergia": "FIMENERGIA SRL",
            "fimenergia srl": "FIMENERGIA SRL",
            "gen solar srls": "GEN SOLAR SRLS",
            "7 piu' energia s.r.l": "7 PIU' ENERGIA S.r.l.",
            "7piu' energia": "7 PIU' ENERGIA S.r.l.",
            "castagna s.r.l": "CASTAGNA S.R.L.",
            "opdenergy tavoliere 3 s.r.l": "OPDENERGY TAVOLIERE 3 S.R.L.",
            "smartenergyit2106 s.r.l": "SMARTENERGYIT2106 S.r.l.",
            "columns energy s.p.a": "COLUMNS ENERGY S.p.A.",
            "tito energia group s.r.l": "TITO ENERGIA GROUP S.R.L.",
        }

        key = value.lower().strip(" .,:;–—-")
        return canon.get(key, value)

    def _extract_municipalities(self, text: str | None) -> list[str]:
        if not text:
            return []

        norm_text = self._normalize_for_match(text)
        found: list[str] = []

        for municipality in PROTECTED_MUNICIPALITIES:
            municipality_norm = self._normalize_for_match(municipality)
            if re.search(rf"\b{re.escape(municipality_norm)}\b", norm_text):
                if municipality not in found:
                    found.append(municipality)

        return found[:10]

    def _extract_province(self, text: str | None, municipalities: list[str]) -> str | None:
        if text:
            matches = re.findall(r"\((PZ|MT)\)|\b(PZ|MT)\b", text.upper())
            for m in matches:
                code = m[0] or m[1]
                if code in {"PZ", "MT"}:
                    return code

        provinces = {MUNICIPALITY_TO_PROVINCE.get(m) for m in municipalities}
        provinces.discard(None)

        if len(provinces) == 1:
            return provinces.pop()

        # Per multi-provincia usa la provincia del primo comune principale.
        if municipalities:
            return MUNICIPALITY_TO_PROVINCE.get(municipalities[0])

        return None

    def _infer_project_type(self, text: str | None) -> str:
        norm = self._normalize_for_match(text or "")

        if "agrivolta" in norm or "agrifotovolta" in norm or "agro fotovolta" in norm or "agrovolta" in norm:
            return "Agrivoltaico"

        return "Fotovoltaico"

    def _build_external_id(self, source_url: str) -> str:
        parsed = urlparse(source_url)
        qs = parse_qs(parsed.query)
        detail_id = (qs.get("id") or [""])[0]

        if detail_id:
            return f"basilicata_{detail_id}"

        cleaned = re.sub(r"\W+", "_", source_url.lower()).strip("_")
        return f"basilicata_{cleaned}"[:250]

    # ------------------------------------------------------------------
    # TEXT HELPERS
    # ------------------------------------------------------------------

    def _normalize_for_match(self, value: str | None) -> str:
        text = self._clean_text(value or "").lower()
        replacements = {
            "à": "a",
            "è": "e",
            "é": "e",
            "ì": "i",
            "ò": "o",
            "ù": "u",
            "’": "'",
            "“": '"',
            "”": '"',
        }
        for old, new in replacements.items():
            text = text.replace(old, new)
        return text

    def _clean_text(self, value: str | None) -> str:
        if not value:
            return ""
        return " ".join(str(value).replace("\xa0", " ").split()).strip()
