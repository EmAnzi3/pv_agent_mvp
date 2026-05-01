from __future__ import annotations

import json
import re
from pathlib import Path
from urllib.parse import urljoin

from app.collectors.base import BaseCollector, CollectorResult
from app.config import settings


PV_KEYWORDS = [
    "fotovolta",
    "agrivolta",
    "agrovolta",
    "bess",
    "accumulo",
    "solare",
    "moduli fotovoltaici",
    "fonte solare",
]

NON_PV_EXCLUDE = [
    "idroelettrico",
    "idropotabile",
    "acquedotto",
    "allevamento",
    "rifiuti",
    "discarica",
    "cava",
    "variazione non sostanziale dell’a.i.a",
    "variazione non sostanziale dell'a.i.a",
]

# Da consts.js:
# SCO = 1
# VER = 2
# VIA = 3
# VAL_PRE = 5
# PAUR = 15
TIPO_PROCEDURA_LIST = "1,2,3,5,15"

# Da getAllSettori.html:
# 2 = IND. ENERGETICA ED ESTRATTIVA
# 8 = ALTRI PROGETTI
LOMBARDIA_SETTORI_TARGET = {"2", "8"}


PROVINCE_NAME_TO_CODE = {
    "BERGAMO": "BG",
    "BRESCIA": "BS",
    "COMO": "CO",
    "CREMONA": "CR",
    "LECCO": "LC",
    "LODI": "LO",
    "MANTOVA": "MN",
    "MILANO": "MI",
    "MONZA": "MB",
    "MONZA E BRIANZA": "MB",
    "PAVIA": "PV",
    "SONDRIO": "SO",
    "VARESE": "VA",
}


MUNICIPALITY_TO_PROVINCE = {
    # BG
    "Caravaggio": "BG",
    # BS
    "Bagnolo Mella": "BS",
    "Calvisano": "BS",
    "Ghedi": "BS",
    "Montichiari": "BS",
    "Pralboino": "BS",
    "Vezza D'Oglio": "BS",
    "Vezza D’oglio": "BS",
    "Manerbio": "BS",
    # CO
    "Figino Serenza": "CO",
    # CR
    "Castelleone": "CR",
    "Piadena Drizzona": "CR",
    # LC
    "Cesana Brianza": "LC",
    # LO
    "Cervignano D'Adda": "LO",
    "Cervignano D’Adda": "LO",
    "Montanaso Lombardo": "LO",
    "Mulazzano": "LO",
    "Terranova Dei Passerini": "LO",
    "Zelo Buon Persico": "LO",
    # MB
    "Bovisio Masciago": "MB",
    "Burago Di Molgora": "MB",
    "Cavenago Di Brianza": "MB",
    "Ornago": "MB",
    # MI
    "Abbiategrasso": "MI",
    "Arconate": "MI",
    "Busto Garolfo": "MI",
    "Cerro Maggiore": "MI",
    "Liscate": "MI",
    "Peschiera Borromeo": "MI",
    "Pozzo D'Adda": "MI",
    "Pozzo D’Adda": "MI",
    "Settala": "MI",
    "Tribiano": "MI",
    # MN
    "Borgo Virgilio": "MN",
    "Casaloldo": "MN",
    "Castel D'Ario": "MN",
    "Castel D’ario": "MN",
    "Commessaggio": "MN",
    "Borgo Mantovano": "MN",
    "Castellucchio": "MN",
    "Marcaria": "MN",
    "Villimpenta": "MN",
    "Curtatone": "MN",
    "Goito": "MN",
    "Guidizzolo": "MN",
    "Marmirolo": "MN",
    "Medole": "MN",
    "Roncoferraro": "MN",
    "San Giorgio Bigarello": "MN",
    "San Giorgio Di Bigarello": "MN",
    "Suzzara": "MN",
    "Volta Mantovana": "MN",
    # PV
    "Battuda": "PV",
    "Bastida Pancarana": "PV",
    "Castelletto Di Branduzzo": "PV",
    "Cergnago": "PV",
    "Chignolo Po": "PV",
    "Dorno": "PV",
    "Mede": "PV",
    "Mezzana Bigli": "PV",
    "Miradolo Terme": "PV",
    "Mortara": "PV",
    "Olevano Di Lomellina": "PV",
    "Ottobiano": "PV",
    "Pieve Albignola": "PV",
    "Pieve Del Cairo": "PV",
    "San Giorgio Di Lomellina": "PV",
    "Santa Cristina E Bissone": "PV",
    "Sannazzaro De' Burgondi": "PV",
    "Sannazzaro De’ Burgondi": "PV",
    "Scaldasole": "PV",
    "Sommo": "PV",
    "Tromello": "PV",
    "Valeggio": "PV",
    "Voghera": "PV",
    "Pizzale": "PV",
}


PROTECTED_MUNICIPALITIES = [
    "Santa Cristina e Bissone",
    "Sannazzaro de' Burgondi",
    "Sannazzaro de’ Burgondi",
    "San Giorgio di Lomellina",
    "San Giorgio Bigarello",
    "San Giorgio di Bigarello",
    "Olevano di Lomellina",
    "Castelletto di Branduzzo",
    "Pieve del Cairo",
    "Pieve Albignola",
    "Mezzana Bigli",
    "Borgo Virgilio",
    "Borgo Mantovano",
    "Busto Garolfo",
    "Pozzo d'Adda",
    "Pozzo d’Adda",
    "Castel d'Ario",
    "Castel d’Ario",
    "Burago di Molgora",
    "Cavenago di Brianza",
    "Cesana Brianza",
    "Montanaso Lombardo",
    "Zelo Buon Persico",
    "Miradolo Terme",
    "Chignolo Po",
    "Bastida Pancarana",
    "Bovisio Masciago",
    "Peschiera Borromeo",
    "Cerro Maggiore",
    "Bagnolo Mella",
    "Volta Mantovana",
]


class LombardiaCollector(BaseCollector):
    source_name = "lombardia"
    base_url = "https://www.silvia.servizirl.it/silviaweb/"

    def fetch(self) -> list[CollectorResult]:
        debug_base = Path("/app/reports/debug_lombardia")
        debug_base.mkdir(parents=True, exist_ok=True)

        settori = self._load_settori(debug_base)
        if not settori:
            self._write_json(
                debug_base / "settori_empty.json",
                {"note": "Nessun settore restituito da getAllSettori.html"},
            )
            return []

        self._write_json(debug_base / "settori_raw.json", settori)

        pv_settori = []
        for s in settori:
            settore_id = s.get("idSettore") or s.get("id_settore") or s.get("id")

            descr = self._clean_text(
                str(
                    s.get("descrSettore")
                    or s.get("siglaSettore")
                    or s.get("descSettore")
                    or s.get("descrizione")
                    or s.get("settore")
                    or s.get("descr")
                    or s.get("label")
                    or ""
                )
            )

            if str(settore_id) in LOMBARDIA_SETTORI_TARGET:
                pv_settori.append(
                    {
                        "id": str(settore_id),
                        "descr": descr,
                        "raw": s,
                    }
                )

        self._write_json(debug_base / "settori_pv.json", pv_settori)

        results: list[CollectorResult] = []
        seen_ids: set[str] = set()

        for settore in pv_settori:
            settore_id = settore["id"]
            rows = self._search_by_settore(str(settore_id), debug_base)

            if not rows:
                continue

            self._write_json(
                debug_base / f"normalized_source_rows_{settore_id}.json",
                rows[:50],
            )

            for row in rows:
                normalized = self._normalize_row(row)
                if not normalized:
                    continue

                title = normalized["title"]

                if not self._is_pv_related(title):
                    continue

                external_id = self._build_external_id(
                    title,
                    normalized.get("proponent"),
                    normalized.get("detail_url"),
                )

                if external_id in seen_ids:
                    continue

                seen_ids.add(external_id)

                results.append(
                    CollectorResult(
                        external_id=external_id,
                        source_url=normalized.get("detail_url") or self.base_url,
                        title=title[:250],
                        payload={
                            "title": title[:700],
                            "proponent": normalized.get("proponent"),
                            "status_raw": normalized.get("status"),
                            "region": "Lombardia",
                            "province": normalized.get("province"),
                            "municipalities": normalized.get("municipalities") or [],
                            "power": normalized.get("power"),
                            "project_type_hint": normalized.get("procedure") or "Lombardia SILVIA",
                            "procedure": normalized.get("procedure"),
                            "detail_url": normalized.get("detail_url"),
                        },
                    )
                )

        return results

    def _load_settori(self, debug_base: Path) -> list[dict]:
        url = urljoin(self.base_url, "getAllSettori.html")

        try:
            response = self.session.get(url, timeout=settings.request_timeout)
            response.raise_for_status()

            self._write_text(debug_base / "getAllSettori_response.txt", response.text)

            data = response.json()
            if isinstance(data, list):
                return data

            self._write_json(
                debug_base / "getAllSettori_unexpected_json.json",
                data,
            )
            return []

        except Exception as exc:
            self._write_text(debug_base / "getAllSettori_error.txt", str(exc))
            return []

    def _search_by_settore(self, settore_id: str, debug_base: Path) -> list[dict]:
        all_rows: list[dict] = []

        years = ["2026", "2025", "2024"]

        for year in years:
            params = {
                "tipoProcedura": TIPO_PROCEDURA_LIST,
                "rgroupAutorita": "",
                "codiceProcedura": "",
                "descrProcedura": "",
                "idMacroStato": "",
                "interessati": "",
                "strFiltroEnte": "",
                "optionSettore": settore_id,
                "dataAvvioDa": "",
                "dataAvvioA": "",
                "dataDepositoDa": "",
                "dataDepositoA": "",
                "checkedAutorita": "",
                "checkedTipologiaProg": "",
                "tipoProponente": "",
                "idReferenteSelect": "",
                "descrProponente": "",
                "idTipoEnte": "",
                "idEnteACSelected": "",
                "accTipoEnte": "",
                "accTipoProc": "",
                "annoAvvio": year,
                "idSett": settore_id,
            }

            try:
                response = self.session.get(
                    urljoin(self.base_url, "avviaRicercaProcedura.html"),
                    params=params,
                    timeout=90,
                )
                response.raise_for_status()

                self._write_text(
                    debug_base / f"avviaRicercaProcedura_{settore_id}_{year}_response.txt",
                    response.text,
                )

                data = response.json()

                if isinstance(data, list):
                    all_rows.extend(data)
                else:
                    self._write_json(
                        debug_base / f"avviaRicercaProcedura_{settore_id}_{year}_unexpected_json.json",
                        data,
                    )

            except Exception as exc:
                self._write_text(
                    debug_base / f"avviaRicercaProcedura_{settore_id}_{year}_error.txt",
                    str(exc),
                )

        return all_rows

    def _normalize_row(self, row: dict) -> dict | None:
        if not isinstance(row, dict):
            return None

        title = self._first_non_empty(
            row,
            [
                "descrProgetto",
                "descrProcedura",
                "titolo",
                "oggetto",
                "descrizione",
                "descProcedura",
                "nomeProcedura",
                "procedura",
            ],
        )

        if not title:
            return None

        proponent = self._clean_proponenti(
            self._first_non_empty(
                row,
                [
                    "proponenti",
                    "proponente",
                    "descrProponente",
                    "descrEnteAzienda",
                    "enteProponente",
                    "referente",
                    "richiedente",
                ],
            )
        )

        status = self._extract_status(row)
        procedure = self._extract_procedure(row)

        proc_id = (
            row.get("idProgetto")
            or row.get("idProcedura")
            or row.get("id_procedura")
            or row.get("id")
            or row.get("idStudio")
        )

        detail_url = None
        if proc_id:
            detail_url = urljoin(self.base_url, f"#/scheda-sintesi/{proc_id}")

        municipalities = self._extract_municipalities(title)
        province = self._extract_province(title, municipalities)
        power = self._extract_power(title)

        return {
            "title": title,
            "proponent": proponent,
            "status": status,
            "procedure": procedure,
            "detail_url": detail_url,
            "municipalities": municipalities,
            "province": province,
            "power": power,
        }

    def _extract_status(self, row: dict) -> str | None:
        macro_stato = row.get("macroStato") or {}

        if isinstance(macro_stato, dict):
            status = self._clean_text(str(macro_stato.get("descrMacroStato") or ""))
            if status:
                return status

        return self._first_non_empty(
            row,
            [
                "descrMacroStato",
                "stato",
                "macroStato",
                "descrStato",
                "descStato",
            ],
        )

    def _extract_procedure(self, row: dict) -> str | None:
        return self._first_non_empty(
            row,
            [
                "group",
                "descrTipoProcedura",
                "tipoProcedura",
                "descTipoProcedura",
                "proceduraTipo",
            ],
        )

    def _first_non_empty(self, row: dict, keys: list[str]) -> str | None:
        for key in keys:
            value = row.get(key)
            if value is None:
                continue

            cleaned = self._clean_text(str(value))
            if cleaned and cleaned.lower() != "none":
                return cleaned

        return None

    def _clean_proponenti(self, value: str | None) -> str | None:
        if not value:
            return None

        text = self._clean_text(value)

        if not text:
            return None

        # SILVIA a volte restituisce wrapper del tipo:
        # (Azienda: NOME)
        # (Persona Fisica): NOME
        # ): NOME
        # La pulizia deve togliere il wrapper senza distruggere nomi reali
        # tipo "HOLCIM (ITALIA) S.P.A.".
        text = re.sub(
            r"\(\s*(?:Azienda|Ente|Persona\s+Fisica)\s*\)\s*:?\s*",
            "",
            text,
            flags=re.IGNORECASE,
        )

        text = re.sub(
            r"\(\s*(?:Azienda|Ente|Persona\s+Fisica)\s*:?\s*",
            "",
            text,
            flags=re.IGNORECASE,
        )

        text = re.sub(
            r"^\s*\)\s*:?\s*",
            "",
            text,
            flags=re.IGNORECASE,
        )

        text = re.sub(
            r";\s*\)\s*:?\s*",
            "; ",
            text,
            flags=re.IGNORECASE,
        )

        text = text.replace(");", ";")
        text = re.sub(r"\s+;", ";", text)
        text = re.sub(r";\s*;", ";", text)
        text = self._clean_text(text.strip(" ;"))

        # Correzione puntuale: SILVIA tronca/espone male questo proponente
        # in almeno una riga del dataset.
        if self._normalize_for_match(text) == "holcim (italia":
            text = "HOLCIM (ITALIA) S.P.A."

        return text or None

    def _is_pv_related(self, text: str | None) -> bool:
        if not text:
            return False

        lowered = self._normalize_for_match(text)

        if not any(keyword in lowered for keyword in PV_KEYWORDS):
            return False

        has_strong_pv = any(
            keyword in lowered
            for keyword in [
                "fotovolta",
                "agrivolta",
                "agrovolta",
                "moduli fotovoltaici",
                "fonte solare",
            ]
        )

        if not has_strong_pv and any(fragment in lowered for fragment in NON_PV_EXCLUDE):
            return False

        if any(fragment in lowered for fragment in NON_PV_EXCLUDE) and "potenza" not in lowered:
            return False

        return True

    def _extract_power(self, text: str | None) -> str | None:
        if not text:
            return None

        number_unit = (
            r"([0-9]+(?:[.\s'’][0-9]{3})*(?:[,\.][0-9]+)?|[0-9]+(?:[,\.][0-9]+)?)"
            r"\s*"
            r"(MWP|MWp|Mwp|mwp|MW|Mw|mw|KWP|KWp|Kwp|kWp|kwp|KW|kW|kw)"
        )

        preferred_patterns = [
            rf"potenza\s+fotovoltaica\s+pari\s+a\s+{number_unit}",
            rf"potenza\s+di\s+picco\s+pari\s+a\s+{number_unit}",
            rf"potenza\s+di\s+picco\s+di\s+{number_unit}",
            rf"potenza\s+complessiva\s+di\s+picco\s+di\s+{number_unit}",
            rf"potenza\s+nominale\s+complessiva\s+pari\s+a\s+{number_unit}",
            rf"potenza\s+nominale\s+complessiva\s+(?:di\s+)?{number_unit}",
            rf"potenza\s+nominale\s+prevista\s+di\s+{number_unit}",
            rf"potenza\s+nominale\s+pari\s+a\s+{number_unit}",
            rf"potenza\s+complessiva\s+(?:pari\s+a\s+|di\s+)?{number_unit}",
            rf"potenza\s+dc\s+di\s+{number_unit}",
            rf"potenza\s+pari\s+a\s+{number_unit}",
            rf"potenza\s+di\s+{number_unit}",
            rf"\bda\s+{number_unit}",
            rf"potenza\s+{number_unit}",
        ]

        for pattern in preferred_patterns:
            for match in re.finditer(pattern, text, flags=re.IGNORECASE):
                if self._is_storage_power_match(text, match.start(), match.end()):
                    continue

                return f"{match.group(1)} {match.group(2)}"

        generic_pattern = number_unit

        for match in re.finditer(generic_pattern, text, flags=re.IGNORECASE):
            if self._is_storage_power_match(text, match.start(), match.end()):
                continue

            return f"{match.group(1)} {match.group(2)}"

        return None

    def _is_storage_power_match(self, text: str, start: int, end: int) -> bool:
        before = text[max(0, start - 140) : start].lower()
        phrase = text[max(0, start - 60) : min(len(text), end + 20)].lower()

        # Se il sito scrive esplicitamente "potenza fotovoltaica", è FV anche se
        # la descrizione generale cita BESS/accumulo.
        if "potenza fotovoltaica" in phrase:
            return False

        storage_words = [
            "bess",
            "accumulo",
            "storage",
            "batteria",
            "batterie",
            "sistema di accumulo",
            "di un sistema di accumulo",
        ]

        return any(word in before for word in storage_words)

    def _extract_province(self, title: str | None, municipalities: list[str] | None = None) -> str | None:
        if not title:
            return None

        text = self._clean_text(title) or ""

        province_codes = re.findall(r"\(([A-Z]{2})\)", text)

        for code in province_codes:
            if code in set(PROVINCE_NAME_TO_CODE.values()):
                return code

        province_name_match = re.search(
            r"\bprovincia\s+di\s+([A-ZÀ-ÚA-Za-zà-ú'’ ]+)",
            text,
            flags=re.IGNORECASE,
        )

        if province_name_match:
            province_name = self._clean_text(province_name_match.group(1))
            province_name = re.split(
                r"\s*(?:,|\.|\(|\)|per\s+una\s+potenza|di\s+potenza|e\s+relative|relative\s+opere)\b",
                province_name,
                maxsplit=1,
                flags=re.IGNORECASE,
            )[0].strip()

            code = PROVINCE_NAME_TO_CODE.get(self._normalize_for_match(province_name).upper())

            if code:
                return code

        for municipality in municipalities or []:
            code = MUNICIPALITY_TO_PROVINCE.get(municipality)

            if code:
                return code

            normalized = self._normalize_for_match(municipality)

            for known_municipality, known_code in MUNICIPALITY_TO_PROVINCE.items():
                if self._normalize_for_match(known_municipality) == normalized:
                    return known_code

        return None

    def _extract_municipalities(self, title: str | None) -> list[str]:
        if not title:
            return []

        text = self._clean_text(title) or ""

        results: list[str] = []

        direct_patterns = [
            r"\b(?:nel|nello|in|sito nel|sita nel|ubicato nel|ubicata nel|localizzato nel|localizzata nel|ricadente nel|da realizzarsi nel|realizzarsi nel)\s+(?:territorio\s+del\s+)?(?:Comune|comune)\s+di\s+(.+?)(?=\s*\([A-Z]{2}\)|,|\.\s|;|\s+e\s+relative|\s+relative\s+opere|\s+con\s+|\s+per\s+|\s+in\s+provincia|$)",
            r"\b(?:nel|nello|in)\s+(?:territorio\s+)?comunale\s+di\s+(.+?)(?=\s*\([A-Z]{2}\)|,|\.\s|;|\s+e\s+relative|\s+relative\s+opere|\s+con\s+|\s+per\s+|\s+in\s+provincia|$)",
            r"\b(?:in|nell')\s+agro\s+di\s+(.+?)(?=\s*\([A-Z]{2}\)|,|\.\s|;|\s+via\s+|\s+loc\.|\s+località|\s+localita|$)",
            r"\bpresso\s+([A-ZÀ-Ú][A-Za-zÀ-Úà-ú'’`\- ]+?)\s*\([A-Z]{2}\)",
        ]

        for pattern in direct_patterns:
            for match in re.finditer(pattern, text, flags=re.IGNORECASE):
                for municipality in self._split_municipality_list(match.group(1)):
                    self._append_unique(results, municipality)

        list_patterns = [
            r"\b(?:nei|nelli|in|sito nei|siti nei|ubicate nei|ubicati nei|localizzato nei|localizzata nei|localizzati nei|localizzate nei|ricadente nei|ricadenti nei|da realizzarsi nei|realizzarsi nei)\s+(?:territori\s+)?(?:Comuni|comuni)\s+di\s+(.+?)(?=\s+e\s+relative|\s+relative\s+opere|\s+opere\s+di\s+connessione|\s+opere\s+connesse|\s+alla\s+RTN|\.|;|$)",
            r"\bterritori\s+comunali\s+di\s+(.+?)(?=,?\s+in\s+provincia|\s+per\s+una\s+potenza|\s+e\s+relative|\s+relative\s+opere|\.|;|$)",
            r"\b(?:che\s+attraversano|interesseranno)\s+i\s+comuni\s+di\s+(.+?)(?=\s+sino\s+|\s+e\s+relative|\s+relative\s+opere|\.|;|$)",
        ]

        for pattern in list_patterns:
            for match in re.finditer(pattern, text, flags=re.IGNORECASE):
                for municipality in self._split_municipality_list(match.group(1)):
                    self._append_unique(results, municipality)

        for municipality in self._extract_known_municipalities_from_title(text):
            self._append_unique(results, municipality)

        return results[:20]

    def _extract_known_municipalities_from_title(self, title: str) -> list[str]:
        lowered = self._normalize_for_match(title)
        found: list[str] = []

        for municipality in MUNICIPALITY_TO_PROVINCE:
            normalized = self._normalize_for_match(municipality)
            pattern = rf"(?<![a-z0-9]){re.escape(normalized)}(?![a-z0-9])"

            if re.search(pattern, lowered, flags=re.IGNORECASE):
                found.append(municipality)

        return found

    def _split_municipality_list(self, value: str | None) -> list[str]:
        if not value:
            return []

        text = self._clean_text(value) or ""

        if not text:
            return []

        text = re.sub(r"\([A-Z]{2}\)", "", text)

        text = re.split(
            r"\s+(?:e\s+relative|relative)\s+opere\b",
            text,
            maxsplit=1,
            flags=re.IGNORECASE,
        )[0]

        text = re.split(
            r"\s+opere\s+(?:di\s+)?connessione\b",
            text,
            maxsplit=1,
            flags=re.IGNORECASE,
        )[0]

        text = re.split(
            r"\s+(?:per|avente|con|della|di)\s+una?\s+potenza\b",
            text,
            maxsplit=1,
            flags=re.IGNORECASE,
        )[0]

        text = re.split(
            r"\s+in\s+provincia\s+di\b",
            text,
            maxsplit=1,
            flags=re.IGNORECASE,
        )[0]

        text = re.split(
            r"\s+localit[àa]\b",
            text,
            maxsplit=1,
            flags=re.IGNORECASE,
        )[0]

        text = re.split(
            r"\s+in\s+(?:via|strada)\s+[A-ZÀ-Ú]|\s+(?:via|strada)\s+[A-ZÀ-Ú]",
            text,
            maxsplit=1,
            flags=re.IGNORECASE,
        )[0]

        protected_map: dict[str, str] = {}

        for idx, municipality in enumerate(PROTECTED_MUNICIPALITIES):
            token = f"__MUNICIPALITY_{idx}__"
            pattern = re.escape(municipality).replace("\\ ", r"\s+")
            text = re.sub(pattern, token, text, flags=re.IGNORECASE)
            protected_map[token] = municipality

        text = text.replace(";", ",")
        text = re.sub(r"\s+ed\s+", ",", text, flags=re.IGNORECASE)

        parts: list[str] = []
        for chunk in [part.strip() for part in text.split(",") if part.strip()]:
            subparts = re.split(r"\s+e\s+", chunk, flags=re.IGNORECASE)
            parts.extend(part.strip() for part in subparts if part.strip())

        cleaned_parts: list[str] = []

        for part in parts:
            for token, municipality in protected_map.items():
                part = part.replace(token, municipality)

            cleaned = self._clean_municipality(part)

            if cleaned:
                cleaned_parts.append(cleaned)

        return cleaned_parts

    def _clean_municipality(self, value: str | None) -> str | None:
        if not value:
            return None

        cleaned = self._clean_text(value) or ""

        if not cleaned:
            return None

        cleaned = re.sub(r"\([A-Z]{2}\)", "", cleaned)
        cleaned = cleaned.replace("’", "'")
        cleaned = cleaned.strip(" .,:;-–—()[]{}\"'")

        cleaned = re.split(
            r"\s+in\s+provincia\s+di\b",
            cleaned,
            maxsplit=1,
            flags=re.IGNORECASE,
        )[0]

        cleaned = re.split(
            r"\s+localit[àa]\b",
            cleaned,
            maxsplit=1,
            flags=re.IGNORECASE,
        )[0]

        cleaned = re.split(
            r"\s+relative\s+opere\b|\s+opere\s+di\s+connessione\b|\s+alla\s+rtn\b",
            cleaned,
            maxsplit=1,
            flags=re.IGNORECASE,
        )[0]

        cleaned = cleaned.strip(" .,:;-–—()[]{}\"'")

        if not cleaned:
            return None

        if len(cleaned) > 80:
            return None

        bad_fragments = [
            "potenza",
            "impianto",
            "progetto",
            "connessione",
            "rtn",
            "rete",
            "provincia",
            "località",
            "localita",
            "opere",
            "relative",
            "cavidotto",
            "cabina",
            "stazione",
            "terreno",
            "area",
            "art.",
            "d.lgs",
            "allevamento",
            "idroelettrico",
        ]

        lowered = cleaned.lower()

        if any(fragment in lowered for fragment in bad_fragments):
            return None

        if re.fullmatch(r".+\b(?:d|de|del|dell|dello|della|di|in)$", cleaned, flags=re.IGNORECASE):
            return None

        return self._title_case_municipality(cleaned)

    def _append_unique(self, values: list[str], candidate: str | None) -> None:
        cleaned = self._clean_municipality(candidate)

        if not cleaned:
            return

        normalized = self._normalize_for_match(cleaned)

        for existing in values:
            if self._normalize_for_match(existing) == normalized:
                return

        values.append(cleaned)

    def _title_case_municipality(self, value: str) -> str:
        text = value.title()

        replacements = {
            "D'": "d'",
            "D’": "d’",
            "De'": "de'",
            "De’": "de’",
            "Di": "di",
            "Del": "del",
            "Della": "della",
            "Dello": "dello",
            "E": "e",
        }

        words = []
        for word in text.split():
            words.append(replacements.get(word, word))

        text = " ".join(words)

        text = text.replace("D'Adda", "d'Adda")
        text = text.replace("D’Oglio", "d’Oglio")
        text = text.replace("D'oglio", "d'Oglio")
        text = text.replace("D'Ario", "d'Ario")
        text = text.replace("De' Burgondi", "de' Burgondi")
        text = text.replace("De’ Burgondi", "de’ Burgondi")

        return text

    def _build_external_id(
        self,
        title: str,
        proponent: str | None,
        detail_url: str | None,
    ) -> str:
        base = f"{title}|{proponent or ''}|{detail_url or ''}".lower()
        base = re.sub(r"\s+", "-", base)
        base = re.sub(r"[^a-z0-9:/._|-]", "", base)
        return base[:250]

    def _normalize_for_match(self, value: str | None) -> str:
        value = self._clean_text(value or "") or ""
        value = value.lower()
        value = value.replace("à", "a")
        value = value.replace("è", "e")
        value = value.replace("é", "e")
        value = value.replace("ì", "i")
        value = value.replace("ò", "o")
        value = value.replace("ù", "u")
        value = value.replace("’", "'")
        return value

    def _clean_text(self, value: str | None) -> str:
        return " ".join((value or "").replace("\xa0", " ").split()).strip()

    def _write_text(self, path: Path, content: str) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(content, encoding="utf-8")

    def _write_json(self, path: Path, obj) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(
            json.dumps(obj, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )


if __name__ == "__main__":
    collector = LombardiaCollector()
    items = collector.fetch()

    print(f"items: {len(items)}")

    for item in items[:100]:
        print(
            item.external_id,
            "|",
            item.title,
            "|",
            item.payload.get("proponent"),
            "|",
            item.payload.get("province"),
            "|",
            item.payload.get("municipalities"),
            "|",
            item.payload.get("power"),
            "|",
            item.payload.get("status_raw"),
            "|",
            item.source_url,
        )