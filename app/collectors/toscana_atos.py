from __future__ import annotations

import ast
import csv
import html
import json
import re
import threading
from datetime import date, datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Any

import requests
from bs4 import BeautifulSoup
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from app.collectors.base import BaseCollector, CollectorResult


MAP_URL = "https://atos.arrr.it/mappa_fer.php?mn=fer&mnin=mappafer"
DETAIL_TEMPLATE = (
    "https://atos.arrr.it/"
    "scheda_impianto_fer.php?mn=fer&id_impianto={id_impianto}"
)

MIN_POWER_MW = 0.5
MAX_WORKERS = 6
REQUEST_TIMEOUT = 90

AUTHORIZED_MAX_AGE_MONTHS = 24

RECENT_ACT_KEYWORDS = (
    "proroga",
    "variante",
    "modifica sostanziale",
    "voltura",
    "rinnovo",
)


ALLOWED_TYPES = {
    "FOTOVOLTAICO",
    "AGRIVOLTAICO",
}

DUPLICATE_ATOS_IDS = {
    "7199",   # GR Rugginosella
    "7201",   # Quarata
    "7215",   # Manciano
    "12186",  # Bientina + Santa Maria a Monte
    "12209",  # Pietraia
    "12210",  # Le Case
    "12211",  # La Maremmana
    "12212",  # Parco Fotovoltaico Vada
    "12448",  # Santa Croce sull'Arno
    "12468",  # Grosseto
}

_thread_local = threading.local()


def clean_text(value: object) -> str:
    return re.sub(r"\s+", " ", str(value or "")).strip()


def thread_session(user_agent: str) -> requests.Session:
    session = getattr(_thread_local, "session", None)

    if session is not None:
        return session

    retry = Retry(
        total=3,
        connect=3,
        read=3,
        backoff_factor=0.6,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET"],
    )

    adapter = HTTPAdapter(
        max_retries=retry,
        pool_connections=MAX_WORKERS,
        pool_maxsize=MAX_WORKERS,
    )

    session = requests.Session()
    session.mount("https://", adapter)
    session.mount("http://", adapter)

    session.headers.update(
        {
            "User-Agent": user_agent,
            "Accept": (
                "text/html,application/xhtml+xml,"
                "application/xml;q=0.9,*/*;q=0.8"
            ),
            "Referer": MAP_URL,
        }
    )

    _thread_local.session = session
    return session


class ToscanaAtosCollector(BaseCollector):
    source_name = "toscana_atos"
    base_url = MAP_URL

    def fetch(self) -> list[CollectorResult]:
        debug_dir = Path("reports/debug_toscana_atos")
        debug_dir.mkdir(parents=True, exist_ok=True)

        html_text = self._fetch_map_results()

        (debug_dir / "map_results.html").write_text(
            html_text,
            encoding="utf-8",
        )

        raw_array = self._extract_javascript_array(
            html_text,
            "const discariche",
        )

        parsed_rows = self._parse_javascript_array(raw_array)

        markers = [
            self._normalize_marker(row)
            for row in parsed_rows
            if isinstance(row, (list, tuple))
        ]

        known_duplicates = [
            marker
            for marker in markers
            if str(marker.get("id_impianto"))
            in DUPLICATE_ATOS_IDS
        ]

        (debug_dir / "known_duplicates_skipped.json").write_text(
            json.dumps(
                known_duplicates,
                ensure_ascii=False,
                indent=2,
            ),
            encoding="utf-8",
        )

        markers = [
            marker
            for marker in markers
            if marker.get("id_impianto")
            and self._marker_is_allowed(marker)
            and str(marker.get("id_impianto"))
            not in DUPLICATE_ATOS_IDS
        ]

        (debug_dir / "markers.json").write_text(
            json.dumps(
                markers,
                ensure_ascii=False,
                indent=2,
            ),
            encoding="utf-8",
        )

        accepted: list[dict] = []
        rejected: list[dict] = []
        detail_errors: list[dict] = []

        user_agent = self.session.headers.get(
            "User-Agent",
            "Mozilla/5.0 PV-Agent-MVP",
        )

        with ThreadPoolExecutor(
            max_workers=MAX_WORKERS
        ) as executor:
            futures = {
                executor.submit(
                    self._read_project,
                    marker,
                    user_agent,
                ): marker
                for marker in markers
            }

            for future in as_completed(futures):
                marker = futures[future]

                try:
                    project = future.result()
                except Exception as exc:
                    detail_errors.append(
                        {
                            "id_impianto": marker.get(
                                "id_impianto"
                            ),
                            "title": marker.get("title"),
                            "url": marker.get("detail_url"),
                            "error": str(exc),
                        }
                    )
                    continue

                # Controllo finale sulla tipologia letta dalla scheda.
                # Evita che eventuali record eolici con icona errata
                # superino il filtro preliminare della mappa.
                source_type = str(
                    project.get("source_type") or ""
                ).strip().upper()

                if source_type not in ALLOWED_TYPES:
                    project["rejection_reason"] = (
                        "source_type_not_allowed"
                    )
                    rejected.append(project)
                    continue

                # I record con denominazione ditta OMISSIS non hanno
                # sufficiente valore commerciale e vengono esclusi.
                proponent = clean_text(
                    project.get("proponent")
                )

                proponent_key = re.sub(
                    r"[^A-Z0-9]+",
                    "",
                    proponent.upper(),
                )

                if proponent_key == "OMISSIS":
                    project["rejection_reason"] = (
                        "proponent_omissis"
                    )
                    rejected.append(project)
                    continue

                power_mw = project.get("power_mw")

                if power_mw is None:
                    project["rejection_reason"] = (
                        "missing_power_mw"
                    )
                    rejected.append(project)
                    continue

                if power_mw < MIN_POWER_MW:
                    project["rejection_reason"] = (
                        f"power_below_{MIN_POWER_MW}_mw"
                    )
                    rejected.append(project)
                    continue

                include, selection_reason = (
                    self._selection_decision(project)
                )

                project["selection_reason"] = (
                    selection_reason
                )

                if not include:
                    project["rejection_reason"] = (
                        selection_reason
                    )
                    rejected.append(project)
                    continue

                accepted.append(project)

        accepted.sort(
            key=lambda row: (
                str(row.get("province") or ""),
                str(row.get("municipality") or ""),
                str(row.get("title") or ""),
            )
        )

        results = [
            CollectorResult(
                external_id=(
                    f"toscana_atos_"
                    f"{project['id_impianto']}"
                ),
                source_url=project["detail_url"],
                title=project["title"][:250],
                payload={
                    "title": project["title"][:500],
                    "proponent": project.get("proponent"),
                    "status_raw": project.get(
                        "authorization_status"
                    ),
                    "region": "Toscana",
                    "province": project.get("province"),
                    "municipalities": (
                        [project["municipality"]]
                        if project.get("municipality")
                        else []
                    ),
                    "power_mw": project["power_mw"],
                    "project_type_hint": project.get(
                        "source_type"
                    ),
                    "authorization_type": project.get(
                        "authorization_type"
                    ),
                    "last_act_date": project.get(
                        "last_act_date"
                    ),
                    "last_act_kind": project.get(
                        "last_act_kind"
                    ),
                    "last_act_text": project.get(
                        "last_act_text"
                    ),
                    "selection_reason": project.get(
                        "selection_reason"
                    ),
                    "atos_id": project["id_impianto"],
                    "latitude": project.get("latitude"),
                    "longitude": project.get("longitude"),
                    "source_label": "Toscana",
                    "source_group": "Toscana",
                },
            )
            for project in accepted
        ]

        self._write_debug_outputs(
            debug_dir=debug_dir,
            markers=markers,
            accepted=accepted,
            rejected=rejected,
            detail_errors=detail_errors,
            result_count=len(results),
        )

        return results

    def _fetch_map_results(self) -> str:
        self.session.headers.update(
            {
                "Accept": (
                    "text/html,application/xhtml+xml,"
                    "application/xml;q=0.9,*/*;q=0.8"
                ),
                "Origin": "https://atos.arrr.it",
                "Referer": MAP_URL,
            }
        )

        initial = self.session.get(
            MAP_URL,
            timeout=REQUEST_TIMEOUT,
        )
        initial.raise_for_status()

        payload = [
            ("n_page", "1"),
            ("prima", "1"),
            ("from", ""),
            ("azione", ""),
            ("op", ""),
            ("mn", "fer"),
            ("stmn", ""),
            ("id_provincia_combo", " "),
            ("id_provincia", ""),
            ("codice_comunale_combo", ""),
            ("codice_comunale", ""),
            ("denominazione", ""),
            ("denominazione_ditta", ""),
            ("id_tipo_autorizzazione", " "),
            ("stato_autorizzazione", " "),
            ("tipologia_fonte[]", "1"),
            ("tipologia_fonte[]", "2"),
        ]

        response = self.session.post(
            MAP_URL,
            data=payload,
            timeout=REQUEST_TIMEOUT,
            allow_redirects=True,
        )
        response.raise_for_status()

        return response.text

    def _read_project(
        self,
        marker: dict,
        user_agent: str,
    ) -> dict:
        detail_url = marker["detail_url"]

        session = thread_session(user_agent)
        response = session.get(
            detail_url,
            timeout=REQUEST_TIMEOUT,
        )
        response.raise_for_status()

        detail = self._parse_detail_page(response.text)
        last_act = self._extract_last_act_metadata(
            response.text
        )

        source_type = (
            detail.get("source_type")
            or self._source_type_from_icon(marker.get("icon"))
        )

        return {
            "id_impianto": marker["id_impianto"],
            "detail_url": detail_url,
            "title": (
                detail.get("title")
                or marker.get("title")
                or f"Impianto ATOS {marker['id_impianto']}"
            ),
            "proponent": (
                detail.get("proponent")
                or marker.get("proponent")
            ),
            "province": (
                detail.get("province")
                or marker.get("province")
            ),
            "municipality": (
                detail.get("municipality")
                or marker.get("municipality")
            ),
            "authorization_type": (
                detail.get("authorization_type")
                or marker.get("authorization_type")
            ),
            "authorization_status": marker.get(
                "authorization_status"
            ),
            "source_type": source_type,
            "power_mw": detail.get("power_mw"),
            "last_act_date": last_act.get("last_act_date"),
            "last_act_text": last_act.get("last_act_text"),
            "last_act_kind": last_act.get("last_act_kind"),
            "latitude": marker.get("latitude"),
            "longitude": marker.get("longitude"),
        }

    def _extract_last_act_metadata(
        self,
        html_text: str,
    ) -> dict:
        soup = BeautifulSoup(html_text, "html.parser")

        plain_text = clean_text(
            soup.get_text(" ", strip=True)
        )

        marker_match = re.search(
            r"dati\s+dell[?']ultimo\s+atto",
            plain_text,
            flags=re.IGNORECASE,
        )

        if not marker_match:
            return {
                "last_act_date": None,
                "last_act_text": None,
                "last_act_kind": None,
            }

        segment = plain_text[marker_match.end():]
        segment = clean_text(segment)[:2500]

        dates: list[date] = []

        for match in re.finditer(
            r"\b(\d{1,2})[/-](\d{1,2})[/-](\d{4})\b",
            segment,
        ):
            day, month, year = match.groups()

            try:
                dates.append(
                    date(
                        int(year),
                        int(month),
                        int(day),
                    )
                )
            except ValueError:
                continue

        for match in re.finditer(
            r"\b(\d{4})-(\d{1,2})-(\d{1,2})\b",
            segment,
        ):
            year, month, day = match.groups()

            try:
                dates.append(
                    date(
                        int(year),
                        int(month),
                        int(day),
                    )
                )
            except ValueError:
                continue

        today = date.today()

        valid_dates = [
            item
            for item in dates
            if item <= today
        ]

        last_act_date = (
            max(valid_dates).isoformat()
            if valid_dates
            else None
        )

        segment_lower = segment.lower()

        last_act_kind = next(
            (
                keyword
                for keyword in RECENT_ACT_KEYWORDS
                if keyword in segment_lower
            ),
            None,
        )

        return {
            "last_act_date": last_act_date,
            "last_act_text": segment or None,
            "last_act_kind": last_act_kind,
        }

    def _authorized_cutoff_date(self) -> date:
        today = date.today()

        target_year = (
            today.year
            - AUTHORIZED_MAX_AGE_MONTHS // 12
        )

        try:
            return today.replace(year=target_year)
        except ValueError:
            return today.replace(
                year=target_year,
                day=28,
            )

    def _selection_decision(
        self,
        project: dict,
    ) -> tuple[bool, str]:
        status = clean_text(
            project.get("authorization_status")
        ).lower()

        title = clean_text(
            project.get("title")
        ).lower()

        # La tipologia dichiarata pu? essere errata:
        # controllo anche il titolo.
        if (
            title.startswith("eol")
            or "parco eolico" in title
            or "impianto eolico" in title
        ):
            return False, "title_indicates_eolico"

        if not status:
            return (
                False,
                "missing_authorization_status",
            )

        if "in iter" in status:
            return (
                True,
                "in_iter_autorizzativo",
            )

        if "autorizzat" not in status:
            return (
                False,
                "unsupported_authorization_status",
            )

        raw_date = clean_text(
            project.get("last_act_date")
        )

        if not raw_date:
            return (
                False,
                "authorized_missing_last_act_date",
            )

        try:
            last_act_date = datetime.strptime(
                raw_date,
                "%Y-%m-%d",
            ).date()
        except ValueError:
            return (
                False,
                "authorized_invalid_last_act_date",
            )

        if last_act_date < self._authorized_cutoff_date():
            return (
                False,
                "authorized_last_act_older_24_months",
            )

        if project.get("last_act_kind"):
            return (
                True,
                "authorized_recent_special_act",
            )

        return (
            True,
            "authorized_recent_last_act",
        )

    def _parse_detail_page(
        self,
        html_text: str,
    ) -> dict:
        soup = BeautifulSoup(html_text, "html.parser")

        plain_text = clean_text(
            soup.get_text(" ", strip=True)
        )

        pattern = re.compile(
            r"Denominazione\s+Impianto\s+"
            r"(?P<title>.*?)\s+"
            r"Denominazione\s+Ditta\s+"
            r"(?P<proponent>.*?)\s+"
            r"Comune\s+"
            r"(?P<municipality>.*?)\s+"
            r"Sigla\s+Provincia\s+"
            r"(?P<province>[A-Z]{2})\s+"
            r"Autorizzazione\s+Vigente\s+"
            r"Tipo\s+di\s+Autorizzazione\s+"
            r"(?P<authorization_type>.*?)\s+"
            r"Tipologia\s+Fonte\s+"
            r"(?P<source_type>"
            r"FOTOVOLTAICO|AGRIVOLTAICO"
            r")\s+"
            r"Potenza\s+MW\s+"
            r"(?P<power>[0-9.,]+)\s+"
            r"Dati\s+dell['’]ultimo\s+Atto",
            flags=re.IGNORECASE,
        )

        match = pattern.search(plain_text)

        if match:
            values = {
                key: clean_text(value)
                for key, value in match.groupdict().items()
            }

            return {
                "title": values.get("title"),
                "proponent": values.get("proponent"),
                "municipality": self._title_case(
                    values.get("municipality")
                ),
                "province": (
                    values.get("province") or ""
                ).upper(),
                "authorization_type": values.get(
                    "authorization_type"
                ),
                "source_type": (
                    values.get("source_type") or ""
                ).upper(),
                "power_mw": self._parse_power(
                    values.get("power")
                ),
            }

        return {
            "title": self._between(
                plain_text,
                "Denominazione Impianto",
                "Denominazione Ditta",
            ),
            "proponent": self._between(
                plain_text,
                "Denominazione Ditta",
                "Comune",
            ),
            "municipality": self._title_case(
                self._between(
                    plain_text,
                    "Comune",
                    "Sigla Provincia",
                )
            ),
            "province": (
                self._between(
                    plain_text,
                    "Sigla Provincia",
                    "Autorizzazione Vigente",
                )
                or ""
            ).upper(),
            "authorization_type": self._between(
                plain_text,
                "Tipo di Autorizzazione",
                "Tipologia Fonte",
            ),
            "source_type": (
                self._between(
                    plain_text,
                    "Tipologia Fonte",
                    "Potenza MW",
                )
                or ""
            ).upper(),
            "power_mw": self._parse_power(
                self._between(
                    plain_text,
                    "Potenza MW",
                    "Dati dell'ultimo Atto",
                )
            ),
        }

    def _between(
        self,
        text: str,
        start_label: str,
        end_label: str,
    ) -> str | None:
        pattern = re.compile(
            re.escape(start_label)
            + r"\s+(.*?)\s+"
            + re.escape(end_label),
            flags=re.IGNORECASE,
        )

        match = pattern.search(text)

        if not match:
            return None

        value = clean_text(match.group(1))
        return value or None

    def _parse_power(
        self,
        value: object,
    ) -> float | None:
        text = clean_text(value)

        if not text:
            return None

        match = re.search(
            r"[0-9]+(?:[.,][0-9]+)*",
            text,
        )

        if not match:
            return None

        number = match.group(0)

        if "," in number and "." in number:
            if number.rfind(",") > number.rfind("."):
                number = (
                    number.replace(".", "")
                    .replace(",", ".")
                )
            else:
                number = number.replace(",", "")
        elif "," in number:
            number = number.replace(",", ".")

        try:
            parsed = float(number)
        except ValueError:
            return None

        if parsed <= 0 or parsed > 1000:
            return None

        return parsed

    def _marker_is_allowed(
        self,
        marker: dict,
    ) -> bool:
        source_type = self._source_type_from_icon(
            marker.get("icon")
        )

        return source_type in ALLOWED_TYPES

    def _source_type_from_icon(
        self,
        icon: object,
    ) -> str | None:
        value = clean_text(icon).lower()

        if "agrivoltaico" in value:
            return "AGRIVOLTAICO"

        if "fotovoltaico" in value:
            return "FOTOVOLTAICO"

        return None

    def _normalize_marker(
        self,
        row: list,
    ) -> dict:
        def value_at(index: int):
            if index >= len(row):
                return None

            value = row[index]

            if isinstance(value, str):
                return clean_text(html.unescape(value))

            return value

        id_impianto = value_at(10)

        return {
            "title": value_at(0),
            "latitude": value_at(1),
            "longitude": value_at(2),
            "province": value_at(4),
            "municipality": self._title_case(
                value_at(5)
            ),
            "proponent": value_at(7),
            "authorization_type": value_at(9),
            "id_impianto": id_impianto,
            "icon": value_at(11),
            "authorization_status": value_at(12),
            "detail_url": (
                DETAIL_TEMPLATE.format(
                    id_impianto=id_impianto
                )
                if id_impianto not in (None, "")
                else None
            ),
        }

    def _extract_javascript_array(
        self,
        text: str,
        declaration: str,
    ) -> str:
        start = text.find(declaration)

        if start == -1:
            raise ValueError(
                f"Dichiarazione non trovata: {declaration}"
            )

        equals = text.find("=", start)
        opening = text.find("[", equals)

        if opening == -1:
            raise ValueError("Apertura array non trovata")

        depth = 0
        quote: str | None = None
        escaped = False

        for index in range(opening, len(text)):
            char = text[index]

            if quote is not None:
                if escaped:
                    escaped = False
                elif char == "\\":
                    escaped = True
                elif char == quote:
                    quote = None

                continue

            if char in {"'", '"'}:
                quote = char
                continue

            if char == "[":
                depth += 1
            elif char == "]":
                depth -= 1

                if depth == 0:
                    return text[opening:index + 1]

        raise ValueError("Chiusura array non trovata")

    def _parse_javascript_array(
        self,
        raw: str,
    ) -> list:
        cleaned = html.unescape(raw)
        cleaned = self._strip_javascript_comments(
            cleaned
        ).strip()

        try:
            value = json.loads(cleaned)

            if isinstance(value, list):
                return value
        except Exception:
            pass

        normalized = re.sub(
            r"\bnull\b",
            "None",
            cleaned,
            flags=re.IGNORECASE,
        )
        normalized = re.sub(
            r"\bundefined\b",
            "None",
            normalized,
            flags=re.IGNORECASE,
        )
        normalized = re.sub(
            r"\btrue\b",
            "True",
            normalized,
            flags=re.IGNORECASE,
        )
        normalized = re.sub(
            r"\bfalse\b",
            "False",
            normalized,
            flags=re.IGNORECASE,
        )

        value = ast.literal_eval(normalized)

        if not isinstance(value, list):
            raise ValueError(
                "Il contenuto estratto non è una lista"
            )

        return value

    def _strip_javascript_comments(
        self,
        source: str,
    ) -> str:
        output: list[str] = []

        index = 0
        quote: str | None = None
        escaped = False

        while index < len(source):
            char = source[index]
            next_char = (
                source[index + 1]
                if index + 1 < len(source)
                else ""
            )

            if quote is not None:
                output.append(char)

                if escaped:
                    escaped = False
                elif char == "\\":
                    escaped = True
                elif char == quote:
                    quote = None

                index += 1
                continue

            if char in {"'", '"'}:
                quote = char
                output.append(char)
                index += 1
                continue

            if char == "/" and next_char == "/":
                index += 2

                while (
                    index < len(source)
                    and source[index] not in {
                        "\r",
                        "\n",
                    }
                ):
                    index += 1

                output.append("\n")
                continue

            if char == "/" and next_char == "*":
                index += 2

                while index + 1 < len(source):
                    if (
                        source[index] == "*"
                        and source[index + 1] == "/"
                    ):
                        index += 2
                        break

                    index += 1

                output.append(" ")
                continue

            output.append(char)
            index += 1

        return "".join(output)

    def _title_case(
        self,
        value: object,
    ) -> str | None:
        text = clean_text(value)

        if not text:
            return None

        return text.title()

    def _write_debug_outputs(
        self,
        debug_dir: Path,
        markers: list[dict],
        accepted: list[dict],
        rejected: list[dict],
        detail_errors: list[dict],
        result_count: int,
    ) -> None:
        for filename, rows in [
            ("accepted.json", accepted),
            ("rejected.json", rejected),
            ("detail_errors.json", detail_errors),
        ]:
            (debug_dir / filename).write_text(
                json.dumps(
                    rows,
                    ensure_ascii=False,
                    indent=2,
                ),
                encoding="utf-8",
            )

        audit_path = debug_dir / "rejected.csv"

        fieldnames = [
            "id_impianto",
            "title",
            "proponent",
            "province",
            "municipality",
            "power_mw",
            "source_type",
            "authorization_status",
            "last_act_date",
            "last_act_kind",
            "selection_reason",
            "rejection_reason",
            "detail_url",
        ]

        with audit_path.open(
            "w",
            newline="",
            encoding="utf-8",
        ) as handle:
            writer = csv.DictWriter(
                handle,
                fieldnames=fieldnames,
                extrasaction="ignore",
            )
            writer.writeheader()
            writer.writerows(rejected)

        summary = {
            "markers_total": len(markers),
            "accepted_over_500_kw": len(accepted),
            "rejected_total": len(rejected),
            "detail_errors": len(detail_errors),
            "collector_results": result_count,
            "minimum_power_mw": MIN_POWER_MW,
            "allowed_types": sorted(ALLOWED_TYPES),
        }

        (debug_dir / "summary.json").write_text(
            json.dumps(
                summary,
                ensure_ascii=False,
                indent=2,
            ),
            encoding="utf-8",
        )


if __name__ == "__main__":
    rows = ToscanaAtosCollector().fetch()
    print("Risultati ATOS:", len(rows))
