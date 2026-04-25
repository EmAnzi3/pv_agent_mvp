from __future__ import annotations

import json
import re
from pathlib import Path
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup


START_URL = "http://www.sistemapiemonte.it/skvia/HomePage.do?ricerca=ArchivioProgetti"
CHANGE_COMPETENZA_URL = (
    "http://www.sistemapiemonte.it/skvia/"
    "cpRicercaArchivioProgetti!handleCbAutoritaCompetente_VALUE_CHANGED.do"
    "?confermacbAutoritaCompetente=conferma"
)

DEBUG_DIR = Path("/app/reports/debug_piemonte_skvia_flow")

KEYWORDS = [
    "fotovoltaico",
    "fotovoltaica",
    "agrivoltaico",
    "agrovoltaico",
    "agrofotovoltaico",
]

YEARS = [
    "2026",
    "2025",
    "2024",
]


def clean_text(value: str | None) -> str:
    return " ".join((value or "").replace("\xa0", " ").split()).strip()


def safe_filename(value: str) -> str:
    value = re.sub(r"[^a-zA-Z0-9_-]+", "_", value)
    return value.strip("_")[:140] or "file"


def parse_soup(response: requests.Response) -> BeautifulSoup:
    text = response.content.decode("utf-8", errors="replace")
    return BeautifulSoup(text, "html.parser")


def get_form(soup: BeautifulSoup):
    form = soup.find("form", {"id": "cpRicercaArchivioProgetti"})
    if form is None:
        form = soup.find("form")

    if form is None:
        raise RuntimeError("Nessun form trovato")

    return form


def get_form_action_and_data(soup: BeautifulSoup, current_url: str) -> tuple[str, dict]:
    form = get_form(soup)
    action = urljoin(current_url, form.get("action") or "")

    data: dict[str, str] = {}

    for field in form.find_all(["input", "select", "textarea"]):
        name = field.get("name")
        if not name:
            continue

        if field.name == "select":
            selected = field.find("option", selected=True)
            data[name] = selected.get("value") if selected else ""
            continue

        field_type = (field.get("type") or "").lower()

        if field_type in ["submit", "button", "image"]:
            continue

        if field_type == "checkbox":
            continue

        data[name] = field.get("value") or ""

    return action, data


def extract_select_options(soup: BeautifulSoup) -> dict:
    out = {}

    for select in soup.find_all("select"):
        name = select.get("name") or select.get("id") or "unknown"

        options = []
        for option in select.find_all("option"):
            options.append(
                {
                    "value": option.get("value") or "",
                    "text": clean_text(option.get_text(" ", strip=True)),
                    "selected": option.has_attr("selected"),
                }
            )

        out[name] = {
            "id": select.get("id"),
            "options_count": len(options),
            "options_sample": options[:160],
        }

    return out


def extract_tables(soup: BeautifulSoup) -> list[dict]:
    tables = []

    for idx, table in enumerate(soup.find_all("table"), start=1):
        table_text = clean_text(table.get_text(" ", strip=True))

        rows = []
        for tr in table.find_all("tr"):
            cells = [
                clean_text(td.get_text(" ", strip=True))
                for td in tr.find_all(["th", "td"])
            ]
            if cells:
                rows.append(cells)

        tables.append(
            {
                "idx": idx,
                "id": table.get("id"),
                "text_len": len(table_text),
                "contains_fotovoltaico": "fotovoltaico" in table_text.lower(),
                "contains_agrivoltaico": "agrivoltaico" in table_text.lower()
                or "agrovoltaico" in table_text.lower(),
                "rows_count": len(rows),
                "rows_sample": rows[:40],
                "first_2000": table_text[:2000],
            }
        )

    return tables


def summarize(name: str, response: requests.Response) -> dict:
    text = response.content.decode("utf-8", errors="replace")
    soup = BeautifulSoup(text, "html.parser")

    safe = safe_filename(name)
    DEBUG_DIR.mkdir(parents=True, exist_ok=True)
    (DEBUG_DIR / f"{safe}.html").write_text(text[:2_000_000], encoding="utf-8")
    (DEBUG_DIR / f"{safe}_plain.txt").write_text(
        clean_text(soup.get_text(" ", strip=True))[:500_000],
        encoding="utf-8",
    )

    return {
        "name": name,
        "url": response.url,
        "status_code": response.status_code,
        "content_type": response.headers.get("content-type"),
        "html_length": len(text),
        "title": clean_text(soup.title.get_text(" ", strip=True)) if soup.title else None,
        "contains_fotovoltaico": "fotovoltaico" in text.lower(),
        "contains_agrivoltaico": "agrivoltaico" in text.lower() or "agrovoltaico" in text.lower(),
        "contains_non_ci_sono_elementi": "Non ci sono elementi da visualizzare" in text,
        "selects": extract_select_options(soup),
        "tables": extract_tables(soup)[:12],
    }


def post_form(
    session: requests.Session,
    url: str,
    data: dict,
    referer: str,
) -> requests.Response:
    response = session.post(
        url,
        data=data,
        timeout=90,
        allow_redirects=True,
        headers={
            "Referer": referer,
            "Origin": "http://www.sistemapiemonte.it",
            "Content-Type": "application/x-www-form-urlencoded",
        },
    )
    response.raise_for_status()
    return response


def main():
    DEBUG_DIR.mkdir(parents=True, exist_ok=True)

    session = requests.Session()
    session.headers.update(
        {
            "User-Agent": "Mozilla/5.0 PV-Agent-MVP/0.1",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        }
    )

    summaries = []

    # 1. GET home.
    home = session.get(START_URL, timeout=90, allow_redirects=True)
    home.raise_for_status()
    summaries.append(summarize("00_home", home))

    soup = parse_soup(home)
    _, data = get_form_action_and_data(soup, home.url)

    # 2. Cambio competenza con endpoint reale browser.
    data_competenza = dict(data)
    data_competenza["appDataRicercaArchivioProgetti.competenza"] = "REGIONE PIEMONTE"
    data_competenza["appDataRicercaArchivioProgetti.tipologia"] = ""
    data_competenza["appDataRicercaArchivioProgetti.annoRegistro"] = ""
    data_competenza["appDataRicercaArchivioProgetti.codice"] = ""
    data_competenza["appDataRicercaArchivioProgetti.denominazioneProgetto"] = ""
    data_competenza["__checkbox_appDataRicercaArchivioProgetti.flagLeggeObiettivo"] = ""
    data_competenza["__checkbox_appDataRicercaArchivioProgetti.incidenza"] = ""
    data_competenza["appDataRicercaArchivioProgetti.cat"] = ""
    data_competenza["appDataRicercaArchivioProgetti.codIstatProvincia"] = ""
    data_competenza["appDataRicercaArchivioProgetti.istatComune"] = ""
    data_competenza["appDataRicercaArchivioProgetti.flagStato"] = ""
    data_competenza["appDataCodiceSitoReteNaturaSelezionato"] = ""
    data_competenza["appDataRicercaArchivioProgetti.idParco"] = ""

    r_competenza = post_form(
        session=session,
        url=CHANGE_COMPETENZA_URL,
        data=data_competenza,
        referer=home.url,
    )
    summaries.append(summarize("01_change_competenza_regione_real_endpoint", r_competenza))

    soup_comp = parse_soup(r_competenza)
    action_comp, base_after_comp = get_form_action_and_data(soup_comp, r_competenza.url)

    (DEBUG_DIR / "base_after_competenza.json").write_text(
        json.dumps(
            {
                "action": action_comp,
                "data": base_after_comp,
                "selects": extract_select_options(soup_comp),
            },
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )

    # 3. Ricerca: usiamo action del form dopo cambio competenza.
    tests = []

    for keyword in KEYWORDS:
        tests.append(
            {
                "name": f"keyword_{keyword}",
                "keyword": keyword,
                "year": "",
                "tipologia": "",
                "stato": "",
            }
        )

    for keyword in KEYWORDS:
        for year in YEARS:
            tests.append(
                {
                    "name": f"keyword_{keyword}_year_{year}",
                    "keyword": keyword,
                    "year": year,
                    "tipologia": "",
                    "stato": "",
                }
            )

    # Le tipologie effettive si leggono da base_after_competenza.json,
    # ma intanto proviamo valori storicamente probabili.
    for keyword in KEYWORDS:
        for year in YEARS:
            for tipologia in ["VAL", "VER", "SPE", "VI", "SCR"]:
                tests.append(
                    {
                        "name": f"keyword_{keyword}_year_{year}_tipologia_{tipologia}",
                        "keyword": keyword,
                        "year": year,
                        "tipologia": tipologia,
                        "stato": "",
                    }
                )

    for keyword in KEYWORDS:
        for year in YEARS:
            tests.append(
                {
                    "name": f"keyword_{keyword}_year_{year}_incorso",
                    "keyword": keyword,
                    "year": year,
                    "tipologia": "",
                    "stato": "IN CORSO",
                }
            )

    for test in tests:
        data_search = dict(base_after_comp)

        data_search["appDataRicercaArchivioProgetti.competenza"] = "REGIONE PIEMONTE"
        data_search["appDataRicercaArchivioProgetti.denominazioneProgetto"] = test["keyword"]

        if test["year"]:
            data_search["appDataRicercaArchivioProgetti.annoRegistro"] = test["year"]

        if test["tipologia"]:
            data_search["appDataRicercaArchivioProgetti.tipologia"] = test["tipologia"]

        if test["stato"]:
            data_search["appDataRicercaArchivioProgetti.flagStato"] = test["stato"]

        data_search["method:handleBtRicercaArchivioProgetti_CLICKED"] = "Ricerca"

        try:
            r_search = post_form(
                session=session,
                url=action_comp,
                data=data_search,
                referer=r_competenza.url,
            )

            summary = summarize(f"02_search_{test['name']}", r_search)
            summary["test"] = test
            summaries.append(summary)

            print(json.dumps(summary, ensure_ascii=False, indent=2))

        except Exception as exc:
            summaries.append(
                {
                    "name": test["name"],
                    "test": test,
                    "error": str(exc),
                }
            )

    (DEBUG_DIR / "summary.json").write_text(
        json.dumps(summaries, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )


if __name__ == "__main__":
    main()