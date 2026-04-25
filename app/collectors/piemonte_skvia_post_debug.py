from __future__ import annotations

import json
import re
from pathlib import Path
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup


START_URL = "http://www.sistemapiemonte.it/skvia/HomePage.do?ricerca=ArchivioProgetti"
DEBUG_DIR = Path("/app/reports/debug_piemonte_skvia_post")

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
]


def clean_text(value: str | None) -> str:
    return " ".join((value or "").replace("\xa0", " ").split()).strip()


def safe_filename(value: str) -> str:
    value = re.sub(r"[^a-zA-Z0-9_-]+", "_", value)
    return value.strip("_")[:140] or "file"


def get_form_data(soup: BeautifulSoup) -> tuple[str, dict]:
    form = soup.find("form", {"id": "cpRicercaArchivioProgetti"})
    if form is None:
        raise RuntimeError("Form cpRicercaArchivioProgetti non trovato")

    action = form.get("action") or ""
    data: dict[str, str] = {}

    for field in form.find_all(["input", "select", "textarea"]):
        name = field.get("name")
        if not name:
            continue

        if field.name == "select":
            selected = field.find("option", selected=True)
            if selected is not None:
                data[name] = selected.get("value") or ""
            else:
                data[name] = ""
            continue

        field_type = (field.get("type") or "").lower()

        if field_type in ["submit", "button", "image"]:
            continue

        if field_type == "checkbox":
            # Non selezioniamo checkbox vere; lasciamo solo i relativi hidden __checkbox_*.
            continue

        data[name] = field.get("value") or ""

    return action, data


def extract_tables(soup: BeautifulSoup) -> list[dict]:
    tables = []

    for idx, table in enumerate(soup.find_all("table"), start=1):
        table_text = clean_text(table.get_text(" ", strip=True))

        headers = [
            clean_text(th.get_text(" ", strip=True))
            for th in table.find_all("th")
        ]

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
                "class": table.get("class"),
                "text_len": len(table_text),
                "contains_fotovoltaico": "fotovoltaico" in table_text.lower(),
                "contains_agrivoltaico": "agrivoltaico" in table_text.lower() or "agrovoltaico" in table_text.lower(),
                "contains_agrofotovoltaico": "agrofotovoltaico" in table_text.lower(),
                "headers": headers,
                "rows_count": len(rows),
                "rows_sample": rows[:25],
                "first_2000": table_text[:2000],
            }
        )

    return tables


def extract_result_links(soup: BeautifulSoup, base_url: str) -> list[dict]:
    links = []

    for a in soup.find_all("a", href=True):
        label = clean_text(a.get_text(" ", strip=True))
        href = a.get("href")
        absolute = urljoin(base_url, href)

        joined = f"{label} {absolute}".lower()

        if any(marker in joined for marker in ["dettaglio", "scheda", "progetto", "archivio"]):
            links.append(
                {
                    "label": label,
                    "url": absolute,
                }
            )

    out = []
    seen = set()
    for item in links:
        key = item["url"]
        if key in seen:
            continue
        seen.add(key)
        out.append(item)

    return out


def summarize_response(name: str, response: requests.Response) -> dict:
    text = response.content.decode("utf-8", errors="replace")
    safe = safe_filename(name)

    DEBUG_DIR.mkdir(parents=True, exist_ok=True)
    (DEBUG_DIR / f"{safe}.html").write_text(text[:2_000_000], encoding="utf-8")

    soup = BeautifulSoup(text, "html.parser")
    plain = clean_text(soup.get_text(" ", strip=True))
    (DEBUG_DIR / f"{safe}_plain.txt").write_text(plain[:500_000], encoding="utf-8")

    tables = extract_tables(soup)
    links = extract_result_links(soup, response.url)

    return {
        "name": name,
        "final_url": response.url,
        "status_code": response.status_code,
        "content_type": response.headers.get("content-type"),
        "html_length": len(text),
        "title": clean_text(soup.title.get_text(" ", strip=True)) if soup.title else None,
        "contains_fotovoltaico": "fotovoltaico" in text.lower(),
        "contains_agrivoltaico": "agrivoltaico" in text.lower() or "agrovoltaico" in text.lower(),
        "contains_agrofotovoltaico": "agrofotovoltaico" in text.lower(),
        "contains_non_ci_sono_elementi": "Non ci sono elementi da visualizzare" in text,
        "tables_count": len(tables),
        "tables_sample": tables[:10],
        "links_count": len(links),
        "links": links[:50],
        "plain_first_2000": plain[:2000],
    }


def main():
    DEBUG_DIR.mkdir(parents=True, exist_ok=True)

    session = requests.Session()
    session.headers.update(
        {
            "User-Agent": "Mozilla/5.0 PV-Agent-MVP/0.1",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Origin": "http://www.sistemapiemonte.it",
            "Referer": START_URL,
        }
    )

    summaries = []

    # 1. GET iniziale: serve anche per JSESSIONID.
    get_response = session.get(
        START_URL,
        timeout=90,
        allow_redirects=True,
    )
    get_response.raise_for_status()

    get_summary = summarize_response("00_get_home", get_response)
    summaries.append(get_summary)

    soup = BeautifulSoup(get_response.content.decode("utf-8", errors="replace"), "html.parser")
    action, base_data = get_form_data(soup)
    post_url = urljoin(get_response.url, action)

    (DEBUG_DIR / "base_form_data.json").write_text(
        json.dumps(
            {
                "post_url": post_url,
                "base_data_keys": list(base_data.keys()),
                "base_data": base_data,
            },
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )

    tests = []

    # Test ampio solo keyword.
    for keyword in KEYWORDS:
        tests.append(
            {
                "name": f"keyword_{keyword}",
                "keyword": keyword,
                "year": "",
                "competenza": "",
                "stato": "",
            }
        )

    # Test keyword + anno 2026/2025.
    for keyword in KEYWORDS:
        for year in YEARS:
            tests.append(
                {
                    "name": f"keyword_{keyword}_year_{year}",
                    "keyword": keyword,
                    "year": year,
                    "competenza": "",
                    "stato": "",
                }
            )

    # Test solo Regione Piemonte.
    for keyword in KEYWORDS:
        for year in YEARS:
            tests.append(
                {
                    "name": f"keyword_{keyword}_year_{year}_regione",
                    "keyword": keyword,
                    "year": year,
                    "competenza": "REGIONE PIEMONTE",
                    "stato": "",
                }
            )

    # Test procedimenti in corso.
    for keyword in KEYWORDS:
        for year in YEARS:
            tests.append(
                {
                    "name": f"keyword_{keyword}_year_{year}_incorso",
                    "keyword": keyword,
                    "year": year,
                    "competenza": "",
                    "stato": "IN CORSO",
                }
            )

    for test in tests:
        data = dict(base_data)

        data["appDataRicercaArchivioProgetti.denominazioneProgetto"] = test["keyword"]

        if test["year"]:
            data["appDataRicercaArchivioProgetti.annoRegistro"] = test["year"]

        if test["competenza"]:
            data["appDataRicercaArchivioProgetti.competenza"] = test["competenza"]

        if test["stato"]:
            data["appDataRicercaArchivioProgetti.flagStato"] = test["stato"]

        # Pulsante "Ricerca"
        data["method:handleBtRicercaArchivioProgetti_CLICKED"] = "Ricerca"

        try:
            response = session.post(
                post_url,
                data=data,
                timeout=90,
                allow_redirects=True,
                headers={
                    "Referer": get_response.url,
                },
            )
            response.raise_for_status()

            summary = summarize_response(test["name"], response)
            summary["test"] = test
            summaries.append(summary)

            print(json.dumps(summary, ensure_ascii=False, indent=2))

        except Exception as exc:
            summary = {
                "name": test["name"],
                "test": test,
                "error": str(exc),
            }
            summaries.append(summary)
            print(json.dumps(summary, ensure_ascii=False, indent=2))

    (DEBUG_DIR / "summary.json").write_text(
        json.dumps(summaries, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )


if __name__ == "__main__":
    main()