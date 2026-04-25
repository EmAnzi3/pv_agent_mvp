from __future__ import annotations

import json
import re
from pathlib import Path
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup


START_URL = "http://www.sistemapiemonte.it/skvia/HomePage.do?ricerca=ArchivioProgetti"
DEBUG_DIR = Path("/app/reports/debug_piemonte_skvia")


def clean_text(value: str | None) -> str:
    return " ".join((value or "").replace("\xa0", " ").split()).strip()


def safe_filename(value: str) -> str:
    value = re.sub(r"[^a-zA-Z0-9_-]+", "_", value)
    return value.strip("_")[:120] or "page"


def extract_links(soup: BeautifulSoup, base_url: str) -> list[dict]:
    links: list[dict] = []

    wanted = [
        "progetti in consultazione",
        "progetti in corso",
        "procedimenti conclusi",
        "ricerca archivio",
        "archivio progetti",
        "consultazione per il pubblico",
        "istruttoria",
        "conclusi",
    ]

    for a in soup.find_all("a", href=True):
        label = clean_text(a.get_text(" ", strip=True))
        href = a.get("href")
        absolute = urljoin(base_url, href)

        label_norm = label.lower()
        href_norm = absolute.lower()

        if any(w in label_norm for w in wanted) or any(w in href_norm for w in ["ricerca", "archivio", "consultazione", "istruttoria", "conclus"]):
            links.append(
                {
                    "label": label,
                    "url": absolute,
                }
            )

    # dedupe mantenendo ordine
    out = []
    seen = set()
    for item in links:
        key = item["url"]
        if key in seen:
            continue
        seen.add(key)
        out.append(item)

    return out


def extract_forms(soup: BeautifulSoup, page_url: str) -> list[dict]:
    forms: list[dict] = []

    for form_index, form in enumerate(soup.find_all("form"), start=1):
        fields: list[dict] = []

        for field in form.find_all(["input", "select", "textarea", "button"]):
            item = {
                "tag": field.name,
                "name": field.get("name"),
                "id": field.get("id"),
                "type": field.get("type"),
                "value": field.get("value"),
                "text": clean_text(field.get_text(" ", strip=True))[:200],
            }

            if field.name == "select":
                options = []
                for option in field.find_all("option"):
                    options.append(
                        {
                            "value": option.get("value"),
                            "text": clean_text(option.get_text(" ", strip=True)),
                            "selected": option.has_attr("selected"),
                        }
                    )
                item["options_sample"] = options[:80]
                item["options_count"] = len(options)

            fields.append(item)

        forms.append(
            {
                "form_index": form_index,
                "action": urljoin(page_url, form.get("action") or ""),
                "method": (form.get("method") or "GET").upper(),
                "id": form.get("id"),
                "name": form.get("name"),
                "fields_count": len(fields),
                "fields_sample": fields[:150],
            }
        )

    return forms


def extract_tables(soup: BeautifulSoup) -> list[dict]:
    tables: list[dict] = []

    for idx, table in enumerate(soup.find_all("table"), start=1):
        table_text = clean_text(table.get_text(" ", strip=True))

        headers = [
            clean_text(th.get_text(" ", strip=True))
            for th in table.find_all("th")
        ]

        rows_sample = []
        for tr in table.find_all("tr")[:15]:
            cells = [
                clean_text(td.get_text(" ", strip=True))
                for td in tr.find_all(["th", "td"])
            ]
            if cells:
                rows_sample.append(cells)

        tables.append(
            {
                "idx": idx,
                "id": table.get("id"),
                "class": table.get("class"),
                "text_len": len(table_text),
                "contains_fotovoltaico": "fotovoltaico" in table_text.lower(),
                "contains_agrivoltaico": "agrivoltaico" in table_text.lower() or "agrovoltaico" in table_text.lower(),
                "contains_proponente": "proponente" in table_text.lower(),
                "contains_comune": "comune" in table_text.lower(),
                "headers": headers[:50],
                "rows_sample": rows_sample,
                "first_1000": table_text[:1000],
            }
        )

    return tables


def extract_scripts(soup: BeautifulSoup, page_url: str) -> list[str]:
    scripts = []
    for script in soup.find_all("script"):
        src = script.get("src")
        if src:
            scripts.append(urljoin(page_url, src))
    return scripts


def summarize_page(name: str, url: str, session: requests.Session) -> dict:
    response = session.get(
        url,
        timeout=90,
        allow_redirects=True,
        headers={
            "User-Agent": "Mozilla/5.0 PV-Agent-MVP/0.1",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        },
    )
    response.raise_for_status()

    text = response.content.decode("utf-8", errors="replace")
    safe_name = safe_filename(name)

    (DEBUG_DIR / f"{safe_name}.html").write_text(text[:2_000_000], encoding="utf-8")

    soup = BeautifulSoup(text, "html.parser")

    forms = extract_forms(soup, response.url)
    tables = extract_tables(soup)
    links = extract_links(soup, response.url)
    scripts = extract_scripts(soup, response.url)

    plain = clean_text(soup.get_text(" ", strip=True))
    (DEBUG_DIR / f"{safe_name}_plain.txt").write_text(plain[:500_000], encoding="utf-8")

    return {
        "name": name,
        "input_url": url,
        "final_url": response.url,
        "status_code": response.status_code,
        "content_type": response.headers.get("content-type"),
        "html_length": len(text),
        "title": clean_text(soup.title.get_text(" ", strip=True)) if soup.title else None,
        "contains_fotovoltaico": "fotovoltaico" in text.lower(),
        "contains_agrivoltaico": "agrivoltaico" in text.lower() or "agrovoltaico" in text.lower(),
        "contains_agrovoltaico": "agrovoltaico" in text.lower(),
        "contains_proponente": "proponente" in text.lower(),
        "contains_comune": "comune" in text.lower(),
        "contains_potenza": "potenza" in text.lower(),
        "forms_count": len(forms),
        "forms": forms[:8],
        "tables_count": len(tables),
        "tables_sample": tables[:30],
        "links_count": len(links),
        "links": links[:80],
        "scripts_count": len(scripts),
        "scripts": scripts[:50],
        "plain_first_2000": plain[:2000],
    }


def main():
    DEBUG_DIR.mkdir(parents=True, exist_ok=True)

    session = requests.Session()

    summaries: list[dict] = []

    home_summary = summarize_page("home", START_URL, session)
    summaries.append(home_summary)

    links = home_summary.get("links") or []

    seen_urls = {START_URL}

    for idx, link in enumerate(links, start=1):
        url = link["url"]

        if url in seen_urls:
            continue

        seen_urls.add(url)

        try:
            summary = summarize_page(f"linked_{idx}_{link.get('label') or 'link'}", url, session)
            summary["source_label"] = link.get("label")
            summaries.append(summary)
        except Exception as exc:
            summaries.append(
                {
                    "name": f"linked_{idx}",
                    "source_label": link.get("label"),
                    "url": url,
                    "error": str(exc),
                }
            )

    (DEBUG_DIR / "summary.json").write_text(
        json.dumps(summaries, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )

    print(json.dumps(summaries, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()