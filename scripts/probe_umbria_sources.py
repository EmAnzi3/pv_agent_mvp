from __future__ import annotations

import csv
import re
import sys
from dataclasses import dataclass
from pathlib import Path
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup


OUT_DIR = Path("tmp/umbria_probe")
OUT_DIR.mkdir(parents=True, exist_ok=True)

CSV_PATH = Path("reports/umbria_probe_candidates.csv")
CSV_PATH.parent.mkdir(parents=True, exist_ok=True)

SOURCES = [
    {
        "name": "umbria_verifica_assoggettabilita_via",
        "procedure": "Verifica di assoggettabilità a VIA",
        "url": "https://www.va.regione.umbria.it/via/elenco-dei-procedimenti-di-verifica-di-assoggettabilita-a-via",
    },
    {
        "name": "umbria_valutazione_preliminare",
        "procedure": "Valutazione preliminare",
        "url": "https://www.va.regione.umbria.it/via/valutazione-preliminare",
    },
    {
        "name": "umbria_via",
        "procedure": "Valutazione di Impatto Ambientale",
        "url": "https://www.va.regione.umbria.it/via/elenco-dei-procedimenti-di-valutazione-di-impatto-ambientale",
    },
]

POSITIVE_KEYWORDS = [
    "fotovolta",
    "agrivolta",
    "agrovolta",
    "impianto fv",
    "solare",
    "mwp",
    " mw",
]

NEGATIVE_KEYWORDS = [
    "archivia",
    "improcedibil",
    "diniego",
    "respint",
    "negativ",
    "annull",
]


@dataclass
class Candidate:
    source_name: str
    procedure: str
    source_page: str
    title: str
    url: str
    text_sample: str
    power_raw: str | None


def fetch(url: str) -> str:
    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120 Safari/537.36"
        ),
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "it-IT,it;q=0.9,en;q=0.8",
    }

    r = requests.get(url, headers=headers, timeout=40)
    r.raise_for_status()
    return r.text


def clean_text(value: str) -> str:
    return re.sub(r"\s+", " ", value or "").strip()


def looks_relevant(text: str) -> bool:
    t = text.lower()
    return any(k in t for k in POSITIVE_KEYWORDS) and not any(k in t for k in NEGATIVE_KEYWORDS)


def extract_power(text: str) -> str | None:
    patterns = [
        r"(\d{1,4}(?:[.,]\d{1,4})?)\s*MWp",
        r"(\d{1,4}(?:[.,]\d{1,4})?)\s*MW",
    ]

    for p in patterns:
        m = re.search(p, text, flags=re.IGNORECASE)
        if m:
            return m.group(0)

    return None


def find_candidates_from_page(source: dict, html: str) -> list[Candidate]:
    soup = BeautifulSoup(html, "html.parser")

    candidates: list[Candidate] = []

    # 1. Link testuali
    for a in soup.find_all("a"):
        title = clean_text(a.get_text(" ", strip=True))
        href = a.get("href")

        if not title and not href:
            continue

        full_url = urljoin(source["url"], href or "")
        surrounding = clean_text(a.parent.get_text(" ", strip=True) if a.parent else title)
        blob = f"{title} {surrounding} {full_url}"

        if not looks_relevant(blob):
            continue

        candidates.append(
            Candidate(
                source_name=source["name"],
                procedure=source["procedure"],
                source_page=source["url"],
                title=title or surrounding[:180],
                url=full_url,
                text_sample=surrounding[:500],
                power_raw=extract_power(blob),
            )
        )

    # 2. Righe tabella
    for tr in soup.find_all("tr"):
        row_text = clean_text(tr.get_text(" ", strip=True))
        if not looks_relevant(row_text):
            continue

        link = tr.find("a")
        href = urljoin(source["url"], link.get("href")) if link and link.get("href") else source["url"]
        title = clean_text(link.get_text(" ", strip=True)) if link else row_text[:180]

        candidates.append(
            Candidate(
                source_name=source["name"],
                procedure=source["procedure"],
                source_page=source["url"],
                title=title,
                url=href,
                text_sample=row_text[:500],
                power_raw=extract_power(row_text),
            )
        )

    # Deduplica per URL + titolo
    unique = {}
    for c in candidates:
        key = (c.url, c.title)
        unique[key] = c

    return list(unique.values())


def main() -> int:
    all_candidates: list[Candidate] = []

    for source in SOURCES:
        print("=" * 100)
        print(f"FETCH: {source['name']}")
        print(source["url"])

        try:
            html = fetch(source["url"])
        except Exception as e:
            print(f"ERRORE fetch: {e}")
            continue

        html_path = OUT_DIR / f"{source['name']}.html"
        html_path.write_text(html, encoding="utf-8")
        print(f"HTML salvato: {html_path}")

        candidates = find_candidates_from_page(source, html)
        all_candidates.extend(candidates)

        print(f"Candidati trovati: {len(candidates)}")
        for c in candidates[:20]:
            print("-" * 80)
            print("title:", c.title)
            print("power:", c.power_raw)
            print("url:", c.url)
            print("sample:", c.text_sample[:250])

    with CSV_PATH.open("w", newline="", encoding="utf-8-sig") as f:
        writer = csv.DictWriter(
            f,
            fieldnames=[
                "source_name",
                "procedure",
                "source_page",
                "title",
                "url",
                "power_raw",
                "text_sample",
            ],
        )
        writer.writeheader()

        for c in all_candidates:
            writer.writerow({
                "source_name": c.source_name,
                "procedure": c.procedure,
                "source_page": c.source_page,
                "title": c.title,
                "url": c.url,
                "power_raw": c.power_raw,
                "text_sample": c.text_sample,
            })

    print("=" * 100)
    print(f"Totale candidati: {len(all_candidates)}")
    print(f"CSV: {CSV_PATH}")
    print(f"HTML raw: {OUT_DIR}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
