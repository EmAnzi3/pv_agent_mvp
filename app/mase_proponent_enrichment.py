from __future__ import annotations

import argparse
import csv
import html as html_lib
import json
import re
import time
from datetime import datetime
from pathlib import Path
from typing import Any

import requests
from bs4 import BeautifulSoup


DEFAULT_DATA = Path("reports/site/data.json")
DEFAULT_AUDIT = Path("reports/mase_proponent_enrichment.csv")

REQUEST_TIMEOUT = 60
SLEEP_SECONDS = 0.25

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/147.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "it-IT,it;q=0.9,en-US;q=0.8,en;q=0.7",
    "Referer": "https://va.mite.gov.it/",
}


def clean_text(value: Any) -> str:
    return re.sub(r"\s+", " ", str(value or "").replace("\xa0", " ")).strip()


def norm(value: Any) -> str:
    text = clean_text(value).lower()
    repl = {
        "Ã ": "a",
        "Ã¨": "e",
        "Ã©": "e",
        "Ã¬": "i",
        "Ã²": "o",
        "Ã¹": "u",
        "â€™": "'",
        "â€˜": "'",
        "â€œ": '"',
        "â€": '"',
    }
    for src, dst in repl.items():
        text = text.replace(src, dst)
    text = re.sub(r"[^a-z0-9]+", " ", text)
    return re.sub(r"\s+", " ", text).strip()


def is_mase_record(record: dict[str, Any]) -> bool:
    source = str(record.get("source") or "")
    source_group = str(record.get("source_group") or "")
    source_label = str(record.get("source_label") or "")
    merged_sources = record.get("_merged_sources") or []

    return (
        source in {"mase", "mase_provvedimenti"}
        or source_group == "mase"
        or source_label == "MASE"
        or "mase" in merged_sources
        or "mase_provvedimenti" in merged_sources
    )


def bad_proponent(value: Any) -> bool:
    cleaned = html_lib.unescape(clean_text(value))
    if not cleaned:
        return True

    n = norm(cleaned)
    if n in {"n d", "nd", "none", "null", "nan", "data", "data pubblicazione", "pubblicato", "pubblicazione"}:
        return True

    if re.fullmatch(r"\d{1,2}[/-]\d{1,2}[/-]\d{2,4}", cleaned):
        return True
    if re.fullmatch(r"\d{4}[/-]\d{1,2}[/-]\d{1,2}", cleaned):
        return True
    if re.fullmatch(
        r"\d{1,2}\s+(gennaio|febbraio|marzo|aprile|maggio|giugno|luglio|agosto|settembre|ottobre|novembre|dicembre)\s+\d{4}",
        cleaned,
        flags=re.IGNORECASE,
    ):
        return True
    if re.fullmatch(r"[0-9\s./:-]+", cleaned):
        return True

    return False


def has_valid_proponent(record: dict[str, Any]) -> bool:
    return not bad_proponent(record.get("proponent"))


def clean_candidate(value: str) -> str:
    value = html_lib.unescape(clean_text(value))
    value = re.sub(
        r"^(Proponente|Soggetto proponente|Societ[aÃ ] proponente|Societa proponente)\s*[:\-]\s*",
        "",
        value,
        flags=re.IGNORECASE,
    )
    value = re.split(
        r"\s+(?:Data\s+pubblicazione|Data\s+avvio|Tipologia|Procedura|Autorit[aÃ ]|Regione|Provincia|Comune|Localizzazione|Documentazione|Codice|ID|Identificativo)\b",
        value,
        maxsplit=1,
        flags=re.IGNORECASE,
    )[0]
    return clean_text(value.strip(" :;-â€“â€”|"))


def looks_like_company(value: Any) -> bool:
    value = clean_candidate(str(value or ""))
    if not value or bad_proponent(value):
        return False

    n = norm(value)

    reject_terms = [
        "progetto",
        "procedura",
        "valutazione",
        "verifica",
        "provvedimento",
        "data pubblicazione",
        "ministero",
        "commissione",
        "documentazione",
        "osservazioni",
        "elenco",
        "comunicazione",
        "dettaglio",
        "fotovoltaico",
        "agrivoltaico",
        "agrovoltaico",
        "comune di",
        "provincia di",
    ]

    if len(value) > 140:
        return False
    if any(term in n for term in reject_terms):
        return False

    company_markers = [
        "srl",
        "s r l",
        "srls",
        "spa",
        "s p a",
        "societa",
        "societÃ ",
        "energy",
        "energia",
        "solar",
        "solare",
        "renewable",
        "renewables",
        "rinnovabili",
        "green",
        "power",
        "pv",
        "agricola",
        "italia",
        "holding",
    ]

    if any(marker in n for marker in company_markers):
        return True

    letters = re.sub(r"[^A-Za-zÃ€-Ã¿]", "", value)
    return len(letters) >= 5 and len(value) <= 60 and value[:1].isupper()


def candidates_from_table_like_html(soup: BeautifulSoup) -> list[tuple[str, str]]:
    candidates: list[tuple[str, str]] = []

    for tr in soup.find_all("tr"):
        cells = tr.find_all(["th", "td"])
        if len(cells) < 2:
            continue

        texts = [clean_text(cell.get_text(" ", strip=True)) for cell in cells]

        for idx, label in enumerate(texts[:-1]):
            label_norm = norm(label)
            if label_norm in {
                "proponente",
                "soggetto proponente",
                "societa proponente",
                "societa proponente richiedente",
            } or ("proponente" in label_norm and len(label_norm) <= 60):
                candidate = clean_candidate(texts[idx + 1])
                if looks_like_company(candidate):
                    candidates.append(("table_row", candidate))

    for label_node in soup.find_all(["dt", "label", "span", "div", "strong"]):
        label = clean_text(label_node.get_text(" ", strip=True))
        label_norm = norm(label)

        if not ("proponente" in label_norm and len(label_norm) <= 90):
            continue

        sibling = label_node.find_next_sibling()
        if sibling:
            candidate = clean_candidate(sibling.get_text(" ", strip=True))
            if looks_like_company(candidate):
                candidates.append(("sibling", candidate))

    return candidates


def candidates_from_plain_text(text: str) -> list[tuple[str, str]]:
    text = clean_text(text)
    candidates: list[tuple[str, str]] = []

    patterns = [
        r"\b(?:Soggetto\s+proponente|Societ[aÃ ]\s+proponente|Societa\s+proponente|Proponente)\s*[:\-]\s*(.{2,180}?)(?=\s+(?:Data\s+pubblicazione|Data\s+avvio|Tipologia|Procedura|Autorit[aÃ ]|Regione|Provincia|Comune|Localizzazione|Documentazione|Codice|ID|Identificativo)\b|$)",
        r"\bProponente\s+(.{2,140}?)(?=\s+(?:Data\s+pubblicazione|Data\s+avvio|Tipologia|Procedura|Autorit[aÃ ]|Regione|Provincia|Comune|Localizzazione|Documentazione|Codice|ID|Identificativo)\b|$)",
    ]

    for pattern in patterns:
        for match in re.finditer(pattern, text, flags=re.IGNORECASE):
            candidate = clean_candidate(match.group(1))
            if looks_like_company(candidate):
                candidates.append(("plain_text", candidate))

    return candidates


def choose_candidate(candidates: list[tuple[str, str]]) -> tuple[str, str]:
    unique: list[tuple[str, str]] = []

    for source, candidate in candidates:
        candidate = clean_candidate(candidate)
        if not candidate:
            continue
        if not any(norm(candidate) == norm(existing) for _, existing in unique):
            unique.append((source, candidate))

    if not unique:
        return "", ""

    priority = {"table_row": 0, "sibling": 1, "plain_text": 2}
    unique.sort(key=lambda item: (priority.get(item[0], 99), len(item[1])))

    return unique[0]


def fetch_html(url: str, session: requests.Session) -> tuple[str, str]:
    try:
        response = session.get(
            url,
            headers=HEADERS,
            timeout=REQUEST_TIMEOUT,
            allow_redirects=True,
        )
        status = str(response.status_code)
        if response.status_code != 200:
            return "", status

        response.encoding = response.apparent_encoding or response.encoding
        return response.text, status
    except Exception as exc:
        return "", f"ERROR: {exc}"


def extract_proponent_from_url(url: str, session: requests.Session) -> dict[str, Any]:
    if not url:
        return {
            "fetch_status": "NO_URL",
            "candidate_source": "",
            "candidate_proponent": "",
            "all_candidates": "",
        }

    html, status = fetch_html(url, session)
    if not html:
        return {
            "fetch_status": status,
            "candidate_source": "",
            "candidate_proponent": "",
            "all_candidates": "",
        }

    soup = BeautifulSoup(html, "html.parser")
    plain_text = clean_text(soup.get_text(" ", strip=True))

    candidates: list[tuple[str, str]] = []
    candidates.extend(candidates_from_table_like_html(soup))
    candidates.extend(candidates_from_plain_text(plain_text))

    candidate_source, candidate_proponent = choose_candidate(candidates)

    return {
        "fetch_status": status,
        "candidate_source": candidate_source,
        "candidate_proponent": candidate_proponent,
        "all_candidates": " | ".join(f"{src}:{value}" for src, value in candidates[:10]),
    }


def write_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)

    if not rows:
        path.write_text("", encoding="utf-8")
        return

    fieldnames = [
        "idx",
        "applied",
        "old_proponent",
        "candidate_proponent",
        "candidate_source",
        "fetch_status",
        "source",
        "source_group",
        "source_label",
        "title",
        "region",
        "province",
        "municipalities",
        "power_mw",
        "url",
        "all_candidates",
    ]

    with path.open("w", newline="", encoding="utf-8-sig") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)


def enrich_data(
    data: dict[str, Any],
    audit_path: Path,
    sleep_seconds: float = SLEEP_SECONDS,
) -> tuple[dict[str, Any], dict[str, int]]:
    records = data.get("records") or []

    if not isinstance(records, list):
        raise ValueError("data.json non contiene una lista records valida")

    targets = [
        (idx, row)
        for idx, row in enumerate(records)
        if isinstance(row, dict)
        and is_mase_record(row)
        and not has_valid_proponent(row)
    ]

    session = requests.Session()
    cache: dict[str, dict[str, Any]] = {}
    audit_rows: list[dict[str, Any]] = []

    recovered = 0
    failed = 0

    for idx, row in targets:
        url = clean_text(row.get("url"))

        if url in cache:
            extracted = cache[url]
        else:
            extracted = extract_proponent_from_url(url, session)
            cache[url] = extracted
            time.sleep(sleep_seconds)

        candidate = clean_candidate(str(extracted.get("candidate_proponent") or ""))
        old_proponent = clean_text(row.get("proponent"))
        applied = False

        if looks_like_company(candidate):
            row["proponent"] = candidate
            row["proponent_source"] = "mase_detail_page"
            applied = True
            recovered += 1
        else:
            failed += 1

        audit_rows.append(
            {
                "idx": idx,
                "applied": applied,
                "old_proponent": old_proponent,
                "candidate_proponent": candidate,
                "candidate_source": extracted.get("candidate_source", ""),
                "fetch_status": extracted.get("fetch_status", ""),
                "source": row.get("source", ""),
                "source_group": row.get("source_group", ""),
                "source_label": row.get("source_label", ""),
                "title": row.get("title", ""),
                "region": row.get("region", ""),
                "province": row.get("province", ""),
                "municipalities": row.get("municipalities", ""),
                "power_mw": row.get("power_mw", ""),
                "url": url,
                "all_candidates": extracted.get("all_candidates", ""),
            }
        )

    write_csv(audit_path, audit_rows)

    still_missing = sum(
        1
        for row in records
        if isinstance(row, dict)
        and is_mase_record(row)
        and not has_valid_proponent(row)
    )

    data_quality = data.setdefault("data_quality", {})
    if isinstance(data_quality, dict):
        data_quality["mase_proponent_enrichment"] = {
            "generated_at": datetime.now().isoformat(timespec="seconds"),
            "targets": len(targets),
            "recovered": recovered,
            "failed": failed,
            "still_missing": still_missing,
            "audit": str(audit_path),
        }

    return data, {
        "targets": len(targets),
        "recovered": recovered,
        "failed": failed,
        "still_missing": still_missing,
    }


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Recupera proponenti mancanti nei record MASE dalle pagine di dettaglio."
    )
    parser.add_argument("--data", default=str(DEFAULT_DATA), help="Percorso data.json")
    parser.add_argument("--audit", default=str(DEFAULT_AUDIT), help="CSV audit output")
    parser.add_argument("--output", default="", help="Output JSON. Se omesso e --in-place non Ã¨ attivo, crea *_mase_proponents.json")
    parser.add_argument("--in-place", action="store_true", help="Sovrascrive data.json dopo backup")
    parser.add_argument("--no-backup", action="store_true", help="Non crea backup con --in-place")
    parser.add_argument("--fail-if-missing", action="store_true", help="Esce con errore se restano record MASE senza proponente")
    parser.add_argument("--sleep", type=float, default=SLEEP_SECONDS, help="Pausa tra richieste HTTP")
    args = parser.parse_args()

    data_path = Path(args.data)
    audit_path = Path(args.audit)

    if not data_path.exists():
        raise FileNotFoundError(f"data.json non trovato: {data_path}")

    data = json.loads(data_path.read_text(encoding="utf-8"))
    if not isinstance(data, dict):
        raise ValueError("data.json non ha root object valida")

    enriched, stats = enrich_data(data, audit_path=audit_path, sleep_seconds=args.sleep)

    if args.in_place:
        output_path = data_path
        if not args.no_backup:
            backup = data_path.with_name(
                data_path.stem + f"_backup_before_mase_proponent_enrichment_{datetime.now().strftime('%Y%m%d_%H%M%S')}" + data_path.suffix
            )
            data_path.replace(backup)
            print(f"[mase-proponent-enrichment] backup creato: {backup}")
    elif args.output:
        output_path = Path(args.output)
    else:
        output_path = data_path.with_name(data_path.stem + "_mase_proponents" + data_path.suffix)

    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(enriched, ensure_ascii=False, indent=2), encoding="utf-8")

    print("[mase-proponent-enrichment]")
    print(f"data: {data_path}")
    print(f"output: {output_path}")
    print(f"audit: {audit_path}")
    print(f"targets: {stats['targets']}")
    print(f"recovered: {stats['recovered']}")
    print(f"failed: {stats['failed']}")
    print(f"still_missing: {stats['still_missing']}")

    if args.fail_if_missing and stats["still_missing"] > 0:
        raise SystemExit("[mase-proponent-enrichment] ERRORE: restano record MASE senza proponente")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())

