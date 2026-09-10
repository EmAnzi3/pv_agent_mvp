from __future__ import annotations

import argparse
import json
import re
import shutil
import unicodedata
from datetime import datetime
from pathlib import Path
from typing import Any


START_MARKER = "const DASHBOARD_DATA = "
END_MARKER = ";\n\n    const summary = DASHBOARD_DATA.summary;"


PROTECTED_GEO_RULES = {
    "sesto al reghena": {
        "province": "PN",
        "region": "Friuli-Venezia Giulia",
    },
}


PROTECTED_GEO_SEARCH_FIELDS = [
    "title",
    "project_name",
    "municipalities",
    "municipality",
    "comuni",
    "comune",
    "localizzazione",
    "location",
]


def stamp() -> str:
    return datetime.now().strftime("%Y%m%d_%H%M%S")


def load_json(path: Path) -> dict[str, Any]:
    if not path.exists():
        raise FileNotFoundError(f"Data JSON non trovato: {path}")

    with path.open("r", encoding="utf-8") as f:
        data = json.load(f)

    if not isinstance(data, dict):
        raise ValueError("Il data.json non ha una root object valida.")

    if "summary" not in data:
        raise ValueError("Il data.json non contiene 'summary'.")

    if "records" not in data:
        raise ValueError("Il data.json non contiene 'records'.")

    return data


def replace_dashboard_data(html: str, data: dict[str, Any]) -> str:
    start_idx = html.find(START_MARKER)
    if start_idx < 0:
        raise ValueError("Marker 'const DASHBOARD_DATA =' non trovato in index.html.")

    payload_start = start_idx + len(START_MARKER)

    end_idx = html.find(END_MARKER, payload_start)
    if end_idx < 0:
        raise ValueError("Marker finale dopo DASHBOARD_DATA non trovato in index.html.")

    payload = json.dumps(data, ensure_ascii=False, separators=(",", ":"))

    return html[:payload_start] + payload + html[end_idx:]


def _norm_geo(value: Any) -> str:
    text = str(value or "").strip().lower()
    text = unicodedata.normalize("NFKD", text)
    text = "".join(ch for ch in text if not unicodedata.combining(ch))
    text = re.sub(r"[^a-z0-9]+", " ", text)
    return re.sub(r"\s+", " ", text).strip()


def _protected_geo_search_text(row: dict[str, Any]) -> str:
    return _norm_geo(
        " ".join(str(row.get(field) or "") for field in PROTECTED_GEO_SEARCH_FIELDS)
    )


def find_bad_protected_geo(data: dict[str, Any]) -> list[dict[str, Any]]:
    """Blocca combinazioni geografiche note come impossibili prima della pubblicazione."""
    bad: list[dict[str, Any]] = []

    collections = [
        ("records", data.get("records", [])),
        ("summary.top_projects", data.get("summary", {}).get("top_projects", [])),
    ]

    for collection_name, rows in collections:
        if not isinstance(rows, list):
            continue

        for row in rows:
            if not isinstance(row, dict):
                continue

            haystack = _protected_geo_search_text(row)
            if not haystack:
                continue

            for municipality_norm, rule in PROTECTED_GEO_RULES.items():
                if not re.search(rf"\b{re.escape(municipality_norm)}\b", haystack):
                    continue

                province = str(row.get("province") or "").strip().upper()
                region = _norm_geo(row.get("region"))

                if province != rule["province"] or region != _norm_geo(rule["region"]):
                    bad.append(
                        {
                            "collection": collection_name,
                            "municipality": municipality_norm,
                            "title": row.get("title", ""),
                            "province": row.get("province", ""),
                            "region": row.get("region", ""),
                            "expected_province": rule["province"],
                            "expected_region": rule["region"],
                        }
                    )

    return bad


def find_bad_top_projects(data: dict[str, Any]) -> list[dict[str, Any]]:
    top_projects = data.get("summary", {}).get("top_projects", [])
    bad = []

    for row in top_projects:
        title = str(row.get("title", "")).lower()
        province = str(row.get("province", "")).upper()
        municipalities = str(row.get("municipalities", "")).lower()
        power = row.get("power_mw")

        if "gravina in puglia" in title and province == "TA" and "crispiano" in municipalities:
            bad.append(row)

        if "gravina in puglia" in title and power == 319.11:
            bad.append(row)

    return bad


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Sincronizza i dati deduplicati dentro index.html statico."
    )
    parser.add_argument(
        "--data",
        default="reports/site/data.json",
        help="Percorso del data.json deduplicato.",
    )
    parser.add_argument(
        "--html",
        default="reports/site/index.html",
        help="Percorso dell'index.html da aggiornare.",
    )
    parser.add_argument(
        "--no-backup",
        action="store_true",
        help="Non crea backup dell'HTML precedente.",
    )

    args = parser.parse_args()

    data_path = Path(args.data)
    html_path = Path(args.html)

    if not html_path.exists():
        raise FileNotFoundError(f"index.html non trovato: {html_path}")

    data = load_json(data_path)

    # Gate finale fail-closed: viene eseguito dopo normalizzazioni/override e
    # prima di modificare l'HTML. Se l'incongruenza riappare, il batch si ferma
    # qui e non può proseguire verso report cambiamenti e copia in docs/.
    bad_protected_geo = find_bad_protected_geo(data)
    if bad_protected_geo:
        sample = bad_protected_geo[0]
        raise SystemExit(
            "[dashboard-data-sync] ERRORE GEO BLOCCANTE: "
            f"{sample['municipality']} in {sample['collection']} risulta "
            f"{sample['province']} / {sample['region']} invece di "
            f"{sample['expected_province']} / {sample['expected_region']}. "
            "Dashboard e report NON pubblicati."
        )

    html = html_path.read_text(encoding="utf-8")

    if not args.no_backup:
        backup_path = html_path.with_name(
            html_path.stem + f"_backup_before_data_sync_{stamp()}" + html_path.suffix
        )
        shutil.copy2(html_path, backup_path)
        print(f"[dashboard-data-sync] backup creato: {backup_path}")

    new_html = replace_dashboard_data(html, data)
    html_path.write_text(new_html, encoding="utf-8")

    summary = data.get("summary", {})
    data_quality = data.get("data_quality", {})
    bad_top = find_bad_top_projects(data)

    print(f"[dashboard-data-sync] data: {data_path}")
    print(f"[dashboard-data-sync] html: {html_path}")
    print(f"[dashboard-data-sync] total_records: {summary.get('total_records')}")
    print(f"[dashboard-data-sync] punctual_records: {summary.get('punctual_records')}")
    print(f"[dashboard-data-sync] terna_records: {summary.get('terna_records')}")
    print(f"[dashboard-data-sync] total_mw_punctual: {summary.get('total_mw_punctual')}")
    print(f"[dashboard-data-sync] total_mw_terna: {summary.get('total_mw_terna')}")
    print(f"[dashboard-data-sync] data_quality_version: {data_quality.get('version')}")
    print(f"[dashboard-data-sync] suspicious_rows: {data_quality.get('suspicious_rows')}")
    print(f"[dashboard-data-sync] top_projects_excluded_suspicious: {data_quality.get('top_projects_excluded_suspicious')}")
    print(f"[dashboard-data-sync] project_key_splits: {data_quality.get('project_key_splits')}")
    print(f"[dashboard-data-sync] bad_protected_geo: {len(bad_protected_geo)}")
    print(f"[dashboard-data-sync] bad_gravina_top_projects: {len(bad_top)}")

    if bad_top:
        raise SystemExit(
            "[dashboard-data-sync] ERRORE: Gravina/Crispiano è ancora nella Top Projects."
        )

    print("[dashboard-data-sync] OK: index.html sincronizzato con data.json deduplicato.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

