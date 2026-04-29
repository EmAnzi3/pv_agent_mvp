from __future__ import annotations

import argparse
import json
import shutil
from datetime import datetime
from pathlib import Path
from typing import Any


START_MARKER = "const DASHBOARD_DATA = "
END_MARKER = ";\n\n    const summary = DASHBOARD_DATA.summary;"


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
        default="/app/reports/site/data.json",
        help="Percorso del data.json deduplicato.",
    )
    parser.add_argument(
        "--html",
        default="/app/reports/site/index.html",
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
    print(f"[dashboard-data-sync] bad_gravina_top_projects: {len(bad_top)}")

    if bad_top:
        raise SystemExit(
            "[dashboard-data-sync] ERRORE: Gravina/Crispiano è ancora nella Top Projects."
        )

    print("[dashboard-data-sync] OK: index.html sincronizzato con data.json deduplicato.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
