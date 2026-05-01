from __future__ import annotations

import argparse
import json
import subprocess
import sys
from pathlib import Path
from typing import Any


DEFAULT_DATA_JSON = Path("/app/reports/site/data.json")
DEFAULT_INDEX_HTML = Path("/app/reports/site/index.html")


def run_step(label: str, cmd: list[str]) -> None:
    print("")
    print("=" * 80)
    print(f"[run-pipeline] STEP: {label}")
    print(f"[run-pipeline] CMD : {' '.join(cmd)}")
    print("=" * 80)

    result = subprocess.run(cmd, text=True)

    if result.returncode != 0:
        raise SystemExit(f"[run-pipeline] ERRORE nello step: {label}")


def load_json(path: Path) -> dict[str, Any]:
    if not path.exists():
        raise SystemExit(f"[run-pipeline] ERRORE: file non trovato: {path}")

    with path.open("r", encoding="utf-8") as f:
        data = json.load(f)

    if not isinstance(data, dict):
        raise SystemExit(f"[run-pipeline] ERRORE: JSON non valido: {path}")

    return data


def count_records_by_source(data: dict[str, Any], source: str) -> int:
    records = data.get("records", [])
    if not isinstance(records, list):
        return 0

    return sum(1 for row in records if isinstance(row, dict) and row.get("source") == source)


def count_top_by_bad_gravina(data: dict[str, Any]) -> int:
    top_projects = data.get("summary", {}).get("top_projects", [])
    if not isinstance(top_projects, list):
        return 0

    bad = 0

    for row in top_projects:
        if not isinstance(row, dict):
            continue

        title = str(row.get("title", "")).lower()
        province = str(row.get("province", "")).upper()
        municipalities = str(row.get("municipalities", "")).lower()
        power = row.get("power_mw")

        if "gravina in puglia" in title and province == "TA" and "crispiano" in municipalities:
            bad += 1

        if "gravina in puglia" in title and power == 319.11:
            bad += 1

    return bad


def count_duplicate_project_keys(data: dict[str, Any]) -> int:
    records = data.get("records", [])
    if not isinstance(records, list):
        return 0

    seen: set[str] = set()
    duplicates = 0

    for row in records:
        if not isinstance(row, dict):
            continue

        key = row.get("project_key")
        if not key:
            continue

        key = str(key)

        if key in seen:
            duplicates += 1
        else:
            seen.add(key)

    return duplicates


def html_contains_stale_values(path: Path) -> list[str]:
    if not path.exists():
        raise SystemExit(f"[run-pipeline] ERRORE: index.html non trovato: {path}")

    html = path.read_text(encoding="utf-8", errors="replace")

    stale_patterns = [
        '"total_records":2107',
        '"total_records": 2107',
        '"punctual_records":2025',
        '"punctual_records": 2025',
        '"total_mw_punctual":65544.398',
        '"total_mw_punctual": 65544.398',
    ]

    found = [p for p in stale_patterns if p in html]
    return found


def validate_outputs(
    data_path: Path,
    html_path: Path,
    fail_on_source_puglia: bool = True,
) -> None:
    print("")
    print("=" * 80)
    print("[run-pipeline] VALIDAZIONE FINALE")
    print("=" * 80)

    data = load_json(data_path)

    summary = data.get("summary", {})
    data_quality = data.get("data_quality", {})

    total_records = summary.get("total_records")
    punctual_records = summary.get("punctual_records")
    terna_records = summary.get("terna_records")
    total_mw_punctual = summary.get("total_mw_punctual")
    total_mw_terna = summary.get("total_mw_terna")
    dq_version = data_quality.get("version")

    source_puglia_count = count_records_by_source(data, "puglia")
    source_sistema_puglia_count = count_records_by_source(data, "sistema_puglia_energia")
    bad_gravina_top = count_top_by_bad_gravina(data)
    duplicate_project_keys = count_duplicate_project_keys(data)
    stale_html = html_contains_stale_values(html_path)

    print(f"[run-pipeline] data_json: {data_path}")
    print(f"[run-pipeline] index_html: {html_path}")
    print(f"[run-pipeline] total_records: {total_records}")
    print(f"[run-pipeline] punctual_records: {punctual_records}")
    print(f"[run-pipeline] terna_records: {terna_records}")
    print(f"[run-pipeline] total_mw_punctual: {total_mw_punctual}")
    print(f"[run-pipeline] total_mw_terna: {total_mw_terna}")
    print(f"[run-pipeline] data_quality.version: {dq_version}")
    print(f"[run-pipeline] records source=puglia: {source_puglia_count}")
    print(f"[run-pipeline] records source=sistema_puglia_energia: {source_sistema_puglia_count}")
    print(f"[run-pipeline] bad Gravina/Crispiano in top_projects: {bad_gravina_top}")
    print(f"[run-pipeline] duplicate project_key: {duplicate_project_keys}")
    print(f"[run-pipeline] stale HTML patterns: {len(stale_html)}")

    errors: list[str] = []

    if total_records in (None, 0):
        errors.append("summary.total_records assente o zero")

    if punctual_records in (None, 0):
        errors.append("summary.punctual_records assente o zero")

    if terna_records != 82:
        errors.append(f"terna_records atteso 82, trovato {terna_records}")

    if not dq_version:
        errors.append("data_quality.version assente")

    if fail_on_source_puglia and source_puglia_count > 0:
        errors.append(f"trovati ancora {source_puglia_count} record con source='puglia'")

    if source_sistema_puglia_count == 0:
        errors.append("nessun record sistema_puglia_energia trovato")

    if bad_gravina_top > 0:
        errors.append("Gravina/Crispiano è ancora in top_projects")

    if duplicate_project_keys > 0:
        errors.append(f"trovate {duplicate_project_keys} project_key duplicate")

    if stale_html:
        errors.append(f"index.html contiene ancora valori vecchi: {stale_html}")

    if errors:
        print("")
        print("[run-pipeline] VALIDAZIONE FALLITA")
        for err in errors:
            print(f"- {err}")
        raise SystemExit("[run-pipeline] Pipeline completata, ma output non valido.")

    print("")
    print("[run-pipeline] OK: pipeline completata e output validato.")


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Esegue raccolta dati, data quality, sync HTML e controlli finali."
    )

    parser.add_argument(
        "--skip-main",
        action="store_true",
        help="Salta app.main run-once.",
    )

    parser.add_argument(
        "--skip-data-quality",
        action="store_true",
        help="Salta app.data_quality --in-place.",
    )

    parser.add_argument(
        "--skip-dashboard-sync",
        action="store_true",
        help="Salta app.dashboard_data_sync.",
    )

    parser.add_argument(
        "--allow-source-puglia",
        action="store_true",
        help="Non blocca la validazione se trova ancora source='puglia'. Da usare solo per debug.",
    )

    parser.add_argument(
        "--data-json",
        default=str(DEFAULT_DATA_JSON),
        help="Percorso data.json finale.",
    )

    parser.add_argument(
        "--index-html",
        default=str(DEFAULT_INDEX_HTML),
        help="Percorso index.html finale.",
    )

    args = parser.parse_args()

    py = sys.executable

    if not args.skip_main:
        run_step(
            "raccolta dati / export / dashboard grezza",
            [py, "-m", "app.main", "run-once"],
        )

    if not args.skip_data_quality:
        run_step(
            "data quality / deduplica / summary / top_projects",
            [py, "-m", "app.data_quality", "--in-place"],
        )

    if not args.skip_dashboard_sync:
        run_step(
            "sync dati puliti dentro index.html",
            [py, "-m", "app.dashboard_data_sync"],
        )

    validate_outputs(
        data_path=Path(args.data_json),
        html_path=Path(args.index_html),
        fail_on_source_puglia=not args.allow_source_puglia,
    )

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
