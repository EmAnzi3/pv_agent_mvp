from __future__ import annotations

import argparse
import json
import subprocess
import sys
from pathlib import Path
from typing import Any


DEFAULT_DATA_JSON = Path("reports/site/data.json")
DEFAULT_INDEX_HTML = Path("reports/site/index.html")

PIEMONTE_BASELINE = {
    "TRINO": {"province": "VC", "power_mw": 4.717},
    "ISOLA SANT'ANTONIO": {"province": "AL", "power_mw": 3.45},
    "CANDELO": {"province": "BI"},
    "CASALE MONFERRATO": {"province": "AL"},
    "PECETTO DI VALENZA, VALENZA": {"province": "AL"},
}
PIEMONTE_PROVINCE_CODES = {"AL", "AT", "BI", "CN", "NO", "TO", "VB", "VC"}


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

    records = data.get("records", [])
    piemonte_rows = [
        row for row in records
        if isinstance(row, dict) and row.get("source") == "piemonte"
    ]

    if len(piemonte_rows) < len(PIEMONTE_BASELINE):
        errors.append(
            f"Piemonte: record troppo pochi per la baseline: {len(piemonte_rows)}"
        )

    by_municipality = {
        str(row.get("municipalities") or "").strip().upper(): row
        for row in piemonte_rows
    }

    for municipality, expected in PIEMONTE_BASELINE.items():
        row = by_municipality.get(municipality.upper())
        if row is None:
            errors.append(f"Piemonte: baseline mancante: {municipality}")
            continue

        province = str(row.get("province") or "").strip().upper()
        expected_province = expected.get("province")
        if expected_province and province != expected_province:
            errors.append(
                f"Piemonte: provincia errata per {municipality}: "
                f"atteso {expected_province}, trovato {province or 'vuoto'}"
            )

        expected_mw = expected.get("power_mw")
        if expected_mw is not None:
            try:
                actual_mw = float(row.get("power_mw"))
            except Exception:
                actual_mw = None
            if actual_mw is None or abs(actual_mw - expected_mw) > 0.001:
                errors.append(
                    f"Piemonte: MW errati per {municipality}: "
                    f"atteso {expected_mw}, trovato {row.get('power_mw')}"
                )

    invalid_piemonte_provinces = sorted({
        str(row.get("province") or "").strip().upper()
        for row in piemonte_rows
        if str(row.get("province") or "").strip()
        and str(row.get("province") or "").strip().upper()
        not in PIEMONTE_PROVINCE_CODES
    })
    if invalid_piemonte_provinces:
        errors.append(
            "Piemonte: sigle provincia non valide: "
            + ", ".join(invalid_piemonte_provinces)
        )

    suspicious_proponents = [
        str(row.get("proponent") or "")
        for row in piemonte_rows
        if any(
            token in str(row.get("proponent") or "").upper()
            for token in ["MWP", "POTENZA IN IMMISSIONE", "POTENZA NOMINALE"]
        )
    ]
    if suspicious_proponents:
        errors.append(
            "Piemonte: proponenti sospetti derivati da testo potenza: "
            + " | ".join(suspicious_proponents[:5])
        )

    if total_records in (None, 0):
        errors.append("summary.total_records assente o zero")

    if punctual_records in (None, 0):
        errors.append("summary.punctual_records assente o zero")

    if terna_records < 70:
        errors.append(f"terna_records troppo basso: atteso almeno 70, trovato {terna_records}")
    elif terna_records != 82:
        print(f"[run-pipeline] WARNING: terna_records diverso dallo storico 82: trovato {terna_records}")

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
        "--skip-mase-proponent-enrichment",
        action="store_true",
        help="Salta il recupero proponenti MASE dalle pagine di dettaglio.",
    )

    parser.add_argument(
        "--allow-mase-missing-proponent",
        action="store_true",
        help="Non blocca la pipeline se restano record MASE senza proponente.",
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
            "migrazione chiave legacy Piemonte",
            [py, "scripts/migrate_piemonte_legacy_project_key.py"],
        )

        run_step(
            "raccolta dati / export / dashboard grezza",
            [py, "-m", "app.main", "run-once"],
        )

    if not args.skip_data_quality:
        run_step(
            "data quality / deduplica / summary / top_projects",
            [py, "-m", "app.data_quality", "--in-place"],
        )

    if not args.skip_mase_proponent_enrichment:
        enrichment_cmd = [py, "-m", "app.mase_proponent_enrichment", "--in-place"]
        if not args.allow_mase_missing_proponent:
            enrichment_cmd.append("--fail-if-missing")

        run_step(
            "recupero proponenti MASE da dettaglio",
            enrichment_cmd,
        )

    # Le correzioni geografiche MASE protette devono essere applicate prima
    # del gate fail-closed di dashboard_data_sync. In questo modo il flusso
    # standalone e quello avviato dal batch hanno lo stesso ordine sicuro.
    run_step(
        "correzioni localizzazione MASE protette",
        [
            py,
            "scripts/manual_mase_location_overrides.py",
            "--data",
            str(args.data_json),
            "--audit",
            "reports/manual_mase_location_overrides_audit.csv",
        ],
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


