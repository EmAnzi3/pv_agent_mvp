from __future__ import annotations

import argparse
import csv
import json
import time
from datetime import datetime
from pathlib import Path
from typing import Any


EXCLUSION_RULES = [
    {
        "name": "exclude_toscana_id_2535_lago_milano_water_basin",
        "url": "https://servizi.patti.regione.toscana.it/star-info/avvisiPubblici/186",
        "title_contains": [
            "[ID 2535]",
            "Lago Milano",
            "bacino di accumulo",
        ],
        "reason": "water_basin_not_photovoltaic_project",
    },
]

PATCH_RULES = [
    {
        "name": "fix_mase_6245_ardea_power_decimal_separator",
        "url": "https://va.mite.gov.it/it-IT/Comunicazione/DettaglioUltimiProvvedimenti/6245",
        "fields": {
            "power_mw": 144.335,
        },
        "reason": "source_uses_comma_as_thousands_separator_144_335_mw",
    },
    {
        "name": "fix_mase_6239_serramanna_municipality",
        "url": "https://va.mite.gov.it/it-IT/Comunicazione/DettaglioUltimiProvvedimenti/6239",
        "fields": {
            "region": "Sardegna",
            "province": "SU",
            "municipalities": "Serramanna",
        },
        "reason": "municipality_missing_in_extracted_record",
    },

    {
        "name": "fix_toscana_id_2407_grosseto_province",
        "url": "https://servizi.patti.regione.toscana.it/star-info/avvisiPubblici/32",
        "fields": {
            "region": "Toscana",
            "province": "GR",
            "municipalities": "GROSSETO",
        },
        "reason": "toscana_id_2407_wrong_province_code_ar_instead_of_gr",
    },
    {
        "name": "fix_mase_spinazzola_location_from_source_document",
        "title_contains": [
            "Progetto di un impianto fotovoltaico",
            "56,31 MW",
        ],
        "fields": {
            "region": "Puglia",
            "province": "BT",
            "municipalities": "Spinazzola",
        },
        "reason": "source_document_places_project_in_spinazzola_bt_not_genzano_di_lucania_pz",
    },

]


def read_json_with_retry(path: Path, attempts: int = 20, delay: float = 0.75) -> dict[str, Any]:
    last_error = None

    for attempt in range(1, attempts + 1):
        try:
            return json.loads(path.read_text(encoding="utf-8"))
        except PermissionError as exc:
            last_error = exc
            print(
                f"[manual-quality-overrides] data.json bloccato in lettura "
                f"tentativo {attempt}/{attempts}; retry tra {delay}s..."
            )
            time.sleep(delay)

    raise SystemExit(
        f"ERRORE: impossibile leggere {path} dopo {attempts} tentativi. "
        f"Ultimo errore: {last_error}"
    )


def write_json_with_retry(
    path: Path,
    data: dict[str, Any],
    attempts: int = 20,
    delay: float = 0.75,
) -> None:
    payload = json.dumps(data, ensure_ascii=False, indent=2)
    last_error = None

    for attempt in range(1, attempts + 1):
        try:
            path.write_text(payload, encoding="utf-8")
            return
        except PermissionError as exc:
            last_error = exc
            print(
                f"[manual-quality-overrides] data.json bloccato in scrittura "
                f"tentativo {attempt}/{attempts}; retry tra {delay}s..."
            )
            time.sleep(delay)

    raise SystemExit(
        f"ERRORE: impossibile scrivere {path} dopo {attempts} tentativi. "
        f"Ultimo errore: {last_error}"
    )


def clean(value: Any) -> str:
    return str(value or "").strip()


def same_url(record_url: Any, target_url: str) -> bool:
    return clean(record_url).rstrip("/") == target_url.rstrip("/")


def exclusion_matches(record: dict[str, Any], rule: dict[str, Any]) -> bool:
    url = clean(record.get("url") or record.get("source_url"))
    title = clean(record.get("title"))

    if same_url(url, rule["url"]):
        return True

    title_lower = title.lower()
    return all(
        fragment.lower() in title_lower
        for fragment in rule.get("title_contains", [])
    )


def title_contains_all(record: dict[str, Any], fragments: list[str]) -> bool:
    title = clean(record.get("title") or record.get("project_name"))
    title_lower = title.lower()

    return all(
        fragment.lower() in title_lower
        for fragment in fragments
    )


def patch_matches(record: dict[str, Any], rule: dict[str, Any]) -> bool:
    url = clean(
        record.get("url")
        or record.get("source_url")
        or record.get("primary_url")
    )

    if rule.get("url") and same_url(url, rule["url"]):
        return True

    fragments = rule.get("title_contains") or []

    if fragments and title_contains_all(record, fragments):
        return True

    return False


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--data", required=True)
    parser.add_argument(
        "--audit",
        default="reports/manual_quality_overrides_audit.csv",
    )
    args = parser.parse_args()

    data_path = Path(args.data)
    audit_path = Path(args.audit)

    if not data_path.exists():
        raise SystemExit(f"ERRORE: file non trovato: {data_path}")

    data = read_json_with_retry(data_path)
    records = data.get("records", [])

    if not isinstance(records, list):
        raise SystemExit("ERRORE: data.json non contiene records validi")

    audit_rows: list[dict[str, Any]] = []
    timestamp = datetime.now().isoformat(timespec="seconds")

    # Esclusioni.
    kept_records = []
    removed = 0

    for idx, record in enumerate(records):
        if not isinstance(record, dict):
            kept_records.append(record)
            continue

        matched_rule = None

        for rule in EXCLUSION_RULES:
            if exclusion_matches(record, rule):
                matched_rule = rule
                break

        if matched_rule:
            removed += 1
            audit_rows.append({
                "timestamp": timestamp,
                "action": "exclude",
                "rule": matched_rule["name"],
                "idx": idx,
                "url": record.get("url", ""),
                "title": record.get("title", ""),
                "proponent": record.get("proponent", ""),
                "old_region": record.get("region", ""),
                "new_region": "",
                "old_province": record.get("province", ""),
                "new_province": "",
                "old_municipalities": record.get("municipalities", ""),
                "new_municipalities": "",
                "old_power_mw": record.get("power_mw"),
                "new_power_mw": "",
                "reason": matched_rule["reason"],
            })
            continue

        kept_records.append(record)

    data["records"] = kept_records
    records = kept_records

    # Patch.
    patched_fields = 0

    for rule in PATCH_RULES:
        matches = [
            record
            for record in records
            if isinstance(record, dict) and patch_matches(record, rule)
        ]

        if len(matches) != 1:
            raise SystemExit(
                f"ERRORE: rule {rule['name']} ha trovato {len(matches)} record; atteso 1"
            )

        record = matches[0]

        before = {
            "region": record.get("region", ""),
            "province": record.get("province", ""),
            "municipalities": record.get("municipalities", ""),
            "power_mw": record.get("power_mw"),
        }

        changed = 0

        for field, value in rule["fields"].items():
            if record.get(field) != value:
                record[field] = value
                changed += 1
                patched_fields += 1

        audit_rows.append({
            "timestamp": timestamp,
            "action": "patch",
            "rule": rule["name"],
            "idx": records.index(record),
            "url": record.get("url", ""),
            "title": record.get("title", ""),
            "proponent": record.get("proponent", ""),
            "old_region": before["region"],
            "new_region": record.get("region", ""),
            "old_province": before["province"],
            "new_province": record.get("province", ""),
            "old_municipalities": before["municipalities"],
            "new_municipalities": record.get("municipalities", ""),
            "old_power_mw": before["power_mw"],
            "new_power_mw": record.get("power_mw"),
            "reason": rule["reason"],
        })

    write_json_with_retry(data_path, data)

    audit_path.parent.mkdir(parents=True, exist_ok=True)

    if audit_rows:
        with audit_path.open("w", newline="", encoding="utf-8-sig") as handle:
            writer = csv.DictWriter(
                handle,
                fieldnames=list(audit_rows[0].keys()),
            )
            writer.writeheader()
            writer.writerows(audit_rows)

    print("[manual-quality-overrides] record esclusi:", removed)
    print("[manual-quality-overrides] campi corretti:", patched_fields)
    print("[manual-quality-overrides] audit:", audit_path)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
