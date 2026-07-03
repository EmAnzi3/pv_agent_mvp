from __future__ import annotations

import argparse
import csv
import json
import time
from pathlib import Path


OVERRIDES = [
    {
        "name": "Ranteghetta - MASE / Lombardia",
        "match_urls_contains": [
            "va.mite.gov.it/it-IT/Oggetti/Info/11630",
            "silvia.servizirl.it/silviaweb/#/scheda-sintesi/18563",
        ],
        "match_title_contains": [
            "Ranteghetta",
        ],
        "region": "Lombardia",
        "province": "MI",
        "municipalities": "Marcallo con Casone, Ossona, Santo Stefano Ticino, Magenta",
    },
]


def read_json_with_retry(path: Path, attempts: int = 20, delay: float = 0.75) -> dict:
    last_error = None

    for attempt in range(1, attempts + 1):
        try:
            return json.loads(path.read_text(encoding="utf-8"))
        except PermissionError as exc:
            last_error = exc
            print(
                f"[manual-location-overrides] data.json bloccato in lettura "
                f"tentativo {attempt}/{attempts}; retry tra {delay}s..."
            )
            time.sleep(delay)

    raise SystemExit(
        f"ERRORE: impossibile leggere {path} dopo {attempts} tentativi. "
        f"Ultimo errore: {last_error}"
    )


def write_json_with_retry(
    path: Path,
    data: dict,
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
                f"[manual-location-overrides] data.json bloccato in scrittura "
                f"tentativo {attempt}/{attempts}; retry tra {delay}s..."
            )
            time.sleep(delay)

    raise SystemExit(
        f"ERRORE: impossibile scrivere {path} dopo {attempts} tentativi. "
        f"Ultimo errore: {last_error}"
    )


def clean(value) -> str:
    return str(value or "").strip()


def record_matches(record: dict, rule: dict) -> bool:
    title = clean(record.get("title") or record.get("project_name")).lower()
    url = clean(record.get("url") or record.get("source_url") or record.get("primary_url"))

    url_hit = any(fragment in url for fragment in rule.get("match_urls_contains", []))
    title_hit = all(fragment.lower() in title for fragment in rule.get("match_title_contains", []))

    return url_hit or title_hit


def apply_overrides(data: dict) -> list[dict]:
    changes = []
    records = data.get("records", [])

    for idx, record in enumerate(records):
        if not isinstance(record, dict):
            continue

        for rule in OVERRIDES:
            if not record_matches(record, rule):
                continue

            old_region = record.get("region", "")
            old_province = record.get("province", "")
            old_municipalities = record.get("municipalities", "")

            record["region"] = rule["region"]
            record["province"] = rule["province"]
            record["municipalities"] = rule["municipalities"]

            changes.append({
                "idx": idx,
                "override": rule["name"],
                "title": record.get("title", ""),
                "source": record.get("source", ""),
                "old_region": old_region,
                "old_province": old_province,
                "old_municipalities": old_municipalities,
                "new_region": rule["region"],
                "new_province": rule["province"],
                "new_municipalities": rule["municipalities"],
                "url": record.get("url", ""),
            })

            break

    top_projects = data.get("summary", {}).get("top_projects", [])
    if isinstance(top_projects, list):
        for idx, record in enumerate(top_projects):
            if not isinstance(record, dict):
                continue

            for rule in OVERRIDES:
                if not record_matches(record, rule):
                    continue

                old_region = record.get("region", "")
                old_province = record.get("province", "")
                old_municipalities = record.get("municipalities", "")

                record["region"] = rule["region"]
                record["province"] = rule["province"]
                record["municipalities"] = rule["municipalities"]

                changes.append({
                    "idx": f"summary.top_projects[{idx}]",
                    "override": rule["name"],
                    "title": record.get("title", ""),
                    "source": record.get("source", ""),
                    "old_region": old_region,
                    "old_province": old_province,
                    "old_municipalities": old_municipalities,
                    "new_region": rule["region"],
                    "new_province": rule["province"],
                    "new_municipalities": rule["municipalities"],
                    "url": record.get("url", ""),
                })

                break

    return changes


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--data", required=True)
    parser.add_argument("--audit", default="reports/manual_location_overrides_audit.csv")
    args = parser.parse_args()

    data_path = Path(args.data)
    audit_path = Path(args.audit)

    if not data_path.exists():
        raise FileNotFoundError(f"File non trovato: {data_path}")

    data = read_json_with_retry(data_path)
    changes = apply_overrides(data)

    write_json_with_retry(data_path, data)

    audit_path.parent.mkdir(parents=True, exist_ok=True)

    with audit_path.open("w", newline="", encoding="utf-8-sig") as handle:
        writer = csv.DictWriter(
            handle,
            fieldnames=[
                "idx",
                "override",
                "title",
                "source",
                "old_region",
                "old_province",
                "old_municipalities",
                "new_region",
                "new_province",
                "new_municipalities",
                "url",
            ],
        )
        writer.writeheader()
        writer.writerows(changes)

    print(f"[manual-location-overrides] file: {data_path}")
    print(f"[manual-location-overrides] override applicati: {len(changes)}")
    print(f"[manual-location-overrides] audit: {audit_path}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
