from __future__ import annotations

import argparse
import csv
import json
from datetime import datetime
from pathlib import Path


OVERRIDES = {
    "https://va.mite.gov.it/it-IT/Oggetti/Info/8018": {
        "region": "Piemonte",
        "province": "TO",
        "municipalities": "Lombardore, San Benigno Canavese",
        "power_mw": 18.77382,
        "reason": "manual_mase_8018_lombardore_location_power_fix",
    },
    "https://va.mite.gov.it/it-IT/Comunicazione/DettaglioUltimiProvvedimenti/6160": {
        "region": "Emilia-Romagna",
        "province": "BO",
        "municipalities": "Malalbergo, Baricella",
        "reason": "manual_mase_6160_location_fix",
    },
    "https://va.mite.gov.it/it-IT/Oggetti/Info/9004": {
        "province": "VS",
        "reason": "manual_mase_9004_villacidro_province_fix",
    },
}


def _records_container(data):
    if isinstance(data, dict) and isinstance(data.get("records"), list):
        return data["records"]
    if isinstance(data, list):
        return data
    raise ValueError("Formato data.json non riconosciuto")


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--data", required=True)
    parser.add_argument("--audit", default="reports/manual_mase_location_overrides_audit.csv")
    args = parser.parse_args()

    data_path = Path(args.data)
    audit_path = Path(args.audit)

    if not data_path.exists():
        raise SystemExit(f"[manual-mase-location-overrides] file non trovato: {data_path}")

    data = json.loads(data_path.read_text(encoding="utf-8"))
    records = _records_container(data)

    rows = []
    changed = 0
    ts = datetime.now().isoformat(timespec="seconds")

    for r in records:
        url = str(r.get("url") or r.get("source_url") or "").strip()

        if url not in OVERRIDES:
            continue

        override = OVERRIDES[url]

        before = {
            "region": r.get("region", ""),
            "province": r.get("province", ""),
            "municipalities": r.get("municipalities", ""),
        }

        for field in ["region", "province", "municipalities", "power_mw"]:
            if field not in override:
                continue

            if r.get(field) != override[field]:
                r[field] = override[field]
                changed += 1

        rows.append({
            "timestamp": ts,
            "url": url,
            "title": r.get("title", ""),
            "proponent": r.get("proponent", ""),
            "old_region": before["region"],
            "new_region": r.get("region", ""),
            "old_province": before["province"],
            "new_province": r.get("province", ""),
            "old_municipalities": before["municipalities"],
            "new_municipalities": r.get("municipalities", ""),
            "reason": override["reason"],
        })

    data_path.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")

    if rows:
        audit_path.parent.mkdir(parents=True, exist_ok=True)
        with audit_path.open("w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
            writer.writeheader()
            writer.writerows(rows)

    print(f"[manual-mase-location-overrides] record intercettati: {len(rows)}")
    print(f"[manual-mase-location-overrides] campi corretti: {changed}")
    print(f"[manual-mase-location-overrides] audit: {audit_path}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
