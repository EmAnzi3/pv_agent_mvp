from __future__ import annotations

import argparse
import csv
import json
from datetime import datetime
from pathlib import Path


TARGET_URL_FRAGMENT = "agrivoltaico_sorgeniaren"

CORRECT_PROPONENT = "SORGENIA RENEWABLES S.R.L."
CORRECT_PROVINCE = "CS"
CORRECT_MUNICIPALITIES = (
    "Altomonte, Castrovillari, Spezzano Albanese, San Lorenzo del Vallo"
)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--data", required=True)
    parser.add_argument(
        "--audit",
        default="reports/manual_calabria_location_override_audit.csv",
    )
    args = parser.parse_args()

    data_path = Path(args.data)
    audit_path = Path(args.audit)

    data = json.loads(data_path.read_text(encoding="utf-8"))
    records = data.get("records", [])

    matches = [
        record
        for record in records
        if TARGET_URL_FRAGMENT
        in str(record.get("url") or "").lower()
    ]

    if len(matches) != 1:
        raise SystemExit(
            f"ERRORE: trovati {len(matches)} record Sorgenia Calabria; atteso 1"
        )

    record = matches[0]

    old_proponent = record.get("proponent")
    old_province = record.get("province")
    old_municipalities = record.get("municipalities")

    record["proponent"] = CORRECT_PROPONENT
    record["province"] = CORRECT_PROVINCE
    record["municipalities"] = CORRECT_MUNICIPALITIES
    record["province_deduced"] = False
    record["municipalities_deduced"] = False

    data_path.write_text(
        json.dumps(data, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )

    audit_path.parent.mkdir(parents=True, exist_ok=True)

    row = {
        "timestamp": datetime.now().isoformat(timespec="seconds"),
        "url": record.get("url", ""),
        "old_proponent": old_proponent,
        "new_proponent": CORRECT_PROPONENT,
        "old_province": old_province,
        "new_province": CORRECT_PROVINCE,
        "old_municipalities": old_municipalities,
        "new_municipalities": CORRECT_MUNICIPALITIES,
        "reason": "verified_calabria_sorgenia_location_override",
    }

    with audit_path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(row.keys()))
        writer.writeheader()
        writer.writerow(row)

    print("[calabria-location-override] record corretti: 1")
    print("[calabria-location-override] proponente:", CORRECT_PROPONENT)
    print("[calabria-location-override] provincia:", CORRECT_PROVINCE)
    print(
        "[calabria-location-override] comuni:",
        CORRECT_MUNICIPALITIES,
    )

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
