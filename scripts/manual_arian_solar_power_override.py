from __future__ import annotations

import argparse
import csv
import json
from datetime import datetime
from pathlib import Path


TARGET_URL = (
    "https://serviziambiente.regione.emilia-romagna.it/"
    "viavasweb/ricerca/dettaglio/6962"
)

CORRECT_POWER_MW = 17.91


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--data", required=True)
    parser.add_argument(
        "--audit",
        default="reports/manual_arian_solar_power_override_audit.csv",
    )
    args = parser.parse_args()

    data_path = Path(args.data)
    audit_path = Path(args.audit)

    if not data_path.exists():
        raise SystemExit(f"ERRORE: file non trovato: {data_path}")

    data = json.loads(data_path.read_text(encoding="utf-8"))
    records = data.get("records", [])

    matches = [
        record
        for record in records
        if str(record.get("url") or "").rstrip("/")
        == TARGET_URL.rstrip("/")
    ]

    if len(matches) != 1:
        raise SystemExit(
            f"ERRORE: record Arian Solar trovati: {len(matches)}; atteso 1"
        )

    record = matches[0]
    old_power = record.get("power_mw")

    record["power_mw"] = CORRECT_POWER_MW

    data_path.write_text(
        json.dumps(data, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )

    audit_path.parent.mkdir(parents=True, exist_ok=True)

    row = {
        "timestamp": datetime.now().isoformat(timespec="seconds"),
        "url": TARGET_URL,
        "title": record.get("title", ""),
        "proponent": record.get("proponent", ""),
        "region": record.get("region", ""),
        "province": record.get("province", ""),
        "municipalities": record.get("municipalities", ""),
        "old_power_mw": old_power,
        "new_power_mw": CORRECT_POWER_MW,
        "reason": "power_confirmed_in_signed_application_4061",
    }

    with audit_path.open(
        "w",
        newline="",
        encoding="utf-8-sig",
    ) as handle:
        writer = csv.DictWriter(
            handle,
            fieldnames=list(row.keys()),
        )
        writer.writeheader()
        writer.writerow(row)

    print("[arian-solar-power] record corretti: 1")
    print("[arian-solar-power] vecchia potenza:", old_power)
    print("[arian-solar-power] nuova potenza:", CORRECT_POWER_MW)
    print("[arian-solar-power] regione:", record.get("region"))
    print("[arian-solar-power] provincia:", record.get("province"))
    print("[arian-solar-power] comuni:", record.get("municipalities"))

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
