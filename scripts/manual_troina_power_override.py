from __future__ import annotations

import argparse
import csv
import json
import re
from datetime import datetime
from pathlib import Path


TARGET_URL = "https://va.mite.gov.it/it-IT/Comunicazione/DettaglioUltimiProvvedimenti/6206"
CORRECT_POWER_MW = 64.7


def fix_title(value: str) -> str:
    value = str(value or "")

    # Corregge il refuso della fonte senza modificare i 15 MW del BESS.
    value = re.sub(
        r"64[\.,]69\s*kWp?\b",
        "64,7 MWp",
        value,
        flags=re.IGNORECASE,
    )

    return value


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--data", required=True)
    parser.add_argument(
        "--audit",
        default="reports/manual_troina_power_override_audit.csv",
    )
    args = parser.parse_args()

    data_path = Path(args.data)
    audit_path = Path(args.audit)

    data = json.loads(data_path.read_text(encoding="utf-8"))
    records = data.get("records", [])

    matches = [
        record for record in records
        if str(record.get("url") or "").strip() == TARGET_URL
    ]

    if len(matches) != 1:
        raise SystemExit(
            f"ERRORE: trovati {len(matches)} record Troina; atteso 1"
        )

    record = matches[0]

    old_power = record.get("power_mw")
    old_title = str(record.get("title") or "")

    record["power_mw"] = CORRECT_POWER_MW
    record["title"] = fix_title(old_title)

    if isinstance(record.get("_dedupe_titles"), list):
        record["_dedupe_titles"] = [
            fix_title(title) for title in record["_dedupe_titles"]
        ]

    data_path.write_text(
        json.dumps(data, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )

    audit_path.parent.mkdir(parents=True, exist_ok=True)

    row = {
        "timestamp": datetime.now().isoformat(timespec="seconds"),
        "proponent": record.get("proponent", ""),
        "old_power_mw": old_power,
        "new_power_mw": CORRECT_POWER_MW,
        "old_title": old_title,
        "new_title": record.get("title", ""),
        "url": TARGET_URL,
        "reason": "storage_power_mistaken_for_generation_power",
    }

    with audit_path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(row.keys()))
        writer.writeheader()
        writer.writerow(row)

    print("[troina-power-override] record corretti: 1")
    print("[troina-power-override] vecchia potenza:", old_power)
    print("[troina-power-override] nuova potenza:", CORRECT_POWER_MW)
    print("[troina-power-override] titolo:", record.get("title"))

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
