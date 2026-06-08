from __future__ import annotations

import argparse
import csv
import json
from datetime import datetime
from pathlib import Path


TARGET_URL = "https://sharing.regione.veneto.it/index.php/s/BzD8WZqGbZo9tGR"
CORRECT_POWER_MW = 16.863


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--data", required=True)
    parser.add_argument(
        "--audit",
        default="reports/manual_ag31_power_override_audit.csv",
    )
    args = parser.parse_args()

    data_path = Path(args.data)
    audit_path = Path(args.audit)

    data = json.loads(data_path.read_text(encoding="utf-8"))
    records = data.get("records", [])

    matches = [
        r for r in records
        if str(r.get("url") or "").strip() == TARGET_URL
    ]

    if len(matches) != 1:
        raise SystemExit(
            f"ERRORE: trovati {len(matches)} record AG 31; atteso esattamente 1"
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
        "title": record.get("title", ""),
        "proponent": record.get("proponent", ""),
        "old_power_mw": old_power,
        "new_power_mw": CORRECT_POWER_MW,
        "url": TARGET_URL,
        "reason": "manual_documented_power_override",
    }

    with audit_path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=list(row.keys()))
        writer.writeheader()
        writer.writerow(row)

    print("[ag31-power-override] record corretti: 1")
    print("[ag31-power-override] vecchia potenza:", old_power)
    print("[ag31-power-override] nuova potenza:", CORRECT_POWER_MW)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
