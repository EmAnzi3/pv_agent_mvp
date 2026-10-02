from __future__ import annotations

import argparse
import csv
import json
from datetime import datetime
from pathlib import Path


POWER_OVERRIDES = [
    {
        "url": "https://sharing.regione.veneto.it/index.php/s/BzD8WZqGbZo9tGR",
        "power_mw": 16.863,
        "reason": "manual_documented_power_override",
    },
    {
        "url": "https://sharing.regione.veneto.it/index.php/s/gqPrcsFPdCmezt6",
        "power_mw": 21.13886,
        "reason": "official_application_states_21_138_86_kwp",
    },
]


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

    audit_rows = []
    timestamp = datetime.now().isoformat(timespec="seconds")

    for override in POWER_OVERRIDES:
        target_url = override["url"]
        correct_power_mw = override["power_mw"]

        matches = [
            record
            for record in records
            if str(record.get("url") or "").strip() == target_url
        ]

        if len(matches) != 1:
            raise SystemExit(
                f"ERRORE: trovati {len(matches)} record AG 31 per {target_url}; "
                "atteso esattamente 1"
            )

        record = matches[0]
        old_power = record.get("power_mw")
        record["power_mw"] = correct_power_mw

        audit_rows.append({
            "timestamp": timestamp,
            "title": record.get("title", ""),
            "proponent": record.get("proponent", ""),
            "old_power_mw": old_power,
            "new_power_mw": correct_power_mw,
            "url": target_url,
            "reason": override["reason"],
        })

    data_path.write_text(
        json.dumps(data, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )

    audit_path.parent.mkdir(parents=True, exist_ok=True)

    with audit_path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(
            handle,
            fieldnames=list(audit_rows[0].keys()),
        )
        writer.writeheader()
        writer.writerows(audit_rows)

    print("[ag31-power-override] record corretti:", len(audit_rows))
    for row in audit_rows:
        print(
            "[ag31-power-override]",
            row["url"],
            f"{row['old_power_mw']} -> {row['new_power_mw']} MW",
        )

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
