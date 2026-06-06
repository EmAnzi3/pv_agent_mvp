from __future__ import annotations

import argparse
import csv
import json
import re
from datetime import datetime
from pathlib import Path


CORRECT_POWER_MW = 25.75104


def records_container(data):
    if isinstance(data, dict) and isinstance(data.get("records"), list):
        return data["records"]
    if isinstance(data, list):
        return data
    raise ValueError("Formato data.json non riconosciuto")


def is_target(record: dict) -> bool:
    source = str(record.get("source") or "").strip().lower().replace("-", "_")
    title = str(record.get("title") or "").lower()

    return (
        source == "emilia_romagna"
        and "vigarano mainarda" in title
        and "impianto agrivoltaico avanzato" in title
    )


def fix_title(value: str) -> str:
    return re.sub(
        r"25\.751,04\s*MW\b",
        "25.751,04 kWp",
        str(value or ""),
        flags=re.IGNORECASE,
    )


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--data", required=True)
    parser.add_argument(
        "--audit",
        default="reports/manual_vigarano_mainarda_power_override_audit.csv",
    )
    args = parser.parse_args()

    data_path = Path(args.data)
    audit_path = Path(args.audit)

    if not data_path.exists():
        raise SystemExit(f"ERRORE: file non trovato: {data_path}")

    data = json.loads(data_path.read_text(encoding="utf-8"))
    records = records_container(data)

    matches = [record for record in records if is_target(record)]

    # Sicurezza: non modifica nulla se il progetto non è identificato univocamente.
    if len(matches) != 1:
        raise SystemExit(
            f"ERRORE: trovati {len(matches)} record Vigarano Mainarda; atteso esattamente 1"
        )

    record = matches[0]

    old_power = record.get("power_mw")
    old_title = str(record.get("title") or "")

    record["power_mw"] = CORRECT_POWER_MW
    record["title"] = fix_title(old_title)

    # Mantiene coerenti anche gli eventuali titoli conservati dalla deduplica.
    dedupe_titles = record.get("_dedupe_titles")
    if isinstance(dedupe_titles, list):
        record["_dedupe_titles"] = [
            fix_title(title) for title in dedupe_titles
        ]

    data_path.write_text(
        json.dumps(data, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )

    audit_path.parent.mkdir(parents=True, exist_ok=True)

    row = {
        "timestamp": datetime.now().isoformat(timespec="seconds"),
        "source": record.get("source", ""),
        "proponent": record.get("proponent", ""),
        "old_power_mw": old_power,
        "new_power_mw": CORRECT_POWER_MW,
        "old_title": old_title,
        "new_title": record.get("title", ""),
        "url": record.get("url", ""),
        "reason": "source_unit_error_kwp_reported_as_mw",
    }

    with audit_path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(row.keys()))
        writer.writeheader()
        writer.writerow(row)

    print("[vigarano-mainarda-override] record corretti: 1")
    print("[vigarano-mainarda-override] vecchia potenza:", old_power)
    print("[vigarano-mainarda-override] nuova potenza:", CORRECT_POWER_MW)
    print("[vigarano-mainarda-override] titolo:", record.get("title"))
    print("[vigarano-mainarda-override] audit:", audit_path)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
