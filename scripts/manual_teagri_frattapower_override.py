from __future__ import annotations

import argparse
import csv
import json
import re
import time
from datetime import datetime
from pathlib import Path


TARGET_URL = "https://serviziambiente.regione.emilia-romagna.it/viavasweb/ricerca/dettaglio/6895"
CORRECT_POWER_MW = 22.377


def read_json_with_retry(path: Path, attempts: int = 20, delay: float = 0.75) -> dict:
    last_error = None

    for attempt in range(1, attempts + 1):
        try:
            return json.loads(path.read_text(encoding="utf-8"))
        except PermissionError as exc:
            last_error = exc
            print(
                f"[teagri-fratta-override] data.json bloccato in lettura "
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
                f"[teagri-fratta-override] data.json bloccato in scrittura "
                f"tentativo {attempt}/{attempts}; retry tra {delay}s..."
            )
            time.sleep(delay)

    raise SystemExit(
        f"ERRORE: impossibile scrivere {path} dopo {attempts} tentativi. "
        f"Ultimo errore: {last_error}"
    )


def fix_title(value: str) -> str:
    return re.sub(
        r"22\.377,6(?:00)?\s*MW\b",
        "22.377,600 kW",
        str(value or ""),
        flags=re.IGNORECASE,
    )


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--data", required=True)
    parser.add_argument(
        "--audit",
        default="reports/manual_teagri_frattapower_override_audit.csv",
    )
    args = parser.parse_args()

    data_path = Path(args.data)
    audit_path = Path(args.audit)

    if not data_path.exists():
        raise SystemExit(f"ERRORE: file non trovato: {data_path}")

    data = read_json_with_retry(data_path)
    records = data.get("records", [])

    matches = [
        record for record in records
        if str(record.get("url") or "").strip() == TARGET_URL
    ]

    if len(matches) != 1:
        raise SystemExit(
            f"ERRORE: trovati {len(matches)} record Teagri/Fratta; atteso 1"
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

    write_json_with_retry(data_path, data)

    audit_path.parent.mkdir(parents=True, exist_ok=True)

    row = {
        "timestamp": datetime.now().isoformat(timespec="seconds"),
        "proponent": record.get("proponent", ""),
        "old_power_mw": old_power,
        "new_power_mw": CORRECT_POWER_MW,
        "old_title": old_title,
        "new_title": record.get("title", ""),
        "url": TARGET_URL,
        "reason": "source_unit_error_kw_reported_as_mw",
    }

    with audit_path.open("w", newline="", encoding="utf-8-sig") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(row.keys()))
        writer.writeheader()
        writer.writerow(row)

    print("[teagri-fratta-override] record corretti: 1")
    print("[teagri-fratta-override] vecchia potenza:", old_power)
    print("[teagri-fratta-override] nuova potenza:", CORRECT_POWER_MW)
    print("[teagri-fratta-override] titolo:", record.get("title"))

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
