from __future__ import annotations

import argparse
import csv
import json
from datetime import datetime
from pathlib import Path


ENERGY4U_URL_NEEDLE = "p-a-u-r-realizzazione-impianto-fv-a-terra-potenza-19-988-mwp-societa-energy4u-s-r-l-comune-di-nuramini-1"

ENERGY4U_PATCH = {
    "source": "Sardegna",
    "region": "Sardegna",
    "province": "SU",
    "municipalities": ["Nuraminis"],
    "power_mw": 19.988,
    "proponent": "Società ENERGY4U S.r.l.",
}


def _records_container(data):
    if isinstance(data, dict) and isinstance(data.get("records"), list):
        return data["records"]
    if isinstance(data, list):
        return data
    raise ValueError("Formato data.json non riconosciuto: atteso dict con records oppure lista")


def _write_audit(path: Path, rows: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)

    fieldnames = [
        "timestamp",
        "action",
        "data_path",
        "details",
    ]

    write_header = not path.exists()

    with path.open("a", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        if write_header:
            writer.writeheader()
        writer.writerows(rows)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--data", default="reports/site/data.json")
    parser.add_argument("--audit", default="reports/manual_sardegna_overrides_audit.csv")
    args = parser.parse_args()

    data_path = Path(args.data)
    audit_path = Path(args.audit)

    if not data_path.exists():
        raise SystemExit(f"[manual-sardegna-overrides] ERRORE: file non trovato: {data_path}")

    data = json.loads(data_path.read_text(encoding="utf-8"))
    records = _records_container(data)

    changed = 0
    matched = 0

    for r in records:
        url = str(r.get("url") or r.get("source_url") or "").lower()
        title = str(r.get("title") or "").lower()

        if ENERGY4U_URL_NEEDLE not in url and "energy4u" not in title:
            continue

        matched += 1

        for key, value in ENERGY4U_PATCH.items():
            if r.get(key) != value:
                r[key] = value
                changed += 1

    data_path.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")

    timestamp = datetime.now().isoformat(timespec="seconds")
    _write_audit(audit_path, [{
        "timestamp": timestamp,
        "action": "energy4u_nuraminis_override",
        "data_path": str(data_path),
        "details": f"matched={matched}; fields_changed={changed}",
    }])

    print(f"[manual-sardegna-overrides] record ENERGY4U/Nuraminis trovati: {matched}")
    print(f"[manual-sardegna-overrides] campi corretti: {changed}")
    print(f"[manual-sardegna-overrides] audit: {audit_path}")


if __name__ == "__main__":
    main()
