from __future__ import annotations

import argparse
import csv
import json
from datetime import datetime
from pathlib import Path


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
    parser.add_argument("--audit", default="reports/manual_umbria_overrides_audit.csv")
    args = parser.parse_args()

    data_path = Path(args.data)
    audit_path = Path(args.audit)

    if not data_path.exists():
        raise SystemExit(f"[manual-umbria-overrides] ERRORE: file non trovato: {data_path}")

    data = json.loads(data_path.read_text(encoding="utf-8"))
    records = _records_container(data)

    source_fixed = 0
    region_fixed = 0
    terna_restored = 0

    for r in records:
        source = str(r.get("source", "")).strip()
        source_l = source.lower()
        region_l = str(r.get("region", "")).strip().lower()
        proponent_l = str(r.get("proponent", "")).strip().lower()

        # Corregge SOLO la fonte tecnica Umbria minuscola.
        # Non tocca Terna, MASE o altre fonti con region = Umbria.
        if source_l == "umbria" and source != "Umbria":
            r["source"] = "Umbria"
            source_fixed += 1

        # La regione invece può essere normalizzata senza rischio.
        if region_l == "umbria" and r.get("region") != "Umbria":
            r["region"] = "Umbria"
            region_fixed += 1

        # Ripristina eventuali record Terna corrotti dal precedente override.
        if source == "Umbria" and proponent_l == "terna - econnextion":
            r["source"] = "Terna Econnextion"
            terna_restored += 1

    data_path.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")

    timestamp = datetime.now().isoformat(timespec="seconds")
    _write_audit(audit_path, [{
        "timestamp": timestamp,
        "action": "normalize_umbria_source_safe",
        "data_path": str(data_path),
        "details": (
            f"source_fixed={source_fixed}; "
            f"region_fixed={region_fixed}; "
            f"terna_restored={terna_restored}"
        ),
    }])

    print(f"[manual-umbria-overrides] source Umbria corretti: {source_fixed}")
    print(f"[manual-umbria-overrides] region Umbria corrette: {region_fixed}")
    print(f"[manual-umbria-overrides] Terna ripristinati: {terna_restored}")
    print(f"[manual-umbria-overrides] audit: {audit_path}")


if __name__ == "__main__":
    main()
