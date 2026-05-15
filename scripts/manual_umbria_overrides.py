from __future__ import annotations

import argparse
import csv
import json
import re
from datetime import datetime
from pathlib import Path


SOURCE_UMBRIA_RE = re.compile(r'("source"\s*:\s*)"umbria"', flags=re.IGNORECASE)


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

    raw = data_path.read_text(encoding="utf-8")

    # Correzione testuale mirata: tocca solo il campo source, non Terna/MASE/URL.
    raw_fixed, source_text_fixed = SOURCE_UMBRIA_RE.subn(r'\1"Umbria"', raw)

    data = json.loads(raw_fixed)
    records = _records_container(data)

    source_fixed = source_text_fixed
    region_fixed = 0
    terna_restored = 0

    for r in records:
        source = str(r.get("source", "")).strip()
        region = str(r.get("region", "")).strip()
        proponent = str(r.get("proponent", "")).strip().lower()

        # Regione normalizzata, senza cambiare la fonte tecnica.
        if region == "umbria":
            r["region"] = "Umbria"
            region_fixed += 1

        # Salvagente: se qualche run precedente avesse convertito Terna in Umbria, ripristina.
        if source == "Umbria" and proponent == "terna - econnextion":
            r["source"] = "Terna Econnextion"
            terna_restored += 1

    data_path.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")

    timestamp = datetime.now().isoformat(timespec="seconds")
    _write_audit(audit_path, [{
        "timestamp": timestamp,
        "action": "normalize_umbria_source_safe",
        "data_path": str(data_path),
        "details": (
            f"source_text_fixed={source_fixed}; "
            f"region_fixed={region_fixed}; "
            f"terna_restored={terna_restored}"
        ),
    }])

    print(f"[manual-umbria-overrides] source umbria corretti nel JSON: {source_fixed}")
    print(f"[manual-umbria-overrides] region Umbria corrette: {region_fixed}")
    print(f"[manual-umbria-overrides] Terna ripristinati: {terna_restored}")
    print(f"[manual-umbria-overrides] audit: {audit_path}")


if __name__ == "__main__":
    main()
