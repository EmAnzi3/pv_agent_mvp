from __future__ import annotations

import argparse
import csv
import json
import re
from datetime import datetime
from pathlib import Path


OLD_TOSCANA_URL_RE = re.compile(
    r"https://servizi\.patti\.regione\.toscana\.it/star-info/avvisiPubblici\?id=(\d+)",
    flags=re.IGNORECASE,
)


def _records_container(data):
    if isinstance(data, dict) and isinstance(data.get("records"), list):
        return data["records"]
    if isinstance(data, list):
        return data
    raise ValueError("Formato data.json non riconosciuto: atteso dict con records oppure lista")


def _normalize_toscana_url(value: str | None) -> tuple[str | None, bool]:
    if not isinstance(value, str) or not value.strip():
        return value, False

    def repl(match: re.Match) -> str:
        project_id = match.group(1)
        return f"https://servizi.patti.regione.toscana.it/star-info/avvisiPubblici/{project_id}"

    new_value = OLD_TOSCANA_URL_RE.sub(repl, value)
    return new_value, new_value != value


def _write_audit(path: Path, rows: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)

    fieldnames = [
        "timestamp",
        "action",
        "data_path",
        "field",
        "old_value",
        "new_value",
        "title",
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
    parser.add_argument("--audit", default="reports/manual_toscana_url_overrides_audit.csv")
    args = parser.parse_args()

    data_path = Path(args.data)
    audit_path = Path(args.audit)

    if not data_path.exists():
        raise SystemExit(f"[manual-toscana-url-overrides] ERRORE: file non trovato: {data_path}")

    data = json.loads(data_path.read_text(encoding="utf-8"))
    records = _records_container(data)

    rows = []
    changed = 0
    timestamp = datetime.now().isoformat(timespec="seconds")

    for r in records:
        source = str(r.get("source", "")).strip().lower()
        region = str(r.get("region", "")).strip().lower()

        # Corregge solo record Toscana o URL Toscana.
        candidate_blob = " ".join([
            str(r.get("url", "")),
            str(r.get("source_url", "")),
        ])

        if source != "toscana" and region != "toscana" and "servizi.patti.regione.toscana.it" not in candidate_blob:
            continue

        for field in ["url", "source_url"]:
            old_value = r.get(field)
            new_value, did_change = _normalize_toscana_url(old_value)

            if did_change:
                r[field] = new_value
                changed += 1
                rows.append({
                    "timestamp": timestamp,
                    "action": "normalize_toscana_star_url",
                    "data_path": str(data_path),
                    "field": field,
                    "old_value": old_value,
                    "new_value": new_value,
                    "title": r.get("title", ""),
                })

    data_path.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")

    if rows:
        _write_audit(audit_path, rows)

    print(f"[manual-toscana-url-overrides] URL Toscana corretti: {changed}")
    print(f"[manual-toscana-url-overrides] audit: {audit_path}")


if __name__ == "__main__":
    main()
