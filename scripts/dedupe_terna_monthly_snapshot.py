from __future__ import annotations

import argparse
import csv
import json
import re
from collections import defaultdict
from datetime import datetime
from pathlib import Path


TERNA_RE = re.compile(
    r"terna_econnextion_(\d{4})_(\d{2})_(.+)$",
    flags=re.IGNORECASE,
)


def is_terna_record(r: dict) -> bool:
    return (
        str(r.get("source") or "").lower() == "terna_econnextion"
        or str(r.get("source_label") or "").lower() == "terna econnextion"
        or "terna_econnextion" in str(r.get("url") or "").lower()
    )


def parse_terna_key(r: dict):
    url = str(r.get("url") or "")
    anchor = url.split("#", 1)[-1]
    m = TERNA_RE.search(anchor)

    if not m:
        return None

    year = int(m.group(1))
    month = int(m.group(2))
    stable_key = m.group(3).lower()

    return stable_key, year, month, anchor


def records_container(data):
    if isinstance(data, dict) and isinstance(data.get("records"), list):
        return data["records"]
    if isinstance(data, list):
        return data
    raise ValueError("Formato data.json non riconosciuto")


def update_summary(data: dict):
    records = data.get("records", [])
    punctual = [r for r in records if r.get("is_punctual") is True]
    terna = [r for r in records if is_terna_record(r)]

    summary = data.setdefault("summary", {})
    summary["total_records"] = len(records)
    summary["punctual_records"] = len(punctual)
    summary["terna_records"] = len(terna)
    summary["total_mw_punctual"] = round(sum(float(r.get("power_mw") or 0) for r in punctual), 3)
    summary["total_mw_terna"] = round(sum(float(r.get("power_mw") or 0) for r in terna), 3)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--data", required=True)
    parser.add_argument("--audit", default="reports/terna_monthly_dedupe_audit.csv")
    parser.add_argument("--apply", action="store_true")
    args = parser.parse_args()

    data_path = Path(args.data)
    audit_path = Path(args.audit)

    data = json.loads(data_path.read_text(encoding="utf-8"))
    records = records_container(data)

    groups = defaultdict(list)
    untouched = []

    for idx, r in enumerate(records):
        parsed = parse_terna_key(r) if is_terna_record(r) else None

        if not parsed:
            untouched.append((idx, r))
            continue

        stable_key, year, month, anchor = parsed
        groups[stable_key].append((year, month, idx, anchor, r))

    keep_indexes = set(idx for idx, _ in untouched)
    audit_rows = []
    ts = datetime.now().isoformat(timespec="seconds")

    for stable_key, items in groups.items():
        items_sorted = sorted(items, key=lambda x: (x[0], x[1], x[2]), reverse=True)
        keep = items_sorted[0]
        keep_indexes.add(keep[2])

        for year, month, idx, anchor, r in items_sorted[1:]:
            audit_rows.append({
                "timestamp": ts,
                "action": "removed_older_terna_month",
                "stable_key": stable_key,
                "removed_month": f"{year:04d}-{month:02d}",
                "kept_month": f"{keep[0]:04d}-{keep[1]:02d}",
                "region": r.get("region", ""),
                "power_mw": r.get("power_mw", ""),
                "title": r.get("title", ""),
                "url": r.get("url", ""),
                "kept_url": keep[4].get("url", ""),
            })

    before = len(records)
    new_records = [r for idx, r in enumerate(records) if idx in keep_indexes]
    after = len(new_records)

    if args.apply:
        if isinstance(data, dict):
            data["records"] = new_records
            update_summary(data)
            data_path.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")
        else:
            data_path.write_text(json.dumps(new_records, ensure_ascii=False, indent=2), encoding="utf-8")

    audit_path.parent.mkdir(parents=True, exist_ok=True)

    fields = [
        "timestamp", "action", "stable_key", "removed_month", "kept_month",
        "region", "power_mw", "title", "url", "kept_url",
    ]

    with audit_path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fields)
        writer.writeheader()
        writer.writerows(audit_rows)

    print(f"[terna-monthly-dedupe] records prima: {before}")
    print(f"[terna-monthly-dedupe] records dopo: {after}")
    print(f"[terna-monthly-dedupe] duplicati Terna rimossi: {before - after}")
    print(f"[terna-monthly-dedupe] apply: {args.apply}")
    print(f"[terna-monthly-dedupe] audit: {audit_path}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
