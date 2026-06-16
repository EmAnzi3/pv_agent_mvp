from __future__ import annotations

import argparse
import json
from pathlib import Path


SOURCE_MAP = {
    "calabria": "Calabria",
    "basilicata": "Basilicata",
    "umbria": "Umbria",
    "sardegna": "Sardegna",
}


def fix_value(value):
    if isinstance(value, str):
        return SOURCE_MAP.get(value.strip().lower(), value)
    return value


def normalize_record(r: dict) -> int:
    changed = 0

    src_raw = str(r.get("source", "")).strip()
    src_key = src_raw.lower()

    url = str(
        r.get("url")
        or r.get("primary_url")
        or r.get("source_url")
        or ""
    ).lower()

    is_atos = (
        src_key == "toscana_atos"
        or "atos.arrr.it/scheda_impianto_fer" in url
    )

    is_toscana = (
        is_atos
        or src_key == "toscana"
    )

    if is_toscana:
        expected = {
            "source": (
                "toscana_atos"
                if is_atos
                else "toscana"
            ),
            "source_group": "toscana",
            "source_label": "Toscana",
        }

        for field, value in expected.items():
            if r.get(field) != value:
                r[field] = value
                changed += 1

        return changed

    src_norm = SOURCE_MAP.get(src_key)

    if src_norm:
        for field in [
            "source",
            "source_label",
            "source_group",
        ]:
            if r.get(field) != src_norm:
                r[field] = src_norm
                changed += 1

        if isinstance(r.get("_merged_sources"), list):
            new_sources = []

            for item in r["_merged_sources"]:
                fixed = fix_value(item)
                new_sources.append(fixed)

                if fixed != item:
                    changed += 1

            r["_merged_sources"] = new_sources

    return changed

def normalize_summary(data: dict) -> int:
    changed = 0
    summary = data.get("summary") or {}

    for item in summary.get("source_counts", []) or []:
        if not isinstance(item, dict):
            continue

        src = str(item.get("source", "")).strip()
        fixed = SOURCE_MAP.get(src.lower())

        if fixed:
            if item.get("source") != fixed:
                item["source"] = fixed
                changed += 1
            if item.get("label") != fixed:
                item["label"] = fixed
                changed += 1

    for key in ["top_projects"]:
        for r in summary.get(key, []) or []:
            if isinstance(r, dict):
                changed += normalize_record(r)

    return changed


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--data", default="reports/site/data.json")
    args = parser.parse_args()

    path = Path(args.data)

    if not path.exists():
        raise SystemExit(f"[normalize-source-display] file non trovato: {path}")

    data = json.loads(path.read_text(encoding="utf-8"))

    records = data.get("records", []) if isinstance(data, dict) else data

    changed = 0

    for r in records:
        if isinstance(r, dict):
            changed += normalize_record(r)

    if isinstance(data, dict):
        changed += normalize_summary(data)

    path.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")

    print(f"[normalize-source-display] file: {path}")
    print(f"[normalize-source-display] campi corretti: {changed}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
