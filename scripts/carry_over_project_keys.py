"""Keep each project's project_key stable between dashboard runs.

project_key is derived from the record chosen as base of a dedupe group (its URL) or, without a
specific URL, from title/proponent/MW. When the base changes (a MASE record joins a regional one,
the proponent list is reordered, a Terna aggregate changes MW) the key changes too, and then:
the change report shows the same project as "removed" + "new", the project card ("Scheda") is no
longer found, and the document queue treats a known project as new.

This step gives each new record the key the same project had in the previous published radar,
matching by the official URLs (url + _dedupe_urls). Rules, conservative:
  1. a record whose base URL was the base URL of exactly one previous record takes that key;
  2. otherwise a record sharing URLs with previous records takes the key of the previous record
     with the largest overlap (each previous key is given to one record only);
  3. a record that already holds a previous key by itself is never changed;
  4. no match, no change (new projects keep their new key).
Every reassignment goes to the audit CSV.
"""
from __future__ import annotations

import argparse
import csv
import json
from collections import defaultdict
from pathlib import Path
from typing import Any


def rows_of(data: Any) -> list[dict[str, Any]]:
    if isinstance(data, list):
        return [r for r in data if isinstance(r, dict)]
    if isinstance(data, dict) and isinstance(data.get("records"), list):
        return [r for r in data["records"] if isinstance(r, dict)]
    return []


def urls_of(row: dict[str, Any]) -> set[str]:
    out = {str(row.get("url") or "").strip()}
    for u in row.get("_dedupe_urls") or []:
        out.add(str(u or "").strip())
    return {u for u in out if u.startswith("http")}


def carry_over(old_rows: list[dict[str, Any]], new_rows: list[dict[str, Any]]) -> list[dict[str, str]]:
    old_by_key = {str(r.get("project_key")): r for r in old_rows if r.get("project_key")}
    base_owner: dict[str, set[str]] = defaultdict(set)
    any_owner: dict[str, set[str]] = defaultdict(set)
    for key, r in old_by_key.items():
        base = str(r.get("url") or "").strip()
        if base:
            base_owner[base].add(key)
        for u in urls_of(r):
            any_owner[u].add(key)

    new_keys = {str(r.get("project_key")) for r in new_rows if r.get("project_key")}
    used: set[str] = set(k for k in new_keys if k in old_by_key)  # rule 3: already stable
    audit: list[dict[str, str]] = []

    def assign(row: dict[str, Any], key: str, rule: str) -> None:
        old_key = str(row.get("project_key") or "")
        row["_project_key_previous_run"] = key
        row["project_key"] = key
        used.add(key)
        audit.append({"rule": rule, "old_key_this_run": old_key, "kept_key": key,
                      "url": str(row.get("url") or ""), "title": str(row.get("title") or "")[:160]})

    pending = [r for r in new_rows if str(r.get("project_key") or "") not in old_by_key]
    rest = []
    for row in pending:  # rule 1
        owners = base_owner.get(str(row.get("url") or "").strip(), set())
        if len(owners) == 1 and next(iter(owners)) not in used:
            assign(row, next(iter(owners)), "same_base_url")
        else:
            rest.append(row)
    for row in rest:  # rule 2
        mine = urls_of(row)
        overlap: dict[str, int] = defaultdict(int)
        for u in mine:
            for k in any_owner.get(u, ()):
                if k not in used:
                    overlap[k] += 1
        if overlap:
            key = sorted(overlap, key=lambda k: (-overlap[k], k))[0]
            assign(row, key, "shared_official_url")
    return audit


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--old", required=True, help="previous published data.json")
    ap.add_argument("--data", required=True, help="data.json of this run (updated in place)")
    ap.add_argument("--audit", required=True)
    args = ap.parse_args()
    old_path, data_path = Path(args.old), Path(args.data)
    if not old_path.exists():
        print("[carry-over-keys] nessun radar precedente: nulla da fare")
        return 0
    old_rows = rows_of(json.loads(old_path.read_text(encoding="utf-8")))
    data = json.loads(data_path.read_text(encoding="utf-8"))
    new_rows = rows_of(data)
    audit = carry_over(old_rows, new_rows)
    keys = [r.get("project_key") for r in new_rows]
    if len(keys) != len(set(keys)):
        print("[carry-over-keys] ATTENZIONE: chiavi duplicate dopo il riallineamento, modifica annullata")
        return 1
    data_path.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")
    Path(args.audit).parent.mkdir(parents=True, exist_ok=True)
    with open(args.audit, "w", newline="", encoding="utf-8-sig") as f:
        w = csv.DictWriter(f, fieldnames=["rule", "old_key_this_run", "kept_key", "url", "title"], delimiter=";")
        w.writeheader()
        w.writerows(audit)
    print(f"[carry-over-keys] chiavi mantenute dal radar precedente: {len(audit)} - audit: {args.audit}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
