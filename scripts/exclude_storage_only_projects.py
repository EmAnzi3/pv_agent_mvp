from __future__ import annotations

import argparse
import csv
import json
import shutil
import sqlite3
from datetime import datetime
from pathlib import Path


TARGET_SLUG = "verifica-impianto-accumulo-elettrico-termomeccanico-a-lunga-durata-co2-battery-nel-comune-di-musei"



def _records_container(data):
    if isinstance(data, dict) and isinstance(data.get("records"), list):
        return data["records"]
    if isinstance(data, list):
        return data
    raise ValueError("Formato data.json non riconosciuto")


def is_candidate_blob(blob: str) -> bool:
    text = blob.lower()

    # Esclusione chirurgica: solo lo specifico progetto CO2 Battery Musei.
    # Non esclude altri progetti nel Comune di Musei.
    return TARGET_SLUG in text


def find_json_candidates(path: Path) -> list[dict]:
    if not path.exists():
        return []

    data = json.loads(path.read_text(encoding="utf-8"))
    records = _records_container(data)

    candidates = []

    for idx, r in enumerate(records):
        blob = json.dumps(r, ensure_ascii=False)

        if is_candidate_blob(blob):
            candidates.append({
                "scope": str(path),
                "record_index": idx,
                "source": r.get("source", ""),
                "region": r.get("region", ""),
                "province": r.get("province", ""),
                "municipalities": r.get("municipalities", ""),
                "proponent": r.get("proponent", ""),
                "power_mw": r.get("power_mw", ""),
                "title": r.get("title", ""),
                "url": r.get("url") or r.get("source_url") or "",
                "reason": "storage_only_musei_co2_battery",
            })

    return candidates


def clean_json(path: Path) -> int:
    if not path.exists():
        return 0

    data = json.loads(path.read_text(encoding="utf-8"))
    records = _records_container(data)

    before = len(records)

    records = [
        r for r in records
        if not is_candidate_blob(json.dumps(r, ensure_ascii=False))
    ]

    if isinstance(data, dict):
        data["records"] = records

    path.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")

    return before - len(records)


def find_db_project_ids(db_path: Path) -> tuple[set[int], list[dict]]:
    project_ids: set[int] = set()
    rows_out: list[dict] = []

    if not db_path.exists():
        return project_ids, rows_out

    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()

    # raw_items
    for row in cur.execute(
        """
        SELECT id, source_name, source_url, payload_json
        FROM raw_items
        WHERE lower(source_url) LIKE ?
           OR lower(payload_json) LIKE ?
        """,
        (f"%{TARGET_SLUG}%", f"%{TARGET_SLUG}%"),
    ):
        blob = " ".join(str(row[k] or "") for k in row.keys())
        if is_candidate_blob(blob):
            rows_out.append({
                "scope": "raw_items",
                "record_index": row["id"],
                "source": row["source_name"],
                "region": "",
                "province": "",
                "municipalities": "",
                "proponent": "",
                "power_mw": "",
                "title": "",
                "url": row["source_url"],
                "reason": "storage_only_musei_co2_battery",
            })

    # project_events
    for row in cur.execute(
        """
        SELECT id, project_id, source_name, source_url, details_json
        FROM project_events
        WHERE lower(source_url) LIKE ?
           OR lower(details_json) LIKE ?
        """,
        (f"%{TARGET_SLUG}%", f"%{TARGET_SLUG}%"),
    ):
        blob = " ".join(str(row[k] or "") for k in row.keys())
        if is_candidate_blob(blob):
            if row["project_id"] is not None:
                project_ids.add(int(row["project_id"]))

            rows_out.append({
                "scope": "project_events",
                "record_index": row["id"],
                "source": row["source_name"],
                "region": "",
                "province": "",
                "municipalities": "",
                "proponent": "",
                "power_mw": "",
                "title": "",
                "url": row["source_url"],
                "reason": "storage_only_musei_co2_battery",
            })

    # projects_master
    for row in cur.execute(
        """
        SELECT id, primary_source, primary_url, project_name, proponent, region, province, municipalities, power_mw
        FROM projects_master
        WHERE lower(primary_url) LIKE ?
           OR lower(project_name) LIKE ?
        """,
        (f"%{TARGET_SLUG}%", f"%{TARGET_SLUG}%"),
    ):
        blob = " ".join(str(row[k] or "") for k in row.keys())
        if is_candidate_blob(blob):
            project_ids.add(int(row["id"]))

            rows_out.append({
                "scope": "projects_master",
                "record_index": row["id"],
                "source": row["primary_source"],
                "region": row["region"],
                "province": row["province"],
                "municipalities": row["municipalities"],
                "proponent": row["proponent"],
                "power_mw": row["power_mw"],
                "title": row["project_name"],
                "url": row["primary_url"],
                "reason": "storage_only_musei_co2_battery",
            })

    conn.close()

    return project_ids, rows_out


def clean_db(db_path: Path, project_ids: set[int]) -> tuple[int, int, int]:
    if not db_path.exists():
        return 0, 0, 0

    conn = sqlite3.connect(db_path)
    cur = conn.cursor()

    raw_deleted = cur.execute(
        """
        DELETE FROM raw_items
        WHERE lower(source_url) LIKE ?
           OR lower(payload_json) LIKE ?
        """,
        (f"%{TARGET_SLUG}%", f"%{TARGET_SLUG}%"),
    ).rowcount

    events_deleted = 0
    master_deleted = 0

    if project_ids:
        placeholders = ",".join("?" for _ in project_ids)

        events_deleted = cur.execute(
            f"DELETE FROM project_events WHERE project_id IN ({placeholders})",
            tuple(project_ids),
        ).rowcount

        master_deleted = cur.execute(
            f"DELETE FROM projects_master WHERE id IN ({placeholders})",
            tuple(project_ids),
        ).rowcount

    conn.commit()
    conn.close()

    return raw_deleted, events_deleted, master_deleted


def write_audit(path: Path, rows: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)

    fields = [
        "timestamp",
        "mode",
        "scope",
        "record_index",
        "source",
        "region",
        "province",
        "municipalities",
        "proponent",
        "power_mw",
        "title",
        "url",
        "reason",
    ]

    ts = datetime.now().isoformat(timespec="seconds")

    normalized = []
    for row in rows:
        out = {"timestamp": ts, **row}
        normalized.append({field: out.get(field, "") for field in fields})

    with path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fields)
        writer.writeheader()
        writer.writerows(normalized)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--data", required=True)
    parser.add_argument("--db", default="data/pv_agent.sqlite")
    parser.add_argument("--audit", default="reports/storage_only_exclusions_latest.csv")
    parser.add_argument("--apply", action="store_true")
    args = parser.parse_args()

    data_path = Path(args.data)
    db_path = Path(args.db)
    audit_path = Path(args.audit)

    json_candidates = find_json_candidates(data_path)
    project_ids, db_candidates = find_db_project_ids(db_path)

    all_candidates = json_candidates + db_candidates

    write_audit(audit_path, all_candidates)

    print(f"[exclude-storage-only] candidati trovati: {len(all_candidates)}")
    print(f"[exclude-storage-only] audit: {audit_path}")

    if all_candidates:
        print("[exclude-storage-only] elenco candidati:")
        for c in all_candidates:
            print("-" * 80)
            print("scope:", c.get("scope"))
            print("source:", c.get("source"))
            print("region:", c.get("region"))
            print("province:", c.get("province"))
            print("municipalities:", c.get("municipalities"))
            print("proponent:", c.get("proponent"))
            print("power_mw:", c.get("power_mw"))
            print("title:", c.get("title"))
            print("url:", c.get("url"))
            print("reason:", c.get("reason"))

    if not args.apply:
        print("[exclude-storage-only] DRY RUN: nessuna cancellazione applicata.")
        return 0

    if not all_candidates:
        print("[exclude-storage-only] APPLY: nessun candidato da rimuovere.")
        return 0

    backup = Path("tmp") / f"pv_agent_before_storage_only_cleanup_{datetime.now().strftime('%Y%m%d_%H%M%S')}.sqlite"
    backup.parent.mkdir(parents=True, exist_ok=True)
    if db_path.exists():
        shutil.copy2(db_path, backup)
        print(f"[exclude-storage-only] backup DB: {backup}")

    removed_json = clean_json(data_path)
    raw_deleted, events_deleted, master_deleted = clean_db(db_path, project_ids)

    print(f"[exclude-storage-only] rimossi da JSON: {removed_json}")
    print(f"[exclude-storage-only] raw_items eliminati: {raw_deleted}")
    print(f"[exclude-storage-only] project_events eliminati: {events_deleted}")
    print(f"[exclude-storage-only] projects_master eliminati: {master_deleted}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
