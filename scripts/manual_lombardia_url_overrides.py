from __future__ import annotations

import argparse
import csv
import json
import re
from datetime import datetime
from pathlib import Path


OLD_RE = re.compile(
    r"https://www\.silvia\.servizirl\.it/silviaweb/#/scheda-sintesi/(\d+)",
    flags=re.IGNORECASE,
)

KNOWN = {
    "21121": ("11", "3", "2"),  # Ranteghetta - VIA Ministero
    "21138": ("6", "2", "2"),   # Pieve Albignola - VER Provinciali
}


def _records_container(data):
    if isinstance(data, dict) and isinstance(data.get("records"), list):
        return data["records"]
    if isinstance(data, list):
        return data
    raise ValueError("Formato data.json non riconosciuto")


def _infer_ids(record: dict, project_id: str):
    if project_id in KNOWN:
        return KNOWN[project_id]

    blob = " ".join(
        str(record.get(k) or "")
        for k in ["title", "project_type", "procedure", "status", "source_label", "source_group"]
    ).lower()

    id_tipo_ente = None
    id_tipo_procedura = None

    if "ministero" in blob:
        id_tipo_ente = "11"
    elif "provincial" in blob:
        id_tipo_ente = "6"

    if "v.i.a" in blob or "valutazione impatto" in blob:
        id_tipo_procedura = "3"
    elif "v.e.r" in blob or "verifica" in blob or "assoggettabil" in blob:
        id_tipo_procedura = "2"
    elif "paur" in blob or "p.a.u.r" in blob:
        id_tipo_procedura = "15"

    if not id_tipo_ente or not id_tipo_procedura:
        return None

    return id_tipo_ente, id_tipo_procedura, "2"


def _full_url(project_id: str, ids: tuple[str, str, str]) -> str:
    id_tipo_ente, id_tipo_procedura, id_provenienza = ids
    return (
        "https://www.silvia.servizirl.it/silviaweb/#/scheda-sintesi"
        f"?idTipoEnte={id_tipo_ente}"
        f"&idTipoProcedura={id_tipo_procedura}"
        f"&idProgetto={project_id}"
        f"&idProvenienza={id_provenienza}"
    )


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--data", required=True)
    parser.add_argument("--audit", default="reports/manual_lombardia_url_overrides_audit.csv")
    args = parser.parse_args()

    data_path = Path(args.data)
    audit_path = Path(args.audit)

    data = json.loads(data_path.read_text(encoding="utf-8"))
    records = _records_container(data)

    changed = 0
    skipped = 0
    rows = []
    ts = datetime.now().isoformat(timespec="seconds")

    for r in records:
        source_blob = " ".join(str(r.get(k) or "") for k in ["source", "source_label", "source_group"]).lower()
        url_blob = " ".join(str(r.get(k) or "") for k in ["url", "source_url"])

        if "lombardia" not in source_blob and "silvia.servizirl.it" not in url_blob:
            continue

        for field in ["url", "source_url"]:
            old = str(r.get(field) or "").strip()
            if not old:
                continue

            m = OLD_RE.search(old)
            if not m:
                continue

            project_id = m.group(1)
            ids = _infer_ids(r, project_id)

            if not ids:
                skipped += 1
                continue

            new = _full_url(project_id, ids)

            if new != old:
                r[field] = new
                changed += 1
                rows.append({
                    "timestamp": ts,
                    "project_id": project_id,
                    "field": field,
                    "old_url": old,
                    "new_url": new,
                    "title": r.get("title", ""),
                })

    data_path.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")

    if rows:
        audit_path.parent.mkdir(parents=True, exist_ok=True)
        write_header = not audit_path.exists()
        with audit_path.open("a", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=["timestamp", "project_id", "field", "old_url", "new_url", "title"])
            if write_header:
                writer.writeheader()
            writer.writerows(rows)

    print(f"[manual-lombardia-url-overrides] URL corretti: {changed}")
    print(f"[manual-lombardia-url-overrides] URL saltati: {skipped}")
    print(f"[manual-lombardia-url-overrides] audit: {audit_path}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
