from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from app.collectors.piemonte import START_URL
from app.db import SessionLocal
from app.models import ProjectEvent, ProjectMaster
from app.pipeline import build_project_key


def main() -> int:
    legacy_key = build_project_key(
        project_name=None,
        proponent=None,
        region=None,
        municipalities=None,
        power_mw=None,
        source_url=START_URL,
        external_id=None,
    )

    if not legacy_key:
        raise SystemExit("[piemonte-key-migration] impossibile calcolare legacy key")

    db = SessionLocal()
    try:
        project = (
            db.query(ProjectMaster)
            .filter(ProjectMaster.project_key == legacy_key)
            .filter(ProjectMaster.primary_source == "piemonte")
            .first()
        )

        if project is None:
            print("[piemonte-key-migration] nessun record legacy da rimuovere")
            return 0

        deleted_events = (
            db.query(ProjectEvent)
            .filter(ProjectEvent.project_id == project.id)
            .delete(synchronize_session=False)
        )
        db.delete(project)
        db.commit()

        print(
            "[piemonte-key-migration] rimosso record Piemonte legacy collassato:",
            legacy_key,
        )
        print("[piemonte-key-migration] eventi rimossi:", deleted_events)
        return 0
    finally:
        db.close()


if __name__ == "__main__":
    raise SystemExit(main())
