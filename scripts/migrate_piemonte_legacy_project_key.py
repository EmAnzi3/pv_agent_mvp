from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from app.collectors.piemonte import START_URL
from app.db import SessionLocal
from app.models import ProjectEvent, ProjectMaster

PIEMONTE_PROVINCE_CODES = {"AL", "AT", "BI", "CN", "NO", "TO", "VB", "VC"}
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
        else:
            deleted_events = (
                db.query(ProjectEvent)
                .filter(ProjectEvent.project_id == project.id)
                .delete(synchronize_session=False)
            )
            db.delete(project)
            print(
                "[piemonte-key-migration] rimosso record Piemonte legacy collassato:",
                legacy_key,
            )
            print("[piemonte-key-migration] eventi rimossi:", deleted_events)

        invalid_rows = (
            db.query(ProjectMaster)
            .filter(ProjectMaster.primary_source == "piemonte")
            .filter(ProjectMaster.province.is_not(None))
            .all()
        )

        repaired = 0
        for row in invalid_rows:
            province = str(row.province or "").strip().upper()
            if province in PIEMONTE_PROVINCE_CODES:
                continue

            municipalities = str(row.municipalities or "").strip().upper()

            # Caso già osservato: "(DC)" della descrizione della potenza era
            # stato scambiato per sigla provinciale. Isola Sant'Antonio è AL.
            if "ISOLA SANT'ANTONIO" in municipalities:
                row.province = "AL"
            else:
                # Per altri codici spurii meglio azzerare che pubblicare una
                # provincia falsa: i passaggi di enrichment successivi possono
                # ricostruirla da comune/titolo.
                row.province = None

            repaired += 1

        db.commit()
        print("[piemonte-key-migration] province spurie riparate:", repaired)
        return 0

    finally:
        db.close()


if __name__ == "__main__":
    raise SystemExit(main())
