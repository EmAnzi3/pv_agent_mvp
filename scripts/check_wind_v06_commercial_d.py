#!/usr/bin/env python3
from __future__ import annotations

import json
from pathlib import Path
from urllib.parse import urlparse

ROOT = Path(__file__).resolve().parents[1]
DATA = ROOT / "docs" / "wind" / "data"
ASSETS = ROOT / "docs" / "wind" / "assets"

payload = json.loads((DATA / "commercial-enrichment-v06d.json").read_text(encoding="utf-8"))
assert payload["version"] == "0.6.0-commercial-enrichment-d"
assert payload["as_of"] == "2026-09-06"
assert set(payload["projects"]) == {"greci-montaguto", "serra-giannina"}

greci = payload["projects"]["greci-montaguto"]
relations = greci.get("relations") or []
assert len(relations) == 1
engineering = relations[0]
assert engineering["company"] == "PROGETTO ENERGIA S.r.l."
assert engineering["role"] == "Project design / executive design"
assert engineering["status"] == "confirmed"
assert engineering["confidence"] == "A1"
assert "does not close Civil BoP" in engineering["scope"]
assert engineering["source_id"] == "greci-progetto-energia-design-a1-2024"

serra = payload["projects"]["serra-giannina"]
assert not (serra.get("relations") or [])
signal = next(s for s in serra.get("signals") or [] if s.get("type") == "contractor-involvement-employee-direct")
assert signal["grade"] == "B"
assert "does not close Civil BoP" in signal["note"]

for project_id, project_payload in payload["projects"].items():
    sources = project_payload.get("sources") or []
    assert sources, f"{project_id}: no sources"
    source_ids = {s.get("id") for s in sources}
    for source in sources:
        assert source.get("grade") in {"A1", "A2", "B", "C"}, (project_id, source)
        assert source.get("id"), (project_id, source)
        parsed = urlparse(source.get("url") or "")
        assert parsed.scheme in {"http", "https"} and parsed.netloc, (project_id, source)
    for relation in project_payload.get("relations") or []:
        assert relation.get("source_id") in source_ids, (project_id, relation)
        assert relation.get("role") not in {
            "Civil BoP", "Electrical BoP", "Electrical BoP / electromechanical",
            "SSE / grid contractor", "Erection contractor", "Dismantling contractor",
            "Logistics / heavy transport", "Foundation contractor",
            "Foundation contractor / concrete works", "Civil BoP / site preparation",
            "Civil BoP / electrical infrastructure (RTI)", "Foundations / substructure / mooring",
            "WTG installation offshore", "Inter-array cables", "Offshore substation / electrical platform",
            "Export cable + landfall", "Onshore SSE / grid", "Marine logistics / port / heavy lift",
            "Civil works onshore connection"
        }, (project_id, relation)

loader = (ASSETS / "commercial-enrichment-v05.js").read_text(encoding="utf-8")
assert "commercial-enrichment-v06d.json" in loader

print("v0.6 commercial enrichment D OK: project-specific engineering and B-level contractor lead added with no execution-scope closure")
