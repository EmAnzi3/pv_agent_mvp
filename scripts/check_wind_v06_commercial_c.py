#!/usr/bin/env python3
from __future__ import annotations

import json
from pathlib import Path
from urllib.parse import urlparse

ROOT = Path(__file__).resolve().parents[1]
DATA = ROOT / "docs" / "wind" / "data"
ASSETS = ROOT / "docs" / "wind" / "assets"

manifest = json.loads((DATA / "projects.json").read_text(encoding="utf-8"))
meta = json.loads((DATA / manifest["meta"]).read_text(encoding="utf-8"))
projects = []
for chunk in manifest["chunks"]:
    projects.extend(json.loads((DATA / chunk).read_text(encoding="utf-8")))
by_id = {p["id"]: p for p in projects}
execution_roles = set(meta.get("execution_roles") or [])

payload = json.loads((DATA / "commercial-enrichment-v06c.json").read_text(encoding="utf-8"))
assert payload["version"] == "0.6.0-commercial-enrichment-c"
assert payload["as_of"] == "2026-09-06"
assert set(payload["projects"]).issubset(by_id), set(payload["projects"]) - set(by_id)
assert {"nulvi-ploaghe", "greci-montaguto", "alia-sclafani", "serra-giannina"}.issubset(payload["projects"])

nulvi = payload["projects"]["nulvi-ploaghe"]
assert not (nulvi.get("relations") or [])
assert any(s.get("type") == "procurement-scale" and s.get("grade") == "A2" for s in nulvi.get("signals") or [])
assert any("€170m" in s.get("title", "") for s in nulvi.get("signals") or [])
assert any("27" in s.get("title", "") or "27" in s.get("note", "") for s in nulvi.get("signals") or [])
assert by_id["nulvi-ploaghe"]["stage"] == "E4"

# Developer-direct kick-off improves timing, not contractor attribution.
greci = payload["projects"]["greci-montaguto"]
assert not (greci.get("relations") or [])
window = next(s for s in greci.get("signals") or [] if s.get("type") == "construction-window")
assert window["grade"] == "A2"
assert "March 2026" in window["note"] and "summer 2027" in window["note"]

# SOCEP is direct company evidence, but belongs to the previous Alia-Sclafani plant.
alia = payload["projects"]["alia-sclafani"]
alia_relations = alia.get("relations") or []
assert len(alia_relations) == 1
socep = alia_relations[0]
assert socep["company"] == "SOCEP S.r.l."
assert socep["status"] == "historical"
assert socep["confidence"] == "A2"
assert socep["role"] not in execution_roles
assert "must not be treated as an award" in socep["scope"]
current = next(s for s in alia.get("signals") or [] if s.get("type") == "configuration-current")
assert current["grade"] == "A1"
assert "9-WTG / 55 MW" in current["title"]

# Serra Giannina hiring remains a B lead only.
serra = payload["projects"]["serra-giannina"]
assert not (serra.get("relations") or [])
hiring = next(s for s in serra.get("signals") or [] if s.get("type") == "contractor-mobilisation-detail")
assert hiring["grade"] == "B"
assert "No Civil BoP" in hiring["note"]

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
        assert not (
            relation.get("role") in execution_roles
            and relation.get("status") == "confirmed"
            and relation.get("confidence") in {"A1", "A2"}
        ), (project_id, relation)

loader = (ASSETS / "commercial-enrichment-v05.js").read_text(encoding="utf-8")
assert "commercial-enrichment-v06c.json" in loader

print("v0.6 commercial enrichment C OK: procurement/timing/history signals added with no new execution-scope closure")
