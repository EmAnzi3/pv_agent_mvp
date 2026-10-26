#!/usr/bin/env python3
import json
from pathlib import Path
from urllib.parse import urlparse

ROOT = Path(__file__).resolve().parents[1]
DATA = ROOT / "docs" / "wind" / "data"

manifest = json.loads((DATA / "projects.json").read_text(encoding="utf-8"))
meta = json.loads((DATA / manifest["meta"]).read_text(encoding="utf-8"))
projects = []
for chunk in manifest["chunks"]:
    projects.extend(json.loads((DATA / chunk).read_text(encoding="utf-8")))
by_id = {p["id"]: p for p in projects}

payload = json.loads((DATA / "commercial-enrichment-v06.json").read_text(encoding="utf-8"))
extra = json.loads((DATA / "commercial-enrichment-v06b.json").read_text(encoding="utf-8"))
assert payload["version"] == "0.6.0-commercial-enrichment"
assert extra["version"] == "0.6.0-commercial-enrichment-b"
assert set(payload["projects"]).issubset(by_id), set(payload["projects"]) - set(by_id)
assert set(extra["projects"]).issubset(by_id), set(extra["projects"]) - set(by_id)
required_projects = {"andretta-bisaccia", "carlentini", "tricarico", "greci-montaguto", "nulvi-ploaghe"}
assert required_projects.issubset(payload["projects"])
assert {"andretta-bisaccia", "serra-giannina"}.issubset(extra["projects"])

execution_roles = set(meta.get("execution_roles") or [])

# Andretta: current supervision/staffing signal is project-specific but non-execution.
andretta = payload["projects"]["andretta-bisaccia"]
andretta_relations = andretta.get("relations") or []
assert len(andretta_relations) == 1, andretta_relations
progeco = andretta_relations[0]
assert progeco["company"] == "Progeco Group / Progeco SE"
assert progeco["confidence"] == "A2"
assert progeco["status"] == "signal", "support recruitment must not be represented as confirmed execution"
assert progeco["role"] not in execution_roles, "project-support relation must not close execution scope"
assert "supervision" in progeco["role"].lower()
assert any(s.get("type") == "project-support-recruitment" for s in andretta.get("signals") or [])
assert any("24-month" in s.get("title", "") for s in andretta.get("signals") or [])

# Latest official MASE compliance metadata resolves the stale 118.80 MW title:
# the current Edison filing explicitly states reduction to 88.5 MW and turbine
# replacement. It is configuration/timing evidence, not an execution award.
andretta_extra = extra["projects"]["andretta-bisaccia"]
assert not (andretta_extra.get("relations") or [])
andretta_extra_signals = andretta_extra.get("signals") or []
current_config = next(s for s in andretta_extra_signals if s.get("type") == "configuration-current")
works_readiness = next(s for s in andretta_extra_signals if s.get("type") == "works-readiness")
assert current_config["grade"] == "A1"
assert "88.5 MW" in current_config["title"] or "88.5 MW" in current_config["note"]
assert "118.80 MW" in current_config["note"], "configuration-history guard must retain stale-title context"
assert works_readiness["grade"] == "A1"
assert "does not by itself prove" in works_readiness["note"], "works-readiness must not become construction-start proof"

# Carlentini: direct project-participant evidence explicitly names Mammana
# as executor of current WTG foundation concrete works. It must not be widened
# to the whole Civil BoP.
carlentini = payload["projects"]["carlentini"]
car_relations = carlentini.get("relations") or []
foundation = next(r for r in car_relations if r.get("company") == "Mammana Michelangelo S.p.A.")
hydro = next(r for r in car_relations if r.get("company") == "Hydro Engineering")
assert foundation["role"] == "Foundation contractor"
assert foundation["role"] in execution_roles
assert foundation["status"] == "confirmed"
assert foundation["confidence"] == "A2"
assert "full Civil BoP" in foundation["scope"], "scope note must preserve the no-overreach guard"
assert hydro["confidence"] == "A2"
assert hydro["status"] == "confirmed"
assert hydro["role"] not in execution_roles, "Direzione Lavori / engineering must remain non-execution"
assert any(s.get("type") == "foundation-execution" for s in carlentini.get("signals") or [])
assert any(s.get("type") == "construction-progress" for s in carlentini.get("signals") or [])

# Tricarico: financial close + lender-side construction monitoring are useful
# execution-timing intelligence but do not identify an executing contractor.
tricarico = payload["projects"]["tricarico"]
tri_relations = tricarico.get("relations") or []
assert len(tri_relations) == 1, tri_relations
vector = tri_relations[0]
assert vector["company"] == "Vector Renewables"
assert vector["confidence"] == "A2"
assert vector["status"] == "confirmed"
assert vector["role"] not in execution_roles, "Lender's Technical Advisor must stay non-execution"
assert "no EPC" in vector["scope"], "Tricarico advisory relation must preserve execution-scope guard"
tri_signals = tricarico.get("signals") or []
assert any(s.get("type") == "financial-close" for s in tri_signals)
assert any(s.get("type") == "construction-monitoring" for s in tri_signals)
assert any("46.5m" in s.get("title", "") or "46,5" in s.get("title", "") for s in tri_signals)

# Greci-Montaguto: use the official regional act to resolve the WTG split.
# This configuration evidence must not create a new execution contractor.
greci = payload["projects"]["greci-montaguto"]
assert not (greci.get("relations") or []), "configuration/service intelligence must not create a contractor relation"
greci_signals = greci.get("signals") or []
config = next(s for s in greci_signals if s.get("type") == "wtg-configuration")
service = next(s for s in greci_signals if s.get("type") == "long-term-service")
assert config["grade"] == "A1"
assert "six Vestas V136" in config["note"]
assert "four Vestas V117" in config["note"]
assert service["grade"] == "A2"
assert "AOM 5000" in service["title"]

# Nulvi-Ploaghe: direct engineering-company evidence identifies Hydro in the
# development chain, but E4 procurement/execution scopes remain fully open.
nulvi = payload["projects"]["nulvi-ploaghe"]
nulvi_relations = nulvi.get("relations") or []
assert len(nulvi_relations) == 1, nulvi_relations
nulvi_hydro = nulvi_relations[0]
assert nulvi_hydro["company"] == "Hydro Engineering"
assert nulvi_hydro["confidence"] == "A2"
assert nulvi_hydro["status"] == "confirmed"
assert nulvi_hydro["role"] not in execution_roles, "project development/engineering must stay non-execution"
assert "does not attribute Civil BoP" in nulvi_hydro["scope"], "Nulvi development evidence must retain execution guard"
assert any(s.get("type") == "development-chain" for s in nulvi.get("signals") or [])

# Serra Giannina: direct EGM material confirms construction-phase technical
# presence. D'Agostino is a project-specific B mobilisation lead only until a
# direct contractor/developer award source states the execution package.
serra = extra["projects"]["serra-giannina"]
serra_relations = serra.get("relations") or []
assert len(serra_relations) == 2, serra_relations
egm = next(r for r in serra_relations if r.get("company") == "EGM Project")
dagostino = next(r for r in serra_relations if r.get("company") == "D'Agostino Costruzioni Generali")
assert egm["confidence"] == "A2" and egm["status"] == "confirmed"
assert egm["role"] not in execution_roles
assert "does not attribute Civil BoP" in egm["scope"]
assert dagostino["confidence"] == "B" and dagostino["status"] == "signal"
assert dagostino["role"] not in execution_roles
assert "no Civil BoP" in dagostino["scope"], "B mobilisation signal must not close Civil BoP"
assert any(s.get("type") == "construction-progress" and s.get("grade") == "A2" for s in serra.get("signals") or [])
assert any(s.get("type") == "contractor-mobilisation-lead" and s.get("grade") == "B" for s in serra.get("signals") or [])

# All v0.6 commercial-enrichment sources must be attributable and navigable.
for document in (payload, extra):
    for project_id, project_payload in document["projects"].items():
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

# Additive enrichment must not silently rewrite the canonical relation/config graph.
canonical_andretta = by_id["andretta-bisaccia"]
assert not any(
    r.get("company") == "Progeco Group / Progeco SE"
    for r in (canonical_andretta.get("relations") or [])
), "v0.6 support signal must stay in additive commercial enrichment"

canonical_carlentini = by_id["carlentini"]
assert any(
    r.get("company") == "Gruppo Mammana" and r.get("confidence") == "B"
    for r in (canonical_carlentini.get("relations") or [])
), "historical seed signal must remain unchanged; v0.6 A2 upgrade is additive and reversible"

canonical_tricarico = by_id["tricarico"]
assert not any(
    r.get("company") == "Vector Renewables"
    for r in (canonical_tricarico.get("relations") or [])
), "Tricarico lender-side advisor must stay in additive enrichment"
assert any(
    r.get("company") == "Vestas" and r.get("confidence") == "A2"
    for r in (canonical_tricarico.get("relations") or [])
), "existing Tricarico OEM evidence must remain canonical"

canonical_greci = by_id["greci-montaguto"]
assert canonical_greci.get("wtg") is None, "v0.6 configuration evidence must not silently rewrite the seed"
assert any(
    r.get("company") == "Vestas" and r.get("confidence") == "A2"
    for r in (canonical_greci.get("relations") or [])
), "existing Greci-Montaguto OEM evidence must remain canonical"

canonical_nulvi = by_id["nulvi-ploaghe"]
assert not any(
    r.get("company") == "Hydro Engineering"
    for r in (canonical_nulvi.get("relations") or [])
), "Nulvi development-chain evidence must stay additive"
assert canonical_nulvi.get("stage") == "E4"
assert len(canonical_nulvi.get("gaps") or []) >= 8, "Nulvi execution/procurement gaps must stay open"

execution_relations = [
    (project_id, relation)
    for document in (payload, extra)
    for project_id, project_payload in document["projects"].items()
    for relation in project_payload.get("relations") or []
    if relation.get("role") in execution_roles
    and relation.get("status") == "confirmed"
    and relation.get("confidence") in {"A1", "A2"}
]
assert len(execution_relations) == 1, execution_relations
assert execution_relations[0][0] == "carlentini"

covered_projects = set(payload["projects"]) | set(extra["projects"])
print(
    f"v0.6 commercial enrichment OK: {len(covered_projects)} projects, "
    f"{len(execution_relations)} A1/A2 execution relation; "
    "Andretta configuration, Serra mobilisation, Tricarico monitoring, Greci configuration and Nulvi development preserve execution guards"
)
