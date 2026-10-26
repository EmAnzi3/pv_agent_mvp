from __future__ import annotations

import json
import math
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
WIND = ROOT / "docs" / "wind"
DATA = WIND / "data"


def fail(message: str) -> None:
    raise SystemExit(f"[FAIL] {message}")


def load_json(path: Path):
    with path.open("r", encoding="utf-8-sig") as handle:
        return json.load(handle)


def activity_class(candidate: dict) -> str:
    if candidate.get("status") == "rejected":
        return "rejected"
    return candidate.get("activity_class") or "current"


def main() -> None:
    allowed = {"onshore", "offshore"}

    seed_registry = load_json(DATA / "discovery-v04.json")
    census_a = load_json(DATA / "discovery-census-v04.json")
    census_b = load_json(DATA / "discovery-census-v04b.json")
    scope_profiles = load_json(DATA / "scope-profiles-v04.json")
    identity_rules = load_json(DATA / "identity-rules-v04.json")
    refresh_log = load_json(DATA / "refresh-log-v04.json")

    if set(seed_registry.get("allowed_site_types", [])) != allowed:
        fail("discovery registry: allowed_site_types inattesi")

    statuses = set(seed_registry.get("workflow", []))
    required_statuses = {
        "discovered",
        "identity_checked",
        "evidence_enriched",
        "canonical_candidate",
        "accepted",
        "needs_reconciliation",
        "rejected",
    }
    if statuses != required_statuses:
        fail("discovery registry: workflow incompleto")

    required = set(seed_registry.get("candidate_required_fields", []))
    candidates = list(seed_registry.get("candidates", [])) + list(census_a.get("candidates", [])) + list(census_b.get("candidates", []))
    if len(candidates) != 47:
        fail(f"discovery: candidati inattesi {len(candidates)}, attesi 47")

    ids = [candidate.get("candidate_id") for candidate in candidates]
    if len(ids) != len(set(ids)):
        fail("discovery: candidate_id duplicati tra registri")

    for candidate in candidates:
        missing = required - set(candidate)
        if missing:
            fail(f"{candidate.get('candidate_id', '?')}: campi discovery mancanti {sorted(missing)}")
        if candidate["site_type"] not in allowed:
            fail(f"{candidate['candidate_id']}: site_type non canonico {candidate['site_type']}")
        if candidate["status"] not in statuses:
            fail(f"{candidate['candidate_id']}: status discovery non canonico {candidate['status']}")
        if activity_class(candidate) not in {"current", "stale_scoping", "rejected"}:
            fail(f"{candidate['candidate_id']}: activity_class non canonica")
        if not candidate.get("sources"):
            fail(f"{candidate['candidate_id']}: almeno una fonte discovery è obbligatoria")

    current = [c for c in candidates if activity_class(c) == "current"]
    stale = [c for c in candidates if activity_class(c) == "stale_scoping"]
    rejected = [c for c in candidates if activity_class(c) == "rejected"]

    if (len(current), len(stale), len(rejected)) != (38, 4, 5):
        fail(f"current/stale/rejected inattesi: {len(current)}/{len(stale)}/{len(rejected)}")
    if (sum(c["site_type"] == "onshore" for c in current), sum(c["site_type"] == "offshore" for c in current)) != (28, 10):
        fail("discovery corrente: distribuzione onshore/offshore inattesa")

    current_wind = sum(float(c.get("wind_mw") or 0) for c in current)
    current_bess = sum(float(c.get("bess_mw") or 0) for c in current)
    stale_wind = sum(float(c.get("wind_mw") or 0) for c in stale)
    rejected_wind = sum(float(c.get("wind_mw") or 0) for c in rejected)
    if not math.isclose(current_wind, 11538.67, abs_tol=0.01):
        fail(f"MW wind current inattesi: {current_wind:.2f}")
    if not math.isclose(current_bess, 661.0, abs_tol=0.01):
        fail(f"MW BESS current inattesi: {current_bess:.2f}")
    if not math.isclose(stale_wind, 3195.0, abs_tol=0.01):
        fail(f"MW stale-scoping inattesi: {stale_wind:.1f}")
    if not math.isclose(rejected_wind, 1195.4, abs_tol=0.01):
        fail(f"MW rejected inattesi: {rejected_wind:.1f}")

    by_id = {c["candidate_id"]: c for c in candidates}
    if by_id["off-med-wind-grecale"].get("mase_procedure_id") != "13027":
        fail("Med Wind Grecale: procedura MASE corrente 13027 non registrata")
    if by_id["off-kailia-current"].get("wind_mw") != 900 or by_id["off-kailia-current"].get("identity_group") != "kailia-offshore-puglia":
        fail("Kailia: configurazione corrente/identity guard incoerente")
    if by_id["off-atis-current"].get("identity_group") != "atis-floating-wind-toscana" or len(by_id["off-atis-current"].get("sources", [])) < 2:
        fail("Atis: scoping + VIA non riconciliati")
    if by_id["off-nurax-ne-sardinia"].get("identity_group") != "nurax-ne-sardinia-462" or len(by_id["off-nurax-ne-sardinia"].get("sources", [])) < 3:
        fail("NURAX: genealogia multi-procedura non riconciliata")
    if by_id["off-poseidon-sardinia-nw"].get("identity_group") != "poseidon-tirreno-nw-1008":
        fail("Poseidon: identity group mancante")
    if by_id["on-le-chiancate"].get("identity_group") != "le-chiancate-amaranth-86-4" or len(by_id["on-le-chiancate"].get("sources", [])) != 2:
        fail("Le Chiancate: nuova istanza + procedura archiviata non riconciliate")
    if "ERG Nulvi-Ploaghe" not in str(by_id["on-nulvi-sedini-wpd"].get("notes", "")):
        fail("Nulvi-Sedini: collision guard con Nulvi-Ploaghe mancante")
    if by_id["on-sv9-monte-camulera"].get("site_type") != "onshore" or "offshore" not in str(by_id["on-sv9-monte-camulera"].get("source_type_label", "")):
        fail("SV9 Monte Camulera: data-quality guard tipologia MASE non conservata")

    rejected_expected = {
        "off-chieuti-legacy",
        "off-puglia-1-archived",
        "on-brindisi-evolve-archived",
        "on-mazara-wind-archived",
        "on-thiesi-naturgy-archived",
    }
    if {c["candidate_id"] for c in rejected} != rejected_expected:
        fail("insieme guardie rejected inatteso")

    profiles = scope_profiles.get("profiles", {})
    if set(profiles) != {"onshore", "offshore"}:
        fail("scope profiles onshore/offshore mancanti")
    onshore_ids = {x["id"] for x in profiles["onshore"]["core_scopes"]}
    offshore_ids = {x["id"] for x in profiles["offshore"]["core_scopes"]}
    if onshore_ids != {"civil", "electrical", "sse_grid", "foundation", "erection", "logistics", "dismantling"}:
        fail("scope profile onshore non coincide con v0.3")
    expected_offshore = {"substructure_mooring", "turbine_installation", "inter_array_cables", "offshore_substation", "export_cable_landfall", "onshore_grid", "marine_logistics_port", "onshore_civil", "dismantling"}
    if offshore_ids != expected_offshore:
        fail(f"scope profile offshore incompleto: {sorted(offshore_ids)}")

    if identity_rules.get("identity_priority") != ["explicit_identity_group", "myterna_anchor", "mase_operation_anchor", "name_site_area_anchor"]:
        fail("identity priority inattesa")
    never = set(identity_rules.get("never_identity_fields", []))
    if not {"wind_mw", "wtg_count", "developer_or_spv", "procedure_state", "stage"}.issubset(never):
        fail("campi mutabili stanno entrando nell'identità")

    runs = refresh_log.get("runs", [])
    if len(runs) != 1 or runs[0].get("run_id") != "2026-09-05-baseline":
        fail("refresh log baseline mancante")
    baseline = runs[0]
    expected_log = {
        "candidate_count": 47,
        "current_candidate_count": 38,
        "stale_candidate_count": 4,
        "rejected_candidate_count": 5,
        "current_onshore_count": 28,
        "current_offshore_count": 10,
    }
    for key, value in expected_log.items():
        if baseline.get(key) != value:
            fail(f"refresh log {key} inatteso: {baseline.get(key)}")
    if baseline.get("canonical_promotions") != 0:
        fail("discovery ha promosso candidati senza gate")

    manifest = load_json(DATA / "projects.json")
    projects = []
    for chunk in manifest["chunks"]:
        projects.extend(load_json(DATA / chunk))
    if len(projects) != 17:
        fail(f"dataset canonico v0.3 alterato: {len(projects)} progetti")

    invalid_site_types = [
        (p.get("id"), p.get("site_type"))
        for p in projects
        if p.get("site_type") is not None and p.get("site_type") not in allowed
    ]
    if invalid_site_types:
        fail(f"site_type canonici non validi: {invalid_site_types}")

    index = (WIND / "index.html").read_text(encoding="utf-8")
    app = (WIND / "assets" / "app.js").read_text(encoding="utf-8")
    discovery_js = (WIND / "assets" / "discovery-v04.js").read_text(encoding="utf-8")
    engine = (ROOT / "scripts" / "wind_discovery_engine.py").read_text(encoding="utf-8")

    for token in ['id="siteType"', 'value="onshore"', 'value="offshore"', 'id="discoveryRows"', 'assets/discovery-v04.js']:
        if token not in index:
            fail(f"UI v0.4 incompleta: manca {token}")
    if "p.site_type||'onshore'" not in app:
        fail("fallback legacy site_type=onshore mancante")
    if "projects.length!==17" in app:
        fail("frontend ancora bloccato sul seed esatto di 17 progetti")
    if "site_type" not in app or "siteType" not in app:
        fail("site_type non cablato nel frontend/export")
    for filename in ["discovery-v04.json", "discovery-census-v04.json", "discovery-census-v04b.json"]:
        if filename not in discovery_js:
            fail(f"Discovery UI non legge {filename}")
        if filename not in engine:
            fail(f"Discovery engine non legge {filename}")
    if "stale_scoping" not in engine or "change_fingerprint" not in engine or "identity_key" not in engine:
        fail("Discovery engine privo di activity/change/identity logic")

    explicit = sum(1 for p in projects if p.get("site_type") in allowed)
    legacy = len(projects) - explicit
    print(f"[OK] canonico invariato: {len(projects)} progetti; {explicit} site_type espliciti, {legacy} legacy -> onshore")
    print(f"[OK] discovery: {len(candidates)} candidati = {len(current)} current + {len(stale)} stale + {len(rejected)} rejected")
    print(f"[OK] current onshore/offshore: 28/10 · {current_wind:.2f} MW wind + {current_bess:.0f} MW BESS")
    print("[OK] identity guards: NURAX, Atis, Poseidon, Kailia, Le Chiancate, Nulvi-Sedini, SV9")
    print("[OK] scope profiles separati onshore/offshore; KPI canonici non contaminati")
    print("[OK] selector + Discovery UI presenti; frontend non più vincolato a 17 progetti")


if __name__ == "__main__":
    main()
