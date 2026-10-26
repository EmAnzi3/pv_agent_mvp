from __future__ import annotations

import json
import math
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
DATA = ROOT / "docs" / "wind" / "data"


def load(name: str):
    with (DATA / name).open("r", encoding="utf-8-sig") as handle:
        return json.load(handle)


def fail(message: str) -> None:
    raise SystemExit(f"[FAIL] {message}")


def main() -> None:
    manifest = load("projects.json")
    expected_chunks = [f"projects-{i}.json" for i in range(1, 10)]
    if manifest.get("chunks") != expected_chunks:
        fail(f"manifest canonico inatteso: {manifest.get('chunks')}")

    projects = []
    for chunk in manifest["chunks"]:
        projects.extend(load(chunk))

    ids = [p.get("id") for p in projects]
    if len(projects) != 51:
        fail(f"canonico v0.5: {len(projects)} progetti, attesi 51")
    if len(ids) != len(set(ids)):
        fail("canonico v0.5: ID duplicati")

    baseline = [p for p in projects if p.get("origin") != "promotion-v05"]
    promoted = [p for p in projects if p.get("origin") == "promotion-v05"]
    if len(baseline) != 17:
        fail(f"baseline storica alterata: {len(baseline)} progetti")
    if not math.isclose(sum(float(p.get("mw") or 0) for p in baseline), 1496.9, abs_tol=0.01):
        fail("baseline storica: MW v0.3 alterati")
    if len(promoted) != 34:
        fail(f"promozione v0.5: {len(promoted)} record, attesi 34")

    gate = load("promotion-gate-v05.json")
    eligible = set(gate.get("eligible", []))
    promoted_source_ids = {p.get("promotion_source_id") for p in promoted}
    if promoted_source_ids != eligible:
        fail(f"promozione != gate: missing={sorted(eligible-promoted_source_ids)} extra={sorted(promoted_source_ids-eligible)}")

    blocked = {x["candidate_id"] for x in gate.get("blocked", [])}
    if blocked & set(ids):
        fail(f"candidati blocked entrati nel canonico: {sorted(blocked & set(ids))}")

    total_wind = sum(float(p.get("mw") or 0) for p in projects)
    promoted_wind = sum(float(p.get("mw") or 0) for p in promoted)
    promoted_bess = sum(float(p.get("bess_mw") or 0) for p in promoted)
    if not math.isclose(promoted_wind, 9705.62, abs_tol=0.01):
        fail(f"MW promossi inattesi: {promoted_wind:.2f}")
    if not math.isclose(promoted_bess, 311.0, abs_tol=0.01):
        fail(f"BESS promosso inatteso: {promoted_bess:.2f}")
    if not math.isclose(total_wind, 11202.52, abs_tol=0.01):
        fail(f"MW canonici totali inattesi: {total_wind:.2f}")

    onshore = [p for p in promoted if p.get("site_type") == "onshore"]
    offshore = [p for p in promoted if p.get("site_type") == "offshore"]
    if (len(onshore), len(offshore)) != (26, 8):
        fail(f"promossi onshore/offshore inattesi: {len(onshore)}/{len(offshore)}")

    stage_counts = {stage: sum(p.get("stage") == stage for p in promoted) for stage in [f"E{i}" for i in range(9)]}
    if stage_counts["E2"] != 30 or stage_counts["E3"] != 4 or sum(stage_counts.values()) != 34:
        fail(f"stage promossi inattesi: {stage_counts}")

    expected_onshore_gaps = {
        "Civil BoP", "Electrical BoP", "SSE / grid contractor", "Erection contractor",
        "Logistics / heavy transport", "Foundation contractor"
    }
    expected_offshore_gaps = {
        "Foundations / substructure / mooring", "WTG installation offshore", "Inter-array cables",
        "Offshore substation / electrical platform", "Export cable + landfall", "Onshore SSE / grid",
        "Marine logistics / port / heavy lift", "Civil works onshore connection"
    }

    for p in promoted:
        if p.get("relations"):
            fail(f"{p['id']}: una relazione contractor è stata inventata in fase di promozione")
        if p.get("priority") != "C" or p.get("score") != 50:
            fail(f"{p['id']}: ranking commerciale non neutro prima dell'enrichment")
        if not p.get("wtg") or float(p.get("mw") or 0) <= 0:
            fail(f"{p['id']}: configurazione MW/WTG incompleta")
        configs = p.get("configs", [])
        if len(configs) != 1 or configs[0].get("wtg_count") != p.get("wtg"):
            fail(f"{p['id']}: baseline config non coerente con WTG canonici")
        if not any(s.get("grade") in {"A1", "A2"} for s in p.get("sources", [])):
            fail(f"{p['id']}: manca fonte A1/A2 di promozione")
        gaps = set(p.get("gaps", []))
        expected = expected_offshore_gaps if p.get("site_type") == "offshore" else expected_onshore_gaps
        if not expected.issubset(gaps):
            fail(f"{p['id']}: scope gap incompleti per {p.get('site_type')}")
        is_repowering = "repowering" in str(p.get("type", "")).lower()
        if is_repowering and "Dismantling contractor" not in gaps:
            fail(f"{p['id']}: repowering senza dismantling gap")

    by_id = {p["id"]: p for p in promoted}
    guards = {
        "on-florinas-repowering": "E3",
        "off-kailia-current": "E3",
        "off-atis-current": "E2",
        "on-minervino-edison-repowering": "E3",
        "on-ripabottoni-edison-repowering": "E3",
    }
    for pid, stage in guards.items():
        if by_id[pid].get("stage") != stage:
            fail(f"{pid}: stage {by_id[pid].get('stage')}, atteso {stage}")
    if by_id["off-atis-current"].get("wtg") != 48:
        fail("Atis: configurazione canonica deve essere 48 WTG")

    discovery_files = ["discovery-v04.json", "discovery-census-v04.json", "discovery-census-v04b.json"]
    discovery = []
    for filename in discovery_files:
        discovery.extend(load(filename).get("candidates", []))
    stale_or_rejected = {
        c["candidate_id"] for c in discovery
        if c.get("status") == "rejected" or c.get("activity_class") == "stale_scoping"
    }
    if stale_or_rejected & promoted_source_ids:
        fail("stale/rejected entrati nella tranche promossa")

    print(f"[OK] canonico v0.5: {len(projects)} progetti / {total_wind:.2f} MW wind")
    print(f"[OK] baseline v0.3 preservata: 17 / 1496.9 MW")
    print(f"[OK] promossi: 34 = 26 onshore + 8 offshore / {promoted_wind:.2f} MW + {promoted_bess:.0f} MW BESS")
    print("[OK] promossi senza contractor dedotti e con ranking commerciale neutro")
    print("[OK] 4 blocked + stale/rejected esclusi dal canonico")
    print(f"[OK] stage promossi: E2={stage_counts['E2']} E3={stage_counts['E3']}")


if __name__ == "__main__":
    main()
