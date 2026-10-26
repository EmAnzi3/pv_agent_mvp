from __future__ import annotations

import copy
import json
import math
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
DATA = ROOT / "docs" / "wind" / "data"


def load(name: str):
    with (DATA / name).open("r", encoding="utf-8-sig") as f:
        return json.load(f)


def fail(msg: str) -> None:
    raise SystemExit(f"[FAIL] {msg}")


def merge_project(project: dict, overlay: dict | None) -> dict:
    p = copy.deepcopy(project)
    if not overlay:
        return p
    p.setdefault("sources", [])
    p.setdefault("relations", [])
    for source in overlay.get("sources", []):
        if not any(x.get("id") == source.get("id") for x in p["sources"]):
            p["sources"].append(copy.deepcopy(source))
    for spec in overlay.get("relations", []):
        rel = {k: v for k, v in spec.items() if k not in {"action", "match"}}
        found = None
        if spec.get("action") == "upgrade_or_add" and spec.get("match"):
            match = spec["match"]
            for current in p["relations"]:
                if current.get("company") != match.get("company"):
                    continue
                contains = match.get("role_contains")
                if contains and contains not in str(current.get("role", "")):
                    continue
                found = current
                break
        if found is not None:
            found.update(copy.deepcopy(rel))
        elif not any(
            x.get("company") == rel.get("company")
            and x.get("role") == rel.get("role")
            and x.get("source_id") == rel.get("source_id")
            for x in p["relations"]
        ):
            p["relations"].append(copy.deepcopy(rel))
    return p


def main() -> None:
    manifest = load("projects.json")
    meta = load(manifest["meta"])
    projects: list[dict] = []
    for chunk in manifest["chunks"]:
        projects.extend(load(chunk))

    if len(projects) != 17:
        fail(f"seed Wind inatteso: {len(projects)} progetti, attesi 17")
    ids = [p["id"] for p in projects]
    if len(ids) != len(set(ids)):
        fail("ID progetto duplicati")
    total_mw = sum(float(p.get("mw") or 0) for p in projects)
    if not math.isclose(total_mw, 1496.9, abs_tol=0.01):
        fail(f"totale MW inatteso: {total_mw:.1f}")

    stages = {x["code"] for x in meta["maturity_scale"]}
    for p in projects:
        if p.get("stage") not in stages:
            fail(f"stage non valido per {p['id']}")

    enrichment = load("enrichment-2026-09-04.json")
    if set(enrichment.get("projects", {})) != set(ids):
        fail("gli ID dell'enrichment non coincidono con i seed")
    merged = [merge_project(p, enrichment["projects"].get(p["id"])) for p in projects]

    docpass = load("enrichment-docpass2-2026-09-04.json")
    expected_docpass = {"alas", "toritto", "fenice", "lama-cupa"}
    if set(docpass.get("projects", {})) != expected_docpass:
        fail(f"docpass2: ID inattesi {sorted(docpass.get('projects', {}))}")
    merged = [merge_project(p, docpass["projects"].get(p["id"])) for p in merged]

    leads = load("contractor-leads-2026-09-05.json")
    expected_leads = {
        "andretta-bisaccia", "tricarico", "venusia", "serra-palino",
        "tarsia-ovest", "castelfranco-cer"
    }
    if set(leads.get("projects", {})) != expected_leads:
        fail(f"contractor leads: ID inattesi {sorted(leads.get('projects', {}))}")
    merged = [merge_project(p, leads["projects"].get(p["id"])) for p in merged]

    core = enrichment["method"]["core_scopes"]
    role_map = enrichment["method"]["scope_role_map"]

    def by_id(pid: str) -> dict:
        return next(p for p in merged if p["id"] == pid)

    def applicable(p: dict) -> list[dict]:
        repowering = "repowering" in str(p.get("type", "")).lower()
        return [s for s in core if s["applicable"] == "all" or (s["applicable"] == "repowering" and repowering)]

    def covered(p: dict, scope: dict) -> bool:
        allowed = set(role_map[scope["id"]])
        return any(
            r.get("role") in allowed
            and r.get("status") == "confirmed"
            and r.get("confidence") in {"A1", "A2"}
            for r in p.get("relations", [])
        )

    def coverage(pid: str) -> set[str]:
        p = by_id(pid)
        return {s["id"] for s in applicable(p) if covered(p, s)}

    for p in merged:
        source_ids = {s.get("id") for s in p.get("sources", [])}
        missing = sorted({
            r.get("source_id") for r in p.get("relations", [])
            if r.get("source_id") and r.get("source_id") not in source_ids
        })
        if missing:
            fail(f"source_id enrichment non risolti in {p['id']}: {', '.join(missing)}")

    progeco = [r for r in by_id("andretta-bisaccia")["relations"] if r.get("source_id") == "andr-progeco-site-manager"]
    if len(progeco) != 1 or progeco[0].get("confidence") != "B" or progeco[0].get("status") != "signal":
        fail("Andretta-Bisaccia: Progeco deve restare signal/B")
    if coverage("andretta-bisaccia"):
        fail("Andretta-Bisaccia: Progeco sta chiudendo indebitamente uno scope")

    tri = [r for r in by_id("tricarico")["relations"] if r.get("source_id") == "tri-vestas-install-lead"]
    if len(tri) != 1 or tri[0].get("confidence") != "B" or tri[0].get("scope_hint") != "erection":
        fail("Tricarico: Vestas installation deve restare B / erection hint")
    if coverage("tricarico"):
        fail("Tricarico: un lead/advisory sta chiudendo indebitamente uno scope")

    venusia = by_id("venusia")
    if coverage("venusia") != {"civil"}:
        fail(f"Venusia: coverage inattesa {sorted(coverage('venusia'))}")
    if venusia.get("wtg") != 8:
        fail(f"Venusia: WTG correnti inattesi {venusia.get('wtg')}; attesi 8")
    if "OEM" in venusia.get("gaps", []):
        fail("Venusia: OEM è ancora presente nei gap nonostante Nordex A2")
    current_cfg = [c for c in venusia.get("configs", []) if c.get("source_id") == "ven-rwe"]
    if len(current_cfg) != 1 or current_cfg[0].get("wtg_count") != 8 or not math.isclose(float(current_cfg[0].get("wtg_mw") or 0), 5.6, abs_tol=0.01):
        fail("Venusia: configurazione RWE corrente deve essere 8 WTG x 5,6 MW")
    ven_nordex = [r for r in venusia["relations"] if r.get("source_id") == "ven-rwe-ceo-nordex"]
    ven_newdev = [r for r in venusia["relations"] if r.get("source_id") == "ven-newdev-cosviluppo"]
    if len(ven_nordex) != 1 or ven_nordex[0].get("role") != "OEM" or ven_nordex[0].get("confidence") != "A2":
        fail("Venusia: Nordex OEM A2 mancante/duplicato")
    if len(ven_newdev) != 1 or ven_newdev[0].get("confidence") != "A2":
        fail("Venusia: New Developments A2 mancante/duplicato")

    serra_palino = by_id("serra-palino")
    if coverage("serra-palino") != {"civil", "electrical"}:
        fail(f"Serra Palino: coverage inattesa {sorted(coverage('serra-palino'))}")
    if "OEM" in serra_palino.get("gaps", []):
        fail("Serra Palino: OEM è ancora presente nei gap nonostante Nordex A2")
    sp_nordex = [r for r in serra_palino["relations"] if r.get("source_id") == "sp-rwe-ceo-nordex"]
    if len(sp_nordex) != 1 or sp_nordex[0].get("role") != "OEM" or sp_nordex[0].get("confidence") != "A2":
        fail("Serra Palino: Nordex OEM A2 mancante/duplicato")

    if coverage("tarsia-ovest") != {"civil", "electrical", "sse_grid"}:
        fail(f"Tarsia Ovest: coverage inattesa {sorted(coverage('tarsia-ovest'))}")
    mammana = [r for r in by_id("tarsia-ovest")["relations"] if r.get("source_id") == "tars-mammana-civil-lead"]
    if len(mammana) != 1 or mammana[0].get("company") != "Michelangelo Mammana" or mammana[0].get("confidence") != "B" or mammana[0].get("scope_hint") != "civil":
        fail("Tarsia Ovest: Mammana deve essere normalizzato e restare B / civil hint")

    if coverage("castelfranco-cer") != {"sse_grid"}:
        fail(f"Castelfranco/CER: coverage inattesa {sorted(coverage('castelfranco-cer'))}")
    energyand = [r for r in by_id("castelfranco-cer")["relations"] if r.get("source_id") == "cas-energyand-site"]
    if len(energyand) != 1 or energyand[0].get("company") != "Energy&" or energyand[0].get("confidence") != "B" or energyand[0].get("status") != "signal":
        fail("Castelfranco/CER: Energy& deve restare signal/B")

    if coverage("serra-giannina"):
        fail("Serra Giannina: un segnale B sta chiudendo uno scope")
    if coverage("carlentini") != {"foundation"}:
        fail(f"Carlentini: coverage inattesa {sorted(coverage('carlentini'))}")
    if coverage("alas"):
        fail("ALAS: progettazione Hydro sta chiudendo uno scope")
    if coverage("fenice"):
        fail("Fenice: engineering ATS sta chiudendo uno scope")
    if any(r.get("company") == "Brulli Trasmissione" for r in by_id("lama-cupa").get("relations", [])):
        fail("Lama Cupa: Brulli attribuita indebitamente al parco")

    covered_slots = 0
    total_slots = 0
    with_scope = []
    for p in merged:
        scopes = applicable(p)
        n = sum(covered(p, s) for s in scopes)
        covered_slots += n
        total_slots += len(scopes)
        if n:
            with_scope.append(p)
    mw_with_scope = sum(float(p.get("mw") or 0) for p in with_scope)

    if (covered_slots, total_slots) != (8, 108):
        fail(f"scope coverage inattesa: {covered_slots}/{total_slots}; attesa 8/108")
    if not math.isclose(mw_with_scope, 230.9, abs_tol=0.01):
        fail(f"MW con scope inattesi: {mw_with_scope:.1f}; attesi 230.9")

    print(f"[OK] seed Wind: 17 progetti / {total_mw:.1f} MW")
    print(f"[OK] scope coverage: {covered_slots}/{total_slots} ({covered_slots / total_slots * 100:.1f}%)")
    print(f"[OK] MW con almeno uno scope A1/A2: {mw_with_scope:.1f}")
    print("[OK] Priority 1 leads separati dagli scope: Progeco, Vestas-install, Mammana, Energy&")
    print("[OK] Venusia 8x5,6 MW e OEM Nordex coerenti; OEM gap risolto anche su Serra Palino")


if __name__ == "__main__":
    main()
