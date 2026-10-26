from __future__ import annotations

import json
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
DATA = ROOT / "docs" / "wind" / "data"
COMMERCIAL_FILES = [
    "commercial-enrichment-v05.json",
    "commercial-enrichment-v05b.json",
    "commercial-enrichment-v05c.json",
    "commercial-enrichment-v05d.json",
]
VALID_GRADES = {"A1", "A2", "B", "C", "D"}
OFFSHORE_EXECUTION_ROLES = {
    "Foundations / substructure / mooring",
    "WTG installation offshore",
    "Inter-array cables",
    "Offshore substation / electrical platform",
    "Export cable + landfall",
    "Onshore SSE / grid",
    "Marine logistics / port / heavy lift",
    "Civil works onshore connection",
}


def load(name: str):
    with (DATA / name).open("r", encoding="utf-8-sig") as handle:
        return json.load(handle)


def fail(message: str) -> None:
    raise SystemExit(f"[FAIL] {message}")


def merge_item(target: list, item: dict) -> None:
    signature = json.dumps(item, ensure_ascii=False, sort_keys=True)
    if not any(json.dumps(existing, ensure_ascii=False, sort_keys=True) == signature for existing in target):
        target.append(item)


def main() -> None:
    manifest = load("projects.json")
    meta = load(manifest["meta"])
    projects = []
    for chunk in manifest["chunks"]:
        projects.extend(load(chunk))

    promoted = {p["id"]: p for p in projects if p.get("origin") == "promotion-v05"}
    if len(promoted) != 34:
        fail(f"promoted canonicals inattesi: {len(promoted)}")

    combined = {}
    for filename in COMMERCIAL_FILES:
        doc = load(filename)
        for project_id, payload in doc.get("projects", {}).items():
            target = combined.setdefault(project_id, {"relations": [], "signals": [], "sources": []})
            for key in ("relations", "signals", "sources"):
                for item in payload.get(key, []):
                    merge_item(target[key], item)

    if set(combined) != set(promoted):
        missing = sorted(set(promoted) - set(combined))
        extra = sorted(set(combined) - set(promoted))
        fail(f"copertura commercial enrichment non completa: missing={missing} extra={extra}")

    execution_roles = set(meta.get("execution_roles", []))
    if not OFFSHORE_EXECUTION_ROLES.issubset(execution_roles):
        fail("meta.execution_roles non contiene l'intero profilo execution offshore v0.5")
    if meta.get("version") != "0.5.0":
        fail(f"meta.version inattesa: {meta.get('version')}")

    execution_relations = []
    for project_id, payload in combined.items():
        sources = payload.get("sources", [])
        source_ids = [s.get("id") for s in sources]
        if len(source_ids) != len(set(source_ids)) or any(not x for x in source_ids):
            fail(f"{project_id}: source_id commerciali mancanti o duplicati")
        for source in sources:
            if source.get("grade") not in VALID_GRADES:
                fail(f"{project_id}: grade fonte non valido {source.get('grade')}")
            if not source.get("url"):
                fail(f"{project_id}: fonte commerciale senza URL")
        for signal in payload.get("signals", []):
            if signal.get("grade") not in VALID_GRADES:
                fail(f"{project_id}: grade segnale non valido {signal.get('grade')}")
        for relation in payload.get("relations", []):
            if relation.get("confidence") not in VALID_GRADES:
                fail(f"{project_id}: confidence relazione non valida {relation.get('confidence')}")
            if relation.get("source_id") not in source_ids:
                fail(f"{project_id}: relation source_id non risolto {relation.get('source_id')}")
            if relation.get("role") in execution_roles:
                execution_relations.append((project_id, relation))
                if relation.get("status") != "confirmed" or relation.get("confidence") not in {"A1", "A2"}:
                    fail(f"{project_id}: execution relation senza A1/A2 confirmed")

    # Nel commercial enrichment corrente non è stato trovato alcun award esecutivo verificabile.
    # Engineering, owner, developer, survey e project-management restano commercial intelligence.
    if execution_relations:
        fail(f"execution relation inattesa nella tranche corrente: {[x[0] for x in execution_relations]}")

    lujentu = promoted["on-lujentu"]
    if lujentu.get("municipalities") != ["Nardò", "Copertino", "Galatina"]:
        fail(f"Lujentu: comuni canonici errati {lujentu.get('municipalities')}")

    lecce_companies = {r.get("company") for r in combined["on-lecce-betanrg"].get("relations", [])}
    if "Leonardo Engineering S.r.l." not in lecce_companies:
        fail("Lecce: manca Leonardo Engineering come engineering relation")
    if any("Siemens Gamesa" in str(company) for company in lecce_companies):
        fail("Lecce: riferimento turbine trasformato indebitamente in OEM award")

    for project_id in ("off-poseidon-sardinia-nw", "off-nurax-ne-sardinia"):
        companies = {r.get("company") for r in combined[project_id].get("relations", [])}
        if "Copenhagen Offshore Partners" not in companies:
            fail(f"{project_id}: manca COP project-management enrichment")
        if any("Saipem" in str(company) for company in companies):
            fail(f"{project_id}: Saipem attribuita per deduzione")

    tramontana_relations = combined["off-tramontana"].get("relations", [])
    tramontana_companies = {r.get("company") for r in tramontana_relations}
    expected_tramontana = {"OWC Ltd.", "MPOWER S.r.l.", "WSP ITALIA S.r.l."}
    if not expected_tramontana.issubset(tramontana_companies):
        fail(f"Tramontana: engineering enrichment incompleto {sorted(tramontana_companies)}")
    for relation in tramontana_relations:
        if relation.get("company") in expected_tramontana and relation.get("role") in execution_roles:
            fail(f"Tramontana: engineering relation classificata indebitamente execution {relation}")

    # Commercial enrichment non deve alterare ranking neutro né popolare le relations canoniche.
    for project_id, project in promoted.items():
        if project.get("priority") != "C" or project.get("score") != 50:
            fail(f"{project_id}: ranking promosso alterato senza criterio documentato")
        if project.get("relations"):
            fail(f"{project_id}: commercial intelligence confluita nelle canonical relations")

    print(f"[OK] commercial enrichment v0.5: {len(combined)}/34 promoted canonicals coperti")
    print("[OK] execution contractor awards: 0; advisor/engineering/owner/survey separati")
    print("[OK] offshore execution profile: 8 scope dedicati")
    print("[OK] Lujentu: Nardò / Copertino / Galatina")
    print("[OK] Leonardo Engineering registrata; Siemens Gamesa resta design reference, non OEM award")
    print("[OK] Poseidon/NURAX: COP/Divento enrichment senza estensione Saipem")
    print("[OK] Tramontana: OWC + MPOWER project design; WSP impact assessment, tutti non-execution")


if __name__ == "__main__":
    main()
