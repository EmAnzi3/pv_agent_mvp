from __future__ import annotations

import json
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
DATA = ROOT / "docs" / "wind" / "data"
REGISTRY = DATA / "industry-press-intelligence-2026-09-05.json"
MANIFEST = DATA / "projects.json"


def load(path: Path):
    with path.open("r", encoding="utf-8-sig") as handle:
        return json.load(handle)


def fail(message: str) -> None:
    raise SystemExit(f"[FAIL] {message}")


def main() -> None:
    manifest = load(MANIFEST)
    seed_ids: set[str] = set()
    for chunk in manifest["chunks"]:
        seed_ids.update(project["id"] for project in load(DATA / chunk))

    registry = load(REGISTRY)
    projects = registry.get("projects", {})
    expected = {"tricarico", "castelfranco-cer", "alia-sclafani", "nulvi-ploaghe", "carlentini"}
    if set(projects) != expected:
        fail(f"industry press: ID inattesi {sorted(projects)}")
    if not set(projects).issubset(seed_ids):
        fail("industry press contiene progetti fuori dai 17 seed")

    forbidden_keys = {"relations", "scope_hint", "execution_scope", "covered_scope"}
    for project_id, payload in projects.items():
        serialized = json.dumps(payload, ensure_ascii=False)
        for key in forbidden_keys:
            if f'"{key}"' in serialized:
                fail(f"{project_id}: chiave vietata nel discovery layer: {key}")
        for signal in payload.get("signals", []):
            grade = signal.get("grade")
            if grade not in {"A1", "A2", "B", "C", "D"}:
                fail(f"{project_id}: grade non valido {grade}")
            source_id = signal.get("source_id")
            if source_id:
                ids = {source.get("id") for source in payload.get("sources", [])}
                if source_id not in ids:
                    fail(f"{project_id}: source_id non risolto {source_id}")

    tri = projects["tricarico"]
    finance = [x for x in tri.get("signals", []) if x.get("type") == "financial-close"]
    if len(finance) != 1 or finance[0].get("grade") != "A2":
        fail("Tricarico: financial close UniCredit A2 mancante o duplicato")
    if "46,5" not in finance[0].get("title", ""):
        fail("Tricarico: importo financial close non riconoscibile")

    castel = projects["castelfranco-cer"]
    collision = [x for x in castel.get("signals", []) if x.get("type") == "identity-collision"]
    if len(collision) != 1 or collision[0].get("grade") != "A1":
        fail("Castelfranco/CER: identity collision A1 mancante")
    source_ids = {x.get("id") for x in castel.get("sources", [])}
    if not {"cas-campania-friel-9207", "cas-campania-cer-9439"}.issubset(source_ids):
        fail("Castelfranco/CER: mancano le due fonti regionali di disambiguazione")

    alia = projects["alia-sclafani"]
    candidate = [x for x in alia.get("signals", []) if x.get("type") == "site-services-candidate"]
    if len(candidate) != 1 or candidate[0].get("grade") != "C":
        fail("Alia-Sclafani: site-services candidate deve restare C")

    print("[OK] industry press registry: 5 progetti con intelligence materiale")
    print("[OK] discovery layer privo di relazioni/scope esecutivi")
    print("[OK] Tricarico financial close A2, Castelfranco identity guard A1, Alia site-services C")


if __name__ == "__main__":
    main()
