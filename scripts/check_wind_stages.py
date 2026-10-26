from __future__ import annotations

import json
from collections import Counter
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
DATA = ROOT / "docs" / "wind" / "data"


def load(name: str):
    with (DATA / name).open("r", encoding="utf-8-sig") as handle:
        return json.load(handle)


def fail(message: str) -> None:
    raise SystemExit(f"[FAIL] {message}")


def main() -> None:
    meta = load("meta.json")
    scale = meta.get("maturity_scale", [])
    expected_codes = [f"E{i}" for i in range(9)]
    expected_labels = {
        "E0": "Pre-sviluppo",
        "E1": "Sviluppo",
        "E2": "Iter autorizzativo",
        "E3": "Iter autorizzativo avanzato",
        "E4": "Autorizzato",
        "E5": "FID / investimento impegnato",
        "E6": "Procurement / affidamenti",
        "E7": "Costruzione",
        "E8": "In esercizio",
    }
    codes = [item.get("code") for item in scale]
    if codes != expected_codes:
        fail(f"scala maturità inattesa: {codes}; attesa {expected_codes}")
    for item in scale:
        code = item.get("code")
        if item.get("label") != expected_labels.get(code):
            fail(f"{code}: label inattesa {item.get('label')!r}; attesa {expected_labels.get(code)!r}")
        if not item.get("description"):
            fail(f"{code}: description canonica mancante")

    if "scala interna del Radar" not in str(meta.get("note", "")):
        fail("meta: deve essere esplicito che E0-E8 è una scala interna del Radar")

    manifest = load("projects.json")
    projects = []
    for chunk in manifest["chunks"]:
        projects.extend(load(chunk))
    if len(projects) != 17:
        fail(f"seed inatteso: {len(projects)} progetti")

    invalid = [(p["id"], p.get("stage")) for p in projects if p.get("stage") not in expected_codes]
    if invalid:
        fail(f"stage non canonici: {invalid}")

    distribution = Counter(p["stage"] for p in projects)
    expected_distribution = {
        "E0": 0,
        "E1": 0,
        "E2": 3,
        "E3": 2,
        "E4": 1,
        "E5": 0,
        "E6": 2,
        "E7": 9,
        "E8": 0,
    }
    actual = {code: distribution.get(code, 0) for code in expected_codes}
    if actual != expected_distribution:
        fail(f"distribuzione stage inattesa: {actual}; attesa {expected_distribution}")

    by_id = {p["id"]: p for p in projects}
    guards = {
        "andretta-bisaccia": "E6",
        "tricarico": "E6",
        "nulvi-ploaghe": "E4",
        "fenice": "E3",
        "toritto": "E2",
        "castelfranco-cer": "E7",
    }
    for project_id, expected_stage in guards.items():
        actual_stage = by_id[project_id].get("stage")
        if actual_stage != expected_stage:
            fail(f"{project_id}: stage {actual_stage}, atteso {expected_stage}")

    if "lavori fisici" not in by_id["castelfranco-cer"].get("status_note", "").lower():
        fail("Castelfranco/CER: E7 deve essere motivato esplicitamente da lavori fisici osservati")

    glossary = load("glossary.json")
    stage_terms = [x for x in glossary.get("terms", []) if x.get("term") == "E0–E8"]
    if len(stage_terms) != 1:
        fail("glossario: voce E0–E8 mancante o duplicata")
    definition = stage_terms[0].get("definition", "")
    for label in expected_labels.values():
        if label not in definition:
            fail(f"glossario: manca la label canonica {label!r}")
    if "scala interna del Radar" not in definition:
        fail("glossario: deve chiarire che E0-E8 non è una nomenclatura tecnica universale")

    print("[OK] scala E0-E8 canonica, sequenziale e completa")
    print("[OK] label sector-aligned: Pre-sviluppo → In esercizio")
    print(f"[OK] distribuzione 17 seed: {actual}")
    print("[OK] Castelfranco/CER E7; Andretta e Tricarico E6; Nulvi E4")
    print("[OK] glossario e meta dichiarano E0-E8 come scala interna del Radar")


if __name__ == "__main__":
    main()
