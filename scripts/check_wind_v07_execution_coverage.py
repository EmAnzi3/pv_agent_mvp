from __future__ import annotations

import json
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
DATA = ROOT / "docs" / "wind" / "data"


def load(path: Path):
    return json.loads(path.read_text(encoding="utf-8"))


def norm(value: object) -> str:
    return " ".join(str(value or "").lower().split())


def main() -> None:
    manifest = load(DATA / "projects.json")
    meta = load(DATA / manifest["meta"])
    projects = [p for chunk in manifest["chunks"] for p in load(DATA / chunk)]
    roles = set(meta["execution_roles"])
    overlay = load(DATA / "execution-coverage-v07.json")

    def strict(rel: dict) -> bool:
        return (
            rel.get("role") in roles
            and rel.get("status") == "confirmed"
            and rel.get("confidence") in {"A1", "A2"}
        )

    def has_exec(project: dict) -> bool:
        return any(strict(r) for r in project.get("relations", []))

    before = {p["id"] for p in projects if has_exec(p)}
    before_mw = sum(float(p.get("mw") or 0) for p in projects if has_exec(p))

    effective = json.loads(json.dumps(projects))
    effective_by_id = {p["id"]: p for p in effective}
    for project_id, patch in overlay.get("projects", {}).items():
        assert project_id in effective_by_id, f"overlay project missing from canon: {project_id}"
        project = effective_by_id[project_id]
        relations = list(project.get("relations") or [])
        for raw in patch.get("relations") or []:
            match_company = norm(raw.get("match_company"))
            match_role = norm(raw.get("match_role"))
            clean = {k: v for k, v in raw.items() if not k.startswith("match_")}
            idx = next(
                (
                    i
                    for i, rel in enumerate(relations)
                    if (not match_company or norm(rel.get("company")) == match_company)
                    and (not match_role or norm(rel.get("role")) == match_role)
                ),
                None,
            )
            if idx is None:
                relations.append(clean)
            else:
                relations[idx] = clean
        project["relations"] = relations

    after = {p["id"] for p in effective if has_exec(p)}
    after_mw = sum(float(p.get("mw") or 0) for p in effective if has_exec(p))
    open_projects = [p for p in effective if not has_exec(p)]
    open_mw = sum(float(p.get("mw") or 0) for p in open_projects)

    assert len(projects) == 51, len(projects)
    assert len(before) == 4, (len(before), sorted(before))
    assert round(before_mw, 2) == 133.90, before_mw
    assert len(after) == 6, (len(after), sorted(after))
    assert round(after_mw, 2) == 272.90, after_mw
    assert len(open_projects) == 45, len(open_projects)
    assert round(open_mw, 2) == 10929.62, open_mw

    carlentini = effective_by_id["carlentini"]
    mammana = [
        r
        for r in carlentini.get("relations", [])
        if r.get("company") == "Mammana Michelangelo S.p.A."
        and r.get("role") == "Foundation contractor"
    ]
    assert len(mammana) == 1 and strict(mammana[0]), mammana

    tricarico = effective_by_id["tricarico"]
    vestas_erection = [
        r
        for r in tricarico.get("relations", [])
        if r.get("company") == "Vestas"
        and r.get("role") == "Erection contractor"
    ]
    assert len(vestas_erection) == 1 and strict(vestas_erection[0]), vestas_erection
    assert "Civil BoP" in tricarico.get("gaps", []), tricarico.get("gaps")
    assert "Electrical BoP" in tricarico.get("gaps", []), tricarico.get("gaps")

    # Known anti-inference guards must remain open after reconciliation.
    for project_id in (
        "andretta-bisaccia",
        "alia-sclafani",
        "serra-giannina",
        "greci-montaguto",
        "alas",
        "nulvi-ploaghe",
    ):
        assert not has_exec(effective_by_id[project_id]), project_id

    # Any project with an A1/A2 confirmed execution relation in additive
    # commercial intelligence must also be represented as execution-covered
    # in the effective graph. This prevents future drawer-only evidence from
    # leaving the KPI/filter/map stale.
    additive_exec_projects: set[str] = set()
    for path in sorted(DATA.glob("commercial-enrichment-v*.json")):
        doc = load(path)
        for project_id, payload in (doc.get("projects") or {}).items():
            if any(strict(r) for r in payload.get("relations") or []):
                additive_exec_projects.add(project_id)

    missing = sorted(
        project_id
        for project_id in additive_exec_projects
        if project_id in effective_by_id and not has_exec(effective_by_id[project_id])
    )
    assert not missing, f"A1/A2 execution evidence exists only in additive intelligence: {missing}"

    print(f"canonical projects: {len(projects)}")
    print(f"execution-covered before v0.7: {len(before)} projects / {before_mw:.2f} MW")
    print(f"execution-covered after v0.7: {len(after)} projects / {after_mw:.2f} MW")
    print(f"contractor gap after v0.7: {len(open_projects)} projects / {open_mw:.2f} MW")
    print(f"additive execution projects reconciled: {sorted(additive_exec_projects)}")


if __name__ == "__main__":
    main()
