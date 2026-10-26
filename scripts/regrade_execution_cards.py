"""Apply the execution-role evidence grading (doc_intel.card.execution_grade) to cards already built,
without re-reading the documents: roles '... (da confermare)' become '(atto di cantiere)' /
'(indicata negli elaborati)' or are dropped (proponent, utility, client), and the card gets the
'esecuzione' block. The index files are rebuilt. Usage: python scripts/regrade_execution_cards.py <schede dir>"""
from __future__ import annotations

import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from doc_intel.card import EXEC_NAME, _ckey, _execution_summary, execution_grade, execution_scope  # noqa: E402
from doc_intel.sync import LIVE_INDEX, build_index  # noqa: E402

OLD = {"EPC (da confermare)": "role.epc", "Impresa esecutrice (da confermare)": "role.contractor", "BoP (da confermare)": "role.bop"}


def regrade(card: dict) -> bool:
    ents = (card.get("soggetti") or []) + (card.get("altri_soggetti") or [])
    props = {_ckey(e["name"]) for e in ents if "Proponente" in (e.get("roles") or [])}
    changed = False
    for e in ents:
        roles = e.get("roles") or []
        if not any(r in OLD for r in roles):
            continue
        new_roles, execution = [], []
        for r in roles:
            if r not in OLD:
                new_roles.append(r)
                continue
            field = OLD[r]
            grade = execution_grade(e["name"], e.get("source"), props)
            if grade:
                txt = f"{EXEC_NAME[field]} ({'atto di cantiere' if grade == 'A' else 'indicata negli elaborati'})"
                if txt not in new_roles:
                    new_roles.append(txt)
                execution.append({"ruolo": EXEC_NAME[field], "evidenza": grade,
                                  "ambito": execution_scope(e.get("source")), "source": e.get("source")})
        e["roles"], e["esecuzione"] = new_roles, execution or None
        changed = True
    if changed or "esecuzione" not in card:
        card["esecuzione"] = _execution_summary(ents)
        card["da_verificare"] = [o for o in card.get("da_verificare") or []
                                 if not (o.get("reason", "").startswith("ruolo esecutivo")
                                         and not any("indicata negli elaborati" in r for e in ents
                                                     if e["name"] == o.get("label") for r in e.get("roles") or []))]
        return True
    return False


def main(folder: str) -> int:
    d = Path(folder)
    n = epc = 0
    for f in sorted(d.glob("*.json")):
        if f.name in ("index.json", LIVE_INDEX):
            continue
        c = json.loads(f.read_text(encoding="utf-8"))
        if regrade(c):
            f.write_text(json.dumps(c, ensure_ascii=False, indent=1), encoding="utf-8")
            n += 1
        epc += bool(c.get("esecuzione"))
    idx = build_index(d)
    (d / LIVE_INDEX).write_text(json.dumps(idx, ensure_ascii=False, indent=0, sort_keys=True), encoding="utf-8")
    if (d / "index.json").exists():
        (d / "index.json").write_text(json.dumps(idx, ensure_ascii=False, indent=0, sort_keys=True), encoding="utf-8")
    print(f"[regrade] schede aggiornate: {n}; con impresa esecutrice: {epc}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1]))
