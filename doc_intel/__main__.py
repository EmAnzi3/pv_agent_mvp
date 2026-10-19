"""CLI.

    python -m doc_intel run --url <record url> --store .doc_intel_store --data docs/data.json --out reports/doc_intel
    python -m doc_intel evaluate --dossier reports/doc_intel/mase-<id>.json --gold doc_intel/gold/mase-<id>.json
    python -m doc_intel render --dossier reports/doc_intel/mase-<id>.json
"""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

from .dossier import render
from .metrics import evaluate, load_gold
from .pipeline import run


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(prog="doc_intel")
    sub = ap.add_subparsers(dest="cmd", required=True)
    r = sub.add_parser("run")
    r.add_argument("--url", required=True)
    r.add_argument("--store", default=".doc_intel_store")
    r.add_argument("--data", default="docs/data.json")
    r.add_argument("--out", default="reports/doc_intel")
    r.add_argument("--no-ocr", action="store_true")
    r.add_argument("--ocr-max-pages", type=int, default=400)
    r.add_argument("--max-docs", type=int)
    r.add_argument("--workers", type=int, default=3)
    r.add_argument("--refresh", action="store_true")
    r.add_argument("--time-budget-min", type=float, help="stop starting OCR after N minutes; pending pages resume next run")
    r.add_argument("--gold")
    e = sub.add_parser("evaluate")
    e.add_argument("--dossier", required=True)
    e.add_argument("--gold", required=True)
    g = sub.add_parser("digest", help="emit ::notice annotations readable via the checks API")
    g.add_argument("--dossier", required=True)
    g.add_argument("--slice", type=int, default=0, help="GitHub allows 10 notices per step: run one step per slice")
    g.add_argument("--slices", type=int, default=1)
    m = sub.add_parser("render")
    m.add_argument("--dossier", required=True)
    a = ap.parse_args(argv)

    if a.cmd == "run":
        d = run(a.url, a.store, a.data if Path(a.data).exists() else None, ocr=not a.no_ocr,
                ocr_max_pages=a.ocr_max_pages, max_docs=a.max_docs, workers=a.workers, refresh=a.refresh,
                time_budget_s=a.time_budget_min * 60 if a.time_budget_min else None)
        if a.gold:
            d["evaluation"] = evaluate(d, load_gold(a.gold))
        out = Path(a.out)
        out.mkdir(parents=True, exist_ok=True)
        name = f"{d['project']['source']}-{d['project']['project_id']}"
        (out / f"{name}.json").write_text(json.dumps(d, ensure_ascii=False, indent=1), encoding="utf-8")
        (out / f"{name}.md").write_text(render(d), encoding="utf-8")
        summary = {"project": name, "review": d["review"]["status"], "acquisition_gate": d["gates"]["acquisition"],
                   "facts": len(d["facts"]), "conflicts": sum(g["status"] == "conflict" for g in d["groups"]),
                   "delta": {r["field"]: r["class"] for r in d["delta"]},
                   "evaluation": {k: d["evaluation"][k] for k in ("recall_value", "recall_anchored", "blocking")} if a.gold else None}
        print("DOC_INTEL_SUMMARY=" + json.dumps(summary, ensure_ascii=False))
        return 0
    if a.cmd == "evaluate":
        d = json.loads(Path(a.dossier).read_text(encoding="utf-8"))
        res = evaluate(d, load_gold(a.gold))
        print(json.dumps(res, ensure_ascii=False, indent=1))
        return 1 if res["blocking"] else 0
    if a.cmd == "digest":
        d = json.loads(Path(a.dossier).read_text(encoding="utf-8"))
        chunks = []
        for title, payload in digest(d):
            msg = json.dumps(payload, ensure_ascii=False)
            parts = [msg[i:i + 3900] for i in range(0, len(msg), 3900)] or ["null"]
            for k, part in enumerate(parts):
                chunks.append((f"{title}#{k + 1}of{len(parts)}", part))
        per = -(-len(chunks) // a.slices)
        mine = chunks[a.slice * per:(a.slice + 1) * per]
        if len(mine) > 10:
            print(f"::warning title=digest::slice {a.slice} has {len(mine)} chunks, only 10 shown; raise --slices")
        for title, part in mine[:10]:
            part = part.replace("%", "%25").replace("\r", "%0D").replace("\n", "%0A")
            print(f"::notice title={title}::{part}")
        return 0
    if a.cmd == "render":
        d = json.loads(Path(a.dossier).read_text(encoding="utf-8"))
        sys.stdout.write(render(d))
        return 0
    return 2


def digest(d: dict) -> list[tuple[str, dict]]:
    """Compact, size-bounded views of a dossier (annotations are the only CI output readable remotely)."""
    a = d["acquisition"]
    out = [("acquisition", {k: a[k] for k in a if k != "procedures"} | {
        "procedures": [{k: p.get(k) for k in ("id", "declared", "enumerated", "index_pages", "errors")} for p in a["procedures"]],
        "gate": d["gates"]["acquisition"], "efficiency": d["efficiency"]})]
    out.append(("delta", {"canonical": d["canonical"], "delta": d["delta"]}))
    conf = [{"field": g["field"], "subject": g["subject"], "values": [
        (v["value"], v["occurrences"], v["example"]["url"].rsplit("/", 1)[-1], v["example"]["page"]) if isinstance(v, dict) else v
        for v in g["values"][:6]], "hint": g.get("preference_hint")} for g in d["groups"] if g["status"] == "conflict"]
    out.append(("conflicts", {"n": len(conf), "groups": conf[:40]}))
    single = [{"field": g["field"], "subject": g["subject"], "value": g["values"][0]["value"],
               "occ": g["values"][0]["occurrences"], "doc": g["values"][0]["example"]["url"].rsplit("/", 1)[-1],
               "page": g["values"][0]["example"]["page"]} for g in d["groups"] if g["status"] == "single_value"]
    out.append(("single_values", {"n": len(single), "groups": single[:80]}))
    gates = {x["fact_id"]: x for x in d["gates"]["proposal"]}
    out.append(("proposal", [{"field": p["field"], "subject": p["subject"], "value": p["value"] if p["field"] != "geo.area_polygon"
                             else {k: p["value"][k] for k in ("crs", "vertex_count", "area_ha", "pages")},
                             "blocking": gates.get(p["fact_id"], {}).get("reasons")} for p in d["proposal"]][:80]))
    pts = {}
    for f in d["facts"]:
        if f["field"] in ("geo.utm_point", "geo.latlon_point"):
            q = f["qualifiers"]
            key = (f["citation"]["url"].rsplit("/", 1)[-1], q.get("object"), str((q.get("crs") or {}).get("zone") if isinstance(q.get("crs"), dict) else q.get("crs")))
            pts.setdefault(key, {"n": 0, "pages": set()})
            pts[key]["n"] += 1
            pts[key]["pages"].add(f["citation"]["page"])
    out.append(("geo_points", [{"doc": k[0], "object": k[1], "zone": k[2], "n": v["n"], "pages": sorted(v["pages"])[:15]}
                              for k, v in sorted(pts.items())][:80]))
    sched = [{"field": f["field"], "value": f["value"], "temporal": f["temporal"],
              "doc": f["citation"]["url"].rsplit("/", 1)[-1], "page": f["citation"]["page"],
              "excerpt": f["citation"]["excerpt"][:140]} for f in d["facts"] if f["field"].startswith("schedule.date")]
    out.append(("schedule_dates", sched[:60]))
    out.append(("coordinate_pages_without_points", d.get("diagnostics", [])[:12]))
    out.append(("unreadable_pages", [(u["url"].rsplit("/", 1)[-1], u["page"], u["method"]) for u in d.get("unreadable_pages", [])][:150]))
    vats = [{"value": f["value"], "subject": f["subject"], "assoc": f["qualifiers"].get("subject_association"),
             "doc": f["citation"]["url"].rsplit("/", 1)[-1], "page": f["citation"]["page"],
             "excerpt": f["citation"]["excerpt"][:120]} for f in d["facts"] if f["field"] == "company.vat"]
    seen, uniq = set(), []
    for v in vats:
        k = (v["value"], v["subject"], v["assoc"])
        if k not in seen:
            seen.add(k)
            uniq.append(v)
    out.append(("vat_occurrences", uniq[:40]))
    if d.get("evaluation"):
        out.append(("evaluation", d["evaluation"]))
    return out


if __name__ == "__main__":
    raise SystemExit(main())
