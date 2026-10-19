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
    r.add_argument("--gold")
    e = sub.add_parser("evaluate")
    e.add_argument("--dossier", required=True)
    e.add_argument("--gold", required=True)
    m = sub.add_parser("render")
    m.add_argument("--dossier", required=True)
    a = ap.parse_args(argv)

    if a.cmd == "run":
        d = run(a.url, a.store, a.data if Path(a.data).exists() else None, ocr=not a.no_ocr,
                ocr_max_pages=a.ocr_max_pages, max_docs=a.max_docs, workers=a.workers, refresh=a.refresh)
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
    if a.cmd == "render":
        d = json.loads(Path(a.dossier).read_text(encoding="utf-8"))
        sys.stdout.write(render(d))
        return 0
    return 2


if __name__ == "__main__":
    raise SystemExit(main())
