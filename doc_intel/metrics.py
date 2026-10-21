"""Precision / recall against a verified gold set.

Gold file format (JSON)::

    {"project_url": "...", "items": [
       {"field": "company.vat", "subject": "Troia Sun S.r.l.", "value": "10148261216",
        "document": "1126265", "page": 1, "category": "companies"}],
     "exhaustive_fields": ["company.vat"],          # FP counted only here
     "forbidden": [{"field": "role.epc"}]}          # any non-rejected fact = blocking error

Two recall levels: *value* (right value somewhere) and *anchored* (right value
on the gold document/page). Precision is computed only on exhaustive fields,
where the gold set lists every correct value.
"""
from __future__ import annotations

import json
from collections import defaultdict
from pathlib import Path
from typing import Any

from .extract.entities import company_key


def _eq(a: Any, b: Any) -> bool:
    try:
        fa, fb = float(a), float(b)
        return abs(fa - fb) <= 1e-3 * max(1.0, abs(fb))
    except (TypeError, ValueError):
        pass
    if isinstance(a, dict) and "min" in a:
        return _eq(a.get("max"), b) or _eq(a.get("min"), b)
    return company_key(str(a)) == company_key(str(b))


def evaluate(dossier: dict[str, Any], gold: dict[str, Any]) -> dict[str, Any]:
    facts = [f for f in dossier["facts"] if f.get("status") != "rejected"]
    per_cat: dict[str, dict[str, int]] = defaultdict(lambda: {"gold": 0, "value_hit": 0, "anchored_hit": 0})
    misses = []
    for item in gold["items"]:
        cat = item.get("category", item["field"].split(".")[0])
        per_cat[cat]["gold"] += 1
        cands = [f for f in facts if f["field"] == item["field"] and _eq(f["value"], item["value"])]
        if item.get("subject"):
            cands = [f for f in cands if not f.get("subject") or company_key(f["subject"]) == company_key(item["subject"])
                     or item["field"].startswith(("tech.", "schedule.", "grid."))]
        if cands:
            per_cat[cat]["value_hit"] += 1
            doc = str(item.get("document", ""))
            if any(f["citation"]["url"].endswith("/" + doc) and (item.get("page") in (None, f["citation"]["page"]))
                   for f in cands):
                per_cat[cat]["anchored_hit"] += 1
        else:
            misses.append(item)
    precision = {}
    for field in gold.get("exhaustive_fields", []):
        golds = [i for i in gold["items"] if i["field"] == field]
        got = [f for f in facts if f["field"] == field]
        tp = sum(1 for f in got if any(_eq(f["value"], g["value"]) for g in golds))
        precision[field] = {"extracted": len(got), "correct": tp,
                            "precision": round(tp / len(got), 3) if got else None,
                            "false_positives": [f["value"] for f in got if not any(_eq(f["value"], g["value"]) for g in golds)][:20]}
    forbidden_hits = []
    for rule in gold.get("forbidden", []):
        forbidden_hits += [{"field": f["field"], "value": f["value"], "url": f["citation"]["url"]}
                           for f in facts if f["field"] == rule["field"]]
    expected = []
    for exp in gold.get("expected_conflicts", []):
        hit = any(g["field"] == exp["field"] and g.get("status") == "conflict" and
                  (not exp.get("subject") or company_key(str(g.get("subject") or "")) == company_key(exp["subject"]))
                  for g in dossier.get("groups", []))
        expected.append({**exp, "detected": hit})
    cats = {c: {**v, "recall_value": round(v["value_hit"] / v["gold"], 3),
                "recall_anchored": round(v["anchored_hit"] / v["gold"], 3)} for c, v in per_cat.items()}
    total = sum(v["gold"] for v in per_cat.values())
    return {"categories": cats, "precision": precision,
            "recall_value": round(sum(v["value_hit"] for v in per_cat.values()) / total, 3) if total else None,
            "recall_anchored": round(sum(v["anchored_hit"] for v in per_cat.values()) / total, 3) if total else None,
            "misses": misses, "forbidden_hits": forbidden_hits, "expected_conflicts": expected,
            "blocking": bool(forbidden_hits) or any(not e["detected"] for e in expected)}


def load_gold(path: str | Path) -> dict[str, Any]:
    return json.loads(Path(path).read_text(encoding="utf-8"))
