"""Group facts, expose versions and conflicts — never pick a winner silently.

For each (field, subject) the reconciler lists every distinct value with its
first/last appearance, procedures and documents. More than one value is a
conflict unless the values are explicitly of different nature (e.g. planned vs
template dates) — those are kept as separate *variants* instead.

Cross-entity checks:
* the same VAT associated with two different companies -> conflict;
* one company with two different valid VATs -> conflict (e.g. Margisolar).

A ``preference_hint`` may be attached when exactly one value appears in an
authoritative act (decree/opinion/provvedimento), but the conflict stays open.
"""
from __future__ import annotations

import re
from collections import defaultdict
from typing import Any

from .extract.entities import company_key
from .schema import Fact

AUTHORITATIVE = re.compile(r"(?i)decreto|provvedimento|parere\s+(?:ctva|commissione)|determina|autorizzazione\s+unica")
NUMERIC_TOL = 0.005


def _subject_key(f: Fact) -> str:
    if f.field.startswith("role."):
        return ""  # all candidates for one role compete in a single group
    if f.subject and (f.field.startswith("company.") or f.field.startswith("contact.")):
        return company_key(f.subject)
    return (f.subject or "").casefold()


def _same_numeric(a: Any, b: Any) -> bool:
    try:
        return abs(float(a) - float(b)) <= NUMERIC_TOL * max(1.0, abs(float(a)))
    except (TypeError, ValueError):
        return False


def group(facts: list[Fact]) -> list[dict[str, Any]]:
    buckets: dict[tuple[str, str], list[Fact]] = defaultdict(list)
    for f in facts:
        if f.status == "rejected":
            continue
        if f.field in ("geo.utm_point", "geo.latlon_point"):
            continue  # points are aggregated by geometry, not by value
        buckets[(f.field, _subject_key(f))].append(f)
    groups = []
    for (field, subject), items in sorted(buckets.items()):
        values: list[dict[str, Any]] = []
        for f in items:
            match = None
            for v in values:
                if v["norm"] == f.normalized_value() or (
                        isinstance(f.value, (int, float)) and _same_numeric(v["value"], f.value)):
                    match = v
                    break
            if match is None:
                match = {"value": f.value, "norm": f.normalized_value(), "facts": [], "temporal": set()}
                values.append(match)
            match["facts"].append(f)
            if f.temporal:
                match["temporal"].add(f.temporal)
        out_values = []
        for v in values:
            cits = [f.citation for f in v["facts"]]
            out_values.append({
                "value": v["value"],
                "occurrences": len(v["facts"]),
                "documents": sorted({c.url for c in cits}),
                "procedures": sorted({c.procedure_id for c in cits if c.procedure_id}),
                "first_document_date": min((c.document_date for c in cits if c.document_date), default=None),
                "last_document_date": max((c.document_date for c in cits if c.document_date), default=None),
                "in_authoritative_act": any(AUTHORITATIVE.search(c.document_title or "") for c in cits),
                "temporal": sorted(v["temporal"]),
                "fact_ids": [f.fact_id() for f in v["facts"]],
                "example": {"url": cits[0].url, "page": cits[0].page, "excerpt": cits[0].excerpt[:240]},
            })
        out_values.sort(key=lambda x: -x["occurrences"])
        # Values of *different* temporal nature are variants, not contradictions
        natures = {t for v in out_values for t in v["temporal"]}
        conflict = len(out_values) > 1 and not (field.startswith("schedule.") and len(natures) > 1
                                                and all(len(v["temporal"]) == 1 for v in out_values))
        if field.startswith("schedule.duration.") and len(out_values) > 1:
            conflict = True  # different durations for the same scope must be explained
        hint = None
        auth = [v for v in out_values if v["in_authoritative_act"]]
        if conflict and len(auth) == 1:
            hint = {"value": auth[0]["value"], "reason": "only value present in an authoritative act",
                    "binding": False}
        gid = f"{field}|{subject}"
        for v in out_values:
            for f in items:
                if f.fact_id() in v["fact_ids"] and conflict:
                    f.conflict_group = gid
                    f.status = "conflicting"
        groups.append({"group_id": gid, "field": field, "subject": subject or None,
                       "status": "conflict" if conflict else ("single_value" if len(out_values) == 1 else "variants"),
                       "values": out_values, "preference_hint": hint})
    groups.extend(_cross_entity(facts))
    return groups


def _cross_entity(facts: list[Fact]) -> list[dict[str, Any]]:
    vat_to_company: dict[str, set[str]] = defaultdict(set)
    company_to_vat: dict[str, set[str]] = defaultdict(set)
    names: dict[str, str] = {}
    for f in facts:
        if f.field == "company.vat" and f.subject and f.status != "rejected":
            k = company_key(f.subject)
            names[k] = f.subject
            vat_to_company[f.value].add(k)
            company_to_vat[k].add(f.value)
    out = []
    for vat, comps in vat_to_company.items():
        if len(comps) > 1:
            out.append({"group_id": f"vat_shared|{vat}", "field": "company.vat_shared", "subject": vat,
                        "status": "conflict", "values": [names[c] for c in sorted(comps)],
                        "preference_hint": None,
                        "note": "Same VAT positionally linked to different companies: association error likely"})
    for comp, vats in company_to_vat.items():
        if len(vats) > 1:
            out.append({"group_id": f"multi_vat|{comp}", "field": "company.multiple_vat", "subject": names[comp],
                        "status": "conflict", "values": sorted(vats), "preference_hint": None,
                        "note": "One company with several valid-checksum VATs: registry check required"})
    return out
