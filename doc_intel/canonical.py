"""Read-only link to the canonical PV Agent record and field-by-field delta.

Matching is by exact source URL (never by substring), as fixed in PR #4 for
the second MASE benchmark. Nothing here writes to ``docs/data.json``.
"""
from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Any

from .extract.entities import company_key

URL_KEYS = ("url", "source_url", "detail_url", "project_url", "link")


def _urls(row: dict) -> list[str]:
    vals = [row[k] for k in URL_KEYS if isinstance(row.get(k), str)]
    vals += [u for u in row.get("_dedupe_urls", []) if isinstance(u, str)]
    for s in row.get("sources", []) if isinstance(row.get("sources"), list) else []:
        if isinstance(s, dict):
            vals += [s[k] for k in URL_KEYS if isinstance(s.get(k), str)]
    return [v.strip().rstrip("/") for v in vals]


def load_records(path: str | Path) -> list[dict]:
    data = json.loads(Path(path).read_text(encoding="utf-8-sig"))
    return data["records"] if isinstance(data, dict) else data


def find_by_url(records: list[dict], url: str) -> dict:
    target = url.strip().rstrip("/")
    matches = [r for r in records if target in _urls(r)]
    keys = sorted({m.get("project_key") for m in matches})
    status = "unique" if len(matches) == 1 or (matches and len(keys) == 1) else ("missing" if not matches else "ambiguous")
    return {"status": status, "record": matches[0] if status == "unique" else None,
            "match_count": len(matches), "project_keys": keys, "policy": "exact source URL"}


def _split_municipalities(value: str | None) -> set[str]:
    if not value:
        return set()
    return {re.sub(r"\s*\([A-Z]{2}\)", "", p).strip().casefold() for p in re.split(r"[,;/]| e ", value) if p.strip()}


def compare(record: dict | None, groups: list[dict[str, Any]], project_meta: dict[str, Any]) -> list[dict[str, Any]]:
    """Compare canonical fields with source-backed groups; returns one row per field."""
    rows: list[dict[str, Any]] = []
    rec = record or {}
    by_field: dict[str, list[dict]] = {}
    for g in groups:
        by_field.setdefault(g["field"], []).append(g)

    def values(field: str, subjects: tuple | None = None) -> list[Any]:
        return [v["value"] for g in by_field.get(field, []) if subjects is None or g.get("subject") in subjects
                for v in g.get("values", []) if isinstance(v, dict)]

    def ranked(field: str) -> list[dict]:
        out = [{"value": v["value"], "occurrences": v["occurrences"], "example": v["example"]}
               for g in by_field.get(field, []) for v in g.get("values", []) if isinstance(v, dict)]
        return sorted(out, key=lambda x: -x["occurrences"])

    # proponent: the authority's project sheet is the registry value; documents give variants
    canon = rec.get("proponent")
    meta_prop = project_meta.get("proponent")
    doc_variants = ranked("role.proponent")
    if meta_prop:
        row = _row("proponent", canon, [meta_prop], key=company_key)
        row["source_kind"] = "authority project sheet"
    else:
        row = _row("proponent", canon, [v["value"] for v in doc_variants], key=company_key)
        row["source_kind"] = "documents"
    row["document_variants"] = doc_variants[:8]
    rows.append(row)

    # power: only plant-level (or unscoped) values are comparable with the canonical power
    canon_p = rec.get("power_mw")
    scopes = ("plant", "unknown", None)
    pv = {f: values(f, scopes) for f in ("tech.pv_dc_mwp", "tech.pv_ac_mw", "tech.nominal_power_mw")}
    flat = [x for v in pv.values() for x in v]
    row = _row("power_mw", canon_p, flat, numeric=True)
    row["detail"] = {k: sorted(set(v)) for k, v in pv.items() if v}
    if canon_p is not None and any(_close(canon_p, v) for v in flat):
        row["class"] = "confermato"
        which = [k for k, v in pv.items() if any(_close(canon_p, x) for x in v)]
        row["note"] = (f"Canonical value matches source field(s) {which}; canonical does not state DC/AC. "
                       "Other values listed in detail are different metrics or revisions, see conflicts.")
    rows.append(row)

    # municipalities: canonical vs official project sheet
    canon_m = _split_municipalities(rec.get("municipalities"))
    src_m = {m.casefold() for m in project_meta.get("municipalities", [])}
    if src_m:
        missing = sorted(src_m - canon_m)
        cls = "confermato" if not missing and canon_m <= src_m else ("nuovo_verificato" if canon_m <= src_m else "conflittuale")
        rows.append({"field": "municipalities", "canonical": rec.get("municipalities"),
                     "source": sorted(src_m), "class": cls,
                     "proposal": {"add": missing} if missing and cls == "nuovo_verificato" else None,
                     "evidence": project_meta.get("municipalities_citation")})
    else:
        rows.append({"field": "municipalities", "canonical": rec.get("municipalities"), "source": None,
                     "class": "non_determinabile", "proposal": None})
    for fld in ("province", "region"):
        src = project_meta.get(fld)
        rows.append(_row(fld, rec.get(fld), [src] if src else []))
    return rows


def _close(a: Any, b: Any) -> bool:
    try:
        return abs(float(a) - float(b)) < 1e-3
    except (TypeError, ValueError):
        return False


def _row(field: str, canon: Any, found: list[Any], key=None, numeric: bool = False) -> dict[str, Any]:
    def norm(x):
        if numeric:
            try:
                return round(float(x), 3)
            except (TypeError, ValueError):
                return None
        return key(x) if key else str(x).casefold().strip()
    distinct = []
    for v in found:
        if v is None or norm(v) in [norm(d) for d in distinct]:
            continue
        distinct.append(v)
    if not distinct:
        cls = "non_determinabile"
    elif canon in (None, "") and len(distinct) == 1:
        cls = "nuovo_verificato"
    elif canon in (None, ""):
        cls = "conflittuale"
    elif any(norm(v) == norm(canon) for v in distinct):
        cls = "confermato" if len(distinct) == 1 else "conflittuale"
    else:
        cls = "conflittuale"
    return {"field": field, "canonical": canon, "source": distinct, "class": cls,
            "proposal": {"set": distinct[0]} if cls == "nuovo_verificato" else None}
