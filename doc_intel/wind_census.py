"""Wind census: every live wind project filed on the MASE VIA portal, plus the GSE FER X wind plants that no
procedure of the census explains, in the same record format as the PV radar (docs/data.json).

It sits next to the curated wind radar (docs/wind/data, 51 verified projects): nothing is promoted into it.
Each census record says whether it is already a curated project ('curato'), whether it holds a FER X
incentive ('ferx') and the statuses of its MASE procedures. Projects whose procedures are all
'Archiviata' are left out (dead projects), as on the PV radar; side procedures (modifica, variante...)
never count as the project's death.

Output: wind/census.json on the memory branch (read by the wind page and by the card queue).
"""
from __future__ import annotations

import json
import re
from datetime import datetime, timezone
from pathlib import Path

from .ferx import _tokens, place_key, wind_projects

INFO_ID = re.compile(r"/Oggetti/Info/(\d+)")
SIDE_PROCEDURE = re.compile(r"(?i)\bmodific|\bvariant|valutazione\s+preliminare|art\.?\s*6,?\s*c(?:omma|\.)\s*9|"
                            r"ottemperanz|proroga")
POWER_RE = re.compile(r"(?i)(?<![\d.,])(\d{1,4}(?:[.,]\d{1,3})?)\s*(MW|kW)\b(?!h)")


def _power(text: str | None) -> float | None:
    """First plant power of a title: MW (or kW), never storage energy (MWh)."""
    prev = 0
    for m in POWER_RE.finditer(text or ""):
        before, prev = (text or "")[max(prev, m.start() - 40):m.start()].lower(), m.end()
        if re.search(r"accumul|storage|bess|batteri", before):
            continue
        v = float(m.group(1).replace(".", "").replace(",", ".") if re.search(r"\.\d{3}$", m.group(1)) else m.group(1).replace(",", "."))
        return round(v / 1000, 3) if m.group(2).lower() == "kw" else v
    return None


def _close(a: float | None, b: float | None, tol: float) -> bool:
    return bool(a and b and abs(a - b) / max(a, b) <= tol)


def build(raw: list[dict], curated: list[dict], ferx_rows: list[dict], today: str | None = None) -> dict:
    """raw: [{external_id, source_url, title, payload, statuses, procedures}] from MaseWindCollector."""
    today = today or datetime.now(timezone.utc).date().isoformat()
    cur_by_info = {}
    for p in curated:
        for oid in INFO_ID.findall(json.dumps(p.get("sources") or [])):
            cur_by_info[oid] = p
    records, archived = [], []
    for r in raw:
        pl = r.get("payload") or {}
        oid = pl.get("object_id") or (INFO_ID.search(r.get("source_url") or "") or [None, None])[1]
        title = pl.get("title") or r.get("title") or ""
        statuses = r.get("statuses") or []
        mun = pl.get("municipalities") or []
        rec = {
            "source": "mase", "source_group": "mase", "source_label": "MASE", "technology": "wind",
            "title": title, "proponent": pl.get("proponent"), "region": pl.get("region"),
            "province": pl.get("province"), "municipalities": ", ".join(mun),
            "project_type": pl.get("project_type_hint") or "Eolico",
            "procedure": pl.get("procedure"), "procedure_status": ", ".join(dict.fromkeys(statuses)),
            "procedures": r.get("procedures") or [],
            "power_mw": _power(title) or _power(pl.get("power")),
            "url": r.get("source_url"), "document_url": pl.get("document_url"),
            "date_presented": pl.get("date_presented"), "updated_at": today,
            "project_key": f"wind:mase_{oid}" if oid else f"wind:{r.get('external_id')}",
            "curato": None, "ferx": [],
        }
        if statuses and all(s == "Archiviata" for s in statuses) and not SIDE_PROCEDURE.search(title):
            archived.append(rec)
            continue
        # link to the curated radar: same MASE project page, else same municipality + compatible power
        p = cur_by_info.get(str(oid))
        if not p:
            mk = {place_key(m) for m in mun}
            p = next((c for c in curated if mk & {place_key(m) for m in c.get("municipalities") or []}
                      and _close(rec["power_mw"], float(c.get("mw") or 0), 0.30)), None)
        if p:
            rec["curato"] = {"id": p["id"], "nome": p["name"], "fase": p.get("stage_label"), "stage": p.get("stage")}
        records.append(rec)

    # FER X wind plants: on a census record (same municipality, same company, compatible power, or lots of
    # the same company adding up to it); otherwise a lead of its own unless the curated radar already has it
    leads = []
    unmatched = []
    for x in ferx_rows:
        mw, ck, words = (x.get("kw") or 0) / 1000, place_key(x.get("comune") or ""), _tokens(x.get("ragione_sociale") or "")
        cands = [r for r in records if ck in {place_key(m) for m in r["municipalities"].split(", ") if m}
                 and words & _tokens(r.get("proponent") or "")]
        best = min(cands, key=lambda r: abs((r["power_mw"] or 0) - mw), default=None)
        if best and _close(best["power_mw"], mw, 0.35):
            best["ferx"].append(_ferx_ref(x))
        else:
            unmatched.append(x)
    for r in records:                     # lots
        rest = [x for x in unmatched if place_key(x.get("comune") or "") in {place_key(m) for m in r["municipalities"].split(", ") if m}
                and _tokens(x.get("ragione_sociale") or "") & _tokens(r.get("proponent") or "")]
        if len(rest) >= 2 and _close(r["power_mw"], sum((x.get("kw") or 0) for x in rest) / 1000, 0.10):
            r["ferx"] += [_ferx_ref(x) for x in rest]
            unmatched = [x for x in unmatched if x not in rest]
    for x in unmatched:
        cur = (x.get("progetto") or {})
        leads.append({
            "source": "gse_ferx", "source_group": "gse", "source_label": "GSE FER X", "technology": "wind",
            "title": f"Impianto eolico {x.get('ragione_sociale')} – {x.get('comune')} (graduatoria FER X, procedura autorizzativa da individuare)",
            "proponent": x.get("ragione_sociale"), "region": x.get("regione"), "province": x.get("provincia"),
            "municipalities": x.get("comune") or "", "project_type": "Eolico", "procedure": None,
            "procedure_status": "", "procedures": [], "power_mw": round((x.get("kw") or 0) / 1000, 3),
            "url": "https://www.gse.it/servizi-per-te/fonti-rinnovabili/ferx/graduatorie", "updated_at": today,
            "project_key": f"wind:ferx_{x.get('codice')}",
            "curato": ({"id": cur["id"], "nome": cur["nome"], "fase": cur.get("fase")} if cur else None),
            "ferx": [_ferx_ref(x)],
        })
    out = records + leads
    out.sort(key=lambda r: -(r["power_mw"] or 0))
    return {
        "updated": today,
        "summary": {
            "progetti": len(out), "mw": round(sum(r["power_mw"] or 0 for r in out), 1),
            "da_mase": len(records), "lead_ferx": len(leads),
            "nel_radar_curato": sum(1 for r in out if r["curato"]),
            "con_ferx": sum(1 for r in out if r["ferx"]),
            "archiviati_esclusi": len(archived), "mw_archiviati": round(sum(r["power_mw"] or 0 for r in archived), 1),
        },
        "records": out,
    }


def _ferx_ref(x: dict) -> dict:
    return {k: x.get(k) for k in ("procedura", "posizione", "codice", "ragione_sociale", "comune", "kw", "kw_ammessi")} | {
        "procedura": "FER X transitorio – eolico (PC_EOL_2025_01)"}


def collect(max_pages: int | None = None) -> list[dict]:
    from app.collectors.mase_wind import MaseWindCollector
    c = MaseWindCollector()
    c.max_pages = max_pages
    res = c.fetch()
    out = []
    for r in res:
        info = c.info_cache.get(r.source_url) or {}
        out.append({"external_id": r.external_id, "source_url": r.source_url, "title": r.title, "payload": r.payload,
                    "statuses": info.get("statuses") or [], "procedures": info.get("procedures") or []})
    return out


def run(memory: str, repo_root: str = ".", raw_json: str | None = None, max_pages: int | None = None) -> dict:
    mem = Path(memory)
    raw = json.loads(Path(raw_json).read_text(encoding="utf-8")) if raw_json else collect(max_pages)
    wm = mem / "ferx" / "wind_matches.json"
    ferx_rows = json.loads(wm.read_text(encoding="utf-8")).get("impianti", []) if wm.exists() else []
    census = build(raw, wind_projects(Path(repo_root)), ferx_rows)
    (mem / "wind").mkdir(parents=True, exist_ok=True)
    (mem / "wind" / "census.json").write_text(json.dumps(census, ensure_ascii=False, indent=0), encoding="utf-8")
    (mem / "wind" / "census_raw.json").write_text(json.dumps(raw, ensure_ascii=False), encoding="utf-8")
    return census["summary"]
