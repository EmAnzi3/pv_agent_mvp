"""Wind change report: what changed in the wind census since the previous one (same idea as the PV report).

Run after the census and the readiness (wind-census.yml). It compares a compact snapshot of the census
(plus readiness, cards, FER X, press) with the baseline saved on the memory branch:

* wind/changes/baseline.json  snapshot of the last census compared (rolled forward at every run)
* wind/changes/latest.json    the most recent report that found changes (read by docs/wind/cambiamenti.html)
* wind/changes/<date>.json    one report per day with changes (last KEEP kept)
* wind/changes/index.json     list of the saved reports, newest first

A census that lost too many projects (portal down, partial reading) does not move the baseline: the
report is marked incomplete, so the next good census is still compared with the last good one.
"""
from __future__ import annotations

import json
import subprocess
from datetime import datetime, timezone
from pathlib import Path

KEEP = 26                     # about six months of weekly reports
MIN_SHARE = 0.8               # a census with fewer than 80% of the previous projects is treated as partial

# fields compared, with the label shown in the report; title and links are cosmetic and not compared
FIELDS = {
    "power_mw": "MW",
    "proponent": "Proponente",
    "project_type": "Tipo",
    "procedure": "Procedura",
    "procedure_status": "Stato procedura",
    "verified_phase": "Fase verificata",
    "contractor": "Contractor verificati",
    "ferx": "FER X",
    "readiness": "Priorità cantiere",
    "card": "Scheda documenti",
    "epc": "EPC nei documenti",
    "news": "Notizie di stampa",
}
# changes that bring a project closer to the site: shown first, as «segnali di avanzamento»
LEVELS = {None: 0, "": 0, "Bassa": 1, "Media": 2, "Alta": 3}
CLOSED = ("conclusa", "approvato", "autorizzazione unica", "parere ctpnrr-pniec emesso", "construction",
          "non necessita di ulteriori valutazioni")


def _load(path: Path) -> dict:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return {}


def _git_head(mem: Path, rel: str) -> dict:
    """The version of a memory file saved by the previous run (before this run overwrote it)."""
    try:
        out = subprocess.run(["git", "-C", str(mem), "show", f"HEAD:{rel}"], capture_output=True, timeout=60)
        return json.loads(out.stdout.decode("utf-8")) if out.returncode == 0 else {}
    except (OSError, ValueError, subprocess.SubprocessError):
        return {}


def snapshot(census: dict, readiness: dict, cards: dict, news: dict) -> dict:
    """Compact view of each census project, with only the fields the report compares and shows."""
    rd, nw = (readiness or {}).get("projects", {}), (news or {}).get("projects", {})
    out = {}
    for r in census.get("records", []):
        key = r.get("project_key")
        if not key:
            continue
        cur, x, card = r.get("curato") or {}, rd.get(key) or {}, (cards or {}).get(key) or {}
        out[key] = {
            "title": (r.get("title") or "")[:240],
            "region": r.get("region") or "",
            "province": r.get("province") or "",
            "municipalities": r.get("municipalities") or "",
            "source_label": r.get("source_label") or "",
            "url": r.get("url") or "",
            "secondaria": bool(r.get("secondaria")),
            "power_mw": r.get("power_mw"),
            "proponent": r.get("proponent") or "",
            "project_type": r.get("project_type") or "",
            "procedure": r.get("procedure") or "",
            "procedure_status": r.get("procedure_status") or "",
            "verified_phase": cur.get("fase") or "",
            "contractor": sorted({(c.get("nome") if isinstance(c, dict) else str(c)) or "" for c in cur.get("contractor") or []} - {""}),
            "ferx": sorted({f.get("codice") or "" for f in r.get("ferx") or []} - {""}),
            "readiness": f"{x['level']} · {x.get('phase_label') or ''}".strip(" ·") if x.get("level") else "",
            "card": bool(card),
            "epc": _epc(card.get("epc")),
            "news": len(nw.get(key) or []) if isinstance(nw.get(key), list) else (1 if nw.get(key) else 0),
        }
    return out


EPC_LABEL = {"A": "in atto di cantiere", "B": "solo negli elaborati"}


def _epc(e) -> str:
    """The card index keeps the strongest evidence of a builder: A (site deed) or B (design documents)."""
    if not e:
        return ""
    if isinstance(e, str):
        return EPC_LABEL.get(e, e)
    if isinstance(e, dict):
        return EPC_LABEL.get(e.get("level") or "", "") or e.get("name") or ""
    return ", ".join(sorted({_epc(i) for i in e} - {""}))


def _same(field: str, a, b) -> bool:
    if field == "power_mw":
        try:
            return abs(float(a or 0) - float(b or 0)) < 0.05
        except (TypeError, ValueError):
            return a == b
    if isinstance(a, str) and isinstance(b, str):
        return " ".join(a.lower().split()) == " ".join(b.lower().split())
    return a == b


def _progress(field: str, old, new) -> bool:
    if field == "readiness":
        return LEVELS.get((new or "").split(" · ")[0], 0) > LEVELS.get((old or "").split(" · ")[0], 0)
    if field in ("ferx", "contractor"):
        return bool(set(new or []) - set(old or []))
    if field == "card":
        return bool(new) and not old
    if field == "epc":
        return (new or "") == EPC_LABEL["A"] != (old or "") or (bool(new) and not old)
    if field == "news":
        return (new or 0) > (old or 0)
    if field == "procedure_status":
        n, o = (new or "").lower(), (old or "").lower()
        return any(c in n for c in CLOSED) and not any(c in o for c in CLOSED)
    if field == "verified_phase":
        return bool(new) and new != old
    return False


def _row(key: str, s: dict) -> dict:
    keep = ("title", "region", "province", "municipalities", "source_label", "url", "power_mw", "proponent",
            "project_type", "procedure_status", "readiness", "secondaria")
    return {"project_key": key, **{k: s.get(k) for k in keep}}


def compare(prev: dict, cur: dict) -> dict:
    new = [_row(k, cur[k]) for k in cur if k not in prev]
    removed = [_row(k, prev[k]) for k in prev if k not in cur]
    changed = []
    for k in cur.keys() & prev.keys():
        diff = [{"field": f, "label": lab, "old": prev[k].get(f), "new": cur[k].get(f),
                 "progress": _progress(f, prev[k].get(f), cur[k].get(f))}
                for f, lab in FIELDS.items() if not _same(f, prev[k].get(f), cur[k].get(f))]
        if diff:
            changed.append({**_row(k, cur[k]), "changes": diff, "progress": any(d["progress"] for d in diff)})
    mw = lambda rows: round(sum(float(r.get("power_mw") or 0) for r in rows), 1)
    on = lambda rows: [r for r in rows if "offshore" not in (r.get("project_type") or "").lower()]
    new.sort(key=lambda r: -(r.get("power_mw") or 0))
    removed.sort(key=lambda r: -(r.get("power_mw") or 0))
    changed.sort(key=lambda r: (not r["progress"], -(r.get("power_mw") or 0)))
    return {
        "counts": {"nuovi": len(new), "rimossi": len(removed), "modificati": len(changed),
                   "avanzamenti": sum(1 for c in changed if c["progress"]),
                   "precedenti": len(prev), "attuali": len(cur)},
        "mw": {"nuovi": mw(new), "rimossi": mw(removed), "nuovi_onshore": mw(on(new)), "rimossi_onshore": mw(on(removed))},
        "nuovi": new, "rimossi": removed, "modificati": changed,
    }


def run(memory: str | Path, today: str | None = None) -> dict:
    mem = Path(memory)
    out = mem / "wind" / "changes"
    out.mkdir(parents=True, exist_ok=True)
    census = _load(mem / "wind" / "census.json")
    cur = snapshot(census, _load(mem / "readiness" / "wind_index.json"), _load(mem / "schede" / "live_index.json"),
                   _load(mem / "news" / "wind_matches.json"))
    now = datetime.now(timezone.utc)
    today = today or now.date().isoformat()
    base_file = out / "baseline.json"
    base = _load(base_file)
    if not base.get("projects"):
        # first run: the census saved by the previous run (memory branch HEAD) is the baseline
        prev_c = _git_head(mem, "wind/census.json")
        if prev_c.get("records"):
            base = {"census_updated": prev_c.get("updated") or "",
                    "projects": snapshot(prev_c, _git_head(mem, "readiness/wind_index.json"),
                                         _git_head(mem, "schede/live_index.json"), _git_head(mem, "news/wind_matches.json"))}
    if not base.get("projects"):
        base_file.write_text(json.dumps({"census_updated": census.get("updated") or today, "projects": cur},
                                        ensure_ascii=False, indent=0), encoding="utf-8")
        return {"baseline": "created", "projects": len(cur)}
    prev = base["projects"]
    if not cur or len(cur) < MIN_SHARE * len(prev):
        print(f"::warning title=wind-changes::censimento parziale ({len(cur)} progetti contro {len(prev)}): confronto sospeso, base invariata")
        return {"partial": True, "attuali": len(cur), "precedenti": len(prev)}
    rep = compare(prev, cur)
    rep.update({"generated": now.strftime("%Y-%m-%dT%H:%M:%SZ"), "date": today,
                "previous_census": base.get("census_updated") or "", "census": census.get("updated") or today})
    base_file.write_text(json.dumps({"census_updated": census.get("updated") or today, "projects": cur},
                                    ensure_ascii=False, indent=0), encoding="utf-8")
    c = rep["counts"]
    if not (c["nuovi"] or c["rimossi"] or c["modificati"]):
        return {"changes": 0, **c}
    # several censuses in one day (PC push + Monday run): the day's report sums them
    day_file = out / f"{today}.json"
    old = _load(day_file)
    if old.get("counts"):
        rep = _merge_day(old, rep)
    for f in (day_file, out / "latest.json"):
        f.write_text(json.dumps(rep, ensure_ascii=False, indent=0), encoding="utf-8")
    idx = [e for e in _load(out / "index.json").get("reports", []) if e.get("date") != today]
    idx.insert(0, {"date": today, "previous_census": rep["previous_census"], **rep["counts"]})
    for e in idx[KEEP:]:
        (out / f"{e['date']}.json").unlink(missing_ok=True)
    (out / "index.json").write_text(json.dumps({"reports": idx[:KEEP]}, ensure_ascii=False, indent=0), encoding="utf-8")
    c = rep["counts"]
    return {"changes": c["nuovi"] + c["rimossi"] + c["modificati"], **c}


def _merge_day(old: dict, rep: dict) -> dict:
    """Two censuses on the same day: compare the start of the day with the end, through the two reports."""
    start = {r["project_key"]: r for r in old["rimossi"]}
    # projects removed earlier today and back now are not news; projects new today stay new
    new = {r["project_key"]: r for r in old["nuovi"] if r["project_key"] not in {x["project_key"] for x in rep["rimossi"]}}
    for r in rep["nuovi"]:
        if r["project_key"] in start:
            start.pop(r["project_key"])
        else:
            new[r["project_key"]] = r
    removed = dict(start)
    for r in rep["rimossi"]:
        if r["project_key"] not in {x["project_key"] for x in old["nuovi"]}:
            removed[r["project_key"]] = r
    changed = {c["project_key"]: c for c in old["modificati"]}
    for c in rep["modificati"]:
        if c["project_key"] in changed:
            prev = {d["field"]: d for d in changed[c["project_key"]]["changes"]}
            for d in c["changes"]:
                d["old"] = prev[d["field"]]["old"] if d["field"] in prev else d["old"]
                d["progress"] = _progress(d["field"], d["old"], d["new"])
                prev[d["field"]] = d
            c["changes"] = [d for d in prev.values() if not _same(d["field"], d["old"], d["new"])]
            c["progress"] = any(d["progress"] for d in c["changes"])
        changed[c["project_key"]] = c
    for k, c in list(changed.items()):
        if k in new:                                  # new earlier today, changed later: still new, latest data
            new[k] = {f: v for f, v in c.items() if f not in ("changes", "progress")}
    changed = {k: v for k, v in changed.items() if v["changes"] and k not in new and k not in removed}
    out = dict(rep)
    out["previous_census"] = old.get("previous_census") or rep.get("previous_census")
    out["nuovi"] = sorted(new.values(), key=lambda r: -(r.get("power_mw") or 0))
    out["rimossi"] = sorted(removed.values(), key=lambda r: -(r.get("power_mw") or 0))
    out["modificati"] = sorted(changed.values(), key=lambda r: (not r["progress"], -(r.get("power_mw") or 0)))
    mw = lambda rows: round(sum(float(r.get("power_mw") or 0) for r in rows), 1)
    on = lambda rows: [r for r in rows if "offshore" not in (r.get("project_type") or "").lower()]
    out["counts"] = {**rep["counts"], "nuovi": len(out["nuovi"]), "rimossi": len(out["rimossi"]),
                     "modificati": len(out["modificati"]),
                     "avanzamenti": sum(1 for c in out["modificati"] if c["progress"]),
                     "precedenti": old["counts"].get("precedenti", rep["counts"]["precedenti"])}
    out["mw"] = {"nuovi": mw(out["nuovi"]), "rimossi": mw(out["rimossi"]),
                 "nuovi_onshore": mw(on(out["nuovi"])), "rimossi_onshore": mw(on(out["rimossi"]))}
    return out
