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
                            r"ottemperanz|proroga|adeguamento\s+tecnico\s+(?:del|delle\s+opere)")
POWER_RE = re.compile(r"(?i)(?<![\d.,])(\d{1,4}(?:[.,]\d{1,3})?)\s*(MW|kW)p?(?![a-zA-Z])|\b(MW)\s*(\d{1,4}(?:[.,]\d{1,3})?)\b")


EXISTING = re.compile(r"(?i)attual|esistent|smantell|dismess|dismission|sostituit|da\s+rimuovere")
TOTAL = re.compile(r"(?i)complessiv|totale|immission|per\s+una\s+potenza|pari\s+complessivamente")
UNIT = re.compile(r"(?i)unitari|ciascun|cadauno|singol|(?:generator\w*|turbin\w*|WTG)[^.;]{0,45}(?:da|di|pari\s+a|fino\s+a|sino\s+a)\s*$")
COUNT = re.compile(r"(?i)\b(?:n[°.]?\s*)?(\d{1,3}|due|tre|quattro|cinque|sei|sette|otto|nove|dieci|undici|dodici)\s+(?:nuov[ie]\s+)?(?:aere?ogenerator\w*|turbin\w*|WTG)")
WORDS = {"due": 2, "tre": 3, "quattro": 4, "cinque": 5, "sei": 6, "sette": 7, "otto": 8, "nove": 9, "dieci": 10, "undici": 11, "dodici": 12}


def _power(text: str | None) -> float | None:
    """Plant power stated in a MASE title: the total ('potenza complessiva ... 72 MW') wins; a turbine rating
    ('10 aerogeneratori da 7,2 MW ciascuno') is multiplied by the number of turbines when no total is given;
    existing turbines being dismantled (repowering) and storage power (BESS, MWh) never count."""
    t = text or ""
    totals, units, plain = [], [], []
    prev = 0
    for m in POWER_RE.finditer(t):
        before, after = t[max(prev, m.start() - 70):m.start()], t[m.end():m.end() + 14]
        prev = m.end()
        raw, unit = (m.group(1), m.group(2)) if m.group(1) else (m.group(4), m.group(3))
        v = float(raw.replace(".", "").replace(",", ".") if re.search(r"\.\d{3}$", raw) else raw.replace(",", "."))
        if unit.upper() == "MW" and v > 3000 and re.search(r"\.\d{3}$", raw):
            v = float(raw)                   # '37.520 MW' written with a dot as decimal separator
        v = round(v / 1000, 3) if unit.lower() == "kw" else v
        seg = before[-45:]
        st = list(re.finditer(r"(?i)accumul|storage|bess|batteri", seg))
        if (st and not re.search(r"(?i)eolic|aerogenerat|wind", seg[st[-1].end():])) or EXISTING.search(before[-60:]):
            continue
        if v <= 22 and (UNIT.search(before[-55:]) and not TOTAL.search(before[-25:]) or
                        re.match(r"(?i)\s*(?:ciascun|cadaun|l'uno|per\s+(?:aerogeneratore|turbina))", after)):
            # a turbine rating (no turbine is larger than ~22 MW: a bigger value is the plant's power)
            units.append((v, before))
        elif TOTAL.search(before[-45:]):
            totals.append(v)
        else:
            plain.append(v)
    if totals:
        return totals[0]
    if units:
        v, before = units[0]
        c = None
        for cm in COUNT.finditer(t[:t.find(before) + len(before)] if before else t):
            if not EXISTING.search(t[max(0, cm.start() - 40):cm.start()]):
                c = cm.group(1)
        n = int(c) if c and c.isdigit() else WORDS.get((c or "").lower())
        if n and not plain:
            return round(v * n, 3)
        return plain[0] if plain else (round(v * n, 3) if n else None)
    return plain[0] if plain else None


NAME_GENERIC = {"wind", "eolico", "eolica", "parco", "repowering", "current", "off", "on", "farm", "energia", "della", "delle", "dell"}


def _name_words(name: str | None) -> set[str]:
    return {w for w in place_key(name or "").split() if len(w) >= 4 and w not in NAME_GENERIC}


def _places(r: dict) -> set[str]:
    return {place_key(m) for m in (r.get("municipalities") or "").split(", ") if m}


def _company(name: str | None, places: str | None) -> set[str]:
    """Distinctive words of a company name, without the place names it often carries ('Brindisi Santa Teresa')."""
    pw = set(place_key((places or "").replace(",", " ")).split())
    return {t for t in _tokens(name or "") if t not in pw and len(t) >= 3}


def _close(a: float | None, b: float | None, tol: float) -> bool:
    return bool(a and b and abs(a - b) / max(a, b) <= tol)


def build(raw: list[dict], curated: list[dict], ferx_rows: list[dict], today: str | None = None,
          regional: list[dict] | None = None) -> dict:
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
            "project_type": "Eolico offshore" if re.search(r"(?i)off-?shore|floating|galleggiant|flottant", title)
                            else (pl.get("project_type_hint") or "Eolico"),
            # a modification, variant, compliance check or extension of a project filed separately: listed, but
            # not counted as a project of its own in the totals
            "secondaria": bool(SIDE_PROCEDURE.search(title)),
            "procedure": pl.get("procedure"), "procedure_status": ", ".join(dict.fromkeys(statuses)),
            "procedures": r.get("procedures") or [],
            # the info page's first 'N MW' is a fallback only for titles without a power (and never a turbine rating)
            "power_mw": _power(title) or (lambda v: v if v and v >= 10 else None)(_power(pl.get("power"))),
            "url": r.get("source_url"), "document_url": pl.get("document_url"),
            "date_presented": pl.get("date_presented"), "updated_at": today,
            "project_key": f"wind:mase_{oid}" if oid else f"wind:{r.get('external_id')}",
            "curato": None, "ferx": [],
        }
        if statuses and all(s == "Archiviata" for s in statuses) and not SIDE_PROCEDURE.search(title):
            archived.append(rec)
            continue
        # link to the curated radar: same MASE project page, else the curated project's name in the title
        # plus a shared municipality and a compatible power (a shared municipality alone is not enough)
        p = cur_by_info.get(str(oid))
        if not p:
            mk, tk = {place_key(m) for m in mun}, set(place_key(title).split())
            p = next((c for c in curated if _name_words(c.get("name")) and _name_words(c.get("name")) <= tk
                      and mk & {place_key(m) for m in c.get("municipalities") or []}
                      and _close(rec["power_mw"], float(c.get("mw") or 0), 0.25)), None)
        if p:
            rec["curato"] = {"id": p["id"], "nome": p["name"], "fase": p.get("stage_label"), "stage": p.get("stage")}
        records.append(rec)

    added, merged = _add_regional(records, regional or [], today)

    # FER X wind plants: on a census record (same municipality, same company, compatible power, or lots of
    # the same company adding up to it); otherwise a lead of its own unless the curated radar already has it
    leads = []
    unmatched = []
    for x in ferx_rows:
        mw, ck = (x.get("kw") or 0) / 1000, place_key(x.get("comune") or "")
        cands = [r for r in records if ck in _places(r) and _company(x.get("ragione_sociale"), x.get("comune")) &
                 _company(r.get("proponent"), r["municipalities"])]
        sized = [r for r in cands if _close(r["power_mw"], mw, 0.35)]
        best = min(sized, key=lambda r: (bool(SIDE_PROCEDURE.search(r["title"])), abs((r["power_mw"] or 0) - mw)), default=None)
        if best:
            best["ferx"].append(_ferx_ref(x))
        elif len(cands) == 1 and not cands[0]["power_mw"]:
            cands[0]["ferx"].append(_ferx_ref(x, "comune + società (potenza non indicata nel titolo del procedimento)"))
        else:
            # regional lists without the company (Puglia open data): same municipality and nearly the same power,
            # only when a single record qualifies
            anon = [r for r in records if r["source"] != "mase" and not r.get("proponent") and ck in _places(r)
                    and _close(r["power_mw"], mw, 0.10)]
            if len(anon) == 1:
                anon[0]["ferx"].append(_ferx_ref(x, "comune + potenza (la Regione non indica il proponente)"))
            else:
                unmatched.append(x)
    for r in records:                     # lots
        rest = [x for x in unmatched if place_key(x.get("comune") or "") in _places(r)
                and _company(x.get("ragione_sociale"), x.get("comune")) & _company(r.get("proponent"), r["municipalities"])]
        if len(rest) >= 2 and _close(r["power_mw"], sum((x.get("kw") or 0) for x in rest) / 1000, 0.10):
            r["ferx"] += [_ferx_ref(x, f"lotti della stessa società ({len(rest)})") for x in rest]
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
            "ferx": [_ferx_ref(x, "graduatoria GSE (procedura autorizzativa da individuare)")],
        })
    out = records + leads
    for r in out:
        r["queue_priority"] = _queue_priority(r)
    out.sort(key=lambda r: -(r["power_mw"] or 0))
    return {
        "updated": today,
        "summary": {
            "progetti": sum(1 for r in out if not r.get("secondaria")),
            "mw": round(sum(r["power_mw"] or 0 for r in out if not r.get("secondaria")), 1),
            "procedure_secondarie": sum(1 for r in out if r.get("secondaria")),
            "mw_onshore": round(sum(r["power_mw"] or 0 for r in out if not r.get("secondaria") and r["project_type"] == "Eolico"), 1),
            "mw_offshore": round(sum(r["power_mw"] or 0 for r in out if not r.get("secondaria") and r["project_type"] != "Eolico"), 1),
            "da_mase": sum(1 for r in records if r["source"] == "mase"), "da_regioni": added,
            "regionali_gia_al_mase": merged, "regionali_trasmessi_non_vivi": getattr(_add_regional, "dropped", 0),
            "lead_ferx": len(leads),
            "nel_radar_curato": len({r["curato"]["id"] for r in out if r["curato"]}),
            "con_ferx": sum(1 for r in out if r["ferx"]),
            "archiviati_esclusi": len(archived), "mw_archiviati": round(sum(r["power_mw"] or 0 for r in archived), 1),
        },
        "records": out,
    }


REGION_LABEL = {"puglia_opendata": "Regione Puglia (open data VIA FER)", "sistema_puglia_energia": "Regione Puglia (Sistema Puglia)",
                "basilicata": "Regione Basilicata", "campania": "Regione Campania", "calabria": "Regione Calabria",
                "sardegna": "Regione Sardegna", "sicilia": "Regione Siciliana (SI-VVI)"}


REGIONAL_MIN_MW = 5.0


def _add_regional(records: list[dict], rows: list[dict], today: str) -> tuple[int, int]:
    """Regional wind procedures (onshore below 30 MW, PAUR, screening). One already on the MASE census or
    already added from another regional list (same municipality, power within 10 %, compatible company) is
    attached to that record as another source instead of becoming a second project."""
    added = merged = dropped = 0
    for r in rows:
        pl = r.get("payload") or {}
        title = pl.get("title") or r.get("title") or ""
        mun = pl.get("municipalities") or []
        mun = mun if isinstance(mun, list) else [m.strip() for m in str(mun).split(",") if m.strip()]
        power = _power(title) or pl.get("power_mw") or _power(pl.get("power"))
        try:
            power = float(power) if power else None
        except (TypeError, ValueError):
            power = None
        rec = {
            "source": r.get("source"), "source_group": "regione", "source_label": REGION_LABEL.get(r.get("source"), "Regione"),
            "technology": "wind", "title": title, "proponent": pl.get("proponent"), "region": pl.get("region"),
            "province": pl.get("province"), "municipalities": ", ".join(mun),
            "project_type": "Eolico offshore" if re.search(r"(?i)off-?shore|galleggiant", title) else "Eolico",
            "secondaria": bool(SIDE_PROCEDURE.search(title)),
            "procedure": pl.get("procedure"), "procedure_status": pl.get("status_raw") or "", "procedures": [],
            "power_mw": power, "url": r.get("source_url"), "document_url": pl.get("pdf_url") or pl.get("document_url"),
            "date_presented": pl.get("publication_date"), "updated_at": today,
            "project_key": "wind:" + re.sub(r"[^A-Za-z0-9_.:-]+", "_", str(r.get("external_id")))[:120],
            "curato": None, "ferx": [], "altre_fonti": [],
        }
        if not rec["municipalities"] and not power:
            continue
        if power is not None and power < REGIONAL_MIN_MW:
            continue                      # single small turbines (mini wind): not a building site for us
        twin = next((x for x in records if _places(x) & _places(rec) and _close(x["power_mw"], power, 0.10)
                     and (not rec["proponent"] or not x.get("proponent")
                          or _company(rec["proponent"], rec["municipalities"]) & _company(x.get("proponent"), x["municipalities"]))), None)
        if not twin and power and power >= 20:
            # a regional list naming one municipality of a national project (Puglia open data 'Trasmesso'):
            # same region, the same power to the decimal and the municipality cited in the national record
            twin = next((x for x in records if x["source"] == "mase" and x.get("region") == rec["region"]
                         and _close(x["power_mw"], power, 0.005)
                         and any(m in place_key(f"{x['title']} {x['municipalities']}") for m in _places(rec))), None)
        if not twin and rec["source"] == "puglia_opendata" and re.search(r"(?i)trasmess", rec["procedure_status"]):
            # 'Trasmesso': the Region forwarded it to the national VIA; MASE is the authority on whether it is
            # alive. Not found among the live MASE projects -> archived or renamed there: not added
            dropped += 1
            continue
        if twin:
            twin.setdefault("altre_fonti", []).append({"fonte": rec["source_label"], "url": rec["url"],
                                                       "procedura": rec["procedure"], "stato": rec["procedure_status"]})
            merged += 1
            continue
        records.append(rec)
        added += 1
    _add_regional.dropped = dropped
    return added, merged


def _queue_priority(r: dict) -> int:
    """Order of the nightly cards: what is closest to a building site first (offshore and scoping last)."""
    proc, st = r.get("procedure") or "", r.get("procedure_status") or ""
    if r.get("secondaria"):
        return 0
    if r.get("ferx"):
        return 4
    if re.search(r"(?i)ottemperanz", proc):
        return 3
    if r.get("project_type") != "Eolico" or re.search(r"(?i)definizione\s+(?:dei\s+)?contenuti|scoping|livello\s+elaborati", proc):
        return 0
    return 2 if re.search(r"(?i)conclus", st) else 1


def _ferx_ref(x: dict, basis: str = "comune + società + potenza") -> dict:
    return {k: x.get(k) for k in ("posizione", "codice", "ragione_sociale", "piva", "comune", "provincia", "kw", "kw_ammessi")} | {
        "procedura": "FER X transitorio – eolico (PC_EOL_2025_01)", "basis": basis}


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


def run(memory: str, repo_root: str = ".", raw_json: str | None = None, max_pages: int | None = None,
        regional_cloud: bool = True) -> dict:
    """MASE census + regional lists: those read in the cloud now (unless regional_cloud=False) and those read
    from the user's PC (docs/wind/data/census-regional-local.json, committed by aggiorna_wind_radar.bat)."""
    mem = Path(memory)
    raw = json.loads(Path(raw_json).read_text(encoding="utf-8")) if raw_json else collect(max_pages)
    cloud_file = mem / "wind" / "census_regional_cloud.json"
    if regional_cloud:
        from app.collectors.wind_regional import run as regional_run
        print("::notice title=wind-regional cloud::" + json.dumps(regional_run(where="cloud", out=str(cloud_file)), ensure_ascii=False)[:900])
    regional = []
    for f in (cloud_file, Path(repo_root) / "docs" / "wind" / "data" / "census-regional-local.json"):
        try:
            regional += json.loads(f.read_text(encoding="utf-8")).get("rows", [])
        except (OSError, ValueError):
            pass
    wm = mem / "ferx" / "wind_matches.json"
    ferx_rows = json.loads(wm.read_text(encoding="utf-8")).get("impianti", []) if wm.exists() else []
    census = build(raw, wind_projects(Path(repo_root)), ferx_rows, regional=regional)
    (mem / "wind").mkdir(parents=True, exist_ok=True)
    (mem / "wind" / "census.json").write_text(json.dumps(census, ensure_ascii=False, indent=0), encoding="utf-8")
    if not raw_json:
        (mem / "wind" / "census_raw.json").write_text(json.dumps(raw, ensure_ascii=False), encoding="utf-8")
    return census["summary"]
