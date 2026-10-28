"""Regional wind census: the PV radar's regional collectors re-run with a wind filter.

Each regional collector keeps its keyword lists at module level (PV_KEYWORDS, SEARCH_KEYWORDS, ...); for
the wind census they are swapped for wind keywords for the duration of one run, and the few methods that
are PV-specific (municipality whitelists, project type) are replaced on the instance. The PV radar is not
touched: the modules are restored when the run ends.

Where each source runs
  cloud   read by the weekly wind census on GitHub (wind-census.yml)
  local   read from the user's PC by aggiorna_wind_radar.bat (portals that refuse foreign IPs). Sicily is read
          from the company network, gently: one CSV download, project pages only for new wind projects,
          a pause of several seconds between pages and a daily cap (see app/collectors/sicilia.py).

Output rows have the same shape as the MASE census rows (doc_intel/wind_census.py builds the records).
"""
from __future__ import annotations

import contextlib
import importlib
import json
import os
import re
import tempfile
import time
from datetime import datetime
from pathlib import Path
from typing import Any

WIND = ["eolico", "eolica", "eolici", "eoliche", "aerogenerator", "aereogenerator", "parco eolico", "impianto eolico"]
WIND_RE = re.compile(r"(?i)eolic|aer[e]?ogenerator")
NOT_WIND = re.compile(r"(?i)mini[- ]?eolic|micro[- ]?eolic|anemometr|torre\s+anemometrica")


def _without_wind(words: list[str]) -> list[str]:
    return [w for w in words if not re.search(r"(?i)eolic|offshore", w)]


def _locate(text: str, region: str) -> tuple[list[str], str | None]:
    """Municipalities of one region named in a text (ISTAT list), longest names first; their province."""
    from doc_intel.admin_areas import _key, _table
    t = f" {_key(text)} "
    found = []
    for key, rows in sorted(_table().items(), key=lambda kv: -len(kv[0])):
        rows = [r for r in rows if r[2].lower().startswith(region.lower()[:6])]
        if rows and len(key) >= 4 and f" {key} " in t:
            found.append(rows[0])
            t = t.replace(f" {key} ", " | ")
    provinces = list(dict.fromkeys(r[1] for r in found))
    return [r[0] for r in found][:10], (provinces[0] if provinces else None)


SPECS: dict[str, dict[str, Any]] = {
    "puglia_opendata": {"module": "app.collectors.puglia", "cls": "PugliaCollector", "region": "Puglia", "where": "cloud",
                        "patch": {"PV_SOURCES": ["eolic"]}},
    "campania": {"module": "app.collectors.campania", "cls": "CampaniaCollector", "region": "Campania", "where": "cloud",
                 # wind projects stay in the regional procedure for years: older filings are still live leads
                 "patch": {"SEARCH_KEYWORDS": ["eolico", "eolica"], "PV_KEYWORDS": WIND, "MIN_DATE": datetime(2019, 1, 1)},
                 "strip": ["NON_PV_EXCLUDE"]},
    "calabria": {"module": "app.collectors.calabria", "cls": "CalabriaCollector", "region": "Calabria", "where": "cloud",
                 "patch": {"SEARCH_TERMS": ["impianto eolico", "parco eolico", "VIA eolico"], "PV_KEYWORDS": WIND}},
    "sardegna": {"module": "app.collectors.sardegna", "cls": "SardegnaCollector", "region": "Sardegna", "where": "cloud",
                 "patch": {"SEARCH_KEYWORDS": ["eolico"], "PV_KEYWORDS": WIND,
                           "SEARCH_YEARS": [str(y) for y in range(datetime.now().year, 2019, -1)]},
                 "strip": ["NON_PV_EXCLUDE"]},
    "sistema_puglia_energia": {"module": "app.collectors.sistema_puglia_energia", "cls": "SistemaPugliaEnergiaCollector",
                               "region": "Puglia", "where": "local", "patch": {"PV_KEYWORDS": WIND},
                               "strip": ["EXCLUDE_KEYWORDS"]},
    "basilicata": {"module": "app.collectors.basilicata", "cls": "BasilicataCollector", "region": "Basilicata", "where": "local",
                   "patch": {"PV_KEYWORDS": WIND}, "strip": ["EXCLUDE_KEYWORDS"]},
    "sicilia": {"module": "app.collectors.sicilia", "cls": "SiciliaCollector", "region": "Sicilia", "where": "local",
                "patch": {"COMMERCIAL_PV_KEYWORDS": WIND, "BESS_KEYWORDS": []}},
}


@contextlib.contextmanager
def _patched(mod, values: dict[str, Any]):
    old = {k: getattr(mod, k) for k in values if hasattr(mod, k)}
    try:
        for k, v in values.items():
            setattr(mod, k, v)
        yield
    finally:
        for k, v in old.items():
            setattr(mod, k, v)


@contextlib.contextmanager
def _debug_dir(mod):
    """Some collectors write debug files under /app/reports (the Docker layout): send them to a temp folder
    where that path is not writable (GitHub runners)."""
    real = getattr(mod, "Path", None)
    if real is None or os.access("/app", os.W_OK) or (os.name == "nt"):
        yield
        return
    tmp = tempfile.mkdtemp(prefix="wind_regional_")
    setattr(mod, "Path", lambda *a: real(*[str(x).replace("/app/reports", tmp) for x in a]))
    try:
        yield
    finally:
        setattr(mod, "Path", real)


def _windify(name: str, col, region: str) -> None:
    """Instance-level replacements of the PV-specific methods."""
    def kind(text, *a, **k):
        return "Eolico offshore" if re.search(r"(?i)off-?shore|galleggiant|floating", str(text or "")) else "Eolico"
    if hasattr(col, "_infer_project_type"):
        col._infer_project_type = kind
    if name == "basilicata":
        col._extract_municipalities = lambda text: _locate(text or "", region)[0]
        col._extract_province = lambda text, municipalities: _locate(" ".join(municipalities or []) or (text or ""), region)[1]
    if name == "sicilia":
        col.wind_mode = True                       # gentle detail reading (see SiciliaCollector._fetch_detail_info)


LAST_STATS: dict[str, dict] = {}


def _trace(col, method: str, stats: dict) -> None:
    """Record the errors a collector swallows (a portal refusing the request looks like 'no results')."""
    f = getattr(col, method, None)
    if not f:
        return

    def wrapped(*a, **k):
        try:
            out = f(*a, **k)
            stats.setdefault("calls", 0)
            stats["calls"] += 1
            if isinstance(out, (list, str)):          # result sizes: an empty page vs a page with rows
                stats.setdefault(f"{method}_sizes", []).append(len(out))
                stats[f"{method}_sizes"] = stats[f"{method}_sizes"][-6:]
            if isinstance(out, str) and method == "_post_search":
                txt = re.sub(r"<[^>]+>", " ", out)
                stats["post_text"] = re.sub(r"\s+", " ", txt)[:260]
            return out
        except Exception as exc:
            stats.setdefault("errors", []).append(f"{type(exc).__name__}: {str(exc)[:160]}")
            raise
    setattr(col, method, wrapped)


def collect(name: str) -> list[dict]:
    spec = SPECS[name]
    stats = LAST_STATS.setdefault(name, {})
    mod = importlib.import_module(spec["module"])
    values = dict(spec.get("patch") or {})
    for attr in spec.get("strip") or []:
        values[attr] = _without_wind(getattr(mod, attr))
    with _patched(mod, values), _debug_dir(mod):
        col = getattr(mod, spec["cls"])()
        _windify(name, col, spec["region"])
        for m in ("_post_search", "_get_html", "_fetch_search", "_get_search_form", "_parse_results_table"):
            _trace(col, m, stats)
        res = col.fetch()
    stats["collector_rows"] = len(res)
    out = []
    for r in res:
        pl = dict(r.payload or {})
        # wind must be the subject of the title (a PV report citing nearby wind farms is not a wind project)
        text = f"{r.title} {pl.get('title') or ''} {pl.get('administrative_title') or ''}"
        if not WIND_RE.search(text) or NOT_WIND.search(text):
            continue                                   # the collector's own filters let PV/BESS rows through
        pl.setdefault("region", spec["region"])
        pl["project_type_hint"] = "Eolico offshore" if re.search(r"(?i)off-?shore|galleggiant", r.title or "") else "Eolico"
        out.append({"external_id": f"{name}:{r.external_id}", "source": name, "source_url": r.source_url,
                    "title": r.title, "payload": pl, "statuses": [], "procedures": []})
    stats["wind_rows"] = len(out)
    stats["sample"] = [f"{(o['payload'].get('power_mw') or o['payload'].get('power') or '?')} | {o['title'][:90]}" for o in out[:4]]
    return out


def run(names: list[str] | None = None, where: str | None = None, out: str | None = None) -> dict:
    """Run the chosen regional sources; each failure is reported and the others go on."""
    chosen = names or [n for n, s in SPECS.items() if where is None or s["where"] == where]
    rows, report = [], {}
    for n in chosen:
        t0 = time.time()
        try:
            got = collect(n)
            rows += got
            report[n] = {"rows": len(got), "seconds": round(time.time() - t0)}
        except Exception as exc:  # one portal down must not stop the others
            report[n] = {"error": f"{type(exc).__name__}: {str(exc)[:300]}"}
        st = LAST_STATS.get(n, {})
        print(f"[wind-regional] {n}: {report[n]} {json.dumps(st, ensure_ascii=False)[:700]}")
        if os.environ.get("GITHUB_ACTIONS"):
            print(f"::notice title=wind-regional {n}::{json.dumps({**report[n], **st}, ensure_ascii=False)[:900]}")
    if out:
        Path(out).parent.mkdir(parents=True, exist_ok=True)
        prev = {}
        try:
            prev = json.loads(Path(out).read_text(encoding="utf-8"))
        except (OSError, ValueError):
            pass
        # a source that failed this time keeps its previous rows (a portal down is not a project withdrawn)
        # and sources not run this time keep theirs too
        ok = {n for n, v in report.items() if "error" not in v}
        keep = [r for r in prev.get("rows", []) if r.get("source") not in ok]
        data = {"updated": time.strftime("%Y-%m-%d"), "sources": {**prev.get("sources", {}), **report}, "rows": rows + keep}
        Path(out).write_text(json.dumps(data, ensure_ascii=False, indent=0), encoding="utf-8")
    return report
