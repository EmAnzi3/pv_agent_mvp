"""Work queue for project cards: new radar projects first, then the backlog, a few dozen per night.

State lives on the memory branch (doc-intel-state), next to manifests and cards:
  queue/seen.json      project_key -> first date the queue saw it in docs/data.json
  queue/attempts.json  project_key -> {last: date, runs: n} for projects sent to analysis

Rules
* only records whose source has an adapter that runs in the cloud are queued; portals that refuse
  foreign IPs (Sicilia, Sistema Puglia, Basilicata) are left to the local runner on the user's PC;
* a project with a card is not queued again here: refreshing existing cards (new files filed) is a
  separate, slower cycle (``mode='refresh'``) so the backlog is not starved;
* a project attempted in the last ``retry_days`` without producing a card is skipped (portal down,
  no documents published yet), so one broken source cannot fill every night.
"""
from __future__ import annotations

import json
import re
from datetime import date, timedelta
from pathlib import Path
from typing import Any

from .adapters import REGISTRY

LOCAL_ONLY_HOSTS = ("regione.sicilia.it", "sistema.puglia.it", "regione.basilicata.it")
REGISTER_ONLY = re.compile(r"^https?://atos\.arrr\.it/")


def _adapter_for(url: str):
    for cls in REGISTRY.values():
        try:
            if cls.handles(url):
                return cls
        except Exception:
            continue
    return None


def cards_in(mem: Path) -> dict[str, str]:
    """project_key -> card file, read from the cards themselves (no shared index to conflict on)."""
    out = {}
    for f in sorted((mem / "schede").glob("*.json")):
        if f.name == "index.json":
            continue
        try:
            key = json.loads(f.read_text(encoding="utf-8")).get("project_key")
        except (OSError, ValueError):
            continue
        if key:
            out[key] = f.name
    return out


def incomplete_cards(mem: Path) -> set[str]:
    """Cards with documents still to read (large projects read over several runs, or time budget hit)."""
    out = set()
    for f in sorted((mem / "schede").glob("*.json")):
        if f.name in ("index.json", "live_index.json"):
            continue
        try:
            c = json.loads(f.read_text(encoding="utf-8"))
        except (OSError, ValueError):
            continue
        if c.get("project_key") and ((c.get("documentazione") or {}).get("da_leggere") or 0) > 0:
            out.add(c["project_key"])
    return out


def _load(path: Path) -> dict:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return {}


def plan(data_json: str | Path, memory_dir: str | Path, max_items: int = 35, mode: str = "both",
         retry_days: int = 14, today: date | None = None, record: bool = True,
         local: bool = False, region: str | None = None) -> list[dict[str, Any]]:
    """Return up to ``max_items`` records to analyse: [{url, project_key, reason, power_mw}].

    mode: 'new' (only projects never seen before), 'backlog' (only older ones), 'both', 'refresh'."""
    today = today or date.today()
    mem = Path(memory_dir)
    seen_path, att_path = mem / "queue" / "seen.json", mem / "queue" / "attempts.json"
    seen, attempts = _load(seen_path), _load(att_path)
    cards = cards_in(mem)
    partial = incomplete_cards(mem)
    first_run = not seen
    records = json.loads(Path(data_json).read_text(encoding="utf-8-sig")).get("records", [])
    new, backlog, refresh = [], [], []
    for r in records:
        key, url = r.get("project_key"), (r.get("url") or "").strip()
        if not key or not url:
            continue
        if region and not (r.get("region") or "").lower().startswith(region.lower()[:6]):
            continue                                   # focus on one region (e.g. 'Emilia-Romagna')
        is_new = key not in seen
        if is_new:
            seen[key] = today.isoformat()
        # cloud batches skip the portals that refuse foreign IPs; the local runner (Italian PC) takes only those
        if (any(h in url for h in LOCAL_ONLY_HOSTS) != local) or _adapter_for(url) is None:
            continue
        if REGISTER_ONLY.search(url):
            # a register sheet alone has no documents: the plant waits until the authority publishes a
            # procedure, which the radar then merges with it (the record's url becomes the procedure)
            continue
        last = attempts.get(key, {}).get("last")
        recent = last and date.fromisoformat(last) > today - timedelta(days=retry_days)
        item = {"url": url, "project_key": key, "power_mw": r.get("power_mw") or 0, "source": r.get("source")}
        if key in cards:
            if key in partial and not (last and date.fromisoformat(last) >= today):
                backlog.append({**item, "reason": "incomplete"})      # continue the reading the next night
            elif not recent:
                refresh.append({**item, "reason": "refresh"})
            continue
        if recent:
            continue
        # on the very first run every record is 'new' for the queue: treat them as backlog
        (new if is_new and not first_run else backlog).append({**item, "reason": "new" if is_new and not first_run else "backlog"})
    backlog.sort(key=lambda x: -(x["power_mw"] or 0))
    refresh.sort(key=lambda x: attempts.get(x["project_key"], {}).get("last") or "")
    chosen: list[dict] = []
    if mode in ("new", "both"):
        chosen += new
    if mode in ("backlog", "both"):
        chosen += backlog
    if mode == "refresh":
        chosen += refresh
    chosen = chosen[:max_items]
    if record:
        for it in chosen:
            a = attempts.setdefault(it["project_key"], {"runs": 0})
            a["last"], a["runs"] = today.isoformat(), a.get("runs", 0) + 1
        seen_path.parent.mkdir(parents=True, exist_ok=True)
        seen_path.write_text(json.dumps(seen, ensure_ascii=False, indent=0, sort_keys=True), encoding="utf-8")
        att_path.write_text(json.dumps(attempts, ensure_ascii=False, indent=0, sort_keys=True), encoding="utf-8")
    return chosen


def status(data_json: str | Path, memory_dir: str | Path) -> dict[str, Any]:
    """Coverage of the radar by cards, for the run summary."""
    mem = Path(memory_dir)
    cards = cards_in(mem)
    records = json.loads(Path(data_json).read_text(encoding="utf-8-sig")).get("records", [])
    cloud = [r for r in records if r.get("url") and not any(h in r["url"] for h in LOCAL_ONLY_HOSTS)
             and not REGISTER_ONLY.search(r["url"]) and _adapter_for(r["url"]) is not None]
    return {"records": len(records), "cloud_supported": len(cloud),
            "with_card": sum(1 for r in records if r.get("project_key") in cards),
            "cloud_without_card": sum(1 for r in cloud if r.get("project_key") not in cards)}
