"""Remove from the radar the MASE projects whose procedures are all 'Archiviata' (dead projects).

On va.mite.gov.it 'Archiviata' means the procedure was closed WITHOUT a decision (withdrawal,
incomplete documentation never integrated, inadmissibility); a project with a decree is 'Conclusa'.
A resubmitted project gets a new procedure ('in corso'), which keeps it alive.

Conservative rule - a record is removed only if:
  * every official URL of the record (url + _dedupe_urls) is a MASE project page (/Oggetti/Info/<id>);
  * each of those pages lists at least one procedure and ALL its procedures are 'Archiviata'.
Records that also carry a regional procedure or a MASE decree page are kept. A page that cannot be
read keeps its record (never removed on a network error).

Status pages are cached (reports/mase_status_cache.json): live statuses re-checked after 7 days,
archived ones after 30. One request per second. Removed records go to the audit CSV.
"""
from __future__ import annotations

import argparse
import csv
import html as html_lib
import json
import re
import time
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

import requests

INFO_RE = re.compile(r"^https?://va\.mite\.gov\.it/[a-z]{2}-[A-Z]{2}/Oggetti/Info/(\d+)")
ROW_RE = re.compile(r"\b(\d{3,6})\s+(\d{2}/\d{2}/\d{4})\s+([^\d:]{3,60}?)\s+Dettagli procedura")
HEADERS = {"User-Agent": "Mozilla/5.0 (PV Agent radar; procedure status check)"}
LIVE_TTL, ARCHIVED_TTL = timedelta(days=7), timedelta(days=30)


def parse_statuses(page_html: str) -> list[str]:
    """Statuses of the procedures listed in the 'Scegli la procedura' table of a MASE project page."""
    text = html_lib.unescape(re.sub(r"<[^>]+>", " ", page_html))
    text = re.sub(r"\s+", " ", text)
    i = text.find("Scegli la procedura")
    if i < 0:
        return []
    # each row of the table: '<procedure code> <start date> <status> Dettagli procedura' (the detail
    # blocks in between use 'label: date', never 'code date')
    return [m.group(3).strip() for m in ROW_RE.finditer(text[i:])]


def rows_of(data: Any) -> list[dict]:
    return data.get("records", []) if isinstance(data, dict) else data


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--data", required=True)
    ap.add_argument("--audit", required=True)
    ap.add_argument("--cache", default="reports/mase_status_cache.json")
    ap.add_argument("--apply", action="store_true")
    ap.add_argument("--max-fetch", type=int, default=600)
    args = ap.parse_args()

    data_path, cache_path = Path(args.data), Path(args.cache)
    data = json.loads(data_path.read_text(encoding="utf-8"))
    records = rows_of(data)
    try:
        cache = json.loads(cache_path.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        cache = {}
    now = datetime.now()

    def status_of(pid: str, url: str, budget: list[int]) -> list[str] | None:
        c = cache.get(pid)
        if c:
            ttl = ARCHIVED_TTL if c["statuses"] and all(s == "Archiviata" for s in c["statuses"]) else LIVE_TTL
            if now - datetime.fromisoformat(c["checked"]) < ttl:
                return c["statuses"]
        if budget[0] <= 0:
            return c["statuses"] if c else None
        budget[0] -= 1
        try:
            r = requests.get(url, headers=HEADERS, timeout=30)
            r.raise_for_status()
            st = parse_statuses(r.text)
        except requests.RequestException as exc:
            print(f"[mase-archived] non letto {url}: {str(exc)[:120]}")
            return c["statuses"] if c else None
        finally:
            time.sleep(1.0)
        cache[pid] = {"statuses": st, "checked": now.isoformat(timespec="seconds")}
        return st

    budget = [args.max_fetch]
    keep, removed = [], []
    for r in records:
        urls = {u for u in [r.get("url"), *(r.get("_dedupe_urls") or [])] if u}
        infos = {u: INFO_RE.match(u) for u in urls}
        mase_infos = {u: m.group(1) for u, m in infos.items() if m}
        if not mase_infos:
            keep.append(r)
            continue
        statuses = {pid: status_of(pid, u, budget) for u, pid in mase_infos.items()}
        # write what we know on the record (shown in the dashboard / cards)
        known = [s for v in statuses.values() if v for s in v]
        if known:
            r["procedure_status"] = ", ".join(dict.fromkeys(known))
        dead = (len(mase_infos) == len(urls)
                and all(v and all(s == "Archiviata" for s in v) for v in statuses.values()))
        (removed if dead else keep).append(r)

    print(f"[mase-archived] record MASE controllati: {sum(1 for r in records if any(INFO_RE.match(u or '') for u in [r.get('url'), *(r.get('_dedupe_urls') or [])]))}")
    print(f"[mase-archived] pagine lette in questo run: {args.max_fetch - budget[0]}")
    print(f"[mase-archived] progetti con tutte le procedure archiviate: {len(removed)} "
          f"({sum(float(r.get('power_mw') or 0) for r in removed):,.1f} MW)")
    for r in sorted(removed, key=lambda x: -float(x.get("power_mw") or 0))[:15]:
        print(f"   - {r.get('power_mw')} MW | {(r.get('title') or '')[:90]} | {r.get('url')}")

    Path(args.audit).parent.mkdir(parents=True, exist_ok=True)
    with open(args.audit, "w", newline="", encoding="utf-8-sig") as f:
        w = csv.writer(f, delimiter=";")
        w.writerow(["power_mw", "region", "province", "proponent", "title", "url", "procedure_status"])
        for r in removed:
            w.writerow([r.get("power_mw"), r.get("region"), r.get("province"), r.get("proponent"),
                        r.get("title"), r.get("url"), r.get("procedure_status")])
    cache_path.parent.mkdir(parents=True, exist_ok=True)
    cache_path.write_text(json.dumps(cache, ensure_ascii=False, indent=0, sort_keys=True), encoding="utf-8")

    if args.apply:
        if isinstance(data, dict):
            data["records"] = keep
        else:
            data = keep
        print(f"[mase-archived] APPLY: rimossi {len(removed)} progetti archiviati")
    data_path.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
