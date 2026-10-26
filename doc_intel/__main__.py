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
    r.add_argument("--refresh", action="store_true", help="re-download and re-analyse every document")
    r.add_argument("--verify-content", action="store_true",
                   help="re-download processed documents to compare SHA-256 even if the server reports no change")
    r.add_argument("--keep-files", action="store_true", help="keep original bytes and page text (test benchmarks only)")
    r.add_argument("--reanalyse-match", default=None,
                   help="with --reanalyse: only documents whose title or URL matches this regular expression")
    r.add_argument("--reanalyse", action="store_true",
                   help="re-extract every document with the current extractors (downloads again unless files are kept)")
    r.add_argument("--registry", choices=["vies", "none"], default="none", help="check extracted VATs on EU VIES")
    r.add_argument("--time-budget-min", type=float, help="stop starting OCR after N minutes; pending pages resume next run")
    r.add_argument("--max-new-docs", type=int, default=None, help="download at most N new documents in this run")
    r.add_argument("--max-mb", type=int, default=1024, help="per-file download cap")
    r.add_argument("--gold")
    e = sub.add_parser("evaluate")
    e.add_argument("--dossier", required=True)
    e.add_argument("--gold", required=True)
    g = sub.add_parser("digest", help="emit ::notice annotations readable via the checks API")
    g.add_argument("--dossier", required=True)
    g.add_argument("--slice", type=int, default=0, help="GitHub allows 10 notices per step: run one step per slice")
    g.add_argument("--slices", type=int, default=1)
    pr = sub.add_parser("probe", help="probe one sample record of a regional source")
    pr.add_argument("--data", default="docs/data.json")
    pr.add_argument("--source", help="source name; omit with --list to print the sources")
    pr.add_argument("--list", action="store_true")
    pr.add_argument("--render", action="store_true", help="also render with Playwright")
    pr.add_argument("--out", default="reports/probe")
    v = sub.add_parser("sources", help="verify every source on one real sample (run from an Italian network)")
    v.add_argument("--data", default="docs/data.json")
    v.add_argument("--store", default=".doc_intel_store")
    v.add_argument("--out", default="reports/doc_intel_fonti")
    v.add_argument("--only", nargs="*")
    v.add_argument("--render", action="store_true")
    v.add_argument("--max-docs", type=int, default=25)
    v.add_argument("--budget-min", type=float, default=10)
    v.add_argument("--no-ocr", action="store_true")
    c = sub.add_parser("card", help="build the project card (scheda) from a dossier")
    c.add_argument("--dossier", required=True)
    c.add_argument("--out", default="reports/schede")
    q = sub.add_parser("queue", help="choose the next projects to analyse (new first, then backlog by power)")
    q.add_argument("--data", required=True)
    q.add_argument("--memory", required=True, help="checkout of the memory branch (doc-intel-state)")
    q.add_argument("--max", type=int, default=35)
    q.add_argument("--mode", default="both", choices=["new", "backlog", "both", "refresh"])
    q.add_argument("--retry-days", type=int, default=14, help="skip projects attempted in the last N days")
    q.add_argument("--region", default=None, help="only projects of this region (e.g. Emilia-Romagna)")
    q.add_argument("--github-output", default=None, help="file where to write 'matrix=<json>' for GitHub Actions")
    qs = sub.add_parser("queue-status", help="coverage of the radar by project cards")
    qs.add_argument("--data", required=True)
    qs.add_argument("--memory", required=True)
    sy = sub.add_parser("sync-schede", help="download the published project cards for the dashboard (docs/schede)")
    sy.add_argument("--out", default="reports/schede_cache")
    fr = sub.add_parser("fill-radar", help="fill EMPTY radar fields (MW, province, region, municipalities) from the project cards")
    fr.add_argument("--data", required=True)
    fr.add_argument("--cards", default="reports/schede_cache")
    fr.add_argument("--audit", default="reports/fill_from_cards_audit.csv")
    lb = sub.add_parser("local-batch", help="analyse the portals reachable only from Italy (run on the user's PC)")
    lb.add_argument("--data", default="docs/data.json")
    lb.add_argument("--max", type=int, default=15)
    lb.add_argument("--budget-min", type=float, default=40, help="OCR time budget per project")
    lb.add_argument("--recompute", action="store_true", help="re-read projects that already have a card (new rules)")
    ci = sub.add_parser("cards-index", help="write the index of the cards in a folder (published for the public page)")
    ci.add_argument("--folder", required=True)
    ci.add_argument("--out", required=True)
    ex = sub.add_parser("explore", help="dump links and API calls of portal pages (to write new adapters)")
    ex.add_argument("urls", nargs="+")
    ex.add_argument("--out", default="reports/explore.json")
    ex.add_argument("--no-render", action="store_true")
    ex.add_argument("--annotate", action="store_true", help="also print the report as GitHub annotations")
    nw = sub.add_parser("news", help="sector press / listed EPC news matched to radar projects (level C, to verify)")
    nw.add_argument("--data", default="docs/data.json")
    nw.add_argument("--memory", required=True)
    nw.add_argument("--backfill-pages", type=int, default=0, help="also read N older feed pages (first run)")
    nw.add_argument("--max-articles", type=int, default=40)
    fx = sub.add_parser("ferx", help="GSE FER X rankings matched to radar projects (information only)")
    fx.add_argument("--data", default="docs/data.json")
    fx.add_argument("--memory", required=True)
    fx.add_argument("--annotate", action="store_true")
    m = sub.add_parser("render")
    m.add_argument("--dossier", required=True)
    a = ap.parse_args(argv)

    if a.cmd == "run":
        from .fetch import Fetcher
        from .store import Store
        fetcher = Fetcher(Store(a.store), max_bytes=a.max_mb * 1024 * 1024)
        d = run(a.url, a.store, a.data if Path(a.data).exists() else None, ocr=not a.no_ocr,
                ocr_max_pages=a.ocr_max_pages, max_docs=a.max_docs, workers=a.workers, refresh=a.refresh, verify_content=a.verify_content, keep_files=a.keep_files, reanalyse=a.reanalyse, reanalyse_match=a.reanalyse_match, registry=None if a.registry == 'none' else a.registry,
                time_budget_s=a.time_budget_min * 60 if a.time_budget_min else None, fetcher=fetcher,
                max_new_docs=a.max_new_docs)
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
    if a.cmd == "digest":
        d = json.loads(Path(a.dossier).read_text(encoding="utf-8"))
        chunks = []
        for title, payload in digest(d):
            msg = json.dumps(payload, ensure_ascii=False)
            parts = [msg[i:i + 3900] for i in range(0, len(msg), 3900)] or ["null"]
            for k, part in enumerate(parts):
                chunks.append((f"{title}#{k + 1}of{len(parts)}", part))
        per = -(-len(chunks) // a.slices)
        mine = chunks[a.slice * per:(a.slice + 1) * per]
        if len(mine) > 10:
            print(f"::warning title=digest::slice {a.slice} has {len(mine)} chunks, only 10 shown; raise --slices")
        for title, part in mine[:10]:
            part = part.replace("%", "%25").replace("\r", "%0D").replace("\n", "%0A")
            print(f"::notice title={title}::{part}")
        return 0
    if a.cmd == "probe":
        from .canonical import load_records
        from .probe import pick_samples, probe_url, summary_line
        samples = pick_samples(load_records(a.data))
        if a.list:
            print(json.dumps(sorted(samples)))
            return 0
        sample = samples[a.source]
        render_fn = _playwright_renderer() if a.render else None
        res = probe_url(sample["url"], render=render_fn)
        out = Path(a.out)
        out.mkdir(parents=True, exist_ok=True)
        (out / f"probe-{a.source}.json").write_text(json.dumps({"sample": sample, "result": res}, ensure_ascii=False,
                                                               indent=1, default=str), encoding="utf-8")
        line = json.dumps(summary_line(a.source, sample, res), ensure_ascii=False, default=str)
        for k, i in enumerate(range(0, len(line), 3900)):
            part = line[i:i + 3900].replace("%", "%25").replace("\r", "%0D").replace("\n", "%0A")
            print(f"::notice title=probe-{a.source}#{k + 1}of{-(-len(line) // 3900)}::{part}")
        return 0
    if a.cmd == "sources":
        from .sources import verify_sources
        rows = verify_sources(a.data, a.store, a.out, a.only, _playwright_renderer() if a.render else None,
                              a.max_docs, a.budget_min, not a.no_ocr)
        print("\nRISULTATO:", {r["source"]: r["status"] for r in rows})
        return 0
    if a.cmd == "card":
        from .card import build_card, write_card
        d = json.loads(Path(a.dossier).read_text(encoding="utf-8"))
        if not d["acquisition"].get("references"):
            print("::warning title=scheda::no documents acquired: existing card left unchanged")
            return 0
        card = build_card(d)
        if not card.get("project_key"):
            print("::warning title=scheda::project not found in the radar (run it with the radar URL): card not saved")
            return 0
        path = write_card(card, a.out)
        print(f"scheda: {path}")
        return 0
    if a.cmd == "queue":
        from .queue import plan
        items = plan(a.data, a.memory, max_items=a.max, mode=a.mode, retry_days=a.retry_days, region=a.region or None)
        print(json.dumps(items, ensure_ascii=False, indent=1))
        print(f"::notice title=queue::{len(items)} projects queued "
              f"({sum(1 for i in items if i['reason'] == 'new')} new, {sum(1 for i in items if i['reason'] == 'backlog')} backlog, "
              f"{sum(1 for i in items if i['reason'] == 'refresh')} refresh)", file=sys.stderr)
        if a.github_output:
            with open(a.github_output, "a", encoding="utf-8") as fh:
                fh.write("matrix=" + json.dumps({"include": [{"url": i["url"]} for i in items]}) + "\n")
                fh.write(f"count={len(items)}\n")
        return 0
    if a.cmd == "queue-status":
        from .queue import status
        st = status(a.data, a.memory)
        print(json.dumps(st))
        print(f"::notice title=coverage::{st['with_card']} cards / {st['records']} radar projects; "
              f"{st['cloud_without_card']} still to analyse in the cloud", file=sys.stderr)
        return 0
    if a.cmd == "sync-schede":
        from .sync import sync_cards
        res = sync_cards(a.out)
        print(f"Schede progetto: {res['downloaded']} aggiornate, {res['total']} disponibili"
              + (f", {res['errors']} non scaricate" if res["errors"] else ""))
        return 0
    if a.cmd == "fill-radar":
        from .fill import fill_radar
        res = fill_radar(a.data, a.cards, a.audit)
        print(f"Dati completati dalle schede: {res['filled']} campi vuoti riempiti, "
              f"{res['differences']} differenze di potenza da verificare (non applicate) - audit: {a.audit}")
        return 0
    if a.cmd == "local-batch":
        from .local_batch import main as local_main
        return local_main(a.data, a.max, a.budget_min, recompute=a.recompute)
    if a.cmd == "cards-index":
        from .sync import build_index
        idx = build_index(a.folder)
        Path(a.out).write_text(json.dumps(idx, ensure_ascii=False, indent=0, sort_keys=True), encoding="utf-8")
        print(f"cards index: {len(idx)} cards -> {a.out}")
        return 0
    if a.cmd == "ferx":
        from . import ferx
        if hasattr(ferx, "run"):
            st = ferx.run(a.data, a.memory)
            print(f"::notice title=ferx::{json.dumps(st, ensure_ascii=False)[:3000]}")
        else:
            for n in ferx.dump():
                print(f"::notice title=ferx-dump::{n[:3800]}")
        return 0

    if a.cmd == "news":
        from .news import run as news_run
        st = news_run(a.data, a.memory, backfill_pages=a.backfill_pages, max_articles=a.max_articles)
        print(f"::notice title=news::{st['new_relevant']} nuovi articoli rilevanti; {st['projects_matched']} progetti con notizie compatibili; feed {st['feeds']}")
        return 0

    if a.cmd == "explore":
        from .explore import main as explore_main
        Path(a.out).parent.mkdir(parents=True, exist_ok=True)
        explore_main(a.urls, a.out, render=not a.no_render)
        print(f"report: {a.out}")
        if a.annotate:
            text = Path(a.out).read_text(encoding="utf-8")
            chunks = [text[i:i + 3900] for i in range(0, len(text), 3900)]
            for k, part in enumerate(chunks):
                print(f"::notice title=explore#{k + 1}of{len(chunks)}::{part.replace(chr(10), ' ')}")
        return 0
    if a.cmd == "render":
        d = json.loads(Path(a.dossier).read_text(encoding="utf-8"))
        sys.stdout.write(render(d))
        return 0
    return 2


def _playwright_renderer():
    def render(url: str) -> str:
        from playwright.sync_api import sync_playwright
        with sync_playwright() as p:
            b = p.chromium.launch(headless=True)
            page = b.new_page()
            xhr = []
            page.on("response", lambda r: xhr.append({"url": r.url[:300], "status": r.status,
                                                       "type": (r.headers or {}).get("content-type", "")[:60]})
                    if r.request.resource_type in ("xhr", "fetch") else None)
            page.goto(url, wait_until="domcontentloaded", timeout=60000)
            try:
                page.wait_for_load_state("networkidle", timeout=25000)
            except Exception:
                pass
            page.wait_for_timeout(3000)
            html = page.content()
            b.close()
            return html, xhr
    return render


def digest(d: dict) -> list[tuple[str, dict]]:
    """Compact, size-bounded views of a dossier (annotations are the only CI output readable remotely)."""
    a = d["acquisition"]
    out = [("acquisition", {k: a[k] for k in a if k != "procedures"} | {
        "procedures": [{k: p.get(k) for k in ("id", "declared", "enumerated", "index_pages", "errors")} for p in a["procedures"]],
        "gate": d["gates"]["acquisition"], "efficiency": d["efficiency"]})]
    out.append(("delta", {"canonical": d["canonical"], "delta": d["delta"]}))
    conf = [{"field": g["field"], "subject": g["subject"], "values": [
        (v["value"], v["occurrences"], v["example"]["url"].rsplit("/", 1)[-1], v["example"]["page"]) if isinstance(v, dict) else v
        for v in g["values"][:6]], "hint": g.get("preference_hint")} for g in d["groups"] if g["status"] == "conflict"]
    out.append(("conflicts", {"n": len(conf), "groups": conf[:40]}))
    single = [{"field": g["field"], "subject": g["subject"], "value": g["values"][0]["value"],
               "occ": g["values"][0]["occurrences"], "doc": g["values"][0]["example"]["url"].rsplit("/", 1)[-1],
               "page": g["values"][0]["example"]["page"]} for g in d["groups"] if g["status"] == "single_value"]
    out.append(("single_values", {"n": len(single), "groups": single[:80]}))
    gates = {x["fact_id"]: x for x in d["gates"]["proposal"]}
    out.append(("proposal", [{"field": p["field"], "subject": p["subject"], "value": p["value"] if p["field"] != "geo.area_polygon"
                             else {k: p["value"][k] for k in ("crs", "vertex_count", "area_ha", "pages")},
                             "blocking": gates.get(p["fact_id"], {}).get("reasons")} for p in d["proposal"]][:80]))
    pts = {}
    for f in d["facts"]:
        if f["field"] in ("geo.utm_point", "geo.latlon_point"):
            q = f["qualifiers"]
            key = (f["citation"]["url"].rsplit("/", 1)[-1], q.get("object"), str((q.get("crs") or {}).get("zone") if isinstance(q.get("crs"), dict) else q.get("crs")))
            pts.setdefault(key, {"n": 0, "pages": set()})
            pts[key]["n"] += 1
            pts[key]["pages"].add(f["citation"]["page"])
    out.append(("geo_points", [{"doc": k[0], "object": k[1], "zone": k[2], "n": v["n"], "pages": sorted(v["pages"])[:15]}
                              for k, v in sorted(pts.items())][:80]))
    sched = [{"field": f["field"], "value": f["value"], "temporal": f["temporal"],
              "doc": f["citation"]["url"].rsplit("/", 1)[-1], "page": f["citation"]["page"],
              "excerpt": f["citation"]["excerpt"][:140]} for f in d["facts"] if f["field"].startswith("schedule.date")]
    out.append(("schedule_dates", sched[:60]))
    out.append(("coordinate_pages_without_points", d.get("diagnostics", [])[:12]))
    out.append(("unreadable_pages", [(u["url"].rsplit("/", 1)[-1], u["page"], u["method"]) for u in d.get("unreadable_pages", [])][:150]))
    vats = [{"value": f["value"], "subject": f["subject"], "assoc": f["qualifiers"].get("subject_association"),
             "doc": f["citation"]["url"].rsplit("/", 1)[-1], "page": f["citation"]["page"],
             "excerpt": f["citation"]["excerpt"][:120]} for f in d["facts"] if f["field"] == "company.vat"]
    seen, uniq = set(), []
    for v in vats:
        k = (v["value"], v["subject"], v["assoc"])
        if k not in seen:
            seen.add(k)
            uniq.append(v)
    out.append(("vat_occurrences", uniq[:40]))
    out.append(("registry", d.get("registry", {})))
    out.append(("failed_documents", a.get("failed_documents", [])[:20]))
    out.append(("schedule_pages", d.get("schedule_pages", [])[:60]))
    pts = [{"x": f["extractor"], "obj": (f.get("qualifiers") or {}).get("object"), "v": f["value"],
            "doc": (f["citation"].get("url") or "")[-40:], "p": f["citation"].get("page"),
            "raw": (f.get("qualifiers") or {}).get("raw")} for f in d["facts"]
           if f["field"] in ("geo.latlon_point", "geo.map_extent") and f["extractor"] != "geo.latlon"]
    out.append(("site_points", pts[:120]))
    roles = [{"field": f["field"], "value": f["value"], "status": f["status"], "doc": f["citation"]["url"].rsplit("/", 1)[-1],
              "page": f["citation"]["page"], "excerpt": f["citation"]["excerpt"][:200]}
             for f in d["facts"] if f["field"] in ("role.epc", "role.bop", "role.contractor", "role.supplier", "role.om",
                                                      "role.works_director", "company.vat_group")]
    seen_r, roles_u = set(), []
    for r in roles:
        k = (r["field"], r["value"], r["doc"])
        if k not in seen_r:
            seen_r.add(k)
            roles_u.append(r)
    out.append(("execution_roles_evidence", roles_u[:30]))
    named = [{"subject": f["subject"], "object": f["qualifiers"].get("object"), "value": f["value"],
              "doc": f["citation"]["url"].rsplit("/", 1)[-1], "page": f["citation"]["page"]}
             for f in d["facts"] if f["field"] == "geo.latlon_point"
             and (f["subject"] or f["qualifiers"].get("object") in ("centroid", "site_bbox_corner", "substation"))]
    out.append(("named_points", named[:40]))
    if d.get("evaluation"):
        out.append(("evaluation", d["evaluation"]))
    return out


if __name__ == "__main__":
    raise SystemExit(main())
