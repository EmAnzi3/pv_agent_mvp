"""Orchestration: record URL -> manifest -> bytes -> text -> facts -> dossier.

Per-file errors never stop the project; every failure is recorded.
"""
from __future__ import annotations

import re
import time
from concurrent.futures import ThreadPoolExecutor
from dataclasses import asdict
from datetime import datetime, timezone
from typing import Any, Callable

from . import EXTRACTOR_VERSION, SCHEMA_VERSION, TOOLBOX_VERSION
from .adapters import adapter_for_url
from .adapters.base import Discovery
from .canonical import compare, find_by_url, load_records
from .extract import extract_all
from .extract.context import PageContext
from .fetch import Fetcher
from .gates import acquisition_gate, proposal_gates
from .parse import OcrBudget, extract_pages
from .reconcile import group
from .schema import Fact, fact_from_dict, facts_to_json
from .store import Store

PROPOSABLE = ("company.vat", "contact.pec", "contact.email", "contact.phone", "role.proponent", "role.designer", "role.geologist",
              "tech.pv_dc_mwp", "tech.pv_ac_mw", "tech.bess_power_mw", "tech.bess_energy_mwh",
              "tech.module_power_wp", "tech.inverter_count", "tech.inverter_power_kw", "tech.mounting",
              "tech.technology", "grid.voltage_kv", "geo.area_polygon",
              "schedule.duration.construction", "schedule.duration.decommissioning",
              "role.epc", "role.bop", "role.contractor")


NO_OCR_DOCS = re.compile(
    r"(?i)disponibilit[aà][\W_]*giuridic|atto[\W_]*di[\W_]*notoriet|atto[\W_]*notori|dichiarazion[ei][\W_]*sostitutiv|(?<![A-Za-z])D\.?S\.?[\W_]*dell|"
    r"visura|ricevut[ae]|pagament[io]|bonifico|documento[\W_]*(?:di[\W_]*)?identit|carta[\W_]*d.identit|procura|"
    r"attestazione[\W_]*(?:di[\W_]*)?veridicit|attestazione[\W_]*istituto|atto[\W_]*di[\W_]*sottomission|contratt[oi][\W_]*(?:preliminar|di[\W_]*(?:affitto|superficie|locazione))|"
    r"certificato[\W_]*di[\W_]*destinazione[\W_]*urbanistica|(?<![A-Za-z])CDU(?![A-Za-z])|piano[\W_]*particellare")


def run(record_url: str, store_dir: str, data_json: str | None = None, ocr: bool = True,
        ocr_max_pages: int = 400, max_docs: int | None = None, workers: int = 3,
        refresh: bool = False, verify_content: bool = False, keep_files: bool = False,
        reanalyse: bool = False,
        reanalyse_match: str | None = None,
        registry: str | None = None, registry_client=None,
        time_budget_s: float | None = None, get_html: Callable[[str], str] | None = None,
        skip_titles: str | None = None, max_new_docs: int | None = None,
        fetcher: Fetcher | None = None, log: Callable[[str], None] = print) -> dict[str, Any]:
    t0 = time.time()
    store = Store(store_dir)
    adapter = adapter_for_url(record_url)
    if get_html:
        adapter.get_html = get_html
    adapter.log = log
    if getattr(adapter, "wants_records", False) and data_json:
        adapter.records = load_records(data_json)      # register adapters link to the procedure holding the documents
    log(f"[identify] {record_url} via {type(adapter).__name__}")
    identity = adapter.identify(record_url)
    if data_json and "register_sheet" not in identity.raw:
        _attach_register(identity, record_url, data_json, adapter.get_html, log)
    disc: Discovery = adapter.discover(identity)
    log(f"[discover] {len(disc.documents)} documents in {len(disc.procedures)} procedures "
        f"({round(time.time() - t0)}s); errors: {disc.errors}")
    manifest = store.load_manifest(adapter.source, identity.project_id)
    manifest["procedures"] = disc.procedures
    fetcher = fetcher or Fetcher(store)
    budget = OcrBudget(ocr_max_pages, deadline=(t0 + time_budget_s) if time_budget_s else None)
    docs = list(disc.documents)
    skipped_titles = 0
    if skip_titles:
        # documents whose title says they are drawings are not downloaded (portals with tight limits);
        # a drawing that is the works programme is always kept
        keep = [d for d in docs if not re.search(skip_titles, d.title or "", re.I)
                or SCHED_RE.search(d.title or "") or PRIORITY_DOCS.search(d.title or "")]
        skipped_titles, docs = len(docs) - len(keep), keep
        if skipped_titles:
            log(f"[discover] {skipped_titles} elaborati grafici non scaricati")
    if max_docs and len(docs) > max_docs:
        # under a download cap the most informative documents go first (stable order otherwise)
        rank = lambda d: 0 if PRIORITY_DOCS.search(d.title or "") else 1
        docs = sorted(docs, key=rank)[:max_docs]
        log(f"[discover] limite documenti: {max_docs} (i piu' informativi per primi)")

    deadline = (t0 + time_budget_s) if time_budget_s else None
    counter = {"n": 0}
    import threading
    lock = threading.Lock()
    reanalysed: set[str] = set()   # sha256 re-extracted in this run (duplicates then reuse them)
    from .fetch import FetchResult

    def extract_document(ref, res, parsed):
        """Facts, diagnostics and summaries of one distinct content; persisted by SHA-256."""
        doc_pages, members, unread, complete = [], [], [], True
        sched: list[dict] = []
        for mem, txt in parsed:
            s_ = txt.get("summary", {})
            members.append({"name": mem.name, "pages": s_.get("pages", 0), "summary": s_,
                            "backend": txt.get("backend"), "error": txt.get("error")})
            if txt.get("error") or (ocr and txt.get("ocr_pending")):
                complete = False  # with OCR disabled by choice, text-less pages cannot improve: document is final
            for p in txt.get("pages", []):
                if p["method"] in ("ocr_failed", "none_after_ocr", "none") and len(unread) < 50:
                    unread.append({"url": ref.url, "member": None if mem.depth == 0 else mem.name,
                                   "page": p["n"], "method": p["method"], "error": p.get("error")})
                if p.get("text") or p.get("geo_extent"):
                    pc = PageContext(p["text"], ref.url, p["n"], res.sha256,
                                     None if mem.depth == 0 else mem.name, ref.procedure_id,
                                     ref.title, ref.extra.get("index_date"), None, p["method"])
                    pc.gantt = p.get("gantt")
                    pc.geo_extent = p.get("geo_extent")
                    doc_pages.append(pc)
                if (SCHED_RE.search(ref.title or "") or SCHED_RE.search((p.get("text") or "")[:3000])) and len(sched) < 12:
                    g = p.get("gantt") or {}
                    sched.append({"page": p["n"], "method": p["method"], "chars": p.get("chars"),
                                  "gantt_rows": len(g.get("rows", [])), "unit": g.get("unit"),
                                  "table_rows": len(g.get("table_rows", [])), "duration_rows": len(g.get("duration_rows", [])),
                                  "gantt_error": p.get("gantt_error"),
                                  "rows": g.get("rows", [])[:12], "head": (p.get("text") or "")[:400]})
        facts = extract_all(doc_pages)
        terms = []
        for pc in doc_pages:
            for ln in pc.lines:
                if TERM_RE.search(ln) and len(terms) < 40:
                    terms.append({"page": pc.page, "line": ln[:220]})
        payload = {"sha256": res.sha256, "url": ref.url, "extracted_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
                   "toolbox_version": TOOLBOX_VERSION, "extractor_version": EXTRACTOR_VERSION, "members": members, "skipped": res.skipped_members,
                   "unreadable": unread, "diagnostics": coordinate_diagnostics(doc_pages, facts, limit=5),
                   "facts": facts_to_json(facts), "complete": complete, "terms": terms,
                   "schedule_pages": sched}
        return payload

    def process(ref):
        t1 = time.time()
        entry = manifest.get("documents", {}).get(ref.url, {})
        force = refresh
        # 1) already fully processed: reuse stored facts unless the remote file changed
        done = (entry.get("processed") or {}).get("complete")
        redo = reanalyse and (not reanalyse_match or bool(re.search(reanalyse_match, f"{ref.title or ''} {ref.url}", re.I)))
        if done and entry.get("sha256") and store.has_facts(entry["sha256"]) and not refresh and not redo:
            change, seen = fetcher.check_remote(ref.url, entry, ref.extra.get("auth"))
            force = change == "changed" or verify_content
            if not force:
                with lock:
                    entry["last_checked"] = datetime.now(timezone.utc).isoformat(timespec="seconds")
                    entry["last_check"] = change
                    counter["n"] += 1
                    log(f"[doc {counter['n']}/{len(docs)}] reused ({change}) {(ref.title or ref.url)[:70]}")
                res = FetchResult(ref.url, entry["sha256"], entry.get("kind"), entry.get("bytes", 0), "reused")
                return ref, res, store.get_facts(entry["sha256"])
        if deadline and time.time() >= deadline:
            return ref, FetchResult(ref.url, None, None, 0, "deferred", "time budget reached before download"), None
        if max_new_docs is not None:
            # very large projects are read over several runs: documents already analysed are reused,
            # at most max_new_docs new ones are downloaded per run (the card is built on what is read)
            with lock:
                if counter.get("new", 0) >= max_new_docs:
                    return ref, FetchResult(ref.url, None, None, 0, "deferred", "per-run document cap reached"), None
                counter["new"] = counter.get("new", 0) + 1
        # 2) new, changed, incomplete or forced: download, parse, extract, then delete the bytes
        res = fetcher.fetch(ref.url, manifest, refresh=force, auth=ref.extra.get("auth"))
        payload = None
        if (res.sha256 and store.has_facts(res.sha256) and res.state in ("unchanged", "cached")
                and not (redo and res.sha256 not in reanalysed)):
            payload = store.get_facts(res.sha256)          # same bytes already analysed (e.g. duplicate URL)
        elif res.sha256:
            parsed = []
            for mem in res.members:
                try:
                    # scanned legal paperwork (land titles, sworn statements, receipts) takes most of the OCR time
                    # and carries nothing for the card: its digital text is still read, scans are skipped
                    low_value = bool(NO_OCR_DOCS.search(f"{ref.title or ''} {mem.name or ''}"))
                    parsed.append((mem, extract_pages(mem.raw, res.sha256, store,
                                                      member=None if mem.depth == 0 else mem.name,
                                                      ocr=ocr and not low_value, budget=budget)))
                except Exception as exc:
                    parsed.append((mem, {"error": str(exc)[:300], "pages": [], "summary": {}}))
            payload = extract_document(ref, res, parsed)
            store.put_facts(res.sha256, payload)
            with lock:
                reanalysed.add(res.sha256)
        with lock:
            counter["n"] += 1
            e = manifest.setdefault("documents", {}).setdefault(ref.url, {})
            if payload is not None:
                e["processed"] = {"at": payload["extracted_at"], "complete": payload["complete"],
                                  "facts": len(payload["facts"]), "toolbox_version": TOOLBOX_VERSION,
                                  "extractor_version": payload.get("extractor_version"),
                                  "pages": sum(m["pages"] for m in payload["members"])}
            e.update({"title": ref.title, "procedure_id": ref.procedure_id, "index_date": ref.extra.get("index_date")})
            log(f"[doc {counter['n']}/{len(docs)}] {res.state} {res.kind or '-'} {res.size // 1024} KB "
                f"{time.time() - t1:.0f}s {(ref.title or ref.url)[:70]}" + (f" ERROR {res.error[:80]}" if res.error else ""))
        # 3) originals are not kept: delete bytes and page text once the document is fully analysed
        if res.sha256 and payload is not None and payload["complete"] and not keep_files:
            store.delete_blob(res.sha256)
            store.delete_text(res.sha256)
        return ref, res, payload

    t_fetch = time.time()
    unique = list({d.url: d for d in reversed(docs)}.values())[::-1]   # one download per URL
    if len(unique) > 150:
        workers = min(workers, 2)          # large projects: fewer big PDFs in memory at once
    with ThreadPoolExecutor(max_workers=max(1, workers)) as pool:
        by_url = {ref.url: (res, payload) for ref, res, payload in pool.map(process, unique)}
    results = [(ref, *by_url[ref.url]) for ref in docs]
    listed = {d.url for d in disc.documents}
    removed = [u for u in manifest.get("documents", {}) if u not in listed]
    for u in removed:
        manifest["documents"][u].setdefault("removed_from_index_at", datetime.now(timezone.utc).isoformat(timespec="seconds"))
    states: dict[str, int] = {}
    for res, _ in by_url.values():          # per distinct URL, not per index reference
        states[res.state] = states.get(res.state, 0) + 1
    manifest.setdefault("runs", []).append({"at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
                                            "listed": len(disc.documents), "states": states, "removed": len(removed),
                                            "toolbox_version": TOOLBOX_VERSION})
    manifest["runs"] = manifest["runs"][-50:]
    store.save_manifest(manifest)
    log(f"[memory] states {states}; removed from index {len(removed)}; store {store.size_report()}")

    facts: list[Fact] = []
    unreadable: list[dict] = []
    diagnostics: list[dict] = []
    term_lines: list[dict] = []
    schedule_pages: list[dict] = []
    sha_aliases: dict[str, list[str]] = {}
    inventory = []
    by_sha: dict[str, str] = {}
    acq = {"skipped_by_title": skipped_titles, "declared_total": sum(p["declared"] or 0 for p in disc.procedures), "procedures": disc.procedures,
           "discovery_errors": list(disc.errors), "info_links": identity.raw.get("info_links"),
           "references": len(disc.documents), "processed_references": len(docs), "download_failures": 0,
           "distinct_sha256": 0, "duplicate_references": 0, "pdf_files": 0, "pages_total": 0,
           "pages_digital": 0, "pages_ocr": 0, "pages_ocr_pending": 0, "pages_unreadable": 0,
           "pages_graphic_no_text": 0, "parse_errors": 0, "skipped_members": 0,
           "states": states, "removed_from_index": removed[:50],
           "facts_from_older_extractor": sum(1 for _, p_ in by_url.values()
                                             if p_ and p_.get("extractor_version") != EXTRACTOR_VERSION),
           "extractor_version": EXTRACTOR_VERSION}
    for ref, res, payload in results:
        entry = {"url": ref.url, "procedure_id": ref.procedure_id, "title": ref.title,
                 "index_page": ref.index_page, "index_date": ref.extra.get("index_date"),
                 "sha256": res.sha256, "kind": res.kind, "bytes": res.size, "state": res.state,
                 "error": res.error, "duplicate_of": None, "members": [], "skipped": []}
        if res.state == "failed":
            acq["download_failures"] += 1
            acq.setdefault("failed_documents", []).append({"url": ref.url, "title": ref.title, "error": res.error})
        if res.state == "deferred":
            acq["deferred_by_deadline"] = acq.get("deferred_by_deadline", 0) + 1
        if res.sha256:
            sha_aliases.setdefault(res.sha256, []).append(ref.url)
            if res.sha256 in by_sha:
                entry["duplicate_of"] = by_sha[res.sha256]
                acq["duplicate_references"] += 1
            else:
                by_sha[res.sha256] = ref.url
        if payload:
            entry["members"] = payload["members"]
            entry["skipped"] = payload.get("skipped", [])
            acq["skipped_members"] += len(entry["skipped"])
            if not entry["duplicate_of"]:
                for m in payload["members"]:
                    s_ = m.get("summary", {})
                    if m.get("error"):
                        acq["parse_errors"] += 1
                    acq["pdf_files"] += 1
                    acq["pages_total"] += s_.get("pages", 0)
                    acq["pages_digital"] += s_.get("digital", 0)
                    acq["pages_ocr"] += s_.get("ocr", 0)
                    acq["pages_ocr_pending"] += s_.get("ocr_deferred", 0) + (0 if ocr else s_.get("no_text", 0))
                    acq["pages_unreadable"] += s_.get("ocr_failed", 0)
                    acq["pages_graphic_no_text"] += s_.get("no_text", 0) if ocr else 0
                unreadable.extend(payload.get("unreadable", [])[: max(0, 200 - len(unreadable))])
                diagnostics.extend(payload.get("diagnostics", [])[: max(0, 40 - len(diagnostics))])
                for sp in payload.get("schedule_pages", []):
                    if len(schedule_pages) < 60:
                        schedule_pages.append({"doc": (ref.title or ref.url)[:70], "url": ref.url, **sp})
                for t in payload.get("terms", []):
                    if len(term_lines) < 400:
                        term_lines.append({"doc": (ref.title or ref.url)[:60], **t})
                facts.extend(fact_from_dict(f) for f in payload["facts"])
        inventory.append(entry)
    acq["distinct_sha256"] = len(by_sha)
    t_extract = time.time()
    from .extract import dedupe
    facts = dedupe(facts)
    registry_results = {}
    if registry == "vies":
        from .registry import ViesClient, check_vats
        registry_results = check_vats(facts, registry_client or ViesClient(store.root / "registry"))
        log(f"[registry] {len(registry_results)} VAT numbers checked on VIES")
    facts.extend(_portal_facts(identity))
    groups = group(facts)
    log(f"[extract] {len(facts)} facts, {sum(g['status']=='conflict' for g in groups)} conflict groups")

    canonical = {"status": "not_checked"}
    record = None
    if data_json:
        canonical = find_by_url(load_records(data_json), identity.raw.get("entry_url", record_url))
        if canonical["status"] == "missing" and identity.raw.get("entry_url") != identity.info_url:
            canonical = find_by_url(load_records(data_json), identity.info_url)
        record = canonical.get("record")
    meta = {"proponent": identity.proponent, "municipalities": identity.municipalities,
            "municipalities_citation": identity.raw.get("municipalities_citation"),
            "province": identity.province, "region": identity.region}
    delta = compare(record, groups, meta)
    proposal = build_proposal(facts, groups)
    gate_rows = proposal_gates(proposal, facts, groups)
    acq_gate = acquisition_gate(acq)
    blocking = acq_gate["blocking"] or any(g["blocking"] for g in gate_rows)
    dossier = {
        "schema": SCHEMA_VERSION, "toolbox_version": TOOLBOX_VERSION,
        "generated_utc": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "project": asdict(identity),
        "acquisition": acq, "inventory": inventory, "unreadable_pages": unreadable,
        "sha_aliases": {k: v for k, v in sha_aliases.items() if len(v) > 1},
        "diagnostics": diagnostics, "registry": registry_results, "term_lines": term_lines,
        "schedule_pages": schedule_pages,
        "facts": facts_to_json(facts), "groups": groups,
        "canonical": {k: v for k, v in canonical.items() if k != "record"}, "canonical_record": record,
        "delta": delta, "proposal": proposal, "gates": {"acquisition": acq_gate, "proposal": gate_rows},
        "review": {"status": "blocked_for_auto_approval" if blocking else "ready_for_human_review",
                   "auto_approval": False,
                   "note": "Proposals never modify the canonical dataset automatically."},
        "efficiency": {"seconds_total": round(time.time() - t0, 1),
                       "seconds_fetch_parse": round(t_extract - t_fetch, 1),
                       "seconds_per_document": round((t_extract - t_fetch) / max(1, len(docs)), 2),
                       "cache": store.stats, "store_bytes": store.size_report(), "ocr_pages_used": budget.used,
                       "ocr_deferred_by_deadline": budget.deferred_by_time},
    }
    return dossier


PRIORITY_DOCS = re.compile(r"(?i)relazion|cron?o?\s*programma|sintesi|istanza|decreto|provvedimento|parere|determin|autorizzaz|computo|quadro\s+economico|preventivo")
DRAWING_TITLES = (r"\btav(?:ola)?\.?\s*\d|\btav\b|planimetr|layout|elaborat\w*\s+grafic|sezion[ei]\b|prospett|ortofoto|"
                  r"inquadramento|particolar\w*\s+costruttiv|cartograf|fotoinserim|rendering|profil[oi]\s+longitudinal")
SCHED_RE = re.compile(r"(?i)cron?o?\s*programma|gantt|diagramma\s+(?:dei\s+)?lavori|programma\s+(?:dei\s+)?lavori")
# lines kept (max 40 per document) to tune extractors for fields the user asked for, since texts are deleted
TERM_RE = re.compile(r"(?i)\bpal[io]\b|infiss|superficie|\bettari\b|\d\s*ha\b|cronoprogramma|gantt|\bmes[ei]\s+\d|"
                     r"\d+\s*°?\s*mese|settiman[ae]\s+\d|lott[io]\b")
COORD_HINT = re.compile(r"(?i)\bUTM\b|WGS\s*-?\s*84|EPSG|coordinat|gauss|monte\s+mario|\bvertic")


def coordinate_diagnostics(pages: list[PageContext], facts: list[Fact], limit: int = 40) -> list[dict]:
    """Pages that talk about coordinates but yielded no point: never report 'absent', show the text."""
    hit = {(f.citation.url, f.citation.member, f.citation.page) for f in facts if f.field.startswith("geo.")}
    out = []
    for p in pages:
        if (p.url, p.member, p.page) in hit:
            continue
        m = COORD_HINT.search(p.text)
        if not m or not re.search(r"\d{6}", p.text):
            continue
        start = max(0, m.start() - 150)
        out.append({"url": p.url, "page": p.page, "method": p.method,
                    "snippet": re.sub(r"\s+", " ", p.text[start:start + 600])})
        if len(out) >= limit:
            break
    return out


def _party_keys(groups: list[dict]) -> set[str]:
    from .extract.entities import company_key
    keys = set()
    for g in groups:
        if g["field"] in ("role.proponent", "role.designer", "role.geologist", "role.works_director"):
            for v in g["values"]:
                if isinstance(v, dict):
                    keys.add(company_key(str(v["value"])))
    return keys


def build_proposal(facts: list[Fact], groups: list[dict]) -> list[dict[str, Any]]:
    """One candidate per (field, subject) group with a single value; conflicts are never proposed.

    Contacts and VAT numbers are proposed only for documented project parties (proponent,
    designer, geologist, works director), not for authorities or third parties cited in letters.
    """
    proposal = []
    parties = _party_keys(groups)
    for g in groups:
        if g["field"] not in PROPOSABLE or g["status"] != "single_value":
            continue
        if g["field"] in ("company.vat", "contact.pec", "contact.email", "contact.phone") and (g["subject"] or "") not in parties:
            continue
        if g["field"].startswith("tech.") and g["field"].endswith(("_mwp", "_mw", "_mwh")) and g["subject"] not in ("plant",):
            continue
        v = g["values"][0]
        proposal.append({"field": g["field"], "subject": g["subject"], "value": v["value"],
                         "fact_id": v["fact_ids"][0], "occurrences": v["occurrences"],
                         "documents": v["documents"][:5]})
    poly = [f for f in facts if f.field == "geo.area_polygon"]
    for f in poly:
        proposal.append({"field": f.field, "subject": f.subject, "value": f.value, "fact_id": f.fact_id(),
                         "occurrences": 1, "documents": [f.citation.url]})
    return proposal


def _attach_register(identity, record_url: str, data_json, get_html, log) -> None:
    """When the radar merged this procedure with a plant-register sheet (Toscana ATOS), read the sheet
    too: the card then reports the last authorisation act recorded by the Region."""
    from .adapters.api_portals import AtosAdapter
    rec = find_by_url(load_records(data_json), record_url).get("record") or {}
    url = next((u for u in rec.get("_dedupe_urls", []) if AtosAdapter.handles(u)), None)
    if not url:
        return
    try:
        reg = AtosAdapter(get_html)
        identity.raw["register_sheet"] = reg.identify(url).raw["register_sheet"]
        identity.raw["register_url"] = url
    except Exception as exc:                       # the register is an extra: never block the analysis
        log(f"[register] {url}: {str(exc)[:120]}")


def _portal_facts(identity) -> list:
    """Facts the portal itself publishes in structured form (official site outline)."""
    from .schema import Citation, Fact
    out = []
    loc = identity.raw.get("localizzazione")
    if isinstance(loc, str):
        try:
            import json as _json
            loc = _json.loads(loc)
        except ValueError:
            loc = None
    feats = (loc or {}).get("features") if isinstance(loc, dict) else None
    for feat in feats or []:
        geom = (feat or {}).get("geometry") or {}
        if geom.get("type") == "MultiPoint":
            for lon, lat, *_ in geom.get("coordinates") or []:
                out.append(Fact(field="geo.latlon_point", value={"lat": lat, "lon": lon},
                                citation=Citation(url=identity.raw.get("official_map_url") or identity.info_url, document_sha256=None, page=None,
                                                  excerpt="punto pubblicato sulla mappa del portale regionale"),
                                precision="official_outline", qualifiers={"object": "official_outline"},
                                limits="Point drawn on the authority's map", extractor="portal.outline"))
            continue
        polys = [geom["coordinates"]] if geom.get("type") == "Polygon" else geom.get("coordinates", []) if geom.get("type") == "MultiPolygon" else []
        for poly in polys:
            ring = poly[0] if poly else []
            if len(ring) < 3:
                continue
            out.append(Fact(field="geo.area_polygon", value={"geojson": {"type": "Polygon", "coordinates": [ring]}},
                            citation=Citation(url=identity.raw.get("official_map_url") or identity.info_url, document_sha256=None, page=None, excerpt="perimetro pubblicato dal portale regionale"),
                            precision="official_outline", qualifiers={"object": "official_outline"},
                            limits="Site outline published by the authority's portal", extractor="portal.outline"))
    return out
