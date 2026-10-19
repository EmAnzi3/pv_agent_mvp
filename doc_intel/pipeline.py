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

from . import SCHEMA_VERSION, TOOLBOX_VERSION
from .adapters import adapter_for_url
from .adapters.base import Discovery
from .canonical import compare, find_by_url, load_records
from .extract import extract_all
from .extract.context import PageContext
from .fetch import Fetcher
from .gates import acquisition_gate, proposal_gates
from .parse import OcrBudget, extract_pages
from .reconcile import group
from .schema import Fact, facts_to_json
from .store import Store

PROPOSABLE = ("company.vat", "contact.pec", "contact.email", "contact.phone", "role.proponent", "role.designer", "role.geologist",
              "tech.pv_dc_mwp", "tech.pv_ac_mw", "tech.bess_power_mw", "tech.bess_energy_mwh",
              "tech.module_power_wp", "tech.inverter_count", "tech.inverter_power_kw", "tech.mounting",
              "tech.technology", "grid.voltage_kv", "geo.area_polygon",
              "schedule.duration.construction", "schedule.duration.decommissioning",
              "role.epc", "role.bop", "role.contractor")


def run(record_url: str, store_dir: str, data_json: str | None = None, ocr: bool = True,
        ocr_max_pages: int = 400, max_docs: int | None = None, workers: int = 3,
        refresh: bool = False, registry: str | None = None, registry_client=None,
        time_budget_s: float | None = None, get_html: Callable[[str], str] | None = None,
        fetcher: Fetcher | None = None, log: Callable[[str], None] = print) -> dict[str, Any]:
    t0 = time.time()
    store = Store(store_dir)
    adapter = adapter_for_url(record_url)
    if get_html:
        adapter.get_html = get_html
    identity = adapter.identify(record_url)
    disc: Discovery = adapter.discover(identity)
    log(f"[discover] {len(disc.documents)} documents in {len(disc.procedures)} procedures "
        f"({round(time.time() - t0)}s); errors: {disc.errors}")
    manifest = store.load_manifest(adapter.source, identity.project_id)
    manifest["procedures"] = disc.procedures
    fetcher = fetcher or Fetcher(store)
    budget = OcrBudget(ocr_max_pages, deadline=(t0 + time_budget_s) if time_budget_s else None)
    docs = disc.documents[: max_docs or None]

    deadline = (t0 + time_budget_s) if time_budget_s else None
    counter = {"n": 0}
    import threading
    lock = threading.Lock()

    def process(ref):
        if deadline and time.time() >= deadline:
            from .fetch import FetchResult
            return ref, FetchResult(ref.url, None, None, 0, "deferred", "time budget reached before download"), []
        t1 = time.time()
        res = fetcher.fetch(ref.url, manifest, refresh=refresh, auth=ref.extra.get("auth"))
        with lock:
            counter["n"] += 1
            log(f"[doc {counter['n']}/{len(docs)}] {res.state} {res.kind or '-'} {res.size // 1024} KB "
                f"{time.time() - t1:.0f}s {(ref.title or ref.url)[:70]}" + (f" ERROR {res.error[:80]}" if res.error else ""))
        parsed = []
        for mem in res.members:
            try:
                parsed.append((mem, extract_pages(mem.raw, res.sha256, store,
                                                  member=None if mem.depth == 0 else mem.name,
                                                  ocr=ocr, budget=budget)))
            except Exception as exc:
                parsed.append((mem, {"error": str(exc)[:300], "pages": [], "summary": {}}))
        return ref, res, parsed

    t_fetch = time.time()
    with ThreadPoolExecutor(max_workers=max(1, workers)) as pool:
        results = list(pool.map(process, docs))
    store.save_manifest(manifest)

    pages: list[PageContext] = []
    unreadable: list[dict] = []
    sha_aliases: dict[str, list[str]] = {}
    inventory = []
    by_sha: dict[str, str] = {}
    acq = {"declared_total": sum(p["declared"] or 0 for p in disc.procedures), "procedures": disc.procedures,
           "references": len(disc.documents), "processed_references": len(docs), "download_failures": 0,
           "distinct_sha256": 0, "duplicate_references": 0, "pdf_files": 0, "pages_total": 0,
           "pages_digital": 0, "pages_ocr": 0, "pages_ocr_pending": 0, "pages_unreadable": 0,
           "pages_graphic_no_text": 0,
           "parse_errors": 0, "skipped_members": 0}
    for ref, res, parsed in results:
        entry = {"url": ref.url, "procedure_id": ref.procedure_id, "title": ref.title,
                 "index_page": ref.index_page, "index_date": ref.extra.get("index_date"),
                 "sha256": res.sha256, "kind": res.kind, "bytes": res.size, "state": res.state,
                 "error": res.error, "duplicate_of": None, "members": [], "skipped": res.skipped_members}
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
        acq["skipped_members"] += len(res.skipped_members)
        for mem, txt in parsed:
            s = txt.get("summary", {})
            entry["members"].append({"name": mem.name, "pages": s.get("pages", 0), "summary": s,
                                     "backend": txt.get("backend"), "error": txt.get("error"),
                                     "from_cache": txt.get("from_cache", False)})
            if txt.get("error"):
                acq["parse_errors"] += 1
            if entry["duplicate_of"]:
                continue  # count and extract distinct content only once
            acq["pdf_files"] += 1
            acq["pages_total"] += s.get("pages", 0)
            acq["pages_digital"] += s.get("digital", 0)
            acq["pages_ocr"] += s.get("ocr", 0)
            acq["pages_ocr_pending"] += s.get("ocr_deferred", 0) + (0 if ocr else s.get("no_text", 0))
            acq["pages_unreadable"] += s.get("ocr_failed", 0)
            acq["pages_graphic_no_text"] += s.get("no_text", 0) if ocr else 0
            for p in txt.get("pages", []):
                if p["method"] in ("ocr_failed", "none_after_ocr", "none") and len(unreadable) < 200:
                    unreadable.append({"url": ref.url, "member": None if mem.depth == 0 else mem.name,
                                       "page": p["n"], "method": p["method"], "error": p.get("error")})
                if p.get("text"):
                    pages.append(PageContext(p["text"], ref.url, p["n"], res.sha256,
                                             None if mem.depth == 0 else mem.name, ref.procedure_id,
                                             ref.title, ref.extra.get("index_date"), None, p["method"]))
        inventory.append(entry)
    acq["distinct_sha256"] = len(by_sha)
    t_extract = time.time()
    facts = extract_all(pages)
    registry_results = {}
    if registry == "vies":
        from .registry import ViesClient, check_vats
        registry_results = check_vats(facts, registry_client or ViesClient(store.root / "registry"))
        log(f"[registry] {len(registry_results)} VAT numbers checked on VIES")
    groups = group(facts)
    diagnostics = coordinate_diagnostics(pages, facts)
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
        "diagnostics": diagnostics, "registry": registry_results,
        "facts": facts_to_json(facts), "groups": groups,
        "canonical": {k: v for k, v in canonical.items() if k != "record"}, "canonical_record": record,
        "delta": delta, "proposal": proposal, "gates": {"acquisition": acq_gate, "proposal": gate_rows},
        "review": {"status": "blocked_for_auto_approval" if blocking else "ready_for_human_review",
                   "auto_approval": False,
                   "note": "Proposals never modify the canonical dataset automatically."},
        "efficiency": {"seconds_total": round(time.time() - t0, 1),
                       "seconds_fetch_parse": round(t_extract - t_fetch, 1),
                       "seconds_per_document": round((t_extract - t_fetch) / max(1, len(docs)), 2),
                       "cache": store.stats, "ocr_pages_used": budget.used,
                       "ocr_deferred_by_deadline": budget.deferred_by_time},
    }
    return dossier


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
