from __future__ import annotations

import hashlib
import re
from datetime import date, datetime
from typing import Any

import requests
from bs4 import BeautifulSoup

from .base import AgentFinding
from .planner import build_company_watch_catalog
from .state import (
    begin_run,
    finish_run,
    get_watch_status,
    mark_watch_attempt,
    upsert_finding,
)


SIGNAL_TERMS = (
    "wind",
    "eolico",
    "eolica",
    "aerogenerator",
    "repowering",
    "turbine",
    "wtg",
    "balance of plant",
    "bop",
    "civil works",
    "opere civili",
    "electrical",
    "elettric",
    "substation",
    "sottostazione",
    "grid connection",
    "connessione",
    "foundation",
    "fondazion",
    "erection",
    "installation",
    "installazione",
    "logistics",
    "logistica",
    "heavy lift",
    "heavy transport",
    "trasporto eccezionale",
    "contract",
    "contratto",
    "award",
    "commessa",
    "order",
    "ordine",
    "procurement",
    "tender",
    "gara",
    "construction",
    "costruzione",
    "cantiere",
    "commissioning",
    "energization",
    "energizzazione",
    "project",
    "progetto",
    "supplier",
    "fornitore",
    "partnership",
    "acquisition",
    "acquisizione",
)


def _parse_day(value: str | None) -> date | None:
    if not value:
        return None
    try:
        return datetime.fromisoformat(value).date()
    except ValueError:
        try:
            return date.fromisoformat(value[:10])
        except ValueError:
            return None


def _clean(value: object) -> str:
    return re.sub(r"\s+", " ", str(value or "")).strip()


def _company_watch_id(company_id: str) -> str:
    return f"company:{company_id}"


def due_company_ids(as_of: date | None = None) -> list[str]:
    as_of = as_of or date.today()
    due: list[str] = []
    for task in build_company_watch_catalog(as_of):
        if not task.watch_urls:
            continue
        runtime = get_watch_status(_company_watch_id(task.task_id)) or {}
        last_runtime = _parse_day(runtime.get("last_success"))
        last_registry = _parse_day(str(task.target.get("last_checked") or ""))
        last_checked = max(
            [candidate for candidate in (last_runtime, last_registry) if candidate is not None],
            default=None,
        )
        if last_checked is None or (as_of - last_checked).days >= int(task.cadence_days):
            due.append(task.task_id)
    return sorted(due)


def _extract_page(url: str, session: requests.Session) -> dict[str, Any]:
    response = session.get(
        url,
        timeout=45,
        allow_redirects=True,
        headers={
            "User-Agent": "Wind-Radar-Company-Watch/0.6",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "it-IT,it;q=0.9,en;q=0.8",
        },
    )
    response.raise_for_status()
    content_type = (response.headers.get("Content-Type") or "").lower()

    if "html" not in content_type and "xml" not in content_type and not response.text.lstrip().startswith("<"):
        return {
            "final_url": response.url,
            "content_type": content_type,
            "title": None,
            "signal_excerpt": "",
            "headings": [],
            "http_status": response.status_code,
        }

    soup = BeautifulSoup(response.text, "html.parser")
    for node in soup(["script", "style", "noscript", "svg", "canvas", "form"]):
        node.decompose()

    title = _clean(soup.title.get_text(" ", strip=True)) if soup.title else None
    root = soup.find("main") or soup.find("article") or soup.body or soup
    text = _clean(root.get_text(" ", strip=True))

    headings: list[str] = []
    for node in root.find_all(["h1", "h2", "h3"], limit=120):
        value = _clean(node.get_text(" ", strip=True))
        if value and any(term in value.lower() for term in SIGNAL_TERMS):
            headings.append(value[:500])
    headings = list(dict.fromkeys(headings))[:40]

    # Keep only commercial/wind-relevant sentence windows. This is less noisy
    # than hashing cookie banners, footers or whole dynamic news pages.
    sentences = re.split(r"(?<=[.!?])\s+|\s*[|•·]\s*", text)
    selected: list[str] = []
    for sentence in sentences:
        lowered = sentence.lower()
        if any(term in lowered for term in SIGNAL_TERMS):
            value = _clean(sentence)
            if 20 <= len(value) <= 1200:
                selected.append(value)
    selected = list(dict.fromkeys(selected))[:80]

    return {
        "final_url": response.url,
        "content_type": content_type,
        "title": title,
        "signal_excerpt": "\n".join(selected)[:30000],
        "headings": headings,
        "http_status": response.status_code,
    }


def run_company_watch(
    company_ids: list[str] | None = None,
    *,
    due_only: bool = False,
) -> dict[str, Any]:
    """Watch direct company URLs without attributing project execution.

    Direct-company pages are commercial intelligence. Findings have A2 ceiling
    because the company can speak for itself, but remain `project_specific=False`
    until a later reconciliation step proves that a statement is about a named
    canonical project and explicit execution scope.
    """

    catalog = {task.task_id: task for task in build_company_watch_catalog()}
    available = {task_id for task_id, task in catalog.items() if task.watch_urls}

    if company_ids is None:
        requested = set(due_company_ids() if due_only else available)
    else:
        requested = set(company_ids)
        unknown = requested - set(catalog)
        if unknown:
            raise ValueError(f"unknown company ids: {sorted(unknown)}")
        requested &= available
        if due_only:
            requested &= set(due_company_ids())

    run_id = begin_run(
        planned_tasks=len(requested),
        note=("company due-only; " if due_only else "company watch; ")
        + f"targets: {', '.join(sorted(requested)) or 'none'}",
    )

    session = requests.Session()
    findings_count = 0
    changed_count = 0
    errors: dict[str, list[str]] = {}
    per_company: dict[str, dict[str, Any]] = {}

    try:
        for company_id in sorted(requested):
            task = catalog[company_id]
            company_name = task.target.get("name") or company_id
            counters: dict[str, Any] = {
                "name": company_name,
                "urls": len(task.watch_urls),
                "fetched": 0,
                "findings": 0,
                "new": 0,
                "changed": 0,
                "unchanged": 0,
                "url_errors": [],
            }

            for url in task.watch_urls:
                try:
                    page = _extract_page(url, session)
                    counters["fetched"] += 1
                    url_hash = hashlib.sha1(url.encode("utf-8")).hexdigest()[:14]
                    finding = AgentFinding(
                        external_id=f"COMPANY-{company_id}-{url_hash}",
                        source_name=f"Company direct: {company_name}",
                        source_url=page.get("final_url") or url,
                        title=page.get("title") or f"{company_name} direct source",
                        finding_type="company_source_snapshot",
                        payload={
                            "company_id": company_id,
                            "company_name": company_name,
                            "commercial_priority": task.priority,
                            "clusters": task.target.get("clusters", []),
                            "relationship_status": task.target.get("relationship_status"),
                            "project_links_registry": task.target.get("project_links", []),
                            "watched_url": url,
                            "final_url": page.get("final_url"),
                            "headings": page.get("headings", []),
                            "signal_excerpt": page.get("signal_excerpt", ""),
                            "http_status": page.get("http_status"),
                            "content_type": page.get("content_type"),
                            "source_grade_ceiling": "A2",
                            "project_specific": False,
                            "execution_scope": None,
                            "evidence_layer": "network_intelligence",
                        },
                    )
                    event = upsert_finding(run_id, "company_watch", finding)
                    counters["findings"] += 1
                    counters[event] += 1
                    findings_count += 1
                    if event in {"new", "changed"}:
                        changed_count += 1
                except Exception as exc:
                    message = f"{url} -> {type(exc).__name__}: {exc}"
                    counters["url_errors"].append(message)

            if counters["fetched"] > 0:
                mark_watch_attempt(
                    _company_watch_id(company_id),
                    run_id,
                    success=True,
                    metadata={
                        "fetched": counters["fetched"],
                        "urls": counters["urls"],
                        "new": counters["new"],
                        "changed": counters["changed"],
                        "partial_errors": counters["url_errors"],
                    },
                )
            else:
                message = "; ".join(counters["url_errors"]) or "no company watch URL fetched"
                errors[company_id] = counters["url_errors"] or [message]
                mark_watch_attempt(
                    _company_watch_id(company_id),
                    run_id,
                    success=False,
                    error=message,
                    metadata={"urls": counters["urls"]},
                )

            if counters["url_errors"]:
                errors.setdefault(company_id, counters["url_errors"])
            per_company[company_id] = counters
    finally:
        finish_run(run_id, findings=findings_count, changed_items=changed_count)

    return {
        "run_id": run_id,
        "planned_tasks": len(requested),
        "due_only": due_only,
        "executed_companies": sorted(requested),
        "findings": findings_count,
        "new_or_changed": changed_count,
        "errors": errors,
        "per_company": per_company,
    }
