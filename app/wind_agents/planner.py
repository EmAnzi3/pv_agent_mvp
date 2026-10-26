from __future__ import annotations

import json
from datetime import date, datetime
from pathlib import Path
from typing import Any

from .models import AgentRunPlan, AgentTask

ROOT = Path(__file__).resolve().parents[2]
DATA = ROOT / "docs" / "wind" / "data"


def _load_json(path: Path) -> Any:
    return json.loads(path.read_text(encoding="utf-8-sig"))


def _parse_date(value: Any) -> date | None:
    if not value:
        return None
    try:
        return datetime.fromisoformat(str(value)[:10]).date()
    except ValueError:
        return None


def _is_due(last_checked: Any, cadence_days: int, as_of: date) -> bool:
    checked = _parse_date(last_checked)
    if checked is None:
        return True
    return (as_of - checked).days >= cadence_days


def _priority_rank(value: str | None) -> int:
    return {"A+": 0, "A": 1, "B": 2, "C": 3}.get(value or "C", 9)


def _merge_company_row(previous: dict[str, Any], row: dict[str, Any]) -> dict[str, Any]:
    merged = {**previous, **row}
    for key in ("cluster", "known_capabilities", "project_links", "watch_urls"):
        merged[key] = list(dict.fromkeys([*(previous.get(key) or []), *(row.get(key) or [])]))
    return merged


def _merge_company_registries() -> tuple[dict[str, Any], list[dict[str, Any]]]:
    """Merge new-node tranches plus explicit current-update overlays.

    v06/b/c/d introduce nodes and therefore must have unique IDs. v06e is the
    controlled exception: its `updates` may only enrich an already-known node.
    This keeps current project links/watch URLs additive without rewriting the
    original evidence tranche.
    """

    base = _load_json(DATA / "company-network-v06.json")
    order: list[str] = []
    by_id: dict[str, dict[str, Any]] = {}

    for row in base.get("companies", []):
        cid = row["id"]
        if cid in by_id:
            raise ValueError(f"duplicate company id in base registry: {cid}")
        order.append(cid)
        by_id[cid] = dict(row)

    for path in [
        DATA / "company-network-v06b.json",
        DATA / "company-network-v06c.json",
        DATA / "company-network-v06d.json",
    ]:
        if not path.exists():
            continue
        extra = _load_json(path)
        for row in extra.get("companies", []):
            cid = row["id"]
            if cid in by_id:
                raise ValueError(f"duplicate company id across new-node registries: {cid}")
            order.append(cid)
            by_id[cid] = dict(row)

    update_path = DATA / "company-network-v06e.json"
    if update_path.exists():
        updates = _load_json(update_path)
        seen_updates: set[str] = set()
        for row in updates.get("updates", []):
            cid = row["id"]
            if cid in seen_updates:
                raise ValueError(f"duplicate company id in current-update overlay: {cid}")
            seen_updates.add(cid)
            if cid not in by_id:
                raise ValueError(f"company update references unknown node: {cid}")
            by_id[cid] = _merge_company_row(by_id[cid], row)

    return base, [by_id[cid] for cid in order]


def _merge_institutional_registries() -> tuple[dict[str, Any], list[dict[str, Any]]]:
    base = _load_json(DATA / "institutional-source-network-v06.json")
    sources = list(base.get("sources", []))
    seen = {row["id"] for row in sources}

    for path in [DATA / "institutional-source-network-v06b.json"]:
        if not path.exists():
            continue
        extra = _load_json(path)
        for row in extra.get("sources", []):
            if row["id"] in seen:
                raise ValueError(f"duplicate institutional source id: {row['id']}")
            seen.add(row["id"])
            sources.append(row)

    return base, sources


def _load_projects() -> list[dict[str, Any]]:
    manifest = _load_json(DATA / "projects.json")
    projects: list[dict[str, Any]] = []
    for chunk in manifest.get("chunks", []):
        projects.extend(_load_json(DATA / chunk))
    return projects


def _company_tasks(as_of: date, include_not_due: bool = False) -> list[AgentTask]:
    base, companies = _merge_company_registries()
    monitoring = base.get("monitoring", {})
    high = int(monitoring.get("high_priority_cadence_days", 7))
    standard = int(monitoring.get("standard_cadence_days", 14))
    universe = int(monitoring.get("universe_refresh_days", 30))

    out: list[AgentTask] = []
    for company in companies:
        priority = company.get("commercial_priority", "C")
        cadence = int(company.get("cadence_days") or (high if priority in {"A", "A+"} else standard if priority == "B" else universe))
        if not include_not_due and not _is_due(company.get("last_checked"), cadence, as_of):
            continue
        urls = [url for url in company.get("watch_urls", []) if url]
        next_action = company.get("next_action") or "Review direct company sources for awards, mobilisations, hiring and capability changes."
        out.append(
            AgentTask(
                agent="company_watch",
                task_id=company["id"],
                priority=priority,
                cadence_days=cadence,
                reason=next_action,
                target={
                    "name": company.get("name"),
                    "clusters": company.get("cluster", []),
                    "relationship_status": company.get("relationship_status"),
                    "project_links": company.get("project_links", []),
                    "last_checked": company.get("last_checked"),
                },
                watch_urls=urls,
            )
        )

    return sorted(out, key=lambda t: (_priority_rank(t.priority), t.target.get("name") or ""))


def build_company_watch_catalog(as_of: date | None = None) -> list[AgentTask]:
    """Return all company-watch definitions regardless of due state."""

    return _company_tasks(as_of or date.today(), include_not_due=True)


def _institutional_tasks(as_of: date, include_not_due: bool = False) -> list[AgentTask]:
    _, sources = _merge_institutional_registries()
    out: list[AgentTask] = []

    for source in sources:
        cadence = int(source.get("cadence_days") or 7)
        if not include_not_due and not _is_due(source.get("last_checked"), cadence, as_of):
            continue
        urls = [source.get(key) for key in ("official_url", "discovery_url", "secondary_url")]
        urls = [url for url in urls if url]
        out.append(
            AgentTask(
                agent="institutional_watch",
                task_id=source["id"],
                priority=source.get("priority", "C"),
                cadence_days=cadence,
                reason=source.get("wind_adaptation") or "Check the public source for new wind procedures, acts and project documents.",
                target={
                    "institution": source.get("institution"),
                    "region": source.get("region"),
                    "channel": source.get("channel"),
                    "status": source.get("status"),
                    "evidence_ceiling": source.get("evidence_ceiling"),
                    "origin_collector": source.get("origin_collector"),
                    "last_checked": source.get("last_checked"),
                },
                watch_urls=urls,
            )
        )

    return sorted(out, key=lambda t: (_priority_rank(t.priority), t.target.get("region") or "", t.task_id))


def build_institutional_watch_catalog(as_of: date | None = None) -> list[AgentTask]:
    """Return all institutional watch definitions regardless of due state.

    Runtime scheduling can combine these declared cadences with persistent
    `watch_status` timestamps without rewriting registry JSON after every run.
    """

    return _institutional_tasks(as_of or date.today(), include_not_due=True)


def _project_tasks(as_of: date) -> list[AgentTask]:
    projects = _load_projects()
    out: list[AgentTask] = []
    for project in projects:
        stage = project.get("stage")
        gaps = project.get("gaps") or []
        if stage not in {"E4", "E5", "E6", "E7"} or not gaps:
            continue

        priority = project.get("priority") or "C"
        cadence = 3 if stage in {"E6", "E7"} or priority in {"A", "A+"} else 7
        next_item = project.get("next") or {}
        next_label = next_item.get("label")
        next_date = next_item.get("date")
        reason_bits = [f"{stage} with {len(gaps)} open execution scopes"]
        if next_label:
            reason_bits.append(f"next milestone: {next_label}{' ' + next_date if next_date else ''}")
        urls = [source.get("url") for source in project.get("sources", []) if source.get("url")]

        out.append(
            AgentTask(
                agent="project_execution_watch",
                task_id=project["id"],
                priority=priority,
                cadence_days=cadence,
                reason="; ".join(reason_bits),
                target={
                    "name": project.get("name"),
                    "stage": stage,
                    "region": project.get("region"),
                    "mw": project.get("mw"),
                    "developer": project.get("developer"),
                    "gaps": gaps,
                    "next": next_item,
                },
                watch_urls=urls,
            )
        )

    return sorted(out, key=lambda t: (_priority_rank(t.priority), 0 if t.target.get("stage") == "E7" else 1, -(float(t.target.get("mw") or 0))))


def build_run_plan(as_of: date | None = None) -> AgentRunPlan:
    """Build the due queue without altering canonical Wind Radar data.

    This is the Wind equivalent of the PV Agent run orchestration: source watches,
    company watches and project execution watches are planned independently and
    can later feed a shared normalisation/reconciliation/history pipeline.
    """

    as_of = as_of or date.today()
    return AgentRunPlan(
        as_of=as_of.isoformat(),
        institutional=_institutional_tasks(as_of),
        companies=_company_tasks(as_of),
        projects=_project_tasks(as_of),
    )
