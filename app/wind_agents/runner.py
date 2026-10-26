from __future__ import annotations

from collections.abc import Iterable
from datetime import date, datetime
from typing import Any

from .adapters import (
    AbruzzoWindAgent,
    BasilicataWindAgent,
    CalabriaWindAgent,
    CampaniaWindAgent,
    EmiliaRomagnaWindAgent,
    LazioWindAgent,
    LiguriaWindAgent,
    LombardiaWindAgent,
    MarcheWindAgent,
    MaseProvvedimentiWindAgent,
    MaseWindAgent,
    MoliseWindAgent,
    PiemonteWindAgent,
    SardegnaWindAgent,
    SiciliaWindAgent,
    SistemaPugliaWindAgent,
    TernaEconnextionWindAgent,
    ToscanaAtosWindAgent,
    ToscanaWindAgent,
    UmbriaWindAgent,
    VenetoWindAgent,
)
from .planner import build_institutional_watch_catalog, build_run_plan
from .state import (
    begin_run,
    finish_run,
    get_watch_status,
    mark_watch_attempt,
    upsert_finding,
)


# IDs deliberately match institutional-source-network-v06*.json so cadence,
# runtime status and adapter identity share the same stable key.
AGENT_FACTORIES = {
    "abruzzo-via": AbruzzoWindAgent,
    "basilicata-via": BasilicataWindAgent,
    "calabria-via": CalabriaWindAgent,
    "campania-viavas": CampaniaWindAgent,
    "emilia-romagna-regional": EmiliaRomagnaWindAgent,
    "lazio-regional": LazioWindAgent,
    "liguria-via-procedimenti": LiguriaWindAgent,
    "lombardia-regional": LombardiaWindAgent,
    "marche-via-regional": MarcheWindAgent,
    "mase-provvedimenti": MaseProvvedimentiWindAgent,
    "mase-via": MaseWindAgent,
    "molise-au-eolico": MoliseWindAgent,
    "piemonte-regional": PiemonteWindAgent,
    "puglia-sistema-energia": SistemaPugliaWindAgent,
    "sardegna-sira": SardegnaWindAgent,
    "sicilia-sivvi": SiciliaWindAgent,
    "terna-econnextion": TernaEconnextionWindAgent,
    "toscana-atos": ToscanaAtosWindAgent,
    "toscana-gea": ToscanaWindAgent,
    "umbria-regional": UmbriaWindAgent,
    "veneto-regional": VenetoWindAgent,
}


def executable_agent_ids() -> list[str]:
    return sorted(AGENT_FACTORIES)


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


def due_agent_ids(as_of: date | None = None) -> list[str]:
    """Return executable institutional adapters due by runtime cadence.

    Registry `last_checked` is the audited baseline; after live execution the
    persistent `watch_status.last_success` becomes the scheduling reference.
    """

    as_of = as_of or date.today()
    catalog = {task.task_id: task for task in build_institutional_watch_catalog(as_of)}
    due: list[str] = []

    for source_id in executable_agent_ids():
        task = catalog.get(source_id)
        if task is None:
            due.append(source_id)
            continue

        runtime = get_watch_status(source_id) or {}
        last_runtime = _parse_day(runtime.get("last_success"))
        last_registry = _parse_day(str(task.target.get("last_checked") or ""))
        last_checked = max(
            [candidate for candidate in (last_runtime, last_registry) if candidate is not None],
            default=None,
        )
        if last_checked is None or (as_of - last_checked).days >= int(task.cadence_days):
            due.append(source_id)

    return sorted(due)


def _source_health(counters: dict[str, Any]) -> str:
    if counters.get("status") == "error":
        return "error"
    if int(counters.get("project_specific_findings") or 0) > 0:
        return "project_data"
    if int(counters.get("findings") or 0) > 0:
        return "channel_or_market_only"
    return "empty_success"


def run_agents(
    source_ids: Iterable[str] | None = None,
    *,
    due_only: bool = False,
) -> dict[str, Any]:
    """Run implemented adapters and persist raw/history/runtime state.

    One external portal failure no longer aborts the entire watch. Errors are
    recorded per source and the remaining due adapters continue. Canonical Wind
    JSON is never written by this runner.

    `data_health` deliberately distinguishes successful project-level extraction
    from channel/market snapshots and empty-but-reachable sources. A green HTTP
    run therefore cannot be mistaken for project-data coverage.
    """

    plan = build_run_plan()
    available = set(AGENT_FACTORIES)

    if source_ids is None:
        requested = set(due_agent_ids() if due_only else executable_agent_ids())
    else:
        requested = set(source_ids)
        unknown = requested - available
        if unknown:
            raise ValueError(f"unknown/unimplemented wind agent ids: {sorted(unknown)}")
        if due_only:
            requested &= set(due_agent_ids())

    run_id = begin_run(
        planned_tasks=len(requested),
        note=("due-only; " if due_only else "") + f"executed adapters: {', '.join(sorted(requested)) or 'none'}",
    )

    findings_count = 0
    changed_count = 0
    per_agent: dict[str, dict[str, Any]] = {}
    errors: dict[str, str] = {}

    try:
        for source_id in sorted(requested):
            agent = AGENT_FACTORIES[source_id]()
            counters: dict[str, Any] = {
                "findings": 0,
                "new": 0,
                "changed": 0,
                "unchanged": 0,
                "project_specific_findings": 0,
                "finding_types": {},
                "status": "running",
                "data_health": "running",
            }
            try:
                findings = agent.fetch()
                for finding in findings:
                    event = upsert_finding(run_id, agent.agent_name, finding)
                    counters["findings"] += 1
                    counters[event] += 1
                    finding_type = str(finding.finding_type or "unknown")
                    counters["finding_types"][finding_type] = counters["finding_types"].get(finding_type, 0) + 1
                    if bool((finding.payload or {}).get("project_specific")):
                        counters["project_specific_findings"] += 1
                    findings_count += 1
                    if event in {"new", "changed"}:
                        changed_count += 1
                counters["status"] = "success"
                counters["data_health"] = _source_health(counters)
                mark_watch_attempt(
                    source_id,
                    run_id,
                    success=True,
                    metadata={
                        "findings": counters["findings"],
                        "new": counters["new"],
                        "changed": counters["changed"],
                        "project_specific_findings": counters["project_specific_findings"],
                        "finding_types": counters["finding_types"],
                        "data_health": counters["data_health"],
                    },
                )
            except Exception as exc:
                message = f"{type(exc).__name__}: {exc}"
                counters["status"] = "error"
                counters["data_health"] = "error"
                counters["error"] = message
                errors[source_id] = message
                mark_watch_attempt(
                    source_id,
                    run_id,
                    success=False,
                    error=message,
                    metadata={
                        "findings_before_error": counters["findings"],
                        "project_specific_findings": counters["project_specific_findings"],
                        "finding_types": counters["finding_types"],
                        "data_health": "error",
                    },
                )
            per_agent[source_id] = counters
    finally:
        finish_run(run_id, findings=findings_count, changed_items=changed_count)

    health_counts: dict[str, int] = {}
    for counters in per_agent.values():
        health = str(counters.get("data_health") or "unknown")
        health_counts[health] = health_counts.get(health, 0) + 1

    return {
        "run_id": run_id,
        "planned_tasks": len(requested),
        "due_only": due_only,
        "executed_agents": sorted(requested),
        "findings": findings_count,
        "new_or_changed": changed_count,
        "errors": errors,
        "data_health": health_counts,
        "per_agent": per_agent,
    }
