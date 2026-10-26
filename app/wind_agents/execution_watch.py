from __future__ import annotations

from datetime import date, datetime
from typing import Any

from .planner import build_run_plan


GAP_PLAYBOOK = {
    "Civil BoP": ["developer/tender pages", "regional AU/PAUR documents", "civil EPC/BoP player watch"],
    "Electrical BoP": ["grid drawings/specifications", "electrical EPC player watch", "substation procurement notices"],
    "SSE / grid contractor": ["Terna/connection acts", "substation drawings", "grid/EPC player watch"],
    "Erection contractor": ["OEM project news", "erection/heavy-lift player watch", "site mobilisation evidence"],
    "Dismantling contractor": ["repowering work plans", "demolition contractor pages", "site logistics documents"],
    "Logistics / heavy transport": ["transport/access plans", "heavy transport player watch", "municipal/provincial road acts"],
    "Foundation contractor": ["foundation/civil drawings", "concrete/civil contractor watch", "site mobilisation evidence"],
    "OEM": ["OEM order releases", "developer procurement releases", "turbine model documents"],
    "Foundations / substructure / mooring": ["offshore FEED/procurement", "fabrication yards", "developer supplier announcements"],
    "WTG installation offshore": ["T&I procurement", "installation vessel/heavy lift", "OEM/developer announcements"],
    "Inter-array cables": ["cable procurement", "marine installation contractor", "offshore electrical package"],
    "Offshore substation / electrical platform": ["OSS/EPC procurement", "HV equipment suppliers", "fabrication contractor"],
    "Export cable + landfall": ["export cable procurement", "landfall permits", "marine cable installation"],
    "Onshore SSE / grid": ["Terna connection acts", "onshore substation EPC", "grid contractor watch"],
    "Marine logistics / port / heavy lift": ["port agreements", "marine logistics", "heavy lift vessel/yard mobilisation"],
    "Civil works onshore connection": ["PAUR/AU civil drawings", "road/landfall works", "civil EPC player watch"],
}


def _parse_day(value: Any) -> date | None:
    if not value:
        return None
    try:
        return datetime.fromisoformat(str(value)[:10]).date()
    except ValueError:
        return None


def _urgency(task, as_of: date) -> tuple[int, list[str]]:
    stage = task.target.get("stage")
    priority = task.priority
    score = {"E7": 88, "E6": 84, "E5": 74, "E4": 66}.get(stage, 50)
    reasons = [f"stage {stage}"]

    if priority == "A+":
        score += 12
        reasons.append("priority A+")
    elif priority == "A":
        score += 8
        reasons.append("priority A")
    elif priority == "B":
        score += 3

    next_item = task.target.get("next") or {}
    next_day = _parse_day(next_item.get("date"))
    if next_day:
        delta = (next_day - as_of).days
        if delta < 0:
            score += 5
            reasons.append("milestone already due/passed")
        elif delta <= 30:
            score += 12
            reasons.append("milestone <=30d")
        elif delta <= 90:
            score += 9
            reasons.append("milestone <=90d")
        elif delta <= 180:
            score += 5
            reasons.append("milestone <=180d")

    gaps = task.target.get("gaps") or []
    if len(gaps) >= 6:
        score += 5
        reasons.append("many open scopes")
    elif len(gaps) >= 3:
        score += 2

    return min(score, 100), reasons


def _gap_actions(gaps: list[str]) -> list[dict[str, Any]]:
    actions: list[dict[str, Any]] = []
    for gap in gaps:
        actions.append(
            {
                "scope": gap,
                "hunt_routes": GAP_PLAYBOOK.get(
                    gap,
                    ["project documents", "developer/company direct sources", "institutional acts"],
                ),
                "evidence_required": "project-specific A1/A2 explicit execution role/award",
            }
        )
    return actions


def build_execution_queue(as_of: date | None = None) -> dict[str, Any]:
    """Build a contractor-hunt queue from canonical E4-E7 gaps.

    The queue is operational prioritisation only. It never changes scope
    coverage, contractor relations, stage or commercial ranking.
    """

    as_of = as_of or date.today()
    plan = build_run_plan(as_of=as_of)
    queue: list[dict[str, Any]] = []

    for task in plan.projects:
        gaps = list(task.target.get("gaps") or [])
        urgency, urgency_reasons = _urgency(task, as_of)
        queue.append(
            {
                "project_id": task.task_id,
                "name": task.target.get("name"),
                "region": task.target.get("region"),
                "mw": task.target.get("mw"),
                "developer": task.target.get("developer"),
                "stage": task.target.get("stage"),
                "priority": task.priority,
                "urgency_score": urgency,
                "urgency_reasons": urgency_reasons,
                "next": task.target.get("next") or {},
                "open_scopes": gaps,
                "investigation": _gap_actions(gaps),
                "watch_urls": task.watch_urls,
            }
        )

    queue.sort(key=lambda row: (-row["urgency_score"], -(float(row.get("mw") or 0)), row.get("name") or ""))
    return {
        "as_of": as_of.isoformat(),
        "projects": len(queue),
        "open_scope_count": sum(len(row["open_scopes"]) for row in queue),
        "priority_projects": queue,
        "guard": "Investigation queue only: no contractor/scope is confirmed without project-specific A1/A2 evidence.",
    }
