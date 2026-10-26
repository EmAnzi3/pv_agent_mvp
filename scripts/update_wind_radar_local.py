#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
import time
from datetime import datetime
from pathlib import Path
from typing import Any, Callable

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from app.wind_agents.company_watch import due_company_ids, run_company_watch
from app.wind_agents.execution_watch import build_execution_queue
from app.wind_agents.reconcile import build_digest
from app.wind_agents.runner import due_agent_ids, executable_agent_ids, run_agents

REPORT_DIR = ROOT / "reports" / "wind-agent"
LOCAL_STATUS = ROOT / "docs" / "wind" / "data" / "local-run-status.json"
WIND_DATA = ROOT / "docs" / "wind" / "data"


def _load_json(path: Path) -> Any:
    return json.loads(path.read_text(encoding="utf-8-sig"))


def _write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def _canonical_summary() -> dict[str, Any]:
    manifest = _load_json(WIND_DATA / "projects.json")
    meta = _load_json(WIND_DATA / manifest["meta"])
    projects: list[dict[str, Any]] = []
    for chunk in manifest["chunks"]:
        projects.extend(_load_json(WIND_DATA / chunk))
    return {
        "projects": len(projects),
        "wind_mw": round(sum(float(row.get("mw") or 0) for row in projects), 2),
        "bess_mw": round(sum(float(row.get("bess_mw") or 0) for row in projects), 2),
        "as_of": meta.get("as_of"),
        "version": meta.get("version"),
    }


def _safe(label: str, func: Callable[[], dict[str, Any]], fallback: dict[str, Any]) -> dict[str, Any]:
    try:
        return func()
    except Exception as exc:  # local launcher must remain usable if one layer fails
        result = dict(fallback)
        result["status"] = "error"
        result["errors"] = {"runner": f"{type(exc).__name__}: {exc}"}
        print(f"[WARN] {label}: {type(exc).__name__}: {exc}", file=sys.stderr)
        return result


def _error_count(payload: dict[str, Any]) -> int:
    errors = payload.get("errors") or {}
    if isinstance(errors, dict):
        return len(errors)
    if isinstance(errors, list):
        return len(errors)
    return 1 if errors else 0


def _compact_institutional(payload: dict[str, Any], due_count: int) -> dict[str, Any]:
    return {
        "due": due_count,
        "executed": len(payload.get("executed_agents") or []),
        "findings": int(payload.get("findings") or 0),
        "new_or_changed": int(payload.get("new_or_changed") or 0),
        "errors": _error_count(payload),
        "data_health": payload.get("data_health") or {},
    }


def _compact_company(payload: dict[str, Any], due_count: int) -> dict[str, Any]:
    return {
        "due": due_count,
        "executed": len(payload.get("executed_companies") or []),
        "findings": int(payload.get("findings") or 0),
        "new_or_changed": int(payload.get("new_or_changed") or 0),
        "errors": _error_count(payload),
    }


def main() -> int:
    parser = argparse.ArgumentParser(description="One-click local updater for Wind Project & Contractor Radar")
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument("--offline", action="store_true", help="skip external network collection")
    mode.add_argument("--all", action="store_true", help="force all implemented institutional and company watches")
    args = parser.parse_args()

    started = time.monotonic()
    now = datetime.now().astimezone()
    REPORT_DIR.mkdir(parents=True, exist_ok=True)

    canonical = _canonical_summary()
    institutional_due = [] if args.offline else _safe(
        "institutional due queue",
        lambda: {"items": due_agent_ids()},
        {"items": []},
    ).get("items", [])
    company_due = [] if args.offline else _safe(
        "company due queue",
        lambda: {"items": due_company_ids()},
        {"items": []},
    ).get("items", [])

    if args.offline:
        institutional = {
            "run_id": None,
            "executed_agents": [],
            "findings": 0,
            "new_or_changed": 0,
            "errors": {},
            "data_health": {},
            "status": "offline",
        }
        company = {
            "run_id": None,
            "executed_companies": [],
            "findings": 0,
            "new_or_changed": 0,
            "errors": {},
            "status": "offline",
        }
    else:
        institutional = _safe(
            "institutional watch",
            lambda: run_agents(None, due_only=not args.all),
            {
                "run_id": None,
                "executed_agents": [],
                "findings": 0,
                "new_or_changed": 0,
                "errors": {},
                "data_health": {},
            },
        )
        company = _safe(
            "company watch",
            lambda: run_company_watch(None, due_only=not args.all),
            {
                "run_id": None,
                "executed_companies": [],
                "findings": 0,
                "new_or_changed": 0,
                "errors": {},
            },
        )

    execution = _safe(
        "execution investigation queue",
        build_execution_queue,
        {"projects": 0, "open_scope_count": 0, "priority_projects": []},
    )

    run_ids = [
        str(value)
        for value in (institutional.get("run_id"), company.get("run_id"))
        if value
    ]
    if run_ids:
        digest = _safe(
            "review-only digest",
            lambda: build_digest(run_ids),
            {"run_ids": run_ids, "events": 0, "actionable_events": 0, "items": [], "action_types": {}},
        )
    else:
        digest = {
            "run_ids": [],
            "events": 0,
            "actionable_events": 0,
            "non_actionable_events": 0,
            "items": [],
            "action_types": {},
        }

    _write_json(REPORT_DIR / "local-institutional-run.json", institutional)
    _write_json(REPORT_DIR / "local-company-run.json", company)
    _write_json(REPORT_DIR / "local-execution-queue.json", execution)
    _write_json(REPORT_DIR / "local-digest.json", digest)

    total_errors = _error_count(institutional) + _error_count(company)
    mode_name = "offline" if args.offline else ("all" if args.all else "due")
    status = {
        "generated_at": now.isoformat(timespec="seconds"),
        "mode": mode_name,
        "duration_seconds": round(time.monotonic() - started, 1),
        "outcome": "partial" if total_errors else "ok",
        "canonical": canonical,
        "institutional": _compact_institutional(institutional, len(institutional_due)),
        "company": _compact_company(company, len(company_due)),
        "execution_queue": {
            "projects": int(execution.get("projects") or 0),
            "open_scope_count": int(execution.get("open_scope_count") or 0),
        },
        "digest": {
            "events": int(digest.get("events") or 0),
            "actionable_events": int(digest.get("actionable_events") or 0),
            "non_actionable_events": int(digest.get("non_actionable_events") or 0),
            "action_types": digest.get("action_types") or {},
        },
        "guard": (
            "Review-only: raw finding e digest non modificano automaticamente il canonico. "
            "Uno scope esecutivo si chiude solo con evidenza project-specific A1/A2."
        ),
    }
    _write_json(LOCAL_STATUS, status)

    print()
    print("=== Wind Radar local update ===")
    print(f"Mode: {mode_name}")
    print(f"Canonical: {canonical['projects']} projects / {canonical['wind_mw']:.2f} MW wind")
    print(
        "Institutional: "
        f"{status['institutional']['executed']} executed, "
        f"{status['institutional']['new_or_changed']} new/changed, "
        f"{status['institutional']['errors']} errors"
    )
    print(
        "Company watch: "
        f"{status['company']['executed']} executed, "
        f"{status['company']['new_or_changed']} new/changed, "
        f"{status['company']['errors']} errors"
    )
    print(
        "Review digest: "
        f"{status['digest']['actionable_events']} actionable / "
        f"{status['digest']['events']} new-or-changed events"
    )
    print(
        "Execution queue: "
        f"{status['execution_queue']['projects']} projects / "
        f"{status['execution_queue']['open_scope_count']} open scopes"
    )
    if total_errors:
        print(f"[WARN] Completed with {total_errors} source/company errors; canonical unchanged.")
    else:
        print("[OK] Local intelligence refresh completed; canonical unchanged pending review.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
