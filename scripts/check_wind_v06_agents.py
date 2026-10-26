#!/usr/bin/env python3
from __future__ import annotations

import sys
import tempfile
from datetime import date
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from app.wind_agents.base import AgentFinding
from app.wind_agents.company_watch import due_company_ids
from app.wind_agents.evidence import can_close_execution_scope, evidence_layer
from app.wind_agents.execution_watch import build_execution_queue
from app.wind_agents.planner import (
    _merge_company_registries,
    _merge_institutional_registries,
    build_company_watch_catalog,
    build_institutional_watch_catalog,
    build_run_plan,
)
from app.wind_agents.reconcile import build_digest, reconcile_finding
from app.wind_agents.runner import due_agent_ids, executable_agent_ids
from app.wind_agents import state


company_base, companies = _merge_company_registries()
institutional_base, sources = _merge_institutional_registries()
assert len(companies) >= 58, len(companies)
assert len(sources) >= 31, len(sources)
assert company_base["monitoring"]["high_priority_cadence_days"] == 7
assert institutional_base["monitoring"]["priority_regional_cadence_days"] == 3

as_of = date(2026, 9, 5)
plan = build_run_plan(as_of=as_of)
project_ids = {task.task_id for task in plan.projects}
for required in ["andretta-bisaccia", "alia-sclafani", "serra-giannina"]:
    assert required in project_ids, f"priority project missing from wind-agent plan: {required}"
assert plan.institutional, "institutional due queue empty"
assert plan.companies, "company due queue empty"

execution_queue = build_execution_queue(as_of=as_of)
execution_ids = {row["project_id"] for row in execution_queue["priority_projects"]}
assert {"andretta-bisaccia", "alia-sclafani", "serra-giannina"}.issubset(execution_ids)
assert execution_queue["projects"] == len(plan.projects), execution_queue
assert execution_queue["open_scope_count"] > 0
assert execution_queue["priority_projects"][0]["urgency_score"] >= 90
assert "A1/A2" in execution_queue["guard"]

catalog = {task.task_id: task for task in build_institutional_watch_catalog(as_of)}
company_catalog = build_company_watch_catalog(as_of)
assert len(company_catalog) >= 58, len(company_catalog)
assert sum(bool(task.watch_urls) for task in company_catalog) >= 50, "company watch URL coverage too low"

implemented = set(executable_agent_ids())
required_adapters = {
    "abruzzo-via",
    "basilicata-via",
    "calabria-via",
    "campania-viavas",
    "emilia-romagna-regional",
    "lazio-regional",
    "liguria-via-procedimenti",
    "lombardia-regional",
    "marche-via-regional",
    "mase-provvedimenti",
    "mase-via",
    "molise-au-eolico",
    "piemonte-regional",
    "puglia-sistema-energia",
    "sardegna-sira",
    "sicilia-sivvi",
    "terna-econnextion",
    "toscana-atos",
    "toscana-gea",
    "umbria-regional",
    "veneto-regional",
}
assert required_adapters.issubset(implemented), implemented
assert len(implemented) >= 21, implemented
assert required_adapters.issubset(catalog), f"adapter/registry id drift: {required_adapters - set(catalog)}"

# Evidence discipline: generic capability / weak signals never close scope.
assert not can_close_execution_scope(
    confidence="B",
    project_specific=True,
    execution_scope="civil_bop",
    status="confirmed",
)
assert not can_close_execution_scope(
    confidence="A1",
    project_specific=False,
    execution_scope="civil_bop",
    status="confirmed",
)
assert not can_close_execution_scope(
    confidence="A1",
    project_specific=True,
    execution_scope=None,
    status="confirmed",
)
assert can_close_execution_scope(
    confidence="A1",
    project_specific=True,
    execution_scope="civil_bop",
    status="confirmed",
)
assert evidence_layer(project_specific=False, execution_scope=None) == "network_intelligence"

# Reconciliation is conservative and advisory only.
synthetic_canonical = [
    {
        "id": "andretta-bisaccia",
        "name": "Andretta-Bisaccia",
        "mw": 88.5,
        "region": "Campania",
        "municipalities": ["Andretta", "Bisaccia", "Vallata"],
        "developer": "Edison Rinnovabili",
        "stage": "E6",
        "priority": "A+",
        "sources": [],
    }
]
project_finding = {
    "source_url": "https://example.com/andretta",
    "title": "Andretta-Bisaccia",
    "finding_type": "project_source",
    "payload": {
        "project_name": "Andretta-Bisaccia",
        "proponent": "Edison Rinnovabili",
        "region": "Campania",
        "municipalities": ["Andretta"],
        "power_mw": 88.5,
        "project_specific": True,
    },
}
project_match = reconcile_finding(project_finding, canonical=synthetic_canonical, discovery=[])
assert project_match["status"] == "high_confidence_match", project_match
assert project_match["auto_reconciled"] is True, project_match
assert project_match["best"]["target_id"] == "andretta-bisaccia", project_match

company_finding = {
    "source_url": "https://example.com/company",
    "title": "Edison direct source",
    "finding_type": "company_source_snapshot",
    "payload": {
        "company_name": "Edison Rinnovabili",
        "project_name": "Andretta-Bisaccia",
        "region": "Campania",
        "project_links_registry": ["andretta-bisaccia"],
        "project_specific": False,
        "signal_excerpt": "Wind construction project update.",
    },
}
company_match = reconcile_finding(company_finding, canonical=synthetic_canonical, discovery=[])
assert company_match["best"]["target_id"] == "andretta-bisaccia", company_match
assert company_match["auto_reconciled"] is False, company_match
assert reconcile_finding(project_finding, canonical=[], discovery=[])["best"] is None

# PV-Agent-style raw/history persistence must detect new / unchanged / changed.
# Operational cursors and live watch timestamps are separate from canonical data.
with tempfile.TemporaryDirectory() as tmp:
    state.DB_PATH = Path(tmp) / "wind_agent_test.sqlite"
    assert state.get_source_cursor("test-cursor") is None
    assert state.get_source_cursor("test-cursor", "100") == "100"
    state.set_source_cursor("test-cursor", 123, {"kind": "validator"})
    assert state.get_source_cursor("test-cursor") == "123"

    initial_due = set(due_agent_ids(as_of=as_of))
    assert required_adapters.issubset(initial_due), initial_due
    future_company_due = set(due_company_ids(as_of=date(2026, 10, 5)))
    assert future_company_due, "company watch future due queue unexpectedly empty"

    run_id = state.begin_run(plan.total_tasks, note="validator")
    state.mark_watch_attempt("mase-via", run_id, success=True, metadata={"validator": True})
    assert state.get_watch_status("mase-via")["last_success"]
    assert "mase-via" not in set(due_agent_ids(as_of=as_of))

    first_company = next(task for task in company_catalog if task.watch_urls)
    company_watch_id = f"company:{first_company.task_id}"
    state.mark_watch_attempt(company_watch_id, run_id, success=True, metadata={"validator": True})
    assert state.get_watch_status(company_watch_id)["last_success"]

    finding = AgentFinding(
        external_id="test-1",
        source_name="validator",
        source_url="https://example.com/wind/1",
        title="Wind test",
        finding_type="project_source",
        payload={"mw": 10, "project_specific": True},
    )
    assert state.upsert_finding(run_id, "test_agent", finding) == "new"
    assert state.upsert_finding(run_id, "test_agent", finding) == "unchanged"
    changed = AgentFinding(
        external_id="test-1",
        source_name="validator",
        source_url="https://example.com/wind/1",
        title="Wind test",
        finding_type="project_source",
        payload={"mw": 12, "project_specific": True},
    )
    assert state.upsert_finding(run_id, "test_agent", changed) == "changed"
    state.finish_run(run_id, findings=3, changed_items=2)

    events = state.get_run_events(run_id)
    assert [event["event_type"] for event in events] == ["new", "changed"], events
    digest = build_digest([run_id])
    assert digest["events"] == 2, digest
    assert digest["actionable_events"] == 2, digest
    assert all(item["action_type"] == "new_project_lead" for item in digest["items"]), digest
    assert "review-only" in digest["guard"].lower(), digest

print(
    f"v0.6 wind agents OK: {len(companies)} companies, {len(sources)} institutional nodes, "
    f"{len(plan.projects)} execution-watch projects, adapters={','.join(executable_agent_ids())}"
)
