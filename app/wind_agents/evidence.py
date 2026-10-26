from __future__ import annotations

EXECUTION_SCOPE_KEYS = {
    "civil_bop",
    "electrical_bop",
    "sse_grid",
    "wtg_foundations",
    "erection",
    "logistics_heavy_transport",
    "dismantling",
    "foundations_substructure_mooring",
    "wtg_installation_offshore",
    "inter_array_cables",
    "offshore_substation",
    "export_cable_landfall",
    "onshore_sse_grid",
    "marine_logistics_port_heavy_lift",
    "civil_onshore_connection",
}


def can_close_execution_scope(
    *,
    confidence: str | None,
    project_specific: bool,
    execution_scope: str | None,
    status: str | None,
) -> bool:
    """Single structural gate for project-scope closure.

    Company capability, association membership, generic press, portfolio pages
    and B/C signals can enrich the network but cannot close a project scope.
    """

    if confidence not in {"A1", "A2"}:
        return False
    if not project_specific:
        return False
    if status not in {"confirmed", "awarded", "executing", "completed"}:
        return False
    if execution_scope not in EXECUTION_SCOPE_KEYS:
        return False
    return True


def evidence_layer(*, project_specific: bool, execution_scope: str | None) -> str:
    if project_specific and execution_scope in EXECUTION_SCOPE_KEYS:
        return "project_execution"
    if project_specific:
        return "project_enrichment"
    return "network_intelligence"
