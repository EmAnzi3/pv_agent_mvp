from __future__ import annotations

import json
import math
import re
import unicodedata
from pathlib import Path
from typing import Any

from .state import get_run_events

ROOT = Path(__file__).resolve().parents[2]
DATA = ROOT / "docs" / "wind" / "data"

LEGAL_TOKENS = {
    "srl", "spa", "sapa", "societa", "soc", "s", "r", "l", "p", "a",
    "renewables", "renewable", "energy", "energie", "power", "italia", "italy",
}
GENERIC_PROJECT_TOKENS = {
    "parco", "impianto", "eolico", "eolica", "eolici", "eoliche", "wind", "farm",
    "progetto", "repowering", "offshore", "onshore", "centrale",
}


def _load_json(path: Path) -> Any:
    return json.loads(path.read_text(encoding="utf-8-sig"))


def _norm(value: Any) -> str:
    text = str(value or "").lower().replace("’", "'")
    text = unicodedata.normalize("NFKD", text)
    text = "".join(ch for ch in text if not unicodedata.combining(ch))
    text = re.sub(r"[^a-z0-9]+", " ", text)
    return re.sub(r"\s+", " ", text).strip()


def _tokens(value: Any, *, project: bool = False) -> set[str]:
    tokens = {token for token in _norm(value).split() if len(token) > 1}
    if project:
        tokens -= GENERIC_PROJECT_TOKENS
    return tokens


def _name_score(a: Any, b: Any) -> tuple[int, str | None]:
    na = _norm(a)
    nb = _norm(b)
    if not na or not nb:
        return 0, None
    if na == nb:
        return 60, "exact_name"
    if min(len(na), len(nb)) >= 6 and (na in nb or nb in na):
        return 45, "contained_name"
    ta = _tokens(a, project=True)
    tb = _tokens(b, project=True)
    if not ta or not tb:
        return 0, None
    overlap = len(ta & tb) / len(ta | tb)
    if overlap >= 0.8:
        return 42, "strong_name_tokens"
    if overlap >= 0.6:
        return 32, "name_tokens"
    if overlap >= 0.4 and len(ta & tb) >= 2:
        return 20, "weak_name_tokens"
    return 0, None


def _company_score(a: Any, b: Any) -> tuple[int, str | None]:
    ta = _tokens(a) - LEGAL_TOKENS
    tb = _tokens(b) - LEGAL_TOKENS
    if not ta or not tb:
        return 0, None
    if ta == tb:
        return 16, "same_company"
    overlap = len(ta & tb) / max(1, min(len(ta), len(tb)))
    if overlap >= 0.8:
        return 13, "company_overlap"
    if overlap >= 0.6:
        return 8, "company_partial"
    return 0, None


def _as_float(value: Any) -> float | None:
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        return None
    return parsed if math.isfinite(parsed) and parsed > 0 else None


def _power_score(a: Any, b: Any) -> tuple[int, str | None]:
    left = _as_float(a)
    right = _as_float(b)
    if left is None or right is None:
        return 0, None
    diff = abs(left - right)
    rel = diff / max(left, right)
    if diff <= 0.5 or rel <= 0.01:
        return 20, "power_match"
    if diff <= 2.0 or rel <= 0.03:
        return 11, "power_near"
    return 0, None


def _place_values(payload: dict[str, Any]) -> set[str]:
    values: list[Any] = []
    municipalities = payload.get("municipalities")
    if isinstance(municipalities, list):
        values.extend(municipalities)
    elif municipalities:
        values.append(municipalities)
    for key in ("municipality", "area"):
        if payload.get(key):
            values.append(payload[key])
    out: set[str] = set()
    for value in values:
        norm = _norm(value)
        if norm:
            out.add(norm)
    return out


def _candidate_places(candidate: dict[str, Any]) -> set[str]:
    values: list[Any] = []
    for key in ("municipalities", "municipality", "area"):
        value = candidate.get(key)
        if isinstance(value, list):
            values.extend(value)
        elif value:
            values.append(value)
    return {_norm(value) for value in values if _norm(value)}


def _place_score(finding: dict[str, Any], candidate: dict[str, Any]) -> tuple[int, str | None]:
    finding_places = _place_values(finding)
    candidate_places = _candidate_places(candidate)
    if not finding_places or not candidate_places:
        return 0, None
    for left in finding_places:
        for right in candidate_places:
            if left == right:
                return 20, "municipality_match"
            if min(len(left), len(right)) >= 5 and (left in right or right in left):
                return 15, "municipality_overlap"
    return 0, None


def _source_urls(candidate: dict[str, Any]) -> list[str]:
    urls: list[str] = []
    for source in candidate.get("sources") or []:
        if isinstance(source, dict) and source.get("url"):
            urls.append(str(source["url"]))
    return urls


def _source_score(finding_url: Any, candidate: dict[str, Any]) -> tuple[int, str | None]:
    url = str(finding_url or "").strip().rstrip("/")
    if not url:
        return 0, None
    for candidate_url in _source_urls(candidate):
        other = candidate_url.strip().rstrip("/")
        if url == other:
            return 70, "exact_source_url"
        if len(url) >= 24 and len(other) >= 24 and (url in other or other in url):
            return 48, "related_source_url"
    return 0, None


def load_canonical_projects() -> list[dict[str, Any]]:
    manifest = _load_json(DATA / "projects.json")
    projects: list[dict[str, Any]] = []
    for chunk in manifest.get("chunks", []):
        projects.extend(_load_json(DATA / chunk))
    return projects


def load_discovery_candidates() -> list[dict[str, Any]]:
    candidates: list[dict[str, Any]] = []
    seen: set[str] = set()
    for path in sorted(DATA.glob("discovery-v04*.json")):
        data = _load_json(path)
        if not isinstance(data, dict):
            continue
        for row in data.get("candidates", []):
            candidate_id = str(row.get("candidate_id") or "")
            if not candidate_id or candidate_id in seen:
                continue
            seen.add(candidate_id)
            candidates.append(row)
    return candidates


def _canonical_candidate(project: dict[str, Any]) -> dict[str, Any]:
    return {
        **project,
        "target_kind": "canonical",
        "target_id": project.get("id"),
        "wind_mw": project.get("mw"),
        "developer_or_spv": project.get("developer") or project.get("spv"),
    }


def _discovery_candidate(candidate: dict[str, Any]) -> dict[str, Any]:
    return {
        **candidate,
        "target_kind": "discovery",
        "target_id": candidate.get("candidate_id"),
    }


def _score_candidate(finding: dict[str, Any], candidate: dict[str, Any]) -> dict[str, Any]:
    payload = finding.get("payload") or {}
    title = payload.get("project_name") or finding.get("title") or ""
    proponent = payload.get("proponent") or payload.get("company_name") or ""
    candidate_name = candidate.get("name") or ""
    candidate_company = candidate.get("developer_or_spv") or candidate.get("developer") or candidate.get("spv") or ""

    score = 0
    reasons: list[str] = []
    strong_identity = False

    for points, reason in (
        _source_score(finding.get("source_url"), candidate),
        _name_score(title, candidate_name),
        _place_score(payload, candidate),
        _power_score(payload.get("power_mw"), candidate.get("wind_mw") or candidate.get("mw")),
        _company_score(proponent, candidate_company),
    ):
        score += points
        if reason:
            reasons.append(reason)
        if reason in {"exact_source_url", "exact_name", "contained_name", "strong_name_tokens"}:
            strong_identity = True

    region = _norm(payload.get("region"))
    candidate_region = _norm(candidate.get("region"))
    if region and candidate_region and region == candidate_region:
        score += 8
        reasons.append("region_match")

    explicit_links = payload.get("project_links_registry") or []
    if isinstance(explicit_links, list) and candidate.get("target_id") in explicit_links:
        score += 25
        reasons.append("registry_project_link")

    return {
        "target_kind": candidate.get("target_kind"),
        "target_id": candidate.get("target_id"),
        "target_name": candidate_name,
        "score": min(score, 100),
        "reasons": reasons,
        "strong_identity": strong_identity,
        "stage": candidate.get("stage"),
        "priority": candidate.get("priority"),
        "activity_class": candidate.get("activity_class"),
        "status": candidate.get("status"),
    }


def reconcile_finding(
    finding: dict[str, Any],
    *,
    canonical: list[dict[str, Any]] | None = None,
    discovery: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    """Conservatively match one raw finding without mutating canonical data."""

    canonical_rows = canonical if canonical is not None else load_canonical_projects()
    discovery_rows = discovery if discovery is not None else load_discovery_candidates()
    canonical_targets = [_canonical_candidate(row) for row in canonical_rows]
    canonical_ids = {row.get("target_id") for row in canonical_targets}
    discovery_targets = [
        _discovery_candidate(row)
        for row in discovery_rows
        if row.get("candidate_id") not in canonical_ids
    ]
    scored = [_score_candidate(finding, target) for target in canonical_targets + discovery_targets]
    scored = [row for row in scored if row["score"] > 0]
    scored.sort(key=lambda row: (-row["score"], 0 if row["target_kind"] == "canonical" else 1, row["target_name"] or ""))

    top = scored[0] if scored else None
    second = scored[1] if len(scored) > 1 else None
    margin = (top["score"] - second["score"]) if top and second else (top["score"] if top else 0)
    payload = finding.get("payload") or {}
    project_specific = bool(payload.get("project_specific"))

    status = "unmatched"
    auto_reconciled = False
    if top:
        if project_specific and top["score"] >= 80 and top["strong_identity"] and margin >= 12:
            status = "high_confidence_match"
            auto_reconciled = True
        elif top["score"] >= 55:
            status = "review_match"
        elif top["score"] >= 35:
            status = "weak_match"

    return {
        "status": status,
        "auto_reconciled": auto_reconciled,
        "margin": margin,
        "best": top,
        "alternatives": scored[1:4],
        "guard": "Reconciliation is advisory only: no canonical/discovery write or promotion is performed.",
    }


def _commercial_weight(item: dict[str, Any]) -> int:
    score = 0
    event_type = item.get("event_type")
    if event_type == "changed":
        score += 12
    elif event_type == "new":
        score += 8

    finding = item.get("finding") or {}
    payload = finding.get("payload") or {}
    result = item.get("reconciliation") or {}
    best = result.get("best") or {}

    if finding.get("finding_type") == "project_source":
        score += 18
    if payload.get("is_aggregated_market_intelligence"):
        score -= 8
    if best.get("target_kind") == "canonical":
        score += 20
        if best.get("stage") in {"E4", "E5", "E6", "E7"}:
            score += 18
        if best.get("priority") in {"A+", "A"}:
            score += 12
    elif best.get("target_kind") == "discovery":
        score += 10

    excerpt = str(payload.get("signal_excerpt") or "")
    if excerpt:
        score += 8
    return score


def _action_type(event: dict[str, Any]) -> str:
    finding = event.get("finding") or {}
    payload = finding.get("payload") or {}
    best = (event.get("reconciliation") or {}).get("best") or {}

    if payload.get("is_aggregated_market_intelligence"):
        return "market_intelligence"
    if finding.get("finding_type") == "company_source_snapshot":
        return "company_project_signal" if best else "company_network_update"
    if best.get("target_kind") == "canonical":
        return "canonical_update_review"
    if best.get("target_kind") == "discovery":
        return "discovery_refresh_review"
    return "new_project_lead"


def build_digest(run_ids: list[str]) -> dict[str, Any]:
    canonical = load_canonical_projects()
    discovery = load_discovery_candidates()
    items: list[dict[str, Any]] = []

    for run_id in run_ids:
        for event in get_run_events(run_id):
            finding = event.get("finding") or {}
            reconciliation = reconcile_finding(finding, canonical=canonical, discovery=discovery)
            item = {
                **event,
                "reconciliation": reconciliation,
            }
            item["action_type"] = _action_type(item)
            item["commercial_weight"] = _commercial_weight(item)

            payload = finding.get("payload") or {}
            # Drop empty company snapshots from the actionable digest. They remain
            # persisted in raw/history and can still be audited later.
            if finding.get("finding_type") == "company_source_snapshot" and not (
                payload.get("signal_excerpt") or payload.get("headings")
            ):
                item["actionable"] = False
            else:
                item["actionable"] = item["commercial_weight"] >= 20
            items.append(item)

    items.sort(key=lambda row: (-row["commercial_weight"], row.get("created_at") or ""))
    actionable = [row for row in items if row.get("actionable")]

    counts: dict[str, int] = {}
    for row in actionable:
        counts[row["action_type"]] = counts.get(row["action_type"], 0) + 1

    return {
        "run_ids": run_ids,
        "events": len(items),
        "actionable_events": len(actionable),
        "action_types": counts,
        "items": actionable,
        "non_actionable_events": len(items) - len(actionable),
        "guard": "Digest is review-only. No evidence, scope, stage, priority or canonical project is mutated automatically.",
    }
