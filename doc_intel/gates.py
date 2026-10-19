"""Layer E: acceptance gates. Any blocking gate disables automatic approval.

Blocking by mandate §11: an invented EPC, a VAT linked to the wrong company,
a non-pertinent coordinate promoted as exact location. We cannot prove the
negative automatically, so the gates block whenever such a claim would enter
the enrichment proposal without an explicit, reviewable basis.
"""
from __future__ import annotations

from typing import Any

from .schema import Fact

NON_PLANT_OBJECTS = {"site_bbox_corner", "cabin", "substation", "connection_point", "simulation_point", "survey_point",
                     "cable_route", "bess", "turbine", "unknown"}
EXECUTION_ROLES = {"role.epc", "role.bop", "role.contractor", "role.supplier", "role.om"}


def acquisition_gate(acq: dict[str, Any]) -> dict[str, Any]:
    issues = []
    for proc in acq.get("procedures", []):
        if proc.get("declared") is None and proc.get("declared_available") is False:
            pass  # the source never declares totals: completeness is against the listing (noted below)
        elif proc.get("declared") is None:
            issues.append(f"procedure {proc['id']}: declared total not found on index page")
        elif proc["declared"] != proc["enumerated"]:
            issues.append(f"procedure {proc['id']}: declared {proc['declared']} vs enumerated {proc['enumerated']}")
    if acq.get("download_failures"):
        issues.append(f"{acq['download_failures']} documents failed to download")
    if acq.get("pages_ocr_pending"):
        issues.append(f"{acq['pages_ocr_pending']} pages without text still awaiting OCR")
    if acq.get("pages_unreadable"):
        issues.append(f"{acq['pages_unreadable']} pages where OCR failed")
    notes = []
    if any(p.get("declared_available") is False for p in acq.get("procedures", [])):
        notes.append("source publishes no declared document total: completeness measured against its own listing")
    if acq.get("pages_graphic_no_text"):
        notes.append(f"{acq['pages_graphic_no_text']} graphic pages without text even after OCR (listed for visual review)")
    return {"gate": "acquisition", "pass": not issues, "blocking": bool(issues), "issues": issues, "notes": notes}


def proposal_gates(proposal: list[dict[str, Any]], facts: list[Fact], groups: list[dict]) -> list[dict[str, Any]]:
    by_id = {f.fact_id(): f for f in facts}
    conflicts = {g["group_id"] for g in groups if g.get("status") == "conflict"}
    multi_vat = {g["subject"] for g in groups if g["field"] == "company.multiple_vat"}
    shared_vat = {g["subject"] for g in groups if g["field"] == "company.vat_shared"}
    results = []
    for item in proposal:
        f = by_id.get(item.get("fact_id"))
        reasons = []
        if f is None:
            reasons.append("proposal item without a source fact")
        else:
            if f.conflict_group and f.conflict_group in conflicts:
                reasons.append("value belongs to an open conflict")
            if f.field in EXECUTION_ROLES:
                reasons.append("execution role (EPC/BoP/contractor/supplier/O&M) requires human confirmation of an award")
            if f.field == "company.vat":
                if not f.subject:
                    reasons.append("VAT without associated company")
                if not f.qualifiers.get("checksum_ok"):
                    reasons.append("VAT check digit invalid")
                confirmed = f.qualifiers.get("registry_match") is True
                if f.qualifiers.get("subject_association") != "same_line" and not confirmed:
                    reasons.append("VAT-company association is not on the same line")
                if (f.subject in multi_vat or f.value in shared_vat) and not confirmed:
                    reasons.append("VAT is part of a multi-VAT / shared-VAT conflict")
                if "registry" not in f.qualifiers:
                    reasons.append("registry identity check (Registro Imprese/VIES) not performed")
                elif not confirmed:
                    reasons.append("registry name does not confirm the associated company")
            if f.field.startswith("contact.") and f.qualifiers.get("subject_association") != "same_line":
                reasons.append("contact-company association is not on the same line")
            if f.field in ("geo.utm_point", "geo.latlon_point"):
                if f.qualifiers.get("object") in NON_PLANT_OBJECTS:
                    reasons.append(f"coordinate of object '{f.qualifiers.get('object')}' cannot be the plant location")
            if f.field == "geo.area_polygon":
                if not f.value.get("geojson"):
                    reasons.append("polygon without explicit UTM zone")
                reasons.append("polygon requires visual check against the drawing before promotion")
            if f.field.startswith("schedule.date."):
                if f.temporal in ("template_reference", "unknown"):
                    reasons.append(f"date of nature '{f.temporal}' cannot be a milestone")
                if f.temporal == "event_occurred":
                    reasons.append("occurred-event claim requires an independent act")
        results.append({"item": item.get("field"), "fact_id": item.get("fact_id"),
                        "blocking": bool(reasons), "reasons": reasons})
    return results
