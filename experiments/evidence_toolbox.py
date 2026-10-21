"""Evidence contracts shared by crawler, PDF parser, OCR and human review.

Fail-closed: no coordinates from municipality centroids and no inferred project roles.
"""
from __future__ import annotations
import re
from dataclasses import dataclass, asdict
from typing import Literal

Status = Literal["verified", "candidate", "conflicting", "not_found", "not_applicable"]
LocationLevel = Literal["coordinates", "parcel", "address", "locality", "municipality", "province", "region", "unknown"]

@dataclass(frozen=True)
class Citation:
    url: str
    document_sha256: str
    page: int | None
    excerpt: str

@dataclass(frozen=True)
class FieldEvidence:
    field: str
    value: str | float | None
    status: Status
    citation: Citation | None
    precision: str | None = None
    qualifier: str | None = None

def valid_vat_it(value: str) -> bool:
    digits = re.sub(r"\D", "", value or "")
    if len(digits) != 11:
        return False
    total = 0
    for index, ch in enumerate(digits[:10]):
        digit = int(ch)
        if index % 2 == 1:
            digit *= 2
            if digit > 9:
                digit -= 9
        total += digit
    return (10 - total % 10) % 10 == int(digits[-1])

def location(value: str | None, level: LocationLevel, citation: Citation | None) -> FieldEvidence:
    if level == "coordinates":
        if citation is None or citation.page is None:
            raise ValueError("Coordinates require page-specific primary evidence")
        if not value or not re.search(r"[-+]?\d+[.,]\d+", value):
            raise ValueError("Coordinates must contain explicit numeric source values")
    return FieldEvidence("location", value, "candidate" if value else "not_found", citation, precision=level)

def schedule(duration_months: int | None, start_date: str | None, source: Citation | None) -> dict:
    if duration_months is not None and duration_months <= 0:
        raise ValueError("Invalid duration")
    return {"duration_months": duration_months, "start_date": start_date,
            "end_date": None, "start_status": "unknown" if not start_date else "candidate",
            "duration_status": "candidate" if duration_months else "not_found",
            "source": asdict(source) if source else None,
            "warning": "A duration is not an absolute start or completion date"}

def company_role(name: str, role: str, citation: Citation | None, direct_statement: bool = False) -> dict:
    blocked = {"EPC", "BoP", "civil_bop", "electrical_bop", "construction_contractor"}
    if role in blocked and (not direct_statement or citation is None):
        raise ValueError("Execution award cannot be inferred from design or mention")
    return {"company":name, "role":role, "evidence":asdict(citation) if citation else None,
            "status":"candidate" if citation else "not_found"}

def closeout(index_declared: int, enumerated: int, opened: int, files_expected: int,
             extraction_unresolved: int, semantic_gaps: list[str], benchmark_done: bool) -> dict:
    complete = (index_declared > 0 and enumerated == index_declared and
                opened == files_expected and extraction_unresolved == 0 and
                not semantic_gaps and benchmark_done)
    return {"complete":complete, "coverage": {"declared":index_declared,
            "enumerated":enumerated,"opened":opened,"expected":files_expected,
            "unresolved":extraction_unresolved},
            "semantic_gaps":semantic_gaps, "benchmark_done":benchmark_done,
            "status":"ready_for_review" if complete else "incomplete"}
