"""Versioned data model shared by every layer.

A ``Fact`` is the atomic unit: one value, one field, one primary citation.
Facts are never merged destructively; reconciliation groups them and records
conflicts instead of choosing a winner.
"""
from __future__ import annotations

import hashlib
import json
from dataclasses import asdict, dataclass, field
from typing import Any, Literal

# Evidence status of a single extracted fact.
FactStatus = Literal[
    "candidate",        # pattern matched, context plausible, not yet reconciled
    "verified",         # deterministic validator passed AND role/object explicit in source
    "conflicting",      # another fact for same field/subject disagrees
    "rejected",         # failed a validator (bad checksum, wrong object, implausible)
]

# Final review classification required by the mandate (dossier level).
ReviewClass = Literal[
    "confermato",          # already present in canonical record and supported by source
    "nuovo_verificato",    # not in canonical, verified with evidence
    "conflittuale",        # sources disagree, or source disagrees with canonical
    "non_determinabile",   # documents checked, value not determinable
    "non_applicabile",     # field does not apply to this technology/project
]

# Temporal nature of a schedule/timeline fact.
TemporalNature = Literal[
    "event_occurred",      # proof that something happened (verbale, comunicazione)
    "declared_date",       # explicit date stated by the proponent as a fact
    "planned",             # forecast in a cronoprogramma/Gantt
    "conditional",         # relative to a condition ("dal rilascio dell'AU")
    "relative_duration",   # duration only, no anchor
    "scenario",            # alternative/organisational scenario
    "template_reference",  # placeholder inside safety plan / form / example
    "historical",          # superseded by a later revision
    "unknown",
]

# Precision ladder for geography (lower index = more precise).
LOCATION_LADDER = (
    "validated_perimeter",
    "component_coordinates",
    "cadastral_parcels",
    "address_or_locality",
    "municipality",
    "province",
    "region",
    "unknown",
)


@dataclass
class DocumentRef:
    """One entry of the document manifest (Layer B output)."""

    url: str
    procedure_id: str | None = None
    title: str | None = None
    index_page: int | None = None
    declared_in_index: bool = True
    source: str = "mase"
    extra: dict[str, Any] = field(default_factory=dict)


@dataclass
class Citation:
    url: str
    document_sha256: str | None
    page: int | None
    excerpt: str
    member: str | None = None         # file name inside ZIP, if any
    procedure_id: str | None = None
    document_title: str | None = None
    document_date: str | None = None  # from index or title block, ISO when parsed
    revision: str | None = None
    text_method: str | None = None    # digital | ocr

    def key(self) -> str:
        return f"{self.url}|{self.member or ''}|{self.page or ''}"


@dataclass
class Fact:
    field: str                         # e.g. company.vat, tech.pv_dc_mwp, geo.utm_point
    value: Any
    citation: Citation
    subject: str | None = None         # entity the value belongs to (company name, area id...)
    role: str | None = None            # documented role, only from explicit label
    unit: str | None = None
    status: FactStatus = "candidate"
    precision: str | None = None
    temporal: TemporalNature | None = None
    qualifiers: dict[str, Any] = field(default_factory=dict)
    limits: str | None = None          # explicit note about what this fact does NOT prove
    extractor: str = ""
    conflict_group: str | None = None

    def normalized_value(self) -> str:
        v = self.value
        if isinstance(v, float):
            return f"{v:.6g}"
        if isinstance(v, (dict, list)):
            return json.dumps(v, sort_keys=True, ensure_ascii=False)
        return " ".join(str(v).casefold().split())

    def fact_id(self) -> str:
        raw = "|".join([self.field, self.subject or "", self.normalized_value(), self.citation.key()])
        return hashlib.sha256(raw.encode("utf-8")).hexdigest()[:16]

    def to_dict(self) -> dict[str, Any]:
        d = asdict(self)
        d["fact_id"] = self.fact_id()
        return d


def facts_to_json(facts: list[Fact]) -> list[dict[str, Any]]:
    return [f.to_dict() for f in facts]


def fact_from_dict(d: dict[str, Any]) -> Fact:
    d = dict(d)
    d.pop("fact_id", None)
    d["citation"] = Citation(**d["citation"])
    return Fact(**d)
