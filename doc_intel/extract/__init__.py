"""Layer D: specialised, deterministic extractors.

Each extractor receives page-level text plus document metadata and returns
``Fact`` objects with a primary citation. Extractors never decide that a fact
is current, never infer execution roles and never invent coordinates or dates.
"""
from __future__ import annotations

from ..schema import Fact
from .context import PageContext
from . import entities, technical, geo, schedule

EXTRACTORS = (entities.extract, technical.extract, geo.extract, schedule.extract)


def extract_all(pages: list[PageContext]) -> list[Fact]:
    facts: list[Fact] = []
    for page in pages:
        for fn in EXTRACTORS:
            facts.extend(fn(page))
    # geometry assembly needs the whole document, not one page
    facts.extend(geo.assemble_polygons(facts))
    return dedupe(facts)


def dedupe(facts: list[Fact]) -> list[Fact]:
    seen: dict[str, Fact] = {}
    for f in facts:
        seen.setdefault(f.fact_id(), f)
    return list(seen.values())
