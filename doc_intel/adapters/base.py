from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Callable

from ..schema import DocumentRef


@dataclass
class AdapterCapabilities:
    source: str
    project_identity: bool
    procedure_ids: bool
    document_index: bool
    pagination: str            # "query_param" | "browser" | "none" | "unknown"
    download: str              # "http" | "browser" | "authenticated" | "none"
    declared_totals: bool
    obstacles: list[str] = field(default_factory=list)
    status: str = "supported"  # supported | partial | blocked | not_assessed


@dataclass
class ProjectIdentity:
    source: str
    project_id: str
    info_url: str
    title: str | None = None
    proponent: str | None = None
    municipalities: list[str] = field(default_factory=list)
    province: str | None = None
    region: str | None = None
    procedures: list[dict[str, Any]] = field(default_factory=list)
    raw: dict[str, Any] = field(default_factory=dict)


@dataclass
class Discovery:
    identity: ProjectIdentity
    documents: list[DocumentRef]
    procedures: list[dict[str, Any]]      # id, url, declared, enumerated, index_pages, errors
    errors: list[str] = field(default_factory=list)


class DocumentAdapter:
    source = "base"
    capabilities: AdapterCapabilities

    def __init__(self, get_html: Callable[[str], str] | None = None):
        self.get_html = get_html or self._requests_get

    @staticmethod
    def handles(url: str) -> bool:
        return False

    def _requests_get(self, url: str) -> str:
        import requests
        from ..fetch import DEFAULT_UA
        r = requests.get(url, timeout=60, headers={"User-Agent": DEFAULT_UA})
        r.raise_for_status()
        return r.text

    def identify(self, url: str) -> ProjectIdentity:
        raise NotImplementedError

    def discover(self, identity: ProjectIdentity) -> Discovery:
        raise NotImplementedError
