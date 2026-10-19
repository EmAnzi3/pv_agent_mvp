"""Layer B adapters: from a record already found by a collector to its document manifest.

Every adapter declares its capabilities so the coverage matrix is explicit.
"""
from __future__ import annotations

from .base import AdapterCapabilities, DocumentAdapter, Discovery, ProjectIdentity
from .mase import MaseAdapter

REGISTRY: dict[str, type[DocumentAdapter]] = {"mase": MaseAdapter}


def adapter_for_url(url: str) -> DocumentAdapter:
    for cls in REGISTRY.values():
        if cls.handles(url):
            return cls()
    raise LookupError(f"no document adapter for {url}")


__all__ = ["AdapterCapabilities", "DocumentAdapter", "Discovery", "ProjectIdentity", "REGISTRY", "adapter_for_url"]
