from __future__ import annotations

import hashlib


def build_project_key(
    project_name: str,
    proponent: str | None,
    region: str | None,
    municipalities: list[str] | None,
    power_mw: str | None,
) -> str:
    parts = [
        (project_name or "").strip().lower(),
        (proponent or "").strip().lower(),
        (region or "").strip().lower(),
        "|".join(sorted([(m or "").strip().lower() for m in (municipalities or [])])),
        (power_mw or "").strip().lower(),
    ]
    raw = "::".join(parts)
    return hashlib.sha1(raw.encode("utf-8")).hexdigest()
