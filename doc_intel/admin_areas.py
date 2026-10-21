"""Province and region of Italian municipalities (ISTAT list, CC BY 3.0 IT, in data/comuni_it.json).

Used to complete records that name the municipalities but not their province or region
(e.g. MASE 'Loreo, Adria' -> RO, Veneto). A name shared by municipalities of different provinces
(e.g. 'Calliano' in TN and AT) is resolved only when the record's own province or region disambiguates it.
"""
from __future__ import annotations

import json
import re
import unicodedata
from functools import lru_cache
from pathlib import Path

DATA = Path(__file__).parent / "data" / "comuni_it.json"


def _key(name: str) -> str:
    s = unicodedata.normalize("NFKD", str(name or "")).encode("ascii", "ignore").decode().lower()
    s = re.sub(r"\([^)]*\)", " ", s)
    s = re.sub(r"[^a-z0-9]+", " ", s).strip()
    return s


@lru_cache(maxsize=1)
def _table() -> dict[str, list[tuple[str, str, str]]]:
    out: dict[str, list[tuple[str, str, str]]] = {}
    try:
        rows = json.loads(DATA.read_text(encoding="utf-8"))["comuni"]
    except (OSError, ValueError, KeyError):
        return out                     # table missing: nothing is filled, nothing breaks
    for name, sigla, region, _code in rows:
        out.setdefault(_key(name), []).append((name, sigla, region))
    return out


def split_names(value: str) -> list[str]:
    value = re.sub(r"(?i)\bcomun[ei]\s+d[ie]\s+", "", str(value or ""))
    return [p.strip() for p in re.split(r"[,;/]|\s+e\s+", value) if p.strip()]


def locate(municipalities: str | list[str], province_hint: str = "", region_hint: str = "") -> dict | None:
    """{'province': 'RO' or 'RO, VE', 'region': 'Veneto', 'matched': [...]} when every recognised name
    is unambiguous (or disambiguated by the hints); None when nothing is recognised or a name is ambiguous."""
    names = municipalities if isinstance(municipalities, list) else split_names(municipalities)
    table, found = _table(), []
    hint_p = {x.strip().upper() for x in re.split(r"[,;/ ]+", province_hint or "") if x.strip()}
    for n in names:
        sigla_in = re.search(r"\(([A-Z]{2})\)", n)
        cands = table.get(_key(n), [])
        if sigla_in:
            cands = [c for c in cands if c[1] == sigla_in.group(1)]
        if len(cands) > 1 and hint_p:
            cands = [c for c in cands if c[1] in hint_p] or cands
        if len(cands) > 1 and region_hint:
            cands = [c for c in cands if _key(c[2]) == _key(region_hint)] or cands
        if len(cands) > 1:
            return None
        if cands:
            found.append(cands[0])
    if not found:
        return None
    provinces = list(dict.fromkeys(c[1] for c in found))
    regions = list(dict.fromkeys(c[2] for c in found))
    return {"province": ", ".join(provinces), "region": " / ".join(regions), "matched": [c[0] for c in found]}
