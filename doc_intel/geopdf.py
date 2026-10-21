"""Map extent of georeferenced PDF drawings (ISO 32000 geospatial extension).

Drawings exported from GIS or CAD (QGIS, ArcGIS, AutoCAD Map) often carry, for each map viewport, a
/Measure dictionary with /GPTS: the latitude/longitude of the map frame corners. The centre of a layout
plan's frame is a good site position even when no coordinate is written on the sheet.
Only geographic points are read (GPTS is always lat/lon); older TerraGo /LGIDict files are not decoded.
"""
from __future__ import annotations

import re
from typing import Any

REF = re.compile(r"(\d+)\s+0\s+R")
GPTS = re.compile(r"/GPTS\s*\[([^\]]+)\]")
BBOX = re.compile(r"/BBox\s*\[([^\]]+)\]")


def _obj(doc: Any, xref: int) -> str:
    try:
        return doc.xref_object(xref, compressed=False) or ""
    except Exception:
        return ""


def _deref(doc: Any, text: str, key: str) -> str:
    """Value of /key inside a dictionary string, following one indirect reference."""
    m = re.search(r"/" + key + r"\s*(\d+\s+0\s+R|\[[^\]]*\]|<<.*?>>)", text, re.S)
    if not m:
        return ""
    v = m.group(1)
    r = REF.fullmatch(v.strip())
    return _obj(doc, int(r.group(1))) if r else v


def extents(doc: Any, page: Any) -> list[dict]:
    """[{lat_min, lat_max, lon_min, lon_max, centre: (lat, lon), share: frame area / page area}] for a page."""
    page_obj = _obj(doc, page.xref)
    vp = _deref(doc, page_obj, "VP")
    if not vp:
        return []
    dicts = []
    for r in REF.finditer(vp):
        dicts.append(_obj(doc, int(r.group(1))))
    if not dicts:
        dicts = re.findall(r"<<.*?>>", vp, re.S) or [vp]
    out = []
    pw, ph = page.rect.width or 1, page.rect.height or 1
    for d in dicts:
        meas = _deref(doc, d, "Measure") or d
        g = GPTS.search(meas)
        if not g:
            continue
        nums = [float(x) for x in re.findall(r"[-+]?\d+(?:\.\d+)?", g.group(1))]
        lats, lons = nums[0::2], nums[1::2]
        if len(lats) < 2 or len(lats) != len(lons):
            continue
        if not (all(35 <= v <= 48 for v in lats) and all(6 <= v <= 19 for v in lons)):
            continue
        share = None
        b = BBOX.search(d)
        if b:
            x0, y0, x1, y1 = [float(x) for x in re.findall(r"[-+]?\d+(?:\.\d+)?", b.group(1))[:4]]
            share = round(abs((x1 - x0) * (y1 - y0)) / (pw * ph), 3)
        out.append({"lat_min": min(lats), "lat_max": max(lats), "lon_min": min(lons), "lon_max": max(lons),
                    "centre": (round(sum(lats) / len(lats), 6), round(sum(lons) / len(lons), 6)), "share": share})
    return out
