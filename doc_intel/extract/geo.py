"""Coordinates, CRS and the *object* each coordinate belongs to.

* UTM is converted to WGS84 lat/lon only when the zone is explicit in the same
  page; otherwise the raw pair is kept with ``zone_unknown``.
* Each point is classified by the object it describes (area vertex, cabin,
  substation, PVGIS simulation point, centroid, turbine...). Only area vertices
  can build a plant perimeter; cabins/SSE/PVGIS points are never the plant.
* Polygons are assembled from consecutive vertex rows of the same area,
  keeping source order; area is computed in metres for plausibility only.
"""
from __future__ import annotations

import math
import re

from ..schema import Fact
from .context import PageContext, parse_number

UTM_NUM = r"\d{6}(?:[.,]\d{1,4})?|\d{3}[.\s]\d{3}(?:,\d{1,4})?"
UTM_N_NUM = r"\d{7}(?:[.,]\d{1,4})?|\d{1}[.\s]\d{3}[.\s]\d{3}(?:,\d{1,4})?"
PAIR_RE = re.compile(r"(?<![\d.,])(?P<e>" + UTM_NUM + r")[\s;,|/]+(?:N\s*[:=]?\s*)?(?P<n>" + UTM_N_NUM + r")(?![\d])")
PAIR_NE_RE = re.compile(r"(?i)\bN(?:ord)?\s*[:=]?\s*(?P<n>" + UTM_N_NUM + r")[\s;,|/]+E(?:st)?\s*[:=]?\s*(?P<e>" + UTM_NUM + r")")
LATLON_RE = re.compile(r"(?<![\d])(?P<lat>3[5-9]|4[0-7])[.,](?P<latd>\d{3,8})\s*°?\s*[N]?[\s;,/]+\s*(?P<lon>0?[6-9]|1[0-8])[.,](?P<lond>\d{3,8})\s*°?\s*[E]?(?![\d])")
DMS_RE = re.compile(r"(?P<d>\d{1,2})\s*°\s*(?P<m>\d{1,2})\s*['’′]\s*(?P<s>\d{1,2}(?:[.,]\d+)?)\s*(?:\"|”|″|'')?\s*(?P<h>[NSEW])")
ZONE_RE = re.compile(r"(?i)(?:fuso|zona|zone|UTM)\s*[-:]?\s*(?P<z>3[23])\s*(?:N|nord)?\b|\b(?P<z2>3[23])\s*N\b|EPSG\s*[:]?\s*(?P<epsg>326(?:32|33)|258(?:32|33)|6707|6708|3003|3004)")
WGS_RE = re.compile(r"(?i)WGS\s*-?\s*84|ETRS\s*-?\s*89|RDN\s*2008")
GB_RE = re.compile(r"(?i)gauss[\s-]*boaga|monte\s+mario|roma\s*40")

OBJECT_RULES = [
    (re.compile(r"(?i)pvgis|simulazion|irraggiament"), "simulation_point"),
    (re.compile(r"(?i)baricentr|centroid|punto\s+centrale"), "centroid"),
    (re.compile(r"(?i)aerogenerator|\bWTG\b|torre\s+eolica"), "turbine"),
    (re.compile(r"(?i)stazione\s+(?:elettrica|utente|di\s+(?:trasformazione|elevazione))|\bSSE\b|sottostazione|\bSE\b"), "substation"),
    (re.compile(r"(?i)punto\s+di\s+(?:connessione|consegna)|stallo"), "connection_point"),
    (re.compile(r"(?i)cabin[ae]"), "cabin"),
    (re.compile(r"(?i)cavidott|elettrodott|tracciato"), "cable_route"),
    (re.compile(r"(?i)spigol[oi]|ingombro\s+(?:del\s+)?sito|bounding"), "site_bbox_corner"),
    (re.compile(r"(?i)vertic[ei]|perimetr|recinzion|poligonal|confin[ei]\s+(?:dell')?area"), "area_vertex"),
    (re.compile(r"(?i)\bBESS\b|accumulo"), "bess"),
    (re.compile(r"(?i)sondagg|prova\s+penetrometric|pozz|piezometr"), "survey_point"),
]
AREA_LABEL_RE = re.compile(r"(?i:\b(?:area|campo|lotto|sottocampo|sezione|impianto|blocco))\s*(?:n\.?\s*|nr\.?\s*)?(?P<id>[A-Z0-9]{1,4}(?:[.\-][A-Z0-9]{1,3})?)\b")
AREA_STOP = {"DI", "EL", "SI", "IN", "DA", "IL", "LA", "LE", "AL", "DEL", "CON", "PER", "UTM", "WGS"}
RING_START = re.compile(r"^(?:V|P|A)?\s?-?0*1$")
POINT_LABEL_RE = re.compile(r"^\s*(?P<lab>(?:[A-Z]{1,3}\s?-?\d{1,3}[A-Z]?|\d{1,3}|V\d{1,3}|P\d{1,3}))\b")

ITALY_UTM = {32: ((280_000, 830_000), (4_150_000, 5_200_000)),
             33: ((180_000, 800_000), (3_900_000, 5_200_000))}


# ---------------------------------------------------------------- conversion
def utm_to_latlon(easting: float, northing: float, zone: int, northern: bool = True) -> tuple[float, float]:
    """WGS84 UTM -> geographic (Snyder/USGS series, sub-metre in the zone)."""
    a, f = 6378137.0, 1 / 298.257223563
    k0 = 0.9996
    e2 = f * (2 - f)
    ep2 = e2 / (1 - e2)
    x = easting - 500000.0
    y = northing if northern else northing - 10_000_000.0
    m = y / k0
    mu = m / (a * (1 - e2 / 4 - 3 * e2 ** 2 / 64 - 5 * e2 ** 3 / 256))
    e1 = (1 - math.sqrt(1 - e2)) / (1 + math.sqrt(1 - e2))
    phi1 = (mu + (3 * e1 / 2 - 27 * e1 ** 3 / 32) * math.sin(2 * mu)
            + (21 * e1 ** 2 / 16 - 55 * e1 ** 4 / 32) * math.sin(4 * mu)
            + (151 * e1 ** 3 / 96) * math.sin(6 * mu) + (1097 * e1 ** 4 / 512) * math.sin(8 * mu))
    n1 = a / math.sqrt(1 - e2 * math.sin(phi1) ** 2)
    t1 = math.tan(phi1) ** 2
    c1 = ep2 * math.cos(phi1) ** 2
    r1 = a * (1 - e2) / (1 - e2 * math.sin(phi1) ** 2) ** 1.5
    d = x / (n1 * k0)
    lat = phi1 - (n1 * math.tan(phi1) / r1) * (
        d ** 2 / 2 - (5 + 3 * t1 + 10 * c1 - 4 * c1 ** 2 - 9 * ep2) * d ** 4 / 24
        + (61 + 90 * t1 + 298 * c1 + 45 * t1 ** 2 - 252 * ep2 - 3 * c1 ** 2) * d ** 6 / 720)
    lon = (d - (1 + 2 * t1 + c1) * d ** 3 / 6
           + (5 - 2 * c1 + 28 * t1 - 3 * c1 ** 2 + 8 * ep2 + 24 * t1 ** 2) * d ** 5 / 120) / math.cos(phi1)
    lon0 = math.radians((zone - 1) * 6 - 180 + 3)
    return round(math.degrees(lat), 7), round(math.degrees(lon + lon0), 7)


def polygon_area_m2(points: list[tuple[float, float]]) -> float:
    s = 0.0
    for (x1, y1), (x2, y2) in zip(points, points[1:] + points[:1]):
        s += x1 * y2 - x2 * y1
    return abs(s) / 2


# ---------------------------------------------------------------- detection
def detect_crs(text: str) -> dict:
    crs: dict = {"datum": None, "zone": None, "epsg": None, "unsupported": None}
    if WGS_RE.search(text):
        crs["datum"] = WGS_RE.search(text).group(0).upper().replace(" ", "").replace("-", "")
    if GB_RE.search(text):
        crs["unsupported"] = "Gauss-Boaga/Monte Mario"
    z = ZONE_RE.search(text)
    if z:
        if z.group("epsg"):
            e = z.group("epsg")
            crs["epsg"] = int(e)
            crs["zone"] = {"32632": 32, "25832": 32, "6707": 32, "32633": 33, "25833": 33, "6708": 33}.get(e)
            if e in ("3003", "3004"):
                crs["unsupported"] = "Gauss-Boaga (EPSG %s)" % e
        else:
            crs["zone"] = int(z.group("z") or z.group("z2"))
    if crs["zone"] and not crs["epsg"] and crs["datum"] in (None, "WGS84"):
        crs["epsg"] = 32600 + crs["zone"]
    return crs


def classify_object(text: str) -> str:
    for rx, obj in OBJECT_RULES:
        if rx.search(text):
            return obj
    return "unknown"


def _in_italy(e: float, n: float, zone: int | None) -> bool:
    zones = [zone] if zone in ITALY_UTM else list(ITALY_UTM)
    return any(ITALY_UTM[z][0][0] <= e <= ITALY_UTM[z][0][1] and ITALY_UTM[z][1][0] <= n <= ITALY_UTM[z][1][1]
               for z in zones)


NUMERIC_ONLY = re.compile(r"^\s*[-+]?\d+(?:[.,]\d+)*\s*°?\s*[NE]?\s*$")  # a single numeric cell


def _logical_lines(lines: list[str]):
    """Table cells are often extracted one per line: join a numeric line with the following
    numeric line (lat/lon or E/N split across cells) before pattern matching."""
    i = 0
    while i < len(lines):
        line = lines[i]
        if NUMERIC_ONLY.match(line or "") and i + 1 < len(lines) and NUMERIC_ONLY.match(lines[i + 1] or ""):
            yield i, f"{line} {lines[i + 1]}"
            i += 2
            continue
        yield i, line
        i += 1


def extract(ctx: PageContext) -> list[Fact]:
    facts: list[Fact] = []
    crs = detect_crs(ctx.text)
    page_object = classify_object(ctx.text[:3000])
    current_area: str | None = None
    current_object = page_object
    for i, line in _logical_lines(ctx.lines):
        if not line:
            continue
        am = AREA_LABEL_RE.search(line)
        if am and not PAIR_RE.search(line) and am.group("id").upper() not in AREA_STOP:
            current_area = am.group("id").upper()
        heading_obj = classify_object(line)
        if heading_obj != "unknown" and not PAIR_RE.search(line):
            current_object = heading_obj
        pairs = [(m, "EN") for m in PAIR_RE.finditer(line)] + [(m, "NE") for m in PAIR_NE_RE.finditer(line)]
        for m, order in pairs:
            e, n = parse_number(m.group("e")), parse_number(m.group("n"))
            if e is None or n is None or not _in_italy(e, n, crs["zone"]):
                continue
            line_obj = classify_object(line)
            obj = line_obj if line_obj != "unknown" else current_object
            label_m = POINT_LABEL_RE.match(line)
            q = {"easting": e, "northing": n, "crs": crs, "object": obj, "area": current_area,
                 "point_label": label_m.group("lab") if label_m else None, "line_index": i,
                 "source_order": order}
            status, limits = "candidate", None
            if crs["unsupported"]:
                limits = f"CRS {crs['unsupported']} not converted"
            elif crs["zone"]:
                q["lat"], q["lon"] = utm_to_latlon(e, n, crs["zone"])
            else:
                limits = "UTM zone not stated on this page: not converted, not geolocated"
            if obj in ("cabin", "substation", "connection_point", "simulation_point", "survey_point", "cable_route", "bess", "site_bbox_corner"):
                limits = (limits + "; " if limits else "") + f"coordinate of a {obj}, NOT the plant location"
            facts.append(Fact(field="geo.utm_point", value={"e": e, "n": n}, subject=current_area,
                              citation=ctx.cite(ctx.window(i, 0, 0)), status=status,
                              precision="component_coordinates" if obj != "area_vertex" else "vertex",
                              qualifiers=q, limits=limits, extractor="geo.utm"))
        for m in LATLON_RE.finditer(line):
            lat = float(f"{m.group('lat')}.{m.group('latd')}")
            lon = float(f"{m.group('lon')}.{m.group('lond')}")
            obj = classify_object(line)
            if obj == "unknown":
                obj = classify_object(ctx.window(i, 2, 0))
            facts.append(Fact(field="geo.latlon_point", value={"lat": lat, "lon": lon},
                              citation=ctx.cite(ctx.window(i, 1, 0)), precision="component_coordinates",
                              qualifiers={"object": obj, "crs": "WGS84 (assumed decimal degrees)"},
                              limits=("Geographic pair; object " + obj + (" — NOT the plant location" if obj in ("simulation_point", "cabin", "substation") else "")
                                      + ("; bounding-box corner of the site as declared, not a perimeter" if obj == "site_bbox_corner" else "")),
                              extractor="geo.latlon"))
    facts.extend(extract_row_tables(ctx))
    return facts


LAT_ROW = re.compile(r"(?i)^\s*lat(?:itudine)?\b[^0-9]*(?P<vals>(?:\s*[-+]?\d{2}[.,]\d{3,8}\s*°?\s*N?)+)\s*$")
LON_ROW = re.compile(r"(?i)^\s*lon(?:gitudine)?\b[^0-9]*(?P<vals>(?:\s*[-+]?\d{1,2}[.,]\d{3,8}\s*°?\s*E?)+)\s*$")
NUM_TOKEN = re.compile(r"[-+]?\d{1,2}[.,]\d{3,8}")


def extract_row_tables(ctx: PageContext) -> list[Fact]:
    """Tables laid out by rows ('Latitudine v1 v2 v3' / 'Longitudine w1 w2 w3'), one column per site."""
    facts: list[Fact] = []
    for i, line in enumerate(ctx.lines):
        lm = LAT_ROW.match(line or "")
        if not lm:
            continue
        lats = [float(x.replace(",", ".")) for x in NUM_TOKEN.findall(lm.group("vals"))]
        for j in range(i + 1, min(i + 4, len(ctx.lines))):
            om = LON_ROW.match(ctx.lines[j] or "")
            if not om:
                continue
            lons = [float(x.replace(",", ".")) for x in NUM_TOKEN.findall(om.group("vals"))]
            if len(lons) != len(lats) or len(lats) < 2:
                break
            header = next((ctx.lines[k] for k in range(i - 1, max(-1, i - 4), -1) if ctx.lines[k]), "")
            labels = re.findall(r"[A-Za-zÀ-ÿ]+\s*\d+[A-Za-z]?", header)
            obj = classify_object(ctx.window(i, 4, 0))
            for k, (la, lo) in enumerate(zip(lats, lons)):
                if not (35 <= la <= 47.5 and 6 <= lo <= 19):
                    continue
                facts.append(Fact(field="geo.latlon_point", value={"lat": la, "lon": lo},
                                  subject=labels[k] if len(labels) == len(lats) else None,
                                  citation=ctx.cite(ctx.window(i, 1, 1)), precision="component_coordinates",
                                  qualifiers={"object": obj, "crs": "WGS84 (assumed decimal degrees)", "layout": "row_table"},
                                  limits=f"Point of object '{obj}' from a row-wise table; not a perimeter",
                                  extractor="geo.latlon_rows"))
            break
    return facts


def assemble_polygons(facts: list[Fact]) -> list[Fact]:
    """Group area-vertex points of the same document/area into ordered rings."""
    groups: dict[tuple, list[Fact]] = {}
    for f in facts:
        if f.field != "geo.utm_point" or f.qualifiers.get("object") != "area_vertex":
            continue
        key = (f.citation.url, f.citation.member, f.qualifiers.get("area"),
               (f.qualifiers.get("crs") or {}).get("zone"))
        groups.setdefault(key, []).append(f)
    out: list[Fact] = []
    for (url, member, area, zone), all_pts in groups.items():
        all_pts.sort(key=lambda f: (f.citation.page or 0, f.qualifiers.get("line_index", 0)))
        # a vertex list restarting at "1"/"V1", or a gap of more than one page, starts a new ring
        runs: list[list[Fact]] = [[]]
        for p in all_pts:
            cur = runs[-1]
            lab = (p.qualifiers.get("point_label") or "").replace(" ", "")
            if cur and (len(cur) >= 3 and RING_START.match(lab) or (p.citation.page or 0) - (cur[-1].citation.page or 0) > 1):
                runs.append([])
            runs[-1].append(p)
        for k, pts in enumerate(runs):
            out.extend(_ring_fact(url, member, area if len(runs) == 1 else f"{area or ''}#{k + 1}", zone, pts))
    return out


def _ring_fact(url, member, area, zone, pts: list[Fact]) -> list[Fact]:
    out: list[Fact] = []
    ring = []
    for p in pts:
        xy = (p.qualifiers["easting"], p.qualifiers["northing"])
        if not ring or ring[-1] != xy:
            ring.append(xy)
    closed = len(ring) > 3 and ring[0] == ring[-1]
    if closed:
        ring = ring[:-1]
    if len(ring) < 3:
        return out
    area_ha = round(polygon_area_m2(ring) / 10000, 3)
    pages = sorted({p.citation.page for p in pts if p.citation.page})
    limits = []
    if not zone:
        limits.append("zone unknown: geometry kept in source coordinates only")
    if area_ha > 3000 or area_ha < 0.05:
        limits.append(f"implausible area {area_ha} ha: check vertex parsing/order")
    if not closed:
        limits.append("ring not explicitly closed in source; closed implicitly")
    lonlat = [list(utm_to_latlon(e, n, zone)[::-1]) for e, n in ring] if zone else None
    first = pts[0]
    out.append(Fact(field="geo.area_polygon", subject=area, value={
        "crs": f"EPSG:{32600 + zone}" if zone else "UTM zone unknown",
        "vertices_source": [list(v) for v in ring],
        "geojson": {"type": "Polygon", "coordinates": [lonlat + [lonlat[0]]]} if lonlat else None,
        "vertex_count": len(ring), "area_ha": area_ha, "pages": pages},
        citation=first.citation, precision="validated_perimeter_candidate",
        qualifiers={"source_document": url, "member": member, "closed_in_source": closed},
        limits="; ".join(limits) or "Perimeter of the area as tabulated; requires visual check against the drawing",
        extractor="geo.polygon"))
    return out
