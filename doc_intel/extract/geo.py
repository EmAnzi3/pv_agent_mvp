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
ED50_RE = re.compile(r"(?i)\bED\s*-?\s*50\b|EPSG\s*:?\s*230(?:32|33)")
# Gauss-Boaga pairs: E 1.4-1.9 M (fuso Ovest) or 2.3-2.8 M (fuso Est), N 4.0-5.2 M
GB_NUM_E = r"[12][.\s]?\d{3}[.\s]?\d{3}(?:[.,]\d{1,3})?"
GB_NUM_N = r"[45][.\s]?\d{3}[.\s]?\d{3}(?:[.,]\d{1,3})?"
GB_PAIR_RE = re.compile(r"(?i)(?<![\d.,])(?:E(?:st)?\s*[:=]?\s*)?(?P<e>" + GB_NUM_E + r")\s*(?:m\b)?[\s;,|/]+(?:N(?:ord)?\s*[:=]?\s*)?(?P<n>" + GB_NUM_N + r")(?![\d])")
GB_PAIR_NE_RE = re.compile(r"(?i)\bN(?:ord)?\s*[:=]?\s*(?P<n>" + GB_NUM_N + r")\s*(?:m\b)?[\s;,|/]+E(?:st)?\s*[:=]?\s*(?P<e>" + GB_NUM_E + r")")
# 'Latitudine 45,091549° N; Longitudine 11,185199° E' and 'Lat. 44.768957 Long. 11.680089'
LAB_LATLON_RE = re.compile(r"(?i)\blat(?:itudine)?\.?\s*[:=]?\s*(?P<lat>3[5-9]|4[0-7])[.,](?P<latd>\d{2,8})\s*°?\s*N?\b.{0,40}?"
                           r"\blon(?:g(?:itudine)?)?\.?\s*[:=]?\s*(?P<lon>0?[6-9]|1[0-8])[.,](?P<lond>\d{2,8})\s*°?\s*E?")

OBJECT_RULES = [
    (re.compile(r"(?i)pvgis|simulazion|irraggiament"), "simulation_point"),
    (re.compile(r"(?i)(?:coordinate|posizione)\s+(?:geografiche\s+)?(?:\([^)]{0,20}\)\s*)?(?:del|dell[a']|di)\s*(?:sito|impianto|area|lotto|baricentro|campo)|"
                r"sito\s+(?:è\s+)?(?:individuato|localizzat)|area\s+(?:interessata\s+)?si\s+trova|google\s+earth|"
                r"\bUBICAZIONE\b.{0,120}\bCOORDINATE\b|localizzabile\s+attraverso"), "site_point"),
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


def tm_inverse(easting: float, northing: float, a: float, f: float, lon0_deg: float, fe: float,
               k0: float = 0.9996) -> tuple[float, float]:
    """Inverse transverse Mercator on any ellipsoid (radians -> degrees), Snyder series."""
    e2 = f * (2 - f)
    ep2 = e2 / (1 - e2)
    x = easting - fe
    m = northing / k0
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
    return math.degrees(lat), lon0_deg + math.degrees(lon)


HAYFORD = (6378388.0, 1 / 297.0)          # International 1924: Roma40 (Gauss-Boaga) and ED50
WGS84 = (6378137.0, 1 / 298.257223563)


def _helmert_to_wgs84(lat: float, lon: float, ell: tuple, tx, ty, tz, rx=0.0, ry=0.0, rz=0.0, ds=0.0) -> tuple[float, float]:
    """Datum shift via geocentric coordinates (position-vector convention, rotations in arc-seconds)."""
    a, f = ell
    e2 = f * (2 - f)
    phi, lam = math.radians(lat), math.radians(lon)
    n = a / math.sqrt(1 - e2 * math.sin(phi) ** 2)
    x, y, z = n * math.cos(phi) * math.cos(lam), n * math.cos(phi) * math.sin(lam), n * (1 - e2) * math.sin(phi)
    s = 1 + ds * 1e-6
    r = math.radians(1 / 3600)
    rx, ry, rz = rx * r, ry * r, rz * r
    x2 = tx + s * (x - rz * y + ry * z)
    y2 = ty + s * (rz * x + y - rx * z)
    z2 = tz + s * (-ry * x + rx * y + z)
    a2, f2 = WGS84
    e22 = f2 * (2 - f2)
    p = math.hypot(x2, y2)
    phi2 = math.atan2(z2, p * (1 - e22))
    for _ in range(6):
        n2 = a2 / math.sqrt(1 - e22 * math.sin(phi2) ** 2)
        phi2 = math.atan2(z2 + e22 * n2 * math.sin(phi2), p)
    return math.degrees(phi2), math.degrees(math.atan2(y2, x2))


def gauss_boaga_to_wgs84(easting: float, northing: float) -> tuple[float, float]:
    """Monte Mario / Roma40 Gauss-Boaga (EPSG 3003 fuso Ovest, 3004 fuso Est) -> WGS84, metre-level."""
    west = easting < 2_000_000
    lat, lon = tm_inverse(easting, northing, *HAYFORD, lon0_deg=9.0 if west else 15.0,
                          fe=1_500_000.0 if west else 2_520_000.0)
    # PROJ definition of EPSG:3003/3004 (+towgs84, position-vector convention): Roma40 is within ~20 m of WGS84 here
    lat, lon = _helmert_to_wgs84(lat, lon, HAYFORD, -104.1, -49.1, -9.9, 0.971, -2.917, 0.714, -11.68)
    return round(lat, 7), round(lon, 7)


def ed50_utm_to_wgs84(easting: float, northing: float, zone: int) -> tuple[float, float]:
    lat, lon = tm_inverse(easting, northing, *HAYFORD, lon0_deg=(zone - 1) * 6 - 180 + 3, fe=500_000.0)
    lat, lon = _helmert_to_wgs84(lat, lon, HAYFORD, -87.0, -98.0, -121.0)
    return round(lat, 7), round(lon, 7)


def dms_to_deg(d: str, m: str, s: str) -> float:
    return int(d) + int(m) / 60 + float(s.replace(",", ".")) / 3600


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
        crs["gauss_boaga"] = True
    if ED50_RE.search(text):
        crs["datum"] = "ED50"
    z = ZONE_RE.search(text)
    if z:
        if z.group("epsg"):
            e = z.group("epsg")
            crs["epsg"] = int(e)
            crs["zone"] = {"32632": 32, "25832": 32, "6707": 32, "32633": 33, "25833": 33, "6708": 33}.get(e)
            if e in ("3003", "3004"):
                crs["gauss_boaga"] = True
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
                q["lat"], q["lon"] = (ed50_utm_to_wgs84(e, n, crs["zone"]) if crs.get("datum") == "ED50"
                                      else utm_to_latlon(e, n, crs["zone"]))
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
    facts.extend(extract_text_coordinates(ctx, crs))
    facts.extend(extract_map_extent(ctx))
    facts.extend(extract_row_tables(ctx))
    return facts


def _point_fact(ctx: PageContext, i: int, lat: float, lon: float, crs_text: str, extractor: str, raw: str) -> Fact | None:
    if not (35.4 <= lat <= 47.2 and 6.5 <= lon <= 18.7):
        return None
    window = ctx.window(i, 2, 0)
    obj = classify_object(ctx.lines[i] if i < len(ctx.lines) else "")
    if obj == "unknown":
        obj = classify_object(window)
    return Fact(field="geo.latlon_point", value={"lat": round(lat, 7), "lon": round(lon, 7)},
                citation=ctx.cite(ctx.window(i, 1, 1)), precision="component_coordinates",
                qualifiers={"object": obj, "crs": crs_text, "raw": raw[:80]},
                limits=f"Point of object '{obj}' as written in the document", extractor=extractor)


def extract_text_coordinates(ctx: PageContext, crs: dict) -> list[Fact]:
    """Formats missed by the tabular extractors: sexagesimal pairs, labelled lat/lon with comma decimals,
    Gauss-Boaga pairs (converted with the Roma40 datum shift)."""
    out: list[Fact] = []
    seen: set[tuple] = set()
    n = len(ctx.lines)
    for i in range(n):
        joined = " ".join(l for l in ctx.lines[i:i + 3] if l)        # pairs are often split over 2-3 lines
        first = ctx.lines[i] or ""
        # sexagesimal: 42° 19' 55,7" N 11° 40' 22,8" E
        dms = list(DMS_RE.finditer(joined))
        for a, b in zip(dms, dms[1:]):
            if a.start() >= len(first) + 1:
                break                                                # pair must start on this line
            if a.group("h") in "NS" and b.group("h") in "EW" and b.start() - a.end() < 60:
                lat = dms_to_deg(a.group("d"), a.group("m"), a.group("s"))
                lon = dms_to_deg(b.group("d"), b.group("m"), b.group("s"))
                key = (round(lat, 5), round(lon, 5))
                if key not in seen:
                    seen.add(key)
                    f = _point_fact(ctx, i, lat, lon, "WGS84 (sessagesimali)", "geo.dms", joined[a.start():b.end()])
                    if f:
                        out.append(f)
        lm = None if LAT_ROW.match(first) else LAB_LATLON_RE.search(joined)    # row tables have their own reader
        if lm and lm.start() < len(first):
            lat = float(f"{lm.group('lat')}.{lm.group('latd')}")
            lon = float(f"{lm.group('lon')}.{lm.group('lond')}")
            key = (round(lat, 5), round(lon, 5))
            if key not in seen:
                seen.add(key)
                f = _point_fact(ctx, i, lat, lon, "WGS84 (gradi decimali)", "geo.latlon_labelled", lm.group(0))
                if f:
                    out.append(f)
        if crs.get("gauss_boaga") or GB_RE.search(ctx.window(i, 3, 3)):
            for rx in (GB_PAIR_RE, GB_PAIR_NE_RE):
                for m in rx.finditer(first):
                    e, nn = parse_number(m.group("e").replace(" ", "")), parse_number(m.group("n").replace(" ", ""))
                    if not e or not nn or not (1_300_000 <= e <= 2_900_000 and 4_000_000 <= nn <= 5_250_000):
                        continue
                    lat, lon = gauss_boaga_to_wgs84(e, nn)
                    key = (round(lat, 5), round(lon, 5))
                    if key not in seen:
                        seen.add(key)
                        f = _point_fact(ctx, i, lat, lon, "Gauss-Boaga Roma40 (convertito)", "geo.gauss_boaga", m.group(0))
                        if f:
                            f.qualifiers["easting"], f.qualifiers["northing"] = e, nn
                            out.append(f)
    return out


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


def extract_map_extent(ctx: PageContext) -> list[Fact]:
    """Frame of a georeferenced drawing (from the PDF itself, not from text)."""
    out = []
    for g in getattr(ctx, "geo_extent", None) or []:
        lat, lon = g["centre"]
        # frame size in km, to tell a layout plan (a few km) from a regional framing map
        import math
        h = (g["lat_max"] - g["lat_min"]) * 111.0
        w = (g["lon_max"] - g["lon_min"]) * 111.0 * math.cos(math.radians(lat))
        out.append(Fact(field="geo.map_extent", value={"lat": lat, "lon": lon, "width_km": round(w, 2), "height_km": round(h, 2)},
                        citation=ctx.cite((ctx.document_title or "tavola georeferenziata")[:120]),
                        precision="map_frame", qualifiers={"bounds": [g["lat_min"], g["lon_min"], g["lat_max"], g["lon_max"]],
                                                           "share": g.get("share"), "object": "map_frame"},
                        limits="Centre of a georeferenced map frame embedded in the PDF; not a surveyed point",
                        extractor="geo.geopdf"))
    return out
