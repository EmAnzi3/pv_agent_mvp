from __future__ import annotations

import csv
import json
import math
import re
from pathlib import Path
from typing import Any

import requests

from app.collectors.sicilia import SiciliaCollector


SIVVI_MAPSERVER = "https://map.sitr.regione.sicilia.it/orbs/rest/services/sivvi/procedure_valutazione_ambientale/MapServer"
COMUNI_LAYER = "https://map.sitr.regione.sicilia.it/gis/rest/services/catasto/cartografia_catastale/MapServer/9"

OUT_DIR = Path("/app/reports")
TIMEOUT = 60


def clean_text(value: Any) -> str:
    return re.sub(r"\s+", " ", str(value or "")).strip()


def norm(value: Any) -> str:
    value = clean_text(value).lower()
    value = value.replace("à", "a").replace("è", "e").replace("é", "e").replace("ì", "i").replace("ò", "o").replace("ù", "u")
    value = re.sub(r"[^a-z0-9]+", " ", value)
    return re.sub(r"\s+", " ", value).strip()


def has_value(value: Any) -> bool:
    if value is None:
        return False
    if isinstance(value, list):
        return len(value) > 0
    return bool(clean_text(value))


def parse_external_id(external_id: str) -> tuple[int | None, int | None]:
    """
    Gli external_id del collector Sicilia hanno normalmente formato:
    id|codproc|slug...
    """
    parts = str(external_id or "").split("|")
    id_value = None
    codproc_value = None

    if len(parts) >= 1 and parts[0].isdigit():
        id_value = int(parts[0])
    if len(parts) >= 2 and parts[1].isdigit():
        codproc_value = int(parts[1])

    return id_value, codproc_value


def get_json(url: str, params: dict[str, Any]) -> dict[str, Any]:
    response = requests.get(url, params=params, timeout=TIMEOUT)
    response.raise_for_status()
    data = response.json()
    if "error" in data:
        raise RuntimeError(json.dumps(data["error"], ensure_ascii=False, indent=2))
    return data


def query_arcgis(url: str, params: dict[str, Any]) -> list[dict[str, Any]]:
    data = get_json(url, params)
    return data.get("features") or []


def chunks(values: list[Any], size: int) -> list[list[Any]]:
    return [values[i:i + size] for i in range(0, len(values), size)]


def fetch_layer0_index() -> tuple[dict[tuple[int, int], dict], dict[int, dict], dict[int, list[dict]]]:
    """
    Scarica le feature del layer Procedure e crea indici per:
    - (id, codproc)
    - id
    - codproc
    """
    ids_data = get_json(
        f"{SIVVI_MAPSERVER}/0/query",
        {
            "f": "json",
            "where": "1=1",
            "returnIdsOnly": "true",
        },
    )

    object_ids = ids_data.get("objectIds") or []
    by_pair: dict[tuple[int, int], dict] = {}
    by_id: dict[int, dict] = {}
    by_codproc: dict[int, list[dict]] = {}

    for batch in chunks(object_ids, 200):
        data = get_json(
            f"{SIVVI_MAPSERVER}/0/query",
            {
                "f": "json",
                "objectIds": ",".join(str(x) for x in batch),
                "outFields": "*",
                "returnGeometry": "true",
                "outSR": 4326,
            },
        )

        for feature in data.get("features") or []:
            attrs = feature.get("attributes") or {}
            geom = feature.get("geometry") or {}

            id_value = attrs.get("id")
            codproc_value = attrs.get("codproc")

            if id_value is None or codproc_value is None:
                continue

            row = {
                "attributes": attrs,
                "geometry": geom,
                "x": geom.get("x"),
                "y": geom.get("y"),
                "id": int(id_value),
                "codproc": int(codproc_value),
                "oggetto": clean_text(attrs.get("oggetto")),
                "procedura": clean_text(attrs.get("procedura")),
                "proponente": clean_text(attrs.get("proponente")),
                "settore": clean_text(attrs.get("settore")),
            }

            by_pair[(row["id"], row["codproc"])] = row
            by_id[row["id"]] = row
            by_codproc.setdefault(row["codproc"], []).append(row)

    return by_pair, by_id, by_codproc


def get_comuni_field_names() -> tuple[str | None, str | None]:
    meta = get_json(COMUNI_LAYER, {"f": "json"})
    fields = meta.get("fields") or []
    names = [field.get("name") for field in fields if field.get("name")]
    upper_map = {name.upper(): name for name in names}

    comune_candidates = [
        "COMUNE",
        "NOME_COM",
        "NOME_COMUNE",
        "DENOMINAZIONE",
        "DENOM",
        "NOME",
    ]
    provincia_candidates = [
        "PROVINCIA",
        "SIGLA_PROVINCIA",
        "SIGLA",
        "PROV",
        "COD_PROV",
        "PR",
    ]

    comune_field = next((upper_map[x] for x in comune_candidates if x in upper_map), None)
    provincia_field = next((upper_map[x] for x in provincia_candidates if x in upper_map), None)

    return comune_field, provincia_field


def rows_to_location(features: list[dict[str, Any]], comune_field: str | None, provincia_field: str | None) -> tuple[str, str]:
    comuni: list[str] = []
    province: list[str] = []

    for feature in features:
        attrs = feature.get("attributes") or {}

        comune = clean_text(attrs.get(comune_field)) if comune_field else ""
        provincia = clean_text(attrs.get(provincia_field)) if provincia_field else ""

        if comune and comune not in comuni:
            comuni.append(comune)
        if provincia and provincia not in province:
            province.append(provincia)

    return ", ".join(comuni), ", ".join(province)


def query_comuni_by_point(x: Any, y: Any, comune_field: str | None, provincia_field: str | None) -> tuple[str, str]:
    if x is None or y is None:
        return "", ""

    features = query_arcgis(
        f"{COMUNI_LAYER}/query",
        {
            "f": "json",
            "geometry": f"{x},{y}",
            "geometryType": "esriGeometryPoint",
            "inSR": 4326,
            "spatialRel": "esriSpatialRelIntersects",
            "outFields": "*",
            "returnGeometry": "false",
        },
    )

    return rows_to_location(features, comune_field, provincia_field)


def query_polygons_by_codproc(layer_id: int, codproc: int) -> list[dict[str, Any]]:
    return query_arcgis(
        f"{SIVVI_MAPSERVER}/{layer_id}/query",
        {
            "f": "json",
            "where": f"codproc = {int(codproc)}",
            "outFields": "*",
            "returnGeometry": "true",
            "outSR": 4326,
        },
    )


def query_comuni_by_polygon(geometry: dict[str, Any], comune_field: str | None, provincia_field: str | None) -> tuple[str, str]:
    if not geometry:
        return "", ""

    features = query_arcgis(
        f"{COMUNI_LAYER}/query",
        {
            "f": "json",
            "geometry": json.dumps(geometry, ensure_ascii=False),
            "geometryType": "esriGeometryPolygon",
            "inSR": 4326,
            "spatialRel": "esriSpatialRelIntersects",
            "outFields": "*",
            "returnGeometry": "false",
        },
    )

    return rows_to_location(features, comune_field, provincia_field)


def merge_csv_values(*values: str) -> str:
    found: list[str] = []
    for value in values:
        for item in str(value or "").split(","):
            item = clean_text(item)
            if item and item not in found:
                found.append(item)
    return ", ".join(found)


def find_layer0_match(
    id_value: int | None,
    codproc_value: int | None,
    title: str,
    by_pair: dict[tuple[int, int], dict],
    by_id: dict[int, dict],
    by_codproc: dict[int, list[dict]],
) -> tuple[dict | None, str]:
    if id_value is not None and codproc_value is not None and (id_value, codproc_value) in by_pair:
        return by_pair[(id_value, codproc_value)], "id+codproc"

    if id_value is not None and id_value in by_id:
        return by_id[id_value], "id"

    if codproc_value is not None:
        candidates = by_codproc.get(codproc_value) or []
        if len(candidates) == 1:
            return candidates[0], "codproc_unique"

        if candidates:
            title_norm = norm(title)
            for candidate in candidates:
                oggetto_norm = norm(candidate.get("oggetto"))
                if title_norm and oggetto_norm and (title_norm in oggetto_norm or oggetto_norm in title_norm):
                    return candidate, "codproc+title"

    return None, ""


def main() -> None:
    OUT_DIR.mkdir(parents=True, exist_ok=True)

    print("[sicilia-map-enrichment] fetch collector Sicilia...")
    items = SiciliaCollector().fetch()

    print("[sicilia-map-enrichment] fetch SI-VVI Procedure layer...")
    by_pair, by_id, by_codproc = fetch_layer0_index()

    print("[sicilia-map-enrichment] fetch Comuni field names...")
    comune_field, provincia_field = get_comuni_field_names()
    print(f"[sicilia-map-enrichment] comune_field={comune_field} provincia_field={provincia_field}")

    point_cache: dict[tuple[float, float], tuple[str, str]] = {}
    polygon_cache: dict[tuple[int, int], tuple[str, str]] = {}

    rows: list[dict[str, Any]] = []

    for item in items:
        payload = item.payload or {}
        title = clean_text(payload.get("title") or item.title)
        municipalities = payload.get("municipalities") or []
        current_municipalities = ", ".join(municipalities) if isinstance(municipalities, list) else clean_text(municipalities)
        current_province = clean_text(payload.get("province"))

        id_value, codproc_value = parse_external_id(item.external_id)
        match, match_mode = find_layer0_match(id_value, codproc_value, title, by_pair, by_id, by_codproc)

        map_comuni = ""
        map_province = ""
        map_source = ""
        map_proponente = ""
        map_oggetto = ""
        map_x = ""
        map_y = ""

        if match:
            map_proponente = clean_text(match.get("proponente"))
            map_oggetto = clean_text(match.get("oggetto"))
            map_x = match.get("x")
            map_y = match.get("y")

            if map_x is not None and map_y is not None:
                key = (round(float(map_x), 7), round(float(map_y), 7))
                if key not in point_cache:
                    point_cache[key] = query_comuni_by_point(map_x, map_y, comune_field, provincia_field)
                map_comuni, map_province = point_cache[key]
                if map_comuni or map_province:
                    map_source = "layer0_point"

        if not map_comuni and codproc_value is not None:
            for layer_id in [8, 9]:
                cache_key = (layer_id, codproc_value)
                if cache_key not in polygon_cache:
                    comuni_all = ""
                    province_all = ""
                    try:
                        polygons = query_polygons_by_codproc(layer_id, codproc_value)
                        for feature in polygons:
                            comuni, province = query_comuni_by_polygon(feature.get("geometry") or {}, comune_field, provincia_field)
                            comuni_all = merge_csv_values(comuni_all, comuni)
                            province_all = merge_csv_values(province_all, province)
                    except Exception as exc:
                        comuni_all = ""
                        province_all = ""

                    polygon_cache[cache_key] = (comuni_all, province_all)

                comuni, province = polygon_cache[cache_key]
                if comuni or province:
                    map_comuni = comuni
                    map_province = province
                    map_source = f"layer{layer_id}_polygon"
                    break

        suggested_municipalities = current_municipalities or map_comuni
        suggested_province = current_province or map_province
        suggested_proponent = clean_text(payload.get("proponent")) or map_proponente

        rows.append({
            "external_id": item.external_id,
            "parsed_id": id_value,
            "parsed_codproc": codproc_value,
            "match_mode": match_mode,
            "map_source": map_source,
            "title": title,
            "current_proponent": clean_text(payload.get("proponent")),
            "map_proponente": map_proponente,
            "suggested_proponent": suggested_proponent,
            "current_province": current_province,
            "current_municipalities": current_municipalities,
            "map_province": map_province,
            "map_municipalities": map_comuni,
            "suggested_province": suggested_province,
            "suggested_municipalities": suggested_municipalities,
            "current_power": clean_text(payload.get("power")),
            "url": item.source_url,
            "map_x": map_x,
            "map_y": map_y,
            "map_oggetto": map_oggetto,
            "needs_location": not current_province or not current_municipalities,
            "location_improved": (not current_province and bool(map_province)) or (not current_municipalities and bool(map_comuni)),
            "proponent_improved": not clean_text(payload.get("proponent")) and bool(map_proponente),
        })

    out_all = OUT_DIR / "sicilia_map_enrichment_audit.csv"
    out_missing = OUT_DIR / "sicilia_map_enrichment_missing_only.csv"
    out_summary = OUT_DIR / "sicilia_map_enrichment_summary.txt"

    fieldnames = list(rows[0].keys()) if rows else []

    with out_all.open("w", newline="", encoding="utf-8-sig") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)

    missing_rows = [row for row in rows if row["needs_location"]]

    with out_missing.open("w", newline="", encoding="utf-8-sig") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(missing_rows)

    total = len(rows)
    current_missing_province = sum(1 for row in rows if not row["current_province"])
    current_missing_municipalities = sum(1 for row in rows if not row["current_municipalities"])
    matched = sum(1 for row in rows if row["match_mode"])
    point_improved = sum(1 for row in rows if row["location_improved"] and row["map_source"] == "layer0_point")
    polygon_improved = sum(1 for row in rows if row["location_improved"] and row["map_source"].endswith("_polygon"))
    improved_location = sum(1 for row in rows if row["location_improved"])
    improved_proponent = sum(1 for row in rows if row["proponent_improved"])
    after_missing_province = sum(1 for row in rows if not row["suggested_province"])
    after_missing_municipalities = sum(1 for row in rows if not row["suggested_municipalities"])

    by_source: dict[str, int] = {}
    for row in rows:
        source = row["map_source"] or "none"
        by_source[source] = by_source.get(source, 0) + 1

    summary = [
        "Sicilia map enrichment audit",
        "=============================",
        "",
        f"Total collector records: {total}",
        f"Layer0 matched records: {matched}",
        "",
        f"Current missing province: {current_missing_province}",
        f"Current missing municipalities: {current_missing_municipalities}",
        f"Suggested missing province after map enrichment: {after_missing_province}",
        f"Suggested missing municipalities after map enrichment: {after_missing_municipalities}",
        "",
        f"Location improved: {improved_location}",
        f"Location improved from layer0 point: {point_improved}",
        f"Location improved from polygon layers: {polygon_improved}",
        f"Proponent improved from map: {improved_proponent}",
        "",
        "Map source distribution:",
    ]

    for source, count in sorted(by_source.items(), key=lambda x: (-x[1], x[0])):
        summary.append(f"- {source}: {count}")

    summary.extend([
        "",
        f"All rows: {out_all}",
        f"Missing only: {out_missing}",
    ])

    out_summary.write_text("\n".join(summary), encoding="utf-8")

    print("\n".join(summary))


if __name__ == "__main__":
    main()
