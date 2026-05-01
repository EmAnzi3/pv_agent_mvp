from __future__ import annotations

import argparse
import csv
import json
import sys
from pathlib import Path
from typing import Any

import requests


SIVVI_MAPSERVER = "https://map.sitr.regione.sicilia.it/orbs/rest/services/sivvi/procedure_valutazione_ambientale/MapServer"
COMUNI_LAYER = "https://map.sitr.regione.sicilia.it/gis/rest/services/catasto/cartografia_catastale/MapServer/9"

DEFAULT_LAYERS = [0, 8, 9]


def get_json(url: str, params: dict[str, Any], timeout: int = 60) -> dict[str, Any]:
    response = requests.get(url, params=params, timeout=timeout)
    response.raise_for_status()
    data = response.json()
    if "error" in data:
        raise RuntimeError(json.dumps(data["error"], ensure_ascii=False, indent=2))
    return data


def layer_metadata(layer_id: int) -> dict[str, Any]:
    return get_json(f"{SIVVI_MAPSERVER}/{layer_id}", {"f": "json"})


def query_layer(layer_id: int, limit: int = 20, return_geometry: bool = True) -> dict[str, Any]:
    params = {
        "f": "json",
        "where": "1=1",
        "outFields": "*",
        "returnGeometry": "true" if return_geometry else "false",
        "resultRecordCount": limit,
        "resultOffset": 0,
        "outSR": 4326,
    }
    return get_json(f"{SIVVI_MAPSERVER}/{layer_id}/query", params)


def flatten_feature(feature: dict[str, Any]) -> dict[str, Any]:
    row = dict(feature.get("attributes") or {})
    geom = feature.get("geometry") or {}

    if "x" in geom and "y" in geom:
        row["_x"] = geom.get("x")
        row["_y"] = geom.get("y")
    elif "rings" in geom:
        row["_geometry_type"] = "polygon"
        row["_rings_count"] = len(geom.get("rings") or [])
    elif "paths" in geom:
        row["_geometry_type"] = "polyline"
        row["_paths_count"] = len(geom.get("paths") or [])

    return row


def write_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    if not rows:
        path.write_text("", encoding="utf-8")
        return

    keys: list[str] = []
    for row in rows:
        for key in row:
            if key not in keys:
                keys.append(key)

    with path.open("w", newline="", encoding="utf-8-sig") as handle:
        writer = csv.DictWriter(handle, fieldnames=keys)
        writer.writeheader()
        writer.writerows(rows)


def main() -> None:
    parser = argparse.ArgumentParser(description="Probe ArcGIS REST layers used by the Regione Sicilia SI-VVI map.")
    parser.add_argument("--out-dir", default="/app/reports", help="Output directory. Default: /app/reports")
    parser.add_argument("--limit", type=int, default=20, help="Sample records per layer. Default: 20")
    parser.add_argument("--layers", default="0,8,9", help="Comma-separated SI-VVI layer IDs. Default: 0,8,9")
    args = parser.parse_args()

    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    layer_ids = [int(x.strip()) for x in args.layers.split(",") if x.strip()]

    summary_lines: list[str] = []
    summary_lines.append("Sicilia SI-VVI map probe")
    summary_lines.append("========================")
    summary_lines.append("")
    summary_lines.append(f"MapServer: {SIVVI_MAPSERVER}")
    summary_lines.append(f"Layers tested: {layer_ids}")
    summary_lines.append("")

    for layer_id in layer_ids:
        print(f"[probe] Layer {layer_id}")
        meta = layer_metadata(layer_id)
        layer_name = meta.get("name") or f"layer_{layer_id}"
        geometry_type = meta.get("geometryType")
        fields = meta.get("fields") or []

        summary_lines.append(f"## Layer {layer_id}: {layer_name}")
        summary_lines.append(f"Geometry: {geometry_type}")
        summary_lines.append("Fields:")
        for field in fields:
            summary_lines.append(f"- {field.get('name')} :: {field.get('type')} :: {field.get('alias')}")
        summary_lines.append("")

        data = query_layer(layer_id, limit=args.limit, return_geometry=True)
        features = data.get("features") or []
        rows = [flatten_feature(feature) for feature in features]

        csv_path = out_dir / f"sicilia_map_layer_{layer_id}_sample.csv"
        json_path = out_dir / f"sicilia_map_layer_{layer_id}_metadata.json"

        write_csv(csv_path, rows)
        json_path.write_text(json.dumps(meta, ensure_ascii=False, indent=2), encoding="utf-8")

        summary_lines.append(f"Sample rows: {len(rows)}")
        summary_lines.append(f"CSV: {csv_path}")
        summary_lines.append(f"Metadata: {json_path}")
        summary_lines.append("")

    summary_path = out_dir / "sicilia_map_probe_summary.txt"
    summary_path.write_text("\n".join(summary_lines), encoding="utf-8")

    print("")
    print("[probe] OK")
    print(f"[probe] Summary: {summary_path}")


if __name__ == "__main__":
    main()
