from __future__ import annotations

import csv
import html
import json
import math
import re
from collections import Counter
from datetime import datetime
from pathlib import Path
from typing import Any

from app.config import settings
from app.geo_enrichment import enrich_geo_from_text


REGION_ORDER = [
    "Abruzzo",
    "Basilicata",
    "Calabria",
    "Campania",
    "Emilia-Romagna",
    "Friuli-Venezia Giulia",
    "Lazio",
    "Liguria",
    "Lombardia",
    "Marche",
    "Molise",
    "Piemonte",
    "Puglia",
    "Sardegna",
    "Sicilia",
    "Toscana",
    "Trentino-Alto Adige",
    "Umbria",
    "Valle d'Aosta",
    "Veneto",
]

SOURCE_LABELS = {
    "mase": "MASE",
    "mase_provvedimenti": "MASE – Provvedimenti",
    "terna_econnextion": "Terna Econnextion",
    "puglia": "Puglia",
    "lazio": "Lazio",
    "sicilia": "Sicilia",
    "lombardia": "Lombardia",
    "veneto": "Veneto",
    "emilia_romagna": "Emilia-Romagna",
    "sardegna": "Sardegna",
    "toscana": "Toscana",
    "piemonte": "Piemonte",
    "campania": "Campania",
}

COLUMN_ALIASES = {
    "source": [
        "source",
        "source_name",
        "primary_source",
        "fonte_dato",
        "fonte",
    ],
    "title": [
        "title",
        "project_name",
        "nome_progetto",
        "progetto",
        "name",
    ],
    "proponent": [
        "proponent",
        "proponente",
        "societa",
        "società",
        "company",
    ],
    "region": [
        "region",
        "regione",
    ],
    "province": [
        "province",
        "provincia",
    ],
    "municipalities": [
        "municipalities",
        "municipality",
        "comune",
        "comuni",
    ],
    "project_type": [
        "project_type",
        "tipo_progetto",
        "project_type_hint",
        "tipologia",
        "tipo",
    ],
    "power_mw": [
        "power_mw",
        "potenza_mw",
        "mw",
        "power",
        "potenza",
    ],
    "status": [
        "status_normalized",
        "status_raw",
        "stato",
        "stato_normalizzato",
        "stato_connessione",
        "procedure",
        "procedura",
    ],
    "url": [
        "primary_url",
        "source_url",
        "url",
        "link",
    ],
    "updated_at": [
        "updated_at",
        "last_update",
        "ultimo_aggiornamento",
        "date_last_update",
    ],
    "numero_pratiche": [
        "numero_pratiche",
        "pratiche",
        "numero pratiche",
        "Numero Pratiche",
    ],
}


class StaticDashboardBuilder:
    def __init__(
        self,
        reports_dir: str | Path | None = None,
        site_dir: str | Path | None = None,
    ) -> None:
        self.reports_dir = Path(reports_dir or settings.reports_dir)
        self.site_dir = Path(site_dir or self.reports_dir / "site")

    def build(self) -> Path:
        snapshot_path = self._find_latest_snapshot()
        rows = self._read_snapshot(snapshot_path)
        records = [self._normalize_record(row) for row in rows]
        records = [record for record in records if record is not None]

        summary = self._build_summary(records, snapshot_path)
        html_content = self._render_html(summary=summary, records=records)

        self.site_dir.mkdir(parents=True, exist_ok=True)

        data_path = self.site_dir / "data.json"
        index_path = self.site_dir / "index.html"

        data_path.write_text(
            json.dumps(
                {
                    "summary": summary,
                    "records": records,
                },
                ensure_ascii=False,
                indent=2,
            ),
            encoding="utf-8",
        )

        index_path.write_text(html_content, encoding="utf-8")

        return index_path

    # ------------------------------------------------------------------
    # INPUT
    # ------------------------------------------------------------------

    def _find_latest_snapshot(self) -> Path:
        candidates = sorted(
            self.reports_dir.glob("projects_snapshot_*.csv"),
            key=lambda path: path.stat().st_mtime,
            reverse=True,
        )

        if not candidates:
            raise FileNotFoundError(
                f"Nessun file projects_snapshot_*.csv trovato in {self.reports_dir}"
            )

        return candidates[0]

    def _read_snapshot(self, path: Path) -> list[dict[str, Any]]:
        raw = self._read_text_with_fallback(path)
        sample = raw[:4096]

        try:
            dialect = csv.Sniffer().sniff(sample, delimiters=",;|\t")
        except csv.Error:
            dialect = csv.excel

        reader = csv.DictReader(raw.splitlines(), dialect=dialect)
        return [dict(row) for row in reader]

    def _read_text_with_fallback(self, path: Path) -> str:
        for encoding in ["utf-8-sig", "utf-8", "cp1252", "latin-1"]:
            try:
                return path.read_text(encoding=encoding)
            except UnicodeDecodeError:
                continue

        return path.read_text(encoding="utf-8", errors="replace")

    # ------------------------------------------------------------------
    # NORMALIZATION
    # ------------------------------------------------------------------

    def _normalize_record(self, row: dict[str, Any]) -> dict[str, Any] | None:
        row_map = {self._normalize_header(key): value for key, value in row.items()}

        source = self._get_value(row_map, "source")
        title = self._get_value(row_map, "title")
        region = self._normalize_region(self._get_value(row_map, "region"))
        province = self._clean_text(self._get_value(row_map, "province"))
        municipalities = self._clean_text(self._get_value(row_map, "municipalities"))
        proponent = self._clean_text(self._get_value(row_map, "proponent"))
        project_type = self._clean_text(self._get_value(row_map, "project_type"))
        status = self._clean_text(self._get_value(row_map, "status"))
        url = self._clean_text(self._get_value(row_map, "url"))
        updated_at = self._clean_text(self._get_value(row_map, "updated_at"))
        numero_pratiche = self._parse_int(self._get_value(row_map, "numero_pratiche"))

        source = self._normalize_source(source)
        title = self._clean_text(title)

        if not source and not title:
            return None

        power_mw = self._parse_float(self._get_value(row_map, "power_mw"))

        is_terna = (
            source == "terna_econnextion"
            or "terna econnextion" in (title or "").lower()
        )

        is_punctual = not is_terna

        geo_text = " ".join(
            part
            for part in [
                title or "",
                municipalities or "",
                province or "",
                region or "",
                status or "",
            ]
            if part
        )

        geo = enrich_geo_from_text(
            geo_text,
            existing_region=region,
            existing_province=province,
            existing_municipalities=municipalities,
        )

        if geo.province and not province:
            province = geo.province

        if geo.region and (not region or region == "ND"):
            region = geo.region

        if geo.municipalities and not municipalities:
            municipalities = ", ".join(geo.municipalities)

        if not title:
            title = self._fallback_title(source, region, project_type, status)

        if is_terna and not numero_pratiche:
            numero_pratiche = 0

        return {
            "source": source or "nd",
            "source_label": SOURCE_LABELS.get(source or "", source or "ND"),
            "title": title or "",
            "proponent": proponent or "",
            "region": region or "ND",
            "province": province or "",
            "municipalities": municipalities or "",
            "project_type": project_type or "",
            "power_mw": power_mw,
            "status": status or "",
            "url": url or "",
            "updated_at": updated_at or "",
            "numero_pratiche": numero_pratiche or 0,
            "is_terna": is_terna,
            "is_punctual": is_punctual,
            "province_deduced": bool(geo.province_deduced),
            "municipalities_deduced": bool(geo.municipalities_deduced),
        }

    def _get_value(self, row_map: dict[str, Any], logical_name: str) -> Any:
        aliases = COLUMN_ALIASES.get(logical_name, [])

        for alias in aliases:
            key = self._normalize_header(alias)
            if key in row_map:
                value = row_map.get(key)
                if value is not None and str(value).strip() != "":
                    return value

        return None

    def _normalize_header(self, value: str | None) -> str:
        value = self._clean_text(value or "") or ""
        value = value.lower()
        value = value.replace("à", "a")
        value = value.replace("è", "e")
        value = value.replace("é", "e")
        value = value.replace("ì", "i")
        value = value.replace("ò", "o")
        value = value.replace("ù", "u")
        value = re.sub(r"[^a-z0-9]+", "_", value)
        return value.strip("_")

    def _normalize_source(self, value: Any) -> str:
        text = self._clean_text(value) or ""
        text = text.lower()
        text = text.replace("-", "_")
        text = re.sub(r"[^a-z0-9_]+", "_", text)
        text = re.sub(r"_+", "_", text).strip("_")
        return text

    def _normalize_region(self, value: Any) -> str:
        text = self._clean_text(value)

        if not text:
            return ""

        text_norm = self._normalize_key(text)

        for region in REGION_ORDER:
            if self._normalize_key(region) == text_norm:
                return region

        return text.title()

    def _normalize_key(self, value: str | None) -> str:
        value = self._clean_text(value or "") or ""
        value = value.lower()
        value = value.replace("à", "a")
        value = value.replace("è", "e")
        value = value.replace("é", "e")
        value = value.replace("ì", "i")
        value = value.replace("ò", "o")
        value = value.replace("ù", "u")
        value = re.sub(r"[^a-z0-9]+", "_", value)
        return value.strip("_")

    def _clean_text(self, value: Any) -> str | None:
        if value is None:
            return None

        text = str(value).replace("\xa0", " ")
        text = " ".join(text.split()).strip()

        if text.lower() in {"none", "nan", "null"}:
            return None

        return text or None

    def _parse_float(self, value: Any) -> float | None:
        if value is None:
            return None

        if isinstance(value, float):
            if math.isnan(value):
                return None
            return value

        if isinstance(value, int):
            return float(value)

        text = str(value).strip()

        if not text:
            return None

        text = text.replace("MW", "").replace("Mw", "").replace("mw", "")
        text = text.replace(" ", "")

        if "," in text and "." in text:
            text = text.replace(".", "").replace(",", ".")
        elif "," in text:
            text = text.replace(",", ".")

        text = re.sub(r"[^0-9.\-]", "", text)

        if not text:
            return None

        try:
            return float(text)
        except ValueError:
            return None

    def _parse_int(self, value: Any) -> int | None:
        if value is None:
            return None

        if isinstance(value, int):
            return value

        if isinstance(value, float):
            if math.isnan(value):
                return None
            return int(value)

        text = str(value).strip()

        if not text:
            return None

        text = re.sub(r"[^0-9\-]", "", text)

        if not text:
            return None

        try:
            return int(text)
        except ValueError:
            return None

    def _fallback_title(
        self,
        source: str,
        region: str,
        project_type: str | None,
        status: str | None,
    ) -> str:
        pieces = [
            SOURCE_LABELS.get(source, source or "Fonte"),
            region or "ND",
            project_type or "",
            status or "",
        ]

        return " - ".join(piece for piece in pieces if piece)

    # ------------------------------------------------------------------
    # SUMMARY
    # ------------------------------------------------------------------

    def _build_summary(
        self,
        records: list[dict[str, Any]],
        snapshot_path: Path,
    ) -> dict[str, Any]:
        punctual_records = [record for record in records if record["is_punctual"]]
        terna_records = [record for record in records if record["is_terna"]]

        total_mw_punctual = self._sum_power(punctual_records)
        total_mw_terna = self._sum_power(terna_records)

        source_counts = Counter(record["source"] for record in records)

        regions = self._build_region_summary(records)
        top_projects = self._build_top_projects(punctual_records)
        terna_summary = self._build_terna_summary(terna_records)
        quality = self._build_quality_summary(records)
        quality_by_source = self._build_quality_by_source(records)

        return {
            "generated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "snapshot_file": snapshot_path.name,
            "total_records": len(records),
            "punctual_records": len(punctual_records),
            "terna_records": len(terna_records),
            "total_mw_punctual": round(total_mw_punctual, 3),
            "total_mw_terna": round(total_mw_terna, 3),
            "source_counts": [
                {
                    "source": source,
                    "label": SOURCE_LABELS.get(source, source),
                    "count": count,
                }
                for source, count in source_counts.most_common()
            ],
            "regions": regions,
            "top_projects": top_projects,
            "terna_summary": terna_summary,
            "quality": quality,
            "quality_by_source": quality_by_source,
        }

    def _sum_power(self, records: list[dict[str, Any]]) -> float:
        return sum(record["power_mw"] or 0 for record in records)

    def _build_region_summary(self, records: list[dict[str, Any]]) -> list[dict[str, Any]]:
        grouped: dict[str, dict[str, Any]] = {}

        for record in records:
            region = record["region"] or "ND"

            if region not in grouped:
                grouped[region] = {
                    "region": region,
                    "punctual_count": 0,
                    "punctual_mw": 0.0,
                    "terna_count": 0,
                    "terna_mw": 0.0,
                    "terna_practices": 0,
                    "total_mw": 0.0,
                    "priority_score": 0.0,
                }

            item = grouped[region]

            if record["is_terna"]:
                item["terna_count"] += 1
                item["terna_mw"] += record["power_mw"] or 0
                item["terna_practices"] += record["numero_pratiche"] or 0
            else:
                item["punctual_count"] += 1
                item["punctual_mw"] += record["power_mw"] or 0

        max_punctual_mw = max((item["punctual_mw"] for item in grouped.values()), default=0)
        max_terna_mw = max((item["terna_mw"] for item in grouped.values()), default=0)
        max_count = max((item["punctual_count"] for item in grouped.values()), default=0)
        max_practices = max((item["terna_practices"] for item in grouped.values()), default=0)

        for item in grouped.values():
            punctual_mw_score = self._safe_ratio(item["punctual_mw"], max_punctual_mw)
            terna_mw_score = self._safe_ratio(item["terna_mw"], max_terna_mw)
            count_score = self._safe_ratio(item["punctual_count"], max_count)
            practices_score = self._safe_ratio(item["terna_practices"], max_practices)

            item["total_mw"] = item["punctual_mw"] + item["terna_mw"]

            item["priority_score"] = round(
                (punctual_mw_score * 40)
                + (count_score * 25)
                + (terna_mw_score * 25)
                + (practices_score * 10),
                1,
            )

            for key in ["punctual_mw", "terna_mw", "total_mw"]:
                item[key] = round(item[key], 3)

        return sorted(
            grouped.values(),
            key=lambda item: (
                item["priority_score"],
                item["punctual_mw"],
                item["terna_mw"],
                item["punctual_count"],
            ),
            reverse=True,
        )

    def _safe_ratio(self, value: float, max_value: float) -> float:
        if not max_value:
            return 0.0

        return value / max_value

    def _build_top_projects(self, records: list[dict[str, Any]]) -> list[dict[str, Any]]:
        records_with_power = [
            record for record in records if record["power_mw"] is not None
        ]

        records_with_power.sort(
            key=lambda record: record["power_mw"] or 0,
            reverse=True,
        )

        return [
            {
                "title": record["title"],
                "region": record["region"],
                "province": record["province"],
                "municipalities": record["municipalities"],
                "source": record["source"],
                "source_label": record["source_label"],
                "project_type": record["project_type"],
                "power_mw": round(record["power_mw"] or 0, 3),
                "status": record["status"],
                "url": record["url"],
                "province_deduced": record.get("province_deduced", False),
                "municipalities_deduced": record.get("municipalities_deduced", False),
            }
            for record in records_with_power[:20]
        ]

    def _build_terna_summary(self, records: list[dict[str, Any]]) -> dict[str, Any]:
        by_status: dict[str, dict[str, Any]] = {}

        for record in records:
            status = record["status"] or "ND"

            if status not in by_status:
                by_status[status] = {
                    "status": status,
                    "mw": 0.0,
                    "count": 0,
                    "practices": 0,
                }

            by_status[status]["mw"] += record["power_mw"] or 0
            by_status[status]["count"] += 1
            by_status[status]["practices"] += record["numero_pratiche"] or 0

        status_rows = list(by_status.values())

        for item in status_rows:
            item["mw"] = round(item["mw"], 3)

        status_rows.sort(key=lambda item: item["mw"], reverse=True)

        return {
            "status_rows": status_rows,
        }

    def _build_quality_summary(self, records: list[dict[str, Any]]) -> dict[str, Any]:
        punctual_records = [record for record in records if record["is_punctual"]]

        missing_mw = sum(1 for record in punctual_records if record["power_mw"] is None)
        missing_region = sum(1 for record in punctual_records if record["region"] in {"", "ND"})
        missing_province = sum(1 for record in punctual_records if not record["province"])
        missing_municipality = sum(1 for record in punctual_records if not record["municipalities"])
        missing_url = sum(1 for record in records if not record["url"])
        province_deduced = sum(1 for record in punctual_records if record.get("province_deduced"))
        municipalities_deduced = sum(1 for record in punctual_records if record.get("municipalities_deduced"))

        return {
            "punctual_records": len(punctual_records),
            "missing_mw": missing_mw,
            "missing_region": missing_region,
            "missing_province": missing_province,
            "missing_municipality": missing_municipality,
            "missing_url": missing_url,
            "province_deduced": province_deduced,
            "municipalities_deduced": municipalities_deduced,
        }

    def _build_quality_by_source(self, records: list[dict[str, Any]]) -> list[dict[str, Any]]:
        grouped: dict[str, dict[str, Any]] = {}

        for record in records:
            source = record["source"]

            if source not in grouped:
                grouped[source] = {
                    "source": source,
                    "source_label": record["source_label"],
                    "count": 0,
                    "missing_mw": 0,
                    "missing_province": 0,
                    "missing_municipality": 0,
                    "province_deduced": 0,
                    "municipalities_deduced": 0,
                }

            item = grouped[source]
            item["count"] += 1

            if record["power_mw"] is None:
                item["missing_mw"] += 1

            if not record["province"] and record["is_punctual"]:
                item["missing_province"] += 1

            if not record["municipalities"] and record["is_punctual"]:
                item["missing_municipality"] += 1

            if record.get("province_deduced"):
                item["province_deduced"] += 1

            if record.get("municipalities_deduced"):
                item["municipalities_deduced"] += 1

        rows = list(grouped.values())

        for item in rows:
            denominator = item["count"] or 1
            item["completeness_pct"] = round(
                100
                - (
                    (
                        item["missing_mw"]
                        + item["missing_province"]
                        + item["missing_municipality"]
                    )
                    / (denominator * 3)
                    * 100
                ),
                1,
            )

        rows.sort(key=lambda item: item["count"], reverse=True)
        return rows

    # ------------------------------------------------------------------
    # HTML
    # ------------------------------------------------------------------

    def _render_html(
        self,
        summary: dict[str, Any],
        records: list[dict[str, Any]],
    ) -> str:
        payload = {
            "summary": summary,
            "records": records,
        }

        data_json = json.dumps(payload, ensure_ascii=False)
        data_json = data_json.replace("</", "<\\/")

        title = "PV Agent Dashboard"

        return f"""<!doctype html>
<html lang="it">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>{html.escape(title)}</title>
  <script src="https://cdn.jsdelivr.net/npm/chart.js"></script>
  <style>
    :root {{
      --bg: #f4f6f8;
      --card: #ffffff;
      --text: #16202a;
      --muted: #6b7280;
      --border: #e5e7eb;
      --accent: #0f766e;
      --accent-dark: #115e59;
      --warning: #f59e0b;
      --danger: #dc2626;
      --shadow: 0 12px 30px rgba(15, 23, 42, 0.08);
      --radius: 18px;
    }}

    * {{
      box-sizing: border-box;
    }}

    body {{
      margin: 0;
      font-family: Arial, Helvetica, sans-serif;
      background: var(--bg);
      color: var(--text);
    }}

    header {{
      padding: 28px 34px;
      background: linear-gradient(135deg, #0f172a, #134e4a);
      color: white;
    }}

    header h1 {{
      margin: 0;
      font-size: 30px;
      letter-spacing: -0.03em;
    }}

    header p {{
      margin: 8px 0 0;
      color: rgba(255,255,255,0.78);
      font-size: 14px;
    }}

    main {{
      padding: 26px 34px 50px;
      max-width: 1800px;
      margin: 0 auto;
    }}

    .section-title {{
      margin: 32px 0 14px;
      font-size: 20px;
      letter-spacing: -0.02em;
    }}

    .grid {{
      display: grid;
      gap: 18px;
    }}

    .kpi-grid {{
      grid-template-columns: repeat(6, minmax(160px, 1fr));
    }}

    .chart-grid {{
      grid-template-columns: repeat(2, minmax(360px, 1fr));
    }}

    .card {{
      background: var(--card);
      border: 1px solid var(--border);
      border-radius: var(--radius);
      box-shadow: var(--shadow);
      padding: 18px;
    }}

    .kpi .label {{
      color: var(--muted);
      font-size: 12px;
      text-transform: uppercase;
      letter-spacing: 0.08em;
    }}

    .kpi .value {{
      margin-top: 8px;
      font-size: 28px;
      font-weight: 800;
      letter-spacing: -0.04em;
    }}

    .kpi .note {{
      margin-top: 5px;
      color: var(--muted);
      font-size: 12px;
    }}

    .split {{
      display: grid;
      grid-template-columns: 1.2fr 0.8fr;
      gap: 18px;
    }}

    .filters {{
      display: grid;
      grid-template-columns: 1.2fr repeat(4, minmax(160px, 1fr));
      gap: 12px;
      margin-bottom: 14px;
    }}

    input, select {{
      width: 100%;
      border: 1px solid var(--border);
      border-radius: 12px;
      padding: 10px 12px;
      font-size: 14px;
      background: white;
    }}

    table {{
      width: 100%;
      border-collapse: collapse;
      font-size: 13px;
    }}

    th {{
      text-align: left;
      background: #f8fafc;
      color: #334155;
      border-bottom: 1px solid var(--border);
      padding: 10px;
      position: sticky;
      top: 0;
      z-index: 1;
    }}

    td {{
      border-bottom: 1px solid var(--border);
      padding: 9px 10px;
      vertical-align: top;
    }}

    tr:hover td {{
      background: #f8fafc;
    }}

    .table-wrap {{
      overflow: auto;
      max-height: 680px;
      border: 1px solid var(--border);
      border-radius: 14px;
    }}

    .badge {{
      display: inline-block;
      padding: 4px 8px;
      border-radius: 999px;
      font-size: 12px;
      background: #e2e8f0;
      color: #0f172a;
      white-space: nowrap;
    }}

    .badge.terna {{
      background: #ccfbf1;
      color: #115e59;
    }}

    .badge.mase {{
      background: #dbeafe;
      color: #1e40af;
    }}

    .deduced {{
      display: inline-block;
      margin-left: 5px;
      padding: 2px 6px;
      border-radius: 999px;
      background: #fef3c7;
      color: #92400e;
      font-size: 11px;
      font-weight: 600;
      white-space: nowrap;
    }}

    .muted {{
      color: var(--muted);
    }}

    .small {{
      font-size: 12px;
    }}

    a {{
      color: var(--accent-dark);
      text-decoration: none;
      font-weight: 600;
    }}

    a:hover {{
      text-decoration: underline;
    }}

    .scroll-small {{
      max-height: 380px;
      overflow: auto;
    }}

    .warning-box {{
      border-left: 5px solid var(--warning);
      background: #fffbeb;
      padding: 14px 16px;
      border-radius: 12px;
      color: #78350f;
      font-size: 14px;
    }}

    .footer-note {{
      margin-top: 28px;
      color: var(--muted);
      font-size: 12px;
    }}

    @media (max-width: 1300px) {{
      .kpi-grid {{
        grid-template-columns: repeat(3, 1fr);
      }}
      .chart-grid,
      .split {{
        grid-template-columns: 1fr;
      }}
      .filters {{
        grid-template-columns: 1fr 1fr;
      }}
    }}

    @media (max-width: 700px) {{
      main {{
        padding: 18px;
      }}
      header {{
        padding: 22px;
      }}
      .kpi-grid {{
        grid-template-columns: 1fr;
      }}
      .filters {{
        grid-template-columns: 1fr;
      }}
    }}
  </style>
</head>
<body>
  <header>
    <h1>PV Agent Dashboard</h1>
    <p>
      Snapshot: <strong id="snapshotFile"></strong> · Generata: <strong id="generatedAt"></strong>
    </p>
  </header>

  <main>
    <section>
      <h2 class="section-title">Executive overview</h2>
      <div class="grid kpi-grid">
        <div class="card kpi">
          <div class="label">Record totali</div>
          <div class="value" id="kpiTotalRecords">-</div>
          <div class="note">Tutte le fonti</div>
        </div>
        <div class="card kpi">
          <div class="label">Progetti puntuali</div>
          <div class="value" id="kpiPunctualRecords">-</div>
          <div class="note">MASE + Regioni</div>
        </div>
        <div class="card kpi">
          <div class="label">Record Terna</div>
          <div class="value" id="kpiTernaRecords">-</div>
          <div class="note">Dato aggregato</div>
        </div>
        <div class="card kpi">
          <div class="label">MW puntuali</div>
          <div class="value" id="kpiPunctualMw">-</div>
          <div class="note">Solo progetti con MW disponibili</div>
        </div>
        <div class="card kpi">
          <div class="label">MW Terna Solare</div>
          <div class="value" id="kpiTernaMw">-</div>
          <div class="note">Radar mercato elettrico</div>
        </div>
        <div class="card kpi">
          <div class="label">Fonti attive</div>
          <div class="value" id="kpiSources">-</div>
          <div class="note">Collector presenti nello snapshot</div>
        </div>
      </div>
    </section>

    <section>
      <h2 class="section-title">Priorità territoriale</h2>
      <div class="split">
        <div class="card">
          <canvas id="regionPriorityChart" height="130"></canvas>
        </div>
        <div class="card">
          <div class="warning-box">
            <strong>Nota:</strong> Terna Econnextion è un dato aggregato per regione/stato/fonte.
            Non rappresenta singoli progetti né proponenti. Va usato come radar di mercato,
            non come lista lead.
          </div>
          <div class="scroll-small" style="margin-top: 14px;">
            <table>
              <thead>
                <tr>
                  <th>Regione</th>
                  <th>Score</th>
                  <th>Progetti</th>
                  <th>MW puntuali</th>
                  <th>MW Terna</th>
                </tr>
              </thead>
              <tbody id="regionRankingBody"></tbody>
            </table>
          </div>
        </div>
      </div>
    </section>

    <section>
      <h2 class="section-title">Grafici principali</h2>
      <div class="grid chart-grid">
        <div class="card">
          <canvas id="punctualMwChart" height="135"></canvas>
        </div>
        <div class="card">
          <canvas id="ternaMwChart" height="135"></canvas>
        </div>
        <div class="card">
          <canvas id="sourceChart" height="135"></canvas>
        </div>
        <div class="card">
          <canvas id="ternaStatusChart" height="135"></canvas>
        </div>
      </div>
    </section>

    <section>
      <h2 class="section-title">Top progetti puntuali per MW</h2>
      <div class="card">
        <div class="table-wrap" style="max-height: 420px;">
          <table>
            <thead>
              <tr>
                <th>Progetto</th>
                <th>Regione</th>
                <th>Provincia</th>
                <th>Comune</th>
                <th>Fonte</th>
                <th>Tipo</th>
                <th>MW</th>
                <th>Stato</th>
                <th>Link</th>
              </tr>
            </thead>
            <tbody id="topProjectsBody"></tbody>
          </table>
        </div>
      </div>
    </section>

    <section>
      <h2 class="section-title">Pipeline completa</h2>
      <div class="card">
        <div class="filters">
          <input id="searchInput" placeholder="Cerca progetto, proponente, comune, stato...">
          <select id="sourceFilter"></select>
          <select id="regionFilter"></select>
          <select id="typeFilter"></select>
          <select id="dataKindFilter">
            <option value="">Tutti i dati</option>
            <option value="punctual">Solo progetti puntuali</option>
            <option value="terna">Solo Terna aggregato</option>
          </select>
        </div>

        <div class="small muted" style="margin-bottom: 10px;">
          Record visualizzati: <strong id="visibleCount">0</strong>
        </div>

        <div class="table-wrap">
          <table>
            <thead>
              <tr>
                <th>Fonte</th>
                <th>Regione</th>
                <th>Provincia</th>
                <th>Comune</th>
                <th>Progetto / aggregato</th>
                <th>Proponente</th>
                <th>Tipo</th>
                <th>MW</th>
                <th>Stato</th>
                <th>Link</th>
              </tr>
            </thead>
            <tbody id="pipelineBody"></tbody>
          </table>
        </div>
      </div>
    </section>

    <section>
      <h2 class="section-title">Qualità dati</h2>
      <div class="grid kpi-grid">
        <div class="card kpi">
          <div class="label">Puntuali senza MW</div>
          <div class="value" id="qualityMissingMw">-</div>
        </div>
        <div class="card kpi">
          <div class="label">Puntuali senza regione</div>
          <div class="value" id="qualityMissingRegion">-</div>
        </div>
        <div class="card kpi">
          <div class="label">Puntuali senza provincia</div>
          <div class="value" id="qualityMissingProvince">-</div>
        </div>
        <div class="card kpi">
          <div class="label">Puntuali senza comune</div>
          <div class="value" id="qualityMissingMunicipality">-</div>
        </div>
        <div class="card kpi">
          <div class="label">Province dedotte</div>
          <div class="value" id="qualityProvinceDeduced">-</div>
        </div>
        <div class="card kpi">
          <div class="label">Comuni dedotti</div>
          <div class="value" id="qualityMunicipalityDeduced">-</div>
        </div>
      </div>

      <div class="card" style="margin-top: 18px;">
        <h3 style="margin-top: 0;">Qualità dati per fonte</h3>
        <div class="table-wrap" style="max-height: 420px;">
          <table>
            <thead>
              <tr>
                <th>Fonte</th>
                <th>Record</th>
                <th>Senza MW</th>
                <th>Senza provincia</th>
                <th>Senza comune</th>
                <th>Province dedotte</th>
                <th>Comuni dedotti</th>
                <th>Completezza</th>
              </tr>
            </thead>
            <tbody id="qualityBySourceBody"></tbody>
          </table>
        </div>
      </div>
    </section>

    <div class="footer-note">
      Dashboard HTML statica generata automaticamente da PV Agent.
      I dati Terna Econnextion sono aggregati e non vanno sommati ai progetti puntuali come se fossero la stessa cosa.
      Le etichette “dedotto” indicano province/comuni ricavati automaticamente dal testo descrittivo.
    </div>
  </main>

  <script>
    const DASHBOARD_DATA = {data_json};

    const summary = DASHBOARD_DATA.summary;
    const records = DASHBOARD_DATA.records;

    const numberFmt = new Intl.NumberFormat('it-IT', {{
      maximumFractionDigits: 0
    }});

    const mwFmt = new Intl.NumberFormat('it-IT', {{
      maximumFractionDigits: 1
    }});

    function fmtNum(value) {{
      return numberFmt.format(value || 0);
    }}

    function fmtMw(value) {{
      if (value === null || value === undefined || Number.isNaN(value)) return '';
      return mwFmt.format(value) + ' MW';
    }}

    function text(value) {{
      return value === null || value === undefined ? '' : String(value);
    }}

    function setText(id, value) {{
      document.getElementById(id).textContent = value;
    }}

    function badgeClass(source) {{
      if (source === 'terna_econnextion') return 'badge terna';
      if (source === 'mase') return 'badge mase';
      return 'badge';
    }}

    function deducedBadge(condition) {{
      return condition ? '<span class="deduced">dedotto</span>' : '';
    }}

    function topN(items, key, n = 15) {{
      return [...items]
        .sort((a, b) => (b[key] || 0) - (a[key] || 0))
        .slice(0, n);
    }}

    function initKpis() {{
      setText('snapshotFile', summary.snapshot_file);
      setText('generatedAt', summary.generated_at);
      setText('kpiTotalRecords', fmtNum(summary.total_records));
      setText('kpiPunctualRecords', fmtNum(summary.punctual_records));
      setText('kpiTernaRecords', fmtNum(summary.terna_records));
      setText('kpiPunctualMw', fmtMw(summary.total_mw_punctual));
      setText('kpiTernaMw', fmtMw(summary.total_mw_terna));
      setText('kpiSources', fmtNum(summary.source_counts.length));

      const q = summary.quality;
      setText('qualityMissingMw', fmtNum(q.missing_mw));
      setText('qualityMissingRegion', fmtNum(q.missing_region));
      setText('qualityMissingProvince', fmtNum(q.missing_province));
      setText('qualityMissingMunicipality', fmtNum(q.missing_municipality));
      setText('qualityProvinceDeduced', fmtNum(q.province_deduced));
      setText('qualityMunicipalityDeduced', fmtNum(q.municipalities_deduced));
    }}

    function initRegionRanking() {{
      const body = document.getElementById('regionRankingBody');
      body.innerHTML = '';

      summary.regions.slice(0, 20).forEach(row => {{
        const tr = document.createElement('tr');

        tr.innerHTML = `
          <td><strong>${{text(row.region)}}</strong></td>
          <td>${{fmtNum(row.priority_score)}}</td>
          <td>${{fmtNum(row.punctual_count)}}</td>
          <td>${{fmtMw(row.punctual_mw)}}</td>
          <td>${{fmtMw(row.terna_mw)}}</td>
        `;

        body.appendChild(tr);
      }});
    }}

    function initTopProjects() {{
      const body = document.getElementById('topProjectsBody');
      body.innerHTML = '';

      summary.top_projects.forEach(row => {{
        const tr = document.createElement('tr');
        const link = row.url ? `<a href="${{row.url}}" target="_blank">Apri</a>` : '';

        tr.innerHTML = `
          <td><strong>${{text(row.title)}}</strong></td>
          <td>${{text(row.region)}}</td>
          <td>${{text(row.province)}} ${{deducedBadge(row.province_deduced)}}</td>
          <td>${{text(row.municipalities)}} ${{deducedBadge(row.municipalities_deduced)}}</td>
          <td><span class="${{badgeClass(row.source)}}">${{text(row.source_label)}}</span></td>
          <td>${{text(row.project_type)}}</td>
          <td>${{fmtMw(row.power_mw)}}</td>
          <td>${{text(row.status)}}</td>
          <td>${{link}}</td>
        `;

        body.appendChild(tr);
      }});
    }}

    function initQualityBySource() {{
      const body = document.getElementById('qualityBySourceBody');
      body.innerHTML = '';

      summary.quality_by_source.forEach(row => {{
        const tr = document.createElement('tr');

        tr.innerHTML = `
          <td><span class="${{badgeClass(row.source)}}">${{text(row.source_label)}}</span></td>
          <td>${{fmtNum(row.count)}}</td>
          <td>${{fmtNum(row.missing_mw)}}</td>
          <td>${{fmtNum(row.missing_province)}}</td>
          <td>${{fmtNum(row.missing_municipality)}}</td>
          <td>${{fmtNum(row.province_deduced)}}</td>
          <td>${{fmtNum(row.municipalities_deduced)}}</td>
          <td>${{text(row.completeness_pct)}}%</td>
        `;

        body.appendChild(tr);
      }});
    }}

    function uniqueValues(key) {{
      return [...new Set(records.map(r => r[key]).filter(Boolean))]
        .sort((a, b) => String(a).localeCompare(String(b), 'it'));
    }}

    function fillSelect(id, label, values, mapper = v => v) {{
      const select = document.getElementById(id);
      select.innerHTML = `<option value="">${{label}}</option>`;

      values.forEach(value => {{
        const option = document.createElement('option');
        option.value = value;
        option.textContent = mapper(value);
        select.appendChild(option);
      }});
    }}

    function initFilters() {{
      fillSelect(
        'sourceFilter',
        'Tutte le fonti',
        uniqueValues('source'),
        value => {{
          const found = records.find(r => r.source === value);
          return found ? found.source_label : value;
        }}
      );

      fillSelect('regionFilter', 'Tutte le regioni', uniqueValues('region'));
      fillSelect('typeFilter', 'Tutti i tipi', uniqueValues('project_type'));
    }}

    function recordMatchesFilters(record) {{
      const query = document.getElementById('searchInput').value.trim().toLowerCase();
      const source = document.getElementById('sourceFilter').value;
      const region = document.getElementById('regionFilter').value;
      const type = document.getElementById('typeFilter').value;
      const dataKind = document.getElementById('dataKindFilter').value;

      if (source && record.source !== source) return false;
      if (region && record.region !== region) return false;
      if (type && record.project_type !== type) return false;
      if (dataKind === 'punctual' && !record.is_punctual) return false;
      if (dataKind === 'terna' && !record.is_terna) return false;

      if (query) {{
        const haystack = [
          record.title,
          record.proponent,
          record.region,
          record.province,
          record.municipalities,
          record.project_type,
          record.status,
          record.source_label
        ].join(' ').toLowerCase();

        if (!haystack.includes(query)) return false;
      }}

      return true;
    }}

    function renderPipeline() {{
      const body = document.getElementById('pipelineBody');
      body.innerHTML = '';

      const filtered = records.filter(recordMatchesFilters);
      setText('visibleCount', fmtNum(filtered.length));

      filtered.slice(0, 600).forEach(record => {{
        const tr = document.createElement('tr');
        const link = record.url ? `<a href="${{record.url}}" target="_blank">Apri</a>` : '';
        const proponent = record.is_terna ? '<span class="muted">Dato aggregato</span>' : text(record.proponent);

        tr.innerHTML = `
          <td><span class="${{badgeClass(record.source)}}">${{text(record.source_label)}}</span></td>
          <td>${{text(record.region)}}</td>
          <td>${{text(record.province)}} ${{deducedBadge(record.province_deduced)}}</td>
          <td>${{text(record.municipalities)}} ${{deducedBadge(record.municipalities_deduced)}}</td>
          <td><strong>${{text(record.title)}}</strong></td>
          <td>${{proponent}}</td>
          <td>${{text(record.project_type)}}</td>
          <td>${{fmtMw(record.power_mw)}}</td>
          <td>${{text(record.status)}}</td>
          <td>${{link}}</td>
        `;

        body.appendChild(tr);
      }});
    }}

    function chartOptions(title) {{
      return {{
        responsive: true,
        plugins: {{
          legend: {{
            display: false
          }},
          title: {{
            display: true,
            text: title,
            font: {{
              size: 15,
              weight: 'bold'
            }}
          }}
        }},
        scales: {{
          y: {{
            beginAtZero: true
          }}
        }}
      }};
    }}

    function initCharts() {{
      const topPriority = topN(summary.regions, 'priority_score', 12);
      new Chart(document.getElementById('regionPriorityChart'), {{
        type: 'bar',
        data: {{
          labels: topPriority.map(x => x.region),
          datasets: [{{
            label: 'Score priorità',
            data: topPriority.map(x => x.priority_score)
          }}]
        }},
        options: chartOptions('Indice priorità commerciale per regione')
      }});

      const topPunctualMw = topN(summary.regions, 'punctual_mw', 12);
      new Chart(document.getElementById('punctualMwChart'), {{
        type: 'bar',
        data: {{
          labels: topPunctualMw.map(x => x.region),
          datasets: [{{
            label: 'MW puntuali',
            data: topPunctualMw.map(x => x.punctual_mw)
          }}]
        }},
        options: chartOptions('MW progetti puntuali per regione')
      }});

      const topTernaMw = topN(summary.regions, 'terna_mw', 12);
      new Chart(document.getElementById('ternaMwChart'), {{
        type: 'bar',
        data: {{
          labels: topTernaMw.map(x => x.region),
          datasets: [{{
            label: 'MW Terna Solare',
            data: topTernaMw.map(x => x.terna_mw)
          }}]
        }},
        options: chartOptions('MW Terna Econnextion Solare per regione')
      }});

      new Chart(document.getElementById('sourceChart'), {{
        type: 'doughnut',
        data: {{
          labels: summary.source_counts.map(x => x.label),
          datasets: [{{
            label: 'Record',
            data: summary.source_counts.map(x => x.count)
          }}]
        }},
        options: {{
          responsive: true,
          plugins: {{
            title: {{
              display: true,
              text: 'Distribuzione record per fonte',
              font: {{
                size: 15,
                weight: 'bold'
              }}
            }}
          }}
        }}
      }});

      new Chart(document.getElementById('ternaStatusChart'), {{
        type: 'bar',
        data: {{
          labels: summary.terna_summary.status_rows.map(x => x.status),
          datasets: [{{
            label: 'MW Terna',
            data: summary.terna_summary.status_rows.map(x => x.mw)
          }}]
        }},
        options: chartOptions('Terna Solare per stato connessione')
      }});
    }}

    function attachEvents() {{
      ['searchInput', 'sourceFilter', 'regionFilter', 'typeFilter', 'dataKindFilter'].forEach(id => {{
        document.getElementById(id).addEventListener('input', renderPipeline);
        document.getElementById(id).addEventListener('change', renderPipeline);
      }});
    }}

    initKpis();
    initRegionRanking();
    initTopProjects();
    initQualityBySource();
    initFilters();
    renderPipeline();
    initCharts();
    attachEvents();
  </script>
</body>
</html>
"""


def main() -> None:
    path = StaticDashboardBuilder().build()
    print(f"Dashboard generata: {path}")


if __name__ == "__main__":
    main()