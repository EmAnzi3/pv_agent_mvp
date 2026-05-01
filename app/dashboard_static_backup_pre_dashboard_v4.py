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
    "sistema_puglia_energia": "Sistema Puglia – Energia",
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
                "proponent": record["proponent"],
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

        template = """<!doctype html>
<html lang="it">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>PV Agent Dashboard</title>
  <script src="https://cdn.jsdelivr.net/npm/chart.js"></script>
  <style>
    :root {
      --bg: #f3f5f7;
      --card: #ffffff;
      --text: #16202a;
      --muted: #64748b;
      --border: #e5e7eb;
      --accent: #0f766e;
      --accent-dark: #115e59;
      --blue: #1d4ed8;
      --warning: #f59e0b;
      --danger: #dc2626;
      --ok: #16a34a;
      --soft-ok: #dcfce7;
      --soft-blue: #dbeafe;
      --soft-warn: #fef3c7;
      --shadow: 0 14px 34px rgba(15, 23, 42, 0.08);
      --radius: 18px;
    }

    * { box-sizing: border-box; }

    body {
      margin: 0;
      font-family: Arial, Helvetica, sans-serif;
      background: var(--bg);
      color: var(--text);
    }

    header {
      padding: 30px 36px;
      background: linear-gradient(135deg, #0f172a, #134e4a);
      color: white;
    }

    header h1 {
      margin: 0;
      font-size: 32px;
      letter-spacing: -0.04em;
    }

    header p {
      margin: 8px 0 0;
      color: rgba(255,255,255,0.78);
      font-size: 14px;
    }

    main {
      padding: 26px 34px 50px;
      max-width: 1900px;
      margin: 0 auto;
    }

    .section-title {
      margin: 34px 0 14px;
      font-size: 21px;
      letter-spacing: -0.03em;
    }

    .section-subtitle {
      margin: -6px 0 14px;
      color: var(--muted);
      font-size: 13px;
    }

    .grid { display: grid; gap: 18px; }

    .kpi-grid { grid-template-columns: repeat(6, minmax(160px, 1fr)); }
    .quality-grid { grid-template-columns: repeat(5, minmax(170px, 1fr)); }
    .chart-grid { grid-template-columns: repeat(2, minmax(360px, 1fr)); }
    .split { display: grid; grid-template-columns: 1.2fr 0.8fr; gap: 18px; }

    .card {
      background: var(--card);
      border: 1px solid var(--border);
      border-radius: var(--radius);
      box-shadow: var(--shadow);
      padding: 18px;
    }

    .kpi .label {
      color: var(--muted);
      font-size: 12px;
      text-transform: uppercase;
      letter-spacing: 0.08em;
    }

    .kpi .value {
      margin-top: 8px;
      font-size: 29px;
      font-weight: 800;
      letter-spacing: -0.045em;
    }

    .kpi .note {
      margin-top: 5px;
      color: var(--muted);
      font-size: 12px;
      line-height: 1.35;
    }

    .filters {
      display: grid;
      grid-template-columns: 1.4fr repeat(6, minmax(145px, 1fr));
      gap: 12px;
      margin-bottom: 14px;
    }

    input, select {
      width: 100%;
      border: 1px solid var(--border);
      border-radius: 12px;
      padding: 10px 12px;
      font-size: 14px;
      background: white;
    }

    table {
      width: 100%;
      border-collapse: collapse;
      font-size: 13px;
    }

    th {
      text-align: left;
      background: #f8fafc;
      color: #334155;
      border-bottom: 1px solid var(--border);
      padding: 10px;
      position: sticky;
      top: 0;
      z-index: 1;
    }

    td {
      border-bottom: 1px solid var(--border);
      padding: 9px 10px;
      vertical-align: top;
    }

    tr:hover td { background: #f8fafc; }

    .table-wrap {
      overflow: auto;
      max-height: 700px;
      border: 1px solid var(--border);
      border-radius: 14px;
    }

    .top-table { max-height: 520px; }

    .badge {
      display: inline-block;
      padding: 4px 8px;
      border-radius: 999px;
      font-size: 12px;
      background: #e2e8f0;
      color: #0f172a;
      white-space: nowrap;
    }

    .badge.terna { background: #ccfbf1; color: #115e59; }
    .badge.mase { background: #dbeafe; color: #1e40af; }
    .badge.quality-ok { background: var(--soft-ok); color: #166534; }
    .badge.warn { background: var(--soft-warn); color: #92400e; }

    .deduced {
      display: inline-block;
      margin-left: 5px;
      padding: 2px 6px;
      border-radius: 999px;
      background: #fef3c7;
      color: #92400e;
      font-size: 11px;
      font-weight: 600;
      white-space: nowrap;
    }

    .muted { color: var(--muted); }
    .small { font-size: 12px; }
    .nowrap { white-space: nowrap; }
    .num { text-align: right; white-space: nowrap; }
    .project-title { min-width: 360px; }
    .proponent-col { min-width: 180px; }

    a {
      color: var(--accent-dark);
      text-decoration: none;
      font-weight: 700;
    }

    a:hover { text-decoration: underline; }

    .scroll-small {
      max-height: 385px;
      overflow: auto;
    }

    .warning-box {
      border-left: 5px solid var(--warning);
      background: #fffbeb;
      padding: 14px 16px;
      border-radius: 12px;
      color: #78350f;
      font-size: 14px;
      line-height: 1.45;
    }

    .quality-box {
      border-left: 5px solid var(--ok);
      background: #f0fdf4;
      padding: 14px 16px;
      border-radius: 12px;
      color: #14532d;
      font-size: 14px;
      line-height: 1.45;
    }

    .mini-list {
      margin: 10px 0 0;
      padding-left: 18px;
      line-height: 1.6;
    }

    .footer-note {
      margin-top: 28px;
      color: var(--muted);
      font-size: 12px;
    }

    @media (max-width: 1400px) {
      .kpi-grid { grid-template-columns: repeat(3, 1fr); }
      .quality-grid { grid-template-columns: repeat(2, 1fr); }
      .chart-grid, .split { grid-template-columns: 1fr; }
      .filters { grid-template-columns: 1fr 1fr; }
    }

    @media (max-width: 740px) {
      main { padding: 18px; }
      header { padding: 22px; }
      .kpi-grid, .quality-grid { grid-template-columns: 1fr; }
      .filters { grid-template-columns: 1fr; }
    }
  </style>
</head>
<body>
  <header>
    <h1>PV Agent Dashboard</h1>
    <p>
      Pipeline nazionale fotovoltaico · Snapshot: <strong id="snapshotFile"></strong> · Generata: <strong id="generatedAt"></strong>
    </p>
  </header>

  <main>
    <section>
      <h2 class="section-title">Executive overview</h2>
      <p class="section-subtitle">Separazione netta tra pipeline operativa e radar Terna. I due numeri non vanno sommati come se fossero la stessa cosa.</p>
      <div class="grid kpi-grid">
        <div class="card kpi">
          <div class="label">Progetti operativi</div>
          <div class="value" id="kpiOperationalRecords">-</div>
          <div class="note">Solo record puntuali, escluso Terna</div>
        </div>
        <div class="card kpi">
          <div class="label">MW pipeline operativa</div>
          <div class="value" id="kpiOperationalMw">-</div>
          <div class="note">Somma MW dei progetti puntuali</div>
        </div>
        <div class="card kpi">
          <div class="label">Con proponente</div>
          <div class="value" id="kpiWithProponent">-</div>
          <div class="note">Lead commercialmente più lavorabili</div>
        </div>
        <div class="card kpi">
          <div class="label">Radar Terna</div>
          <div class="value" id="kpiTernaMw">-</div>
          <div class="note">Dato aggregato, non lead puntuali</div>
        </div>
        <div class="card kpi">
          <div class="label">Fonti attive</div>
          <div class="value" id="kpiSources">-</div>
          <div class="note">Collector presenti nello snapshot</div>
        </div>
        <div class="card kpi">
          <div class="label">Ultimo aggiornamento</div>
          <div class="value" id="kpiGeneratedShort">-</div>
          <div class="note">Snapshot dati più recente</div>
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
            <strong>Nota operativa:</strong> la priorità è costruita sui progetti puntuali. Terna Econnextion resta un radar macro:
            utile per capire pressione di mercato e concentrazione geografica, non per contattare proponenti.
          </div>
          <div class="scroll-small" style="margin-top: 14px;">
            <table>
              <thead>
                <tr>
                  <th>Regione</th>
                  <th class="num">Score</th>
                  <th class="num">Progetti</th>
                  <th class="num">MW puntuali</th>
                  <th class="num">MW Terna</th>
                </tr>
              </thead>
              <tbody id="regionRankingBody"></tbody>
            </table>
          </div>
        </div>
      </div>
    </section>

    <section>
      <h2 class="section-title">Letture operative</h2>
      <div class="grid chart-grid">
        <div class="card">
          <canvas id="operationalCountChart" height="135"></canvas>
        </div>
        <div class="card">
          <canvas id="ternaMwChart" height="135"></canvas>
        </div>
        <div class="card">
          <canvas id="sourceChart" height="135"></canvas>
        </div>
        <div class="card">
          <canvas id="statusChart" height="135"></canvas>
        </div>
      </div>
    </section>

    <section>
      <h2 class="section-title">Top progetti operativi per MW</h2>
      <p class="section-subtitle">Vista pensata per scouting: progetto, proponente, localizzazione, fonte, stato e link.</p>
      <div class="card">
        <div class="table-wrap top-table">
          <table>
            <thead>
              <tr>
                <th>Progetto</th>
                <th>Proponente</th>
                <th>Regione</th>
                <th>Provincia</th>
                <th>Comune/i</th>
                <th>Fonte</th>
                <th>Tipo</th>
                <th class="num">MW</th>
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
          <select id="dataKindFilter">
            <option value="">Tutti i dati</option>
            <option value="punctual">Solo pipeline operativa</option>
            <option value="terna">Solo radar Terna</option>
          </select>
          <select id="sourceFilter"></select>
          <select id="regionFilter"></select>
          <select id="provinceFilter"></select>
          <select id="statusFilter"></select>
          <select id="sortFilter">
            <option value="mw_desc">Ordina: MW decrescente</option>
            <option value="mw_asc">Ordina: MW crescente</option>
            <option value="title_asc">Ordina: progetto A-Z</option>
            <option value="proponent_asc">Ordina: proponente A-Z</option>
            <option value="region_asc">Ordina: regione A-Z</option>
            <option value="source_asc">Ordina: fonte A-Z</option>
          </select>
        </div>

        <div class="small muted" style="margin-bottom: 10px;">
          Record visualizzati: <strong id="visibleCount">0</strong> · limite tabella: 700 righe
        </div>

        <div class="table-wrap">
          <table>
            <thead>
              <tr>
                <th>Fonte</th>
                <th>Regione</th>
                <th>Provincia</th>
                <th>Comune/i</th>
                <th>Progetto / aggregato</th>
                <th>Proponente</th>
                <th>Tipo</th>
                <th class="num">MW</th>
                <th>Stato</th>
                <th>Link</th>
              </tr>
            </thead>
            <tbody id="pipelineBody"></tbody>
          </table>
        </div>
      </div>
    </section>

    <div class="footer-note">
      Dashboard HTML statica generata automaticamente da PV Agent.
      Terna Econnextion è un dato aggregato e non deve essere sommato ai progetti puntuali come se fosse una lista di impianti.
      I controlli di qualità restano nei report tecnici, non nella vista condivisa.
    </div>
  </main>

  <script>
    const DASHBOARD_DATA = __DASHBOARD_DATA__;

    const summary = DASHBOARD_DATA.summary;
    const records = DASHBOARD_DATA.records;
    const dataQuality = DASHBOARD_DATA.data_quality || {};

    const numberFmt = new Intl.NumberFormat('it-IT', { maximumFractionDigits: 0 });
    const mwFmt = new Intl.NumberFormat('it-IT', { maximumFractionDigits: 3 });
    const pctFmt = new Intl.NumberFormat('it-IT', { maximumFractionDigits: 0 });

    function toNumber(value) {
      const n = Number(value);
      return Number.isFinite(n) ? n : 0;
    }

    function fmtNum(value) {
      return numberFmt.format(toNumber(value));
    }

    function fmtMw(value) {
      if (value === null || value === undefined || value === '') return '';
      return mwFmt.format(toNumber(value)) + ' MW';
    }

    function text(value) {
      return value === null || value === undefined ? '' : String(value);
    }

    function setText(id, value) {
      const node = document.getElementById(id);
      if (node) node.textContent = value;
    }

    function isTerna(record) {
      return record.is_terna || record.source === 'terna_econnextion';
    }

    function operationalRecords() {
      return records.filter(r => !isTerna(r));
    }

    function ternaRecords() {
      return records.filter(r => isTerna(r));
    }

    function badgeClass(source) {
      if (source === 'terna_econnextion') return 'badge terna';
      if (source === 'mase' || source === 'mase_provvedimenti') return 'badge mase';
      return 'badge';
    }

    function deducedBadge(condition) {
      return condition ? '<span class="deduced">dedotto</span>' : '';
    }

    function topN(items, key, n = 15) {
      return [...items]
        .sort((a, b) => (b[key] || 0) - (a[key] || 0))
        .slice(0, n);
    }

    function sourceLabel(source) {
      const found = records.find(r => r.source === source);
      return found ? found.source_label : source;
    }

    function countDuplicateProjectKeys() {
      const seen = new Set();
      let dup = 0;
      records.forEach(r => {
        if (!r.project_key) return;
        if (seen.has(r.project_key)) dup += 1;
        seen.add(r.project_key);
      });
      return dup;
    }

    function countSource(source) {
      return records.filter(r => r.source === source).length;
    }

    function initKpis() {
      const operational = operationalRecords();
      const terna = ternaRecords();
      const withProponent = operational.filter(r => text(r.proponent).trim()).length;
      const missingProponent = operational.length - withProponent;
      setText('snapshotFile', summary.snapshot_file || '');
      setText('generatedAt', summary.generated_at || '');

      setText('kpiOperationalRecords', fmtNum(operational.length || summary.punctual_records));
      setText('kpiOperationalMw', fmtMw(summary.total_mw_punctual));
      setText('kpiWithProponent', fmtNum(withProponent));
      setText('kpiTernaMw', fmtMw(summary.total_mw_terna));
      setText('kpiSources', fmtNum((summary.source_counts || []).length));
      setText('kpiGeneratedShort', (summary.generated_at || '').slice(0, 10));
    }

    function initRegionRanking() {
      const body = document.getElementById('regionRankingBody');
      body.innerHTML = '';

      (summary.regions || []).slice(0, 20).forEach(row => {
        const tr = document.createElement('tr');

        tr.innerHTML = `
          <td><strong>${text(row.region)}</strong></td>
          <td class="num">${fmtNum(row.priority_score)}</td>
          <td class="num">${fmtNum(row.punctual_count)}</td>
          <td class="num">${fmtMw(row.punctual_mw)}</td>
          <td class="num">${fmtMw(row.terna_mw)}</td>
        `;

        body.appendChild(tr);
      });
    }

    function initTopProjects() {
      const body = document.getElementById('topProjectsBody');
      body.innerHTML = '';

      const topRows = (summary.top_projects && summary.top_projects.length)
        ? summary.top_projects
        : topN(operationalRecords().filter(r => r.power_mw), 'power_mw', 20);

      topRows.forEach(row => {
        const tr = document.createElement('tr');
        const link = row.url ? `<a href="${row.url}" target="_blank">Apri</a>` : '';
        const proponent = text(row.proponent) || '<span class="muted">n/d</span>';

        tr.innerHTML = `
          <td class="project-title"><strong>${text(row.title)}</strong></td>
          <td class="proponent-col">${proponent}</td>
          <td>${text(row.region)}</td>
          <td>${text(row.province)} ${deducedBadge(row.province_deduced)}</td>
          <td>${text(row.municipalities)} ${deducedBadge(row.municipalities_deduced)}</td>
          <td><span class="${badgeClass(row.source)}">${text(row.source_label || sourceLabel(row.source))}</span></td>
          <td>${text(row.project_type)}</td>
          <td class="num">${fmtMw(row.power_mw)}</td>
          <td>${text(row.status)}</td>
          <td>${link}</td>
        `;

        body.appendChild(tr);
      });
    }

    function initQualityBySource() {
      const body = document.getElementById('qualityBySourceBody');
      body.innerHTML = '';

      (summary.quality_by_source || []).forEach(row => {
        const tr = document.createElement('tr');

        tr.innerHTML = `
          <td><span class="${badgeClass(row.source)}">${text(row.source_label)}</span></td>
          <td class="num">${fmtNum(row.count)}</td>
          <td class="num">${fmtNum(row.missing_mw)}</td>
          <td class="num">${fmtNum(row.missing_province)}</td>
          <td class="num">${fmtNum(row.missing_municipality)}</td>
          <td class="num">${text(row.completeness_pct)}%</td>
        `;

        body.appendChild(tr);
      });
    }

    function uniqueValues(key, dataset = records) {
      return [...new Set(dataset.map(r => r[key]).filter(Boolean))]
        .sort((a, b) => String(a).localeCompare(String(b), 'it'));
    }

    function uniqueStatuses() {
      const values = records
        .map(r => r.status)
        .filter(Boolean)
        .map(v => String(v).trim())
        .filter(Boolean);
      return [...new Set(values)].sort((a, b) => a.localeCompare(b, 'it'));
    }

    function fillSelect(id, label, values, mapper = v => v) {
      const select = document.getElementById(id);
      select.innerHTML = `<option value="">${label}</option>`;

      values.forEach(value => {
        const option = document.createElement('option');
        option.value = value;
        option.textContent = mapper(value);
        select.appendChild(option);
      });
    }

    function initFilters() {
      fillSelect('sourceFilter', 'Tutte le fonti', uniqueValues('source'), value => sourceLabel(value));
      fillSelect('regionFilter', 'Tutte le regioni', uniqueValues('region'));
      fillSelect('provinceFilter', 'Tutte le province', uniqueValues('province'));
      fillSelect('statusFilter', 'Tutti gli stati', uniqueStatuses());
    }

    function recordMatchesFilters(record) {
      const query = document.getElementById('searchInput').value.trim().toLowerCase();
      const dataKind = document.getElementById('dataKindFilter').value;
      const source = document.getElementById('sourceFilter').value;
      const region = document.getElementById('regionFilter').value;
      const province = document.getElementById('provinceFilter').value;
      const status = document.getElementById('statusFilter').value;

      if (dataKind === 'punctual' && isTerna(record)) return false;
      if (dataKind === 'terna' && !isTerna(record)) return false;
      if (source && record.source !== source) return false;
      if (region && record.region !== region) return false;
      if (province && record.province !== province) return false;
      if (status && record.status !== status) return false;

      if (query) {
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
      }

      return true;
    }

    function sortRecords(items) {
      const sort = document.getElementById('sortFilter').value || 'mw_desc';
      const copy = [...items];

      const byText = key => (a, b) => text(a[key]).localeCompare(text(b[key]), 'it', { sensitivity: 'base' });
      const byMw = (a, b) => toNumber(a.power_mw) - toNumber(b.power_mw);

      switch (sort) {
        case 'mw_asc':
          return copy.sort(byMw);
        case 'title_asc':
          return copy.sort(byText('title'));
        case 'proponent_asc':
          return copy.sort(byText('proponent'));
        case 'region_asc':
          return copy.sort(byText('region'));
        case 'source_asc':
          return copy.sort((a, b) => text(a.source_label).localeCompare(text(b.source_label), 'it', { sensitivity: 'base' }));
        case 'mw_desc':
        default:
          return copy.sort((a, b) => byMw(b, a));
      }
    }

    function renderPipeline() {
      const body = document.getElementById('pipelineBody');
      body.innerHTML = '';

      const filtered = sortRecords(records.filter(recordMatchesFilters));
      setText('visibleCount', fmtNum(filtered.length));

      filtered.slice(0, 700).forEach(record => {
        const tr = document.createElement('tr');
        const link = record.url ? `<a href="${record.url}" target="_blank">Apri</a>` : '';
        const proponent = isTerna(record)
          ? '<span class="muted">Dato aggregato</span>'
          : (text(record.proponent) || '<span class="muted">n/d</span>');

        tr.innerHTML = `
          <td><span class="${badgeClass(record.source)}">${text(record.source_label)}</span></td>
          <td>${text(record.region)}</td>
          <td>${text(record.province)} ${deducedBadge(record.province_deduced)}</td>
          <td>${text(record.municipalities)} ${deducedBadge(record.municipalities_deduced)}</td>
          <td class="project-title"><strong>${text(record.title)}</strong></td>
          <td class="proponent-col">${proponent}</td>
          <td>${text(record.project_type)}</td>
          <td class="num">${fmtMw(record.power_mw)}</td>
          <td>${text(record.status)}</td>
          <td>${link}</td>
        `;

        body.appendChild(tr);
      });
    }

    function chartOptions(title, isMw = false) {
      return {
        responsive: true,
        plugins: {
          legend: { display: false },
          title: {
            display: true,
            text: title,
            font: { size: 15, weight: 'bold' }
          },
          tooltip: {
            callbacks: {
              label: context => isMw ? fmtMw(context.parsed.y) : fmtNum(context.parsed.y)
            }
          }
        },
        scales: {
          y: {
            beginAtZero: true,
            ticks: {
              callback: value => isMw ? fmtMw(value).replace(' MW', '') : fmtNum(value)
            }
          }
        }
      };
    }

    function topStatuses() {
      const counter = {};
      operationalRecords().forEach(r => {
        const key = r.status || 'n/d';
        counter[key] = (counter[key] || 0) + 1;
      });

      return Object.entries(counter)
        .map(([status, count]) => ({ status, count }))
        .sort((a, b) => b.count - a.count)
        .slice(0, 10);
    }

    function initCharts() {
      const topPriority = topN(summary.regions || [], 'priority_score', 12);
      new Chart(document.getElementById('regionPriorityChart'), {
        type: 'bar',
        data: {
          labels: topPriority.map(x => x.region),
          datasets: [{ label: 'Score priorità', data: topPriority.map(x => x.priority_score) }]
        },
        options: chartOptions('Indice priorità commerciale per regione')
      });

      const topOperationalCount = topN(summary.regions || [], 'punctual_count', 12);
      new Chart(document.getElementById('operationalCountChart'), {
        type: 'bar',
        data: {
          labels: topOperationalCount.map(x => x.region),
          datasets: [{ label: 'Progetti operativi', data: topOperationalCount.map(x => x.punctual_count) }]
        },
        options: chartOptions('Pipeline operativa: numero progetti per regione')
      });

      const topTernaMw = topN(summary.regions || [], 'terna_mw', 12);
      new Chart(document.getElementById('ternaMwChart'), {
        type: 'bar',
        data: {
          labels: topTernaMw.map(x => x.region),
          datasets: [{ label: 'MW Terna Solare', data: topTernaMw.map(x => x.terna_mw) }]
        },
        options: chartOptions('Radar Terna: MW solare per regione', true)
      });

      new Chart(document.getElementById('sourceChart'), {
        type: 'doughnut',
        data: {
          labels: (summary.source_counts || []).map(x => x.label),
          datasets: [{ label: 'Record', data: (summary.source_counts || []).map(x => x.count) }]
        },
        options: {
          responsive: true,
          plugins: {
            title: {
              display: true,
              text: 'Distribuzione record per fonte',
              font: { size: 15, weight: 'bold' }
            }
          }
        }
      });

      const statuses = topStatuses();
      new Chart(document.getElementById('statusChart'), {
        type: 'bar',
        data: {
          labels: statuses.map(x => x.status),
          datasets: [{ label: 'Record', data: statuses.map(x => x.count) }]
        },
        options: chartOptions('Pipeline operativa: record per stato')
      });
    }

    function attachEvents() {
      ['searchInput', 'dataKindFilter', 'sourceFilter', 'regionFilter', 'provinceFilter', 'statusFilter', 'sortFilter'].forEach(id => {
        document.getElementById(id).addEventListener('input', renderPipeline);
        document.getElementById(id).addEventListener('change', renderPipeline);
      });
    }

    initKpis();
    initRegionRanking();
    initTopProjects();
    initFilters();
    renderPipeline();
    initCharts();
    attachEvents();
  </script>
</body>
</html>
"""
        return template.replace("__DASHBOARD_DATA__", data_json)


def main() -> None:
    path = StaticDashboardBuilder().build()
    print(f"Dashboard generata: {path}")


if __name__ == "__main__":
    main()