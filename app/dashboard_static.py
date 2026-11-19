from __future__ import annotations

import csv
import html
import json
import math
import re
from urllib.parse import urlparse
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
    "mase_provvedimenti": "MASE",
    "terna_econnextion": "Terna Econnextion",
    "puglia": "Puglia",
    "sistema_puglia_energia": "Puglia",
    "lazio": "Lazio",
    "sicilia": "Sicilia",
    "lombardia": "Lombardia",
    "veneto": "Veneto",
    "emilia_romagna": "Emilia-Romagna",
    "sardegna": "Sardegna",
    "toscana": "Toscana",
    "toscana_atos": "Toscana",
    "piemonte": "Piemonte",
    "campania": "Campania",
}

SOURCE_GROUPS = {
    "toscana_atos": "toscana",
    "mase": "mase",
    "mase_provvedimenti": "mase",
    "puglia": "puglia",
    "sistema_puglia_energia": "puglia",
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

        # Il file projects_snapshot_*.csv viene generato internamente dal ReportBuilder.
        # Non usare csv.Sniffer: con titoli lunghi e virgole nel testo pu? interpretare male
        # il dialetto e sfalsare le colonne, producendo source assurde tipo "positivo",
        # "187_0", "pz", ecc.
        reader = csv.DictReader(
            raw.splitlines(),
            delimiter=",",
            quotechar='"',
            doublequote=True,
            skipinitialspace=False,
        )

        if not reader.fieldnames:
            raise ValueError(f"CSV snapshot senza header: {path}")

        expected = [
            "project_name",
            "proponent",
            "region",
            "province",
            "municipalities",
            "project_type",
            "power_mw",
            "status",
            "source",
            "url",
            "updated_at",
        ]

        missing = [col for col in expected if col not in reader.fieldnames]
        if missing:
            raise ValueError(
                f"CSV snapshot con colonne mancanti {missing}. "
                f"Colonne trovate: {reader.fieldnames}"
            )

        rows: list[dict[str, Any]] = []

        for line_no, row in enumerate(reader, start=2):
            # Se esiste la chiave None, csv ha trovato pi? campi dell'header:
            # significa riga malformata o parsing errato. Meglio fallire che produrre dashboard sporca.
            if None in row:
                raise ValueError(
                    f"CSV snapshot malformato alla riga {line_no}: campi extra={row[None]!r}"
                )

            rows.append(dict(row))

        return rows


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

        title = self._sanitize_text(title, 500)
        proponent = self._sanitize_text(proponent, 300)
        region = self._sanitize_text(region, 120)
        province = self._sanitize_text(province, 120)
        municipalities = self._sanitize_text(municipalities, 500)
        project_type = self._sanitize_text(project_type, 200)
        status = self._sanitize_text(status, 500)
        updated_at = self._sanitize_text(updated_at, 120)
        url = self._sanitize_url(url)

        source_group = self._source_group(source)

        return {
            "source": source or "nd",
            "source_group": source_group,
            "source_label": SOURCE_LABELS.get(source_group, SOURCE_LABELS.get(source or "", source or "ND")),
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

    def _sanitize_text(self, value: Any, max_len: int = 2000) -> str:
        text = self._clean_text(value) or ""
        text = re.sub(r"[\x00-\x08\x0b\x0c\x0e-\x1f\x7f]", "", text)
        return text[:max_len]

    def _sanitize_url(self, value: Any) -> str:
        text = self._clean_text(value) or ""

        if not text:
            return ""

        try:
            parsed = urlparse(text)
        except Exception:
            return ""

        if parsed.scheme not in {"http", "https"}:
            return ""

        if not parsed.netloc:
            return ""

        return text

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

    def _source_group(self, source: str | None) -> str:
        source = source or "nd"
        return SOURCE_GROUPS.get(source, source)

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

        source_counts = Counter(record.get("source_group") or record["source"] for record in records)

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
                "source_group": record.get("source_group", record["source"]),
                "source_label": record["source_label"],
                "project_type": record["project_type"],
                "power_mw": round(record["power_mw"] or 0, 3),
                "status": record["status"],
                "url": record["url"],
                "project_key": record.get("project_key"),
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
            source = record.get("source_group") or record["source"]

            if source not in grouped:
                grouped[source] = {
                    "source": source,
                    "source_label": SOURCE_LABELS.get(source, record["source_label"]),
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
  <style>
    :root {
      --bg: #eef2f5;
      --bg-soft: #f7f9fb;
      --card: #ffffff;
      --text: #111827;
      --muted: #64748b;
      --muted-2: #94a3b8;
      --border: #dfe5eb;
      --border-soft: #edf1f5;
      --accent: #0f766e;
      --accent-dark: #0f4f4a;
      --ink: #0f172a;
      --steel: #334155;
      --blue: #1d4ed8;
      --warning: #d97706;
      --danger: #dc2626;
      --ok: #16a34a;
      --soft-ok: #dcfce7;
      --soft-blue: #dbeafe;
      --soft-warn: #fef3c7;
      --shadow: 0 18px 42px rgba(15, 23, 42, 0.08);
      --shadow-soft: 0 8px 24px rgba(15, 23, 42, 0.06);
      --radius: 20px;
    }

    * { box-sizing: border-box; }

    body {
      margin: 0;
      font-family: Arial, Helvetica, sans-serif;
      background:
        radial-gradient(circle at top left, rgba(15, 118, 110, 0.10), transparent 34%),
        linear-gradient(180deg, #f8fafc 0%, var(--bg) 38%, #f5f7f9 100%);
      color: var(--text);
    }

    header {
      padding: 34px 36px 30px;
      background:
        radial-gradient(circle at 88% 12%, rgba(20, 184, 166, 0.24), transparent 30%),
        linear-gradient(135deg, #0b1220 0%, #102a2f 58%, #0f4f4a 100%);
      color: white;
      border-bottom: 1px solid rgba(255,255,255,0.08);
    }

    .hero {
      display: grid;
      grid-template-columns: 1.4fr 0.9fr;
      gap: 28px;
      align-items: end;
      max-width: 1900px;
      margin: 0 auto;
    }

    /* switch between the PV and the wind radar (same control on both pages) */
    .radar-switch { display: inline-flex; gap: 4px; padding: 4px; border: 1px solid rgba(255,255,255,0.22);
      border-radius: 999px; background: rgba(255,255,255,0.08); }
    .radar-switch a { color: rgba(255,255,255,0.80); text-decoration: none; font-weight: 800; font-size: 14px;
      padding: 8px 20px; border-radius: 999px; letter-spacing: 0.01em; }
    .radar-switch a.on { background: #ffffff; color: #0f4f4a; }
    .radar-switch a:not(.on):hover { background: rgba(255,255,255,0.16); color: #ffffff; text-decoration: none; }

    .eyebrow {
      display: inline-flex;
      align-items: center;
      gap: 8px;
      padding: 5px 10px;
      border: 1px solid rgba(255,255,255,0.18);
      border-radius: 999px;
      color: rgba(255,255,255,0.78);
      font-size: 12px;
      text-transform: uppercase;
      letter-spacing: 0.10em;
      background: rgba(255,255,255,0.06);
    }

    header h1 {
      margin: 14px 0 0;
      font-size: 38px;
      letter-spacing: -0.055em;
      line-height: 1.04;
    }

    header p {
      max-width: 860px;
      margin: 10px 0 0;
      color: rgba(255,255,255,0.76);
      font-size: 15px;
      line-height: 1.5;
    }

    .hero-stats {
      display: grid;
      grid-template-columns: repeat(3, minmax(120px, 1fr));
      gap: 10px;
    }

    .hero-chip {
      padding: 13px 14px;
      border: 1px solid rgba(255,255,255,0.16);
      border-radius: 16px;
      background: rgba(255,255,255,0.08);
      backdrop-filter: blur(4px);
    }

    .hero-chip .label {
      color: rgba(255,255,255,0.66);
      font-size: 11px;
      text-transform: uppercase;
      letter-spacing: 0.08em;
    }

    .hero-chip .value {
      margin-top: 6px;
      font-size: 18px;
      font-weight: 800;
      letter-spacing: -0.02em;
      white-space: nowrap;
    }

    main {
      padding: 28px 34px 52px;
      max-width: 1900px;
      margin: 0 auto;
    }

    .section-title {
      margin: 34px 0 14px;
      font-size: 22px;
      letter-spacing: -0.035em;
      color: #111827;
    }

    .section-subtitle {
      margin: -6px 0 16px;
      color: var(--muted);
      font-size: 13px;
      line-height: 1.45;
    }

    .grid { display: grid; gap: 18px; }

    .kpi-grid { grid-template-columns: repeat(5, minmax(170px, 1fr)); }
    .quality-grid { grid-template-columns: repeat(5, minmax(170px, 1fr)); }
    .chart-grid { grid-template-columns: repeat(2, minmax(360px, 1fr)); }
    .split { display: grid; grid-template-columns: 1.2fr 0.8fr; gap: 18px; }

    .card {
      background: rgba(255,255,255,0.94);
      border: 1px solid var(--border);
      border-radius: var(--radius);
      box-shadow: var(--shadow-soft);
      padding: 18px;
    }

    .card:hover {
      box-shadow: var(--shadow);
    }

    .kpi {
      min-height: 122px;
      position: relative;
      overflow: hidden;
    }

    .kpi.kpi-primary {
      border-color: rgba(15, 118, 110, 0.24);
      background: linear-gradient(180deg, #ffffff 0%, #f1fbfa 100%);
    }

    .kpi.kpi-primary::after {
      content: "";
      position: absolute;
      inset: auto 16px 12px auto;
      width: 46px;
      height: 46px;
      border-radius: 999px;
      background: rgba(15, 118, 110, 0.09);
    }

    .kpi .label {
      color: var(--muted);
      font-size: 11px;
      text-transform: uppercase;
      letter-spacing: 0.095em;
      font-weight: 700;
    }

    .kpi .value {
      margin-top: 8px;
      font-size: 30px;
      font-weight: 850;
      letter-spacing: -0.05em;
      color: var(--ink);
    }

    .kpi.kpi-primary .value {
      color: var(--accent-dark);
    }

    .kpi .note {
      margin-top: 5px;
      color: var(--muted);
      font-size: 12px;
      line-height: 1.35;
    }

    .filters {
      display: grid;
      grid-template-columns: 1.3fr repeat(6, minmax(140px, 1fr));
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
      color: var(--text);
    }

    input:focus, select:focus {
      outline: none;
      border-color: rgba(15, 118, 110, 0.55);
      box-shadow: 0 0 0 3px rgba(15, 118, 110, 0.10);
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
      font-size: 12px;
      text-transform: uppercase;
      letter-spacing: 0.035em;
    }

    td {
      border-bottom: 1px solid var(--border-soft);
      padding: 9px 10px;
      vertical-align: top;
    }

    tr:hover td { background: #f8fbfb; }

    tr.mw-xl td:first-child { border-left: 4px solid rgba(15, 118, 110, 0.72); }
    tr.mw-lg td:first-child { border-left: 4px solid rgba(29, 78, 216, 0.46); }

    .table-wrap {
      overflow: auto;
      max-height: 700px;
      border: 1px solid var(--border);
      border-radius: 14px;
      background: white;
      -webkit-overflow-scrolling: touch;
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
      font-weight: 700;
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
      font-weight: 700;
      white-space: nowrap;
    }

    .muted { color: var(--muted); }
    .small { font-size: 12px; }
    .nowrap { white-space: nowrap; }
    .num { text-align: right; white-space: nowrap; font-variant-numeric: tabular-nums; }
    .project-title { min-width: 380px; }
    .project-title strong {
      display: -webkit-box;
      -webkit-line-clamp: 2;
      -webkit-box-orient: vertical;
      overflow: hidden;
      line-height: 1.35;
    }
    .proponent-col { min-width: 180px; font-weight: 600; }

    a {
      color: var(--accent-dark);
      text-decoration: none;
      font-weight: 800;
    }

    a:hover { text-decoration: underline; }

    .scroll-small {
      max-height: 385px;
      overflow: auto;
      -webkit-overflow-scrolling: touch;
    }

    .warning-box {
      border-left: 5px solid rgba(15, 118, 110, 0.62);
      background: #f0fdfa;
      padding: 14px 16px;
      border-radius: 12px;
      color: #134e4a;
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

    .table-legend {
      display: flex;
      gap: 10px;
      flex-wrap: wrap;
      margin: 0 0 12px;
      color: var(--muted);
      font-size: 12px;
    }

    .legend-dot {
      display: inline-flex;
      align-items: center;
      gap: 6px;
    }

    .legend-dot::before {
      content: "";
      width: 10px;
      height: 10px;
      border-radius: 999px;
      background: #cbd5e1;
    }

    .legend-dot.xl::before { background: rgba(15, 118, 110, 0.72); }
    .legend-dot.lg::before { background: rgba(29, 78, 216, 0.46); }

    .footer-note {
      margin-top: 28px;
      color: var(--muted);
      font-size: 12px;
      line-height: 1.45;
    }

    @media (max-width: 1400px) {
      .hero { grid-template-columns: 1fr; align-items: start; }
      .hero-stats { max-width: 760px; }
      .kpi-grid { grid-template-columns: repeat(3, 1fr); }
      .quality-grid { grid-template-columns: repeat(2, 1fr); }
      .chart-grid, .split { grid-template-columns: 1fr; }
      .filters { grid-template-columns: 1fr 1fr; }
    }

    @media (max-width: 900px) {
      main { padding: 18px; }
      header { padding: 22px; }
      .hero { gap: 18px; }
      .hero h1,
      header h1 {
        font-size: 29px;
        line-height: 1.05;
      }
      .hero p {
        font-size: 13px;
        line-height: 1.45;
      }
      .hero-stats {
        grid-template-columns: repeat(3, minmax(0, 1fr));
        gap: 10px;
        width: 100%;
      }
      .hero-chip { padding: 12px; }
      .hero-chip .value { font-size: 19px; }
      .section-title {
        margin-top: 26px;
        font-size: 19px;
      }
      .section-subtitle {
        font-size: 12px;
        line-height: 1.45;
      }
      .card {
        padding: 14px;
        border-radius: 15px;
      }
      .kpi { min-height: auto; }
      .kpi .value { font-size: 27px; }
      .kpi-grid,
      .quality-grid { grid-template-columns: 1fr; }
      .filters {
        grid-template-columns: 1fr;
        gap: 9px;
      }
      input,
      select {
        font-size: 13px;
        padding: 10px 11px;
      }
      .chart-grid { gap: 14px; }
      canvas { max-height: 280px; }
      .table-wrap {
        max-height: 560px;
        border-radius: 12px;
      }
      table {
        min-width: 980px;
        font-size: 12px;
      }
      th,
      td { padding: 8px; }
      .project-title { min-width: 280px; }
      .proponent-col { min-width: 150px; }
      .warning-box {
        font-size: 13px;
        padding: 12px 13px;
      }
    }

    @media (max-width: 520px) {
      main { padding: 14px; }
      header { padding: 18px; }
      .eyebrow { font-size: 10px; }
      .hero h1,
      header h1 { font-size: 26px; }
      .hero-stats { grid-template-columns: 1fr; }
      .hero-chip {
        display: grid;
        grid-template-columns: 1fr auto;
        align-items: end;
        gap: 12px;
      }
      .hero-chip .label { margin-bottom: 0; }
      .hero-chip .value { font-size: 20px; }
      .section-title { font-size: 18px; }
      .kpi .label { font-size: 10px; }
      .kpi .value { font-size: 25px; }
      .kpi .note { font-size: 11px; }
      .scroll-small { max-height: 340px; }
      .top-table { max-height: 500px; }
      table { min-width: 920px; }
      .footer-note {
        font-size: 11px;
        line-height: 1.45;
      }
    }










__SCHEDA_CSS__
__ZONE_CSS__
</style>
<!-- MOBILE REDIRECT START -->
<script>
(function () {
  const mobile = window.matchMedia && window.matchMedia('(max-width: 760px)').matches;
  const desktopRequested = new URLSearchParams(window.location.search).get('desktop') === '1';
  const isMobilePage = /\\/mobile\\.html$/i.test(window.location.pathname);

  if (mobile && !desktopRequested && !isMobilePage) {
    const base = window.location.pathname.replace(/index\\.html$/i, '').replace(/\\/?$/, '/');
    window.location.href = base + 'mobile.html';
  }
})();
</script>
<!-- MOBILE REDIRECT END -->
</head>
<body>
  <header class="compact">
    <div class="hero">
      <div>
        <nav class="radar-switch" aria-label="Cambia radar"><a class="on" href="./" aria-current="page">Fotovoltaico</a><a href="wind/">Eolico</a></nav>
        <h1>PV Agent Dashboard</h1>
      </div>
      <div class="nat-line" id="natLine">-</div>
    </div>
  </header>

  <div class="zone-bar" id="zoneBar">
    <span class="zone-label">La mia zona</span>
    <label>Regione<select id="regionFilter"></select></label>
    <label>Provincia<select id="provinceFilter"></select></label>
    <button type="button" class="zone-reset" id="zoneReset" hidden>Tutta Italia</button>
    <span class="zone-summary" id="zoneSummary"></span>
  </div>

  <main>
    <section class="zone-section">
      <div class="zone-kpis" id="zoneKpis"></div>
      <div class="zone-card">
        <div class="zone-head"><h2 id="nearTitle">Vicini al cantiere</h2><span class="small" id="nearByProv"></span></div>
        <div id="nearList"></div>
        <button type="button" class="zone-more" id="nearMore" hidden></button>
      </div>
      <div class="zone-two">
        <div class="zone-card"><div class="zone-head"><h2>Avanzamenti e realizzati in zona</h2><span class="small" id="changesSince"></span></div><div id="progressList"></div></div>
        <div class="zone-card"><div class="zone-head"><h2>Nuovi in zona</h2><span class="small">dall'ultimo aggiornamento</span></div><div id="newList"></div></div>
      </div>
    </section>

    <section id="pipelineSection">
      <h2 class="section-title">Pipeline completa</h2>
      <p class="section-subtitle">La tabella segue la zona scelta in alto.</p>
      <div class="card">
        <div class="filters">
          <input id="searchInput" placeholder="Cerca progetto, proponente, comune...">
          <select id="dataKindFilter">
            <option value="">Tutti i dati</option>
            <option value="punctual">Solo pipeline operativa</option>
            <option value="terna">Solo radar Terna</option>
            <option value="card">Solo con scheda</option>
            <option value="ferx">Solo con incentivo FER X assegnato</option>
            <option value="cantiere">Vicini al cantiere (priorità alta e media)</option>
          </select>
          <select id="epcFilter" title="Impresa esecutrice / EPC trovata nei documenti">
            <option value="">EPC: tutti i record</option>
            <option value="any">EPC trovato (qualsiasi evidenza)</option>
            <option value="A">EPC in atto di cantiere</option>
            <option value="B">EPC solo negli elaborati</option>
            <option value="news">Notizie di stampa compatibili (da verificare)</option>
          </select>
          <select id="sourceFilter"></select>
          <select id="sortFieldFilter">
            <option value="">Ordina per...</option>
            <option value="power_mw">MW</option>
            <option value="readiness">Priorità cantiere</option>
            <option value="title">Progetto</option>
            <option value="proponent">Proponente</option>
            <option value="region">Regione</option>
            <option value="province">Provincia</option>
            <option value="source_label">Fonte</option>
          </select>
          <select id="sortDirectionFilter">
            <option value="desc">Decrescente</option>
            <option value="asc">Crescente</option>
          </select>
        </div>

        <div class="small muted" style="margin-bottom: 10px;">
          Record visualizzati: <strong id="visibleCount">0</strong> · limite tabella: 700 righe
        </div>

          <button
            type="button"
            id="exportFilteredCsv"
            style="
              display:block;
              margin:0 0 12px auto;
              border:0;
              border-radius:12px;
              padding:10px 16px;
              background:#0f766e;
              color:#ffffff;
              font-size:13px;
              font-weight:800;
              cursor:pointer;
            "
          >
            Esporta CSV
          </button>

        <div class="table-legend">
          <span class="legend-dot xl">≥ 100 MW</span>
          <span class="legend-dot lg">50–99 MW</span>
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
                <th>Link</th>
              </tr>
            </thead>
            <tbody id="pipelineBody"></tbody>
          </table>
        </div>
      </div>
    </section>

    <section>
      <h2 class="section-title">Quadro nazionale</h2>
      <div class="nat-grid">
        <div class="zone-card">
          <div class="zone-head"><h2>Vicini al cantiere per regione</h2><span class="small">clic su una regione per sceglierla</span></div>
          <div class="tile-map" id="tileMap"></div>
          <div class="tile-legend"><span class="sw" style="background:#eef2f6"></span>0 <span class="sw" style="background:#dbe9f1"></span>1–4 <span class="sw" style="background:#b9d9e8"></span>5–29 <span class="sw" style="background:#5d9fc0"></span>30–79 <span class="sw" style="background:#1f5f86"></span>80+</div>
        </div>
        <div class="nat-side" id="natCharts"></div>
      </div>
    </section>

    <div class="footer-note">
      Dashboard HTML statica generata automaticamente da PV Agent.
      Terna Econnextion è un dato aggregato e non deve essere sommato ai progetti puntuali come se fosse una lista di impianti.
      I controlli di qualità restano nei report tecnici, non nella vista condivisa.
    </div>
  </main>

  <script>__SCHEDA_JS__</script>
  <script>__ZONE_JS__</script>
  <script>
    const DASHBOARD_DATA = __DASHBOARD_DATA__;

    const summary = DASHBOARD_DATA.summary;
    const records = DASHBOARD_DATA.records;
    const dataQuality = DASHBOARD_DATA.data_quality || {};

    function toNumber(value) {
      const n = Number(value);
      return Number.isFinite(n) ? n : 0;
    }

    function formatItalianNumber(value, maxDecimals = 0) {
      const n = toNumber(value);
      const sign = n < 0 ? '-' : '';
      const abs = Math.abs(n);
      const fixed = abs.toFixed(maxDecimals);
      let [integerPart, decimalPart = ''] = fixed.split('.');

      // Forza sempre il separatore delle migliaia anche sui numeri a 4 cifre.
      integerPart = integerPart.replace(/\\B(?=(\\d{3})+(?!\\d))/g, '.');
      decimalPart = decimalPart.replace(/0+$/, '');

      return sign + integerPart + (decimalPart ? ',' + decimalPart : '');
    }

    function fmtNum(value) {
      return formatItalianNumber(value, 0);
    }

    function fmtMw(value) {
      if (value === null || value === undefined || value === '') return '';
      return formatItalianNumber(value, 3) + ' MW';
    }

    function text(value) {
      return value === null || value === undefined ? '' : String(value);
    }

    function escapeHtml(value) {
      return text(value)
        .replace(/&/g, '&amp;')
        .replace(/</g, '&lt;')
        .replace(/>/g, '&gt;')
        .replace(/"/g, '&quot;')
        .replace(/'/g, '&#039;');
    }

    function safeUrl(value) {
      const raw = text(value).trim();

      if (!raw || !/^https?:\\/\\//i.test(raw)) {
        return '';
      }

      try {
        const parsed = new URL(raw);

        if (parsed.protocol !== 'http:' && parsed.protocol !== 'https:') {
          return '';
        }

        return parsed.href;
      } catch {
        return '';
      }
    }

    function safeExternalLink(value) {
      const url = safeUrl(value);

      if (!url) {
        return '';
      }

      return `<a href="${escapeHtml(url)}" target="_blank" rel="noopener noreferrer">Apri</a>`;
    }

    // Project cards (schede): built in the cloud from the official documents, published in docs/schede
    let schedeIndex = {};

    function schedaButton(row) {
      const s = row && row.project_key ? schedeIndex[row.project_key] : null;
      const nw = row && row.project_key && (window.PVNews || {})[row.project_key];
      const rd = row && row.project_key && (window.PVReady || {})[row.project_key];
      const ready = rd && (rd.level === 'Alta' || rd.level === 'Media')
        ? ` <span class="sc-epc sc-ready sc-ready-${rd.level === 'Alta' ? 'a' : 'm'}" title="${escapeHtml(rd.phase_label + (rd.window ? ': ' + rd.window : ''))}">Cantiere ${rd.level === 'Alta' ? '●●' : '●'}</span>` : '';
      const fx = row && row.project_key && (window.PVFerx || {})[row.project_key];
      const ferx = fx ? ` <span class="sc-epc sc-ferx" title="Incentivo FER X assegnato (GSE, graduatoria ${escapeHtml(fx[0].procedura)})">FER X</span>` : '';
      const press = nw ? ` <span class="sc-epc sc-press" title="Notizie di stampa compatibili con il progetto: da verificare">Stampa ${nw.length}</span>` : '';
      if (!s) return ready + ferx + press;
      const epc = s.epc ? ` <span class="sc-epc" title="${s.epc === 'A' ? 'Impresa esecutrice in un atto di cantiere' : 'Impresa esecutrice indicata negli elaborati'}">EPC ${s.epc === 'A' ? '✓' : '?'}</span>` : '';
      return ` <button class="sc-btn" type="button" data-scheda="${escapeHtml(s.file)}"${s.live ? ' data-live="1"' : ''}>Scheda</button>${ready}${epc}${ferx}${press}`;
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
      const found = records.find(r => (r.source_group || r.source) === source);
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

    function mwBandClass(value) {
      const mw = toNumber(value);
      if (mw >= 100) return 'mw-xl';
      if (mw >= 50) return 'mw-lg';
      return '';
    }

    function uniqueValues(key, dataset = records) {
      return [...new Set(dataset.map(r => r[key]).filter(Boolean))]
        .sort((a, b) => String(a).localeCompare(String(b), 'it'));
    }

    function fillSelect(id, label, values, mapper = v => v) {
      const select = document.getElementById(id);
      while (select.firstChild) {
        select.removeChild(select.firstChild);
      }

      const placeholder = document.createElement('option');
      placeholder.value = '';
      placeholder.textContent = label;
      select.appendChild(placeholder);

      values.forEach(value => {
        const option = document.createElement('option');
        option.value = value;
        option.textContent = mapper(value);
        select.appendChild(option);
      });
    }


    // geography: the province list follows the region (sigla + name + number of projects); picking a
    // province with no region selected sets its region
    const PROV_NAMES = {"AG":"Agrigento","AL":"Alessandria","AN":"Ancona","AO":"Aosta","AP":"Ascoli Piceno","AQ":"L'Aquila","AR":"Arezzo","AT":"Asti","AV":"Avellino","BA":"Bari","BG":"Bergamo","BI":"Biella","BL":"Belluno","BN":"Benevento","BO":"Bologna","BR":"Brindisi","BS":"Brescia","BT":"Barletta-Andria-Trani","BZ":"Bolzano","CA":"Cagliari","CB":"Campobasso","CE":"Caserta","CH":"Chieti","CL":"Caltanissetta","CN":"Cuneo","CO":"Como","CR":"Cremona","CS":"Cosenza","CT":"Catania","CZ":"Catanzaro","EN":"Enna","FC":"Forlì-Cesena","FE":"Ferrara","FG":"Foggia","FI":"Firenze","FM":"Fermo","FR":"Frosinone","GE":"Genova","GO":"Gorizia","GR":"Grosseto","IM":"Imperia","IS":"Isernia","KR":"Crotone","LC":"Lecco","LE":"Lecce","LI":"Livorno","LO":"Lodi","LT":"Latina","LU":"Lucca","MB":"Monza e Brianza","MC":"Macerata","ME":"Messina","MI":"Milano","MN":"Mantova","MO":"Modena","MS":"Massa-Carrara","MT":"Matera","NA":"Napoli","NO":"Novara","NU":"Nuoro","OR":"Oristano","PA":"Palermo","PC":"Piacenza","PD":"Padova","PE":"Pescara","PG":"Perugia","PI":"Pisa","PN":"Pordenone","PO":"Prato","PR":"Parma","PT":"Pistoia","PU":"Pesaro e Urbino","PV":"Pavia","PZ":"Potenza","RA":"Ravenna","RC":"Reggio Calabria","RE":"Reggio Emilia","RG":"Ragusa","RI":"Rieti","RM":"Roma","RN":"Rimini","RO":"Rovigo","SA":"Salerno","SI":"Siena","SO":"Sondrio","SP":"La Spezia","SR":"Siracusa","SS":"Sassari","SU":"Sud Sardegna","SV":"Savona","TA":"Taranto","TE":"Teramo","TN":"Trento","TO":"Torino","TP":"Trapani","TR":"Terni","TS":"Trieste","TV":"Treviso","UD":"Udine","VA":"Varese","VB":"Verbano-Cusio-Ossola","VC":"Vercelli","VE":"Venezia","VI":"Vicenza","VR":"Verona","VT":"Viterbo","VV":"Vibo Valentia"};
    function provLabel(code) {
      return PROV_NAMES[code] ? code + ' – ' + PROV_NAMES[code] : code;
    }
    function refreshProvinces() {
      const region = document.getElementById('regionFilter').value;
      const select = document.getElementById('provinceFilter');
      const current = select.value;
      const counts = {};
      records.forEach(r => {
        if (!r.province || (region && r.region !== region)) return;
        counts[r.province] = (counts[r.province] || 0) + 1;
      });
      const codes = Object.keys(counts).sort((a, b) => provLabel(a).localeCompare(provLabel(b), 'it'));
      fillSelect('provinceFilter', region ? 'Tutte le province (' + region + ')' : 'Tutte le province', codes,
        code => provLabel(code) + ' (' + counts[code] + ')');
      select.value = counts[current] ? current : '';
    }
    function regionForProvince(code) {
      const counts = {};
      records.forEach(r => { if (r.province === code && r.region) counts[r.region] = (counts[r.region] || 0) + 1; });
      return Object.keys(counts).sort((a, b) => counts[b] - counts[a])[0] || '';
    }

    function initFilters() {
      fillSelect('sourceFilter', 'Tutte le fonti', uniqueValues('source_group'), value => sourceLabel(value));
      fillSelect('regionFilter', 'Tutte le regioni', uniqueValues('region'));
      refreshProvinces();
    }

    function recordMatchesFilters(record) {
      const query = document.getElementById('searchInput').value.trim().toLowerCase();
      const dataKind = document.getElementById('dataKindFilter').value;
      const source = document.getElementById('sourceFilter').value;
      const region = document.getElementById('regionFilter').value;
      const province = document.getElementById('provinceFilter').value;

      if (dataKind === 'punctual' && isTerna(record)) return false;
      if (dataKind === 'terna' && !isTerna(record)) return false;
      if (dataKind === 'card' && !(record.project_key && schedeIndex[record.project_key])) return false;
      if (dataKind === 'ferx' && !(record.project_key && (window.PVFerx || {})[record.project_key])) return false;
      if (dataKind === 'cantiere') {
        const rd = record.project_key && (window.PVReady || {})[record.project_key];
        if (!rd || !(rd.level === 'Alta' || rd.level === 'Media')) return false;
      }
      const epcSel = document.getElementById('epcFilter').value;
      if (epcSel === 'news') {
        if (!(record.project_key && (window.PVNews || {})[record.project_key])) return false;
      } else if (epcSel) {
        const s = record.project_key ? schedeIndex[record.project_key] : null;
        const lv = s && s.epc;
        if (!lv || (epcSel !== 'any' && lv !== epcSel)) return false;
      }
      if (source && (record.source_group || record.source) !== source) return false;
      if (region && record.region !== region) return false;
      if (province && record.province !== province) return false;

      if (query) {
        const haystack = [
          record.title,
          record.proponent,
          record.region,
          record.province,
          record.municipalities,
          record.project_type,
          record.source_label
        ].join(' ').toLowerCase();

        if (!haystack.includes(query)) return false;
      }

      return true;
    }

    function sortRecords(items) {
      const field = document.getElementById('sortFieldFilter').value;
      const direction = document.getElementById('sortDirectionFilter').value || 'desc';
      const copy = [...items];

      // Nessun ordinamento automatico: la tabella resta nell'ordine prodotto dal dataset.
      if (!field) return copy;

      const multiplier = direction === 'asc' ? 1 : -1;

      return copy.sort((a, b) => {
        if (field === 'readiness') {
          const sc = r => { const x = r.project_key && (window.PVReady || {})[r.project_key]; return x && x.level ? x.score : -1; };
          return ((sc(a) - sc(b)) || (toNumber(a.power_mw || 0) - toNumber(b.power_mw || 0))) * multiplier;
        }
        if (field === 'power_mw') {
          const av = a.power_mw === null || a.power_mw === undefined || a.power_mw === '' ? -Infinity : toNumber(a.power_mw);
          const bv = b.power_mw === null || b.power_mw === undefined || b.power_mw === '' ? -Infinity : toNumber(b.power_mw);
          return (av - bv) * multiplier;
        }

        const av = text(a[field]);
        const bv = text(b[field]);
        return av.localeCompare(bv, 'it', { sensitivity: 'base' }) * multiplier;
      });
    }

    function csvCell(value) {
      const cleaned = text(value)
        .split(String.fromCharCode(13)).join(' ')
        .split(String.fromCharCode(10)).join(' ')
        .trim();

      return '"'
        + cleaned.split('"').join('""')
        + '"';
    }

    function exportFilteredCsv() {
      const filtered = sortRecords(
        records.filter(recordMatchesFilters)
      );

      if (!filtered.length) {
        return;
      }

      const headers = [
        'Fonte',
        'Regione',
        'Provincia',
        'Comune/i',
        'Progetto / aggregato',
        'Proponente',
        'Tipo',
        'MW',
        'Stato',
        'Link',
        'Vicini al cantiere',
        'Fase',
        'Finestra',
        'Motivi'
      ];

      const rows = filtered.map(record => [
        record.source_label || record.source || '',
        record.region || '',
        record.province || '',
        record.municipalities || '',
        record.title || '',
        isTerna(record)
          ? 'Dato aggregato'
          : (record.proponent || ''),
        record.project_type || '',
        record.power_mw === null
          || record.power_mw === undefined
          || record.power_mw === ''
            ? ''
            : formatItalianNumber(
                record.power_mw,
                3
              ),
        record.status || '',
        record.url || '',
        ...(rd => rd ? [rd.level || '', rd.phase_label || '', rd.window || '', (rd.reasons || []).join(' | ')] : ['', '', '', ''])(
          record.project_key && (window.PVReady || {})[record.project_key])
      ]);

      const lineBreak =
        String.fromCharCode(13, 10);

      const csv = [headers, ...rows]
        .map(
          row => row.map(csvCell).join(';')
        )
        .join(lineBreak);

      const blob = new Blob(
        [
          String.fromCharCode(0xFEFF),
          csv
        ],
        {
          type: 'text/csv;charset=utf-8'
        }
      );

      const now = new Date();
      const pad = value =>
        String(value).padStart(2, '0');

      const filename =
        'pv_pipeline_filtrata_'
        + now.getFullYear()
        + pad(now.getMonth() + 1)
        + pad(now.getDate())
        + '_'
        + pad(now.getHours())
        + pad(now.getMinutes())
        + '.csv';

      const objectUrl =
        URL.createObjectURL(blob);

      const link =
        document.createElement('a');

      link.href = objectUrl;
      link.download = filename;

      document.body.appendChild(link);
      link.click();
      link.remove();

      setTimeout(
        () => URL.revokeObjectURL(objectUrl),
        0
      );
    }

    function renderPipeline() {
      const body = document.getElementById('pipelineBody');
      body.innerHTML = '';

      const filtered = sortRecords(records.filter(recordMatchesFilters));
      setText('visibleCount', fmtNum(filtered.length));

      const exportButton =
        document.getElementById(
          'exportFilteredCsv'
        );

      if (exportButton) {
        exportButton.disabled =
          filtered.length === 0;

        exportButton.textContent =
          filtered.length
            ? 'Esporta CSV ('
              + fmtNum(filtered.length)
              + ')'
            : 'Nessun risultato';
      }


      filtered.slice(0, 700).forEach(record => {
        const tr = document.createElement('tr');
        tr.className = mwBandClass(record.power_mw);
        const link = safeExternalLink(record.url);
        const proponent = isTerna(record)
          ? '<span class="muted">Dato aggregato</span>'
          : (text(record.proponent) ? escapeHtml(record.proponent) : '<span class="muted">n/d</span>');

        tr.innerHTML = `
          <td><span class="${badgeClass(record.source_group || record.source)}">${escapeHtml(record.source_label)}</span></td>
          <td>${escapeHtml(record.region)}</td>
          <td>${escapeHtml(record.province)} ${deducedBadge(record.province_deduced)}</td>
          <td>${escapeHtml(record.municipalities)} ${deducedBadge(record.municipalities_deduced)}</td>
          <td class="project-title"><strong>${escapeHtml(record.title)}</strong></td>
          <td class="proponent-col">${proponent}</td>
          <td>${escapeHtml(record.project_type)}</td>
          <td class="num">${fmtMw(record.power_mw)}</td>
          <td><span class="sc-cell">${link}${schedaButton(record)}</span></td>
        `;

        body.appendChild(tr);
      });
    }

    function attachEvents() {
      // registered first, so the province list is updated before the table is redrawn
      document.getElementById('regionFilter').addEventListener('change', refreshProvinces);
      document.getElementById('provinceFilter').addEventListener('change', () => {
        const code = document.getElementById('provinceFilter').value;
        const regionSelect = document.getElementById('regionFilter');
        if (code && !regionSelect.value) {
          regionSelect.value = regionForProvince(code);
          refreshProvinces();
          document.getElementById('provinceFilter').value = code;
        }
      });
      ['searchInput', 'dataKindFilter', 'epcFilter', 'sourceFilter', 'regionFilter', 'provinceFilter', 'sortFieldFilter', 'sortDirectionFilter'].forEach(id => {
        document.getElementById(id).addEventListener('input', renderPipeline);
        document.getElementById(id).addEventListener('change', renderPipeline);
      });
    }

    document.addEventListener('click', event => {
      const button = event.target.closest('[data-scheda]');
      if (!button || !window.PVScheda) return;
      const base = button.dataset.live ? SCHEDE_LIVE : 'schede/';
      fetch(base + encodeURIComponent(button.dataset.scheda), { cache: 'no-cache' })
        .then(r => r.ok ? r.json() : Promise.reject(r.status))
        .then(card => window.PVScheda.open(card))
        .catch(() => {});
    });

    // cards published with the dashboard (docs/schede) first; cards produced in the cloud after the
    // last dashboard update are read from the memory branch of the public repository
    const SCHEDE_LIVE = 'https://raw.githubusercontent.com/EmAnzi3/pv_agent_mvp/doc-intel-state/schede/';
    const getIndex = url => fetch(url, { cache: 'no-cache' }).then(r => r.ok ? r.json() : {}).catch(() => ({}));
    // sector press matched to the projects (level C, to verify): read from the memory branch
    getIndex(SCHEDE_LIVE.replace(/schede\/$/, 'readiness/index.json')).then(n => {
      window.PVReady = (n && n.projects) || {};
      if (Object.keys(window.PVReady).length) { renderPipeline(); window.ZoneView.render(); }
    });
    getIndex(SCHEDE_LIVE.replace(/schede\/$/, 'ferx/matches.json')).then(n => {
      window.PVFerx = (n && n.projects) || {};
      if (Object.keys(window.PVFerx).length) { renderPipeline(); }
    });
    getIndex(SCHEDE_LIVE.replace(/schede\/$/, 'news/matches.json')).then(n => {
      window.PVNews = (n && n.projects) || {};
      if (Object.keys(window.PVNews).length) { renderPipeline(); }
    });
    Promise.all([getIndex('schede/index.json'), getIndex(SCHEDE_LIVE + 'live_index.json')])
      .then(([local, live]) => {
        const merged = {};
        Object.entries(live || {}).forEach(([k, v]) => { merged[k] = Object.assign({}, v, { live: true }); });
        // the cloud copy is always the latest (cards are rebuilt there several times a day):
        // the copy published with the dashboard is used only for cards the cloud index does not list
        Object.entries(local || {}).forEach(([k, v]) => { if (!merged[k]) merged[k] = v; });
        schedeIndex = merged;
        if (Object.keys(schedeIndex).length) {
          renderPipeline();
          window.ZoneView.render();
        }
      });

    // «La mia zona»: near-site projects, news and national picture for the chosen region / province
    let zoneChanges = { loaded: false };
    const provincesOfRecord = r => text(r.province).split(/[,/ ]+/).filter(Boolean);
    function zoneItem(r) {
      return {
        key: r.project_key, region: r.region, provinces: provincesOfRecord(r), place: r.municipalities,
        proponent: r.proponent, title: r.title, mw: r.power_mw, url: r.url,
        rd: r.project_key ? (window.PVReady || {})[r.project_key] : null,
        card: !!(r.project_key && schedeIndex[r.project_key]), file: r.project_key && schedeIndex[r.project_key]
      };
    }
    const changeItem = x => ({
      key: x.project_key, region: x.region, provinces: provincesOfRecord(x), place: x.municipalities,
      proponent: x.proponent, title: x.title, mw: x.power_mw, what: x.what
    });
    function zoneActions(x) {
      const u = safeUrl(x.url);
      const portal = u ? `<a href="${escapeHtml(u)}" target="_blank" rel="noopener noreferrer">Portale</a>` : '';
      const card = x.file ? `<button class="sc-btn" type="button" data-scheda="${escapeHtml(x.file.file)}"${x.file.live ? ' data-live="1"' : ''}>Scheda</button>` : '';
      return card + portal;
    }
    function zoneNational() {
      const near = operationalRecords().map(zoneItem).filter(x => x.rd && (x.rd.level === 'Alta' || x.rd.level === 'Media'));
      const byPhase = {};
      near.forEach(x => { const k = x.rd.phase_label || x.rd.level; byPhase[k] = (byPhase[k] || 0) + toNumber(x.mw); });
      return [
        { title: 'MW vicini al cantiere per fase', note: 'Italia', unit: 'mw',
          rows: Object.entries(byPhase).sort((a, b) => b[1] - a[1]).map(([label, value]) => ({ label, value })) },
        { title: 'MW richiesti a Terna per regione', note: 'prime 5, dato aggregato', unit: 'mw',
          rows: topN(summary.regions || [], 'terna_mw', 5).filter(x => x.terna_mw).map(x => ({ label: x.region, value: x.terna_mw })) }
      ];
    }

    initFilters();
    renderPipeline();
    attachEvents();
    window.ZoneView.mount({
      items: () => operationalRecords().map(zoneItem),
      changes: () => zoneChanges,
      actions: zoneActions,
      national: zoneNational,
      provinceLabel: provLabel,
      updated: () => summary.generated_at || '',
      scopeNote: 'escluso radar Terna',
      showNear: () => {
        const kind = document.getElementById('dataKindFilter');
        kind.value = 'cantiere';
        kind.dispatchEvent(new Event('change'));
      }
    });
    // changes since the previous dashboard update, written by the local update next to data.json
    getIndex('changes_latest.json').then(c => {
      if (!c || !c.date) return;
      zoneChanges = { loaded: true, date: c.date, since: c.since,
        nuovi: (c.nuovi || []).map(changeItem), avanzamenti: (c.avanzamenti || []).map(changeItem), realizzati: (c.realizzati || []).map(changeItem) };
      window.ZoneView.render();
    });

    const pipelineExportButton =
      document.getElementById(
        'exportFilteredCsv'
      );

    if (pipelineExportButton) {
      pipelineExportButton.addEventListener(
        'click',
        exportFilteredCsv
      );
    }
  </script>
</body>
</html>
"""
        panel_dir = Path(__file__).resolve().parent.parent / "doc_intel"
        try:
            scheda_css = (panel_dir / "scheda_panel.css").read_text(encoding="utf-8")
            scheda_js = (panel_dir / "scheda_panel.js").read_text(encoding="utf-8").replace("</", "<\\/")
        except OSError:
            scheda_css, scheda_js = "", ""
        assets_dir = Path(__file__).resolve().parent.parent / "docs" / "assets"
        zone_css = (assets_dir / "zone-view.css").read_text(encoding="utf-8")
        zone_js = (assets_dir / "zone-view.js").read_text(encoding="utf-8").replace("</", "<\\/")
        return (template.replace("__SCHEDA_CSS__", scheda_css).replace("__SCHEDA_JS__", scheda_js)
                .replace("__ZONE_CSS__", zone_css).replace("__ZONE_JS__", zone_js)
                .replace("__DASHBOARD_DATA__", data_json))


def main() -> None:
    path = StaticDashboardBuilder().build()
    print(f"Dashboard generata: {path}")


if __name__ == "__main__":
    main()
