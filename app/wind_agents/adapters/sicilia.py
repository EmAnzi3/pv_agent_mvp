from __future__ import annotations

import csv
import hashlib
import io
import re
from urllib.parse import urlencode

from app.wind_agents.base import AgentFinding, BaseWindAgent


CSV_URL = (
    "https://dati.regione.sicilia.it/download/dataset/"
    "progetti-sottoposti-valutazione-ambientale/filesystem/"
    "progetti-sottoposti-valutazione-ambientale_csv.csv"
)
SOURCE_URL = "https://si-vvi.regione.sicilia.it/viavas/"
SIVVI_MAPSERVER = (
    "https://map.sitr.regione.sicilia.it/orbs/rest/services/"
    "sivvi/procedure_valutazione_ambientale/MapServer"
)
SIVVI_LAYER_QUERY = f"{SIVVI_MAPSERVER}/0/query"

WIND_TERMS = (
    "eolico",
    "eolica",
    "aerogenerator",
    "repowering",
    "parco eolico",
    "wind farm",
)
CSV_TIMEOUT = (8, 20)
GIS_TIMEOUT = (8, 20)
GIS_BATCH_SIZE = 200


class SiciliaWindAgent(BaseWindAgent):
    """Wind adaptation of pv_agent_mvp's Sicilia collector.

    Primary path: official regional CSV.
    Fallback path: official SI-VVI ArcGIS layer already used by pv_agent_mvp
    for GIS enrichment. The fallback activates when the CSV is unavailable or
    yields no wind records, without converting channel availability into an
    automatic canonical update.
    """

    agent_name = "institutional_watch"
    source_name = "Regione Sicilia SI-VVI"
    base_url = SOURCE_URL

    @staticmethod
    def _clean(value: object) -> str:
        return re.sub(r"\s+", " ", str(value or "")).strip()

    @classmethod
    def _column(cls, value: object) -> str:
        text = cls._clean(value).lower()
        text = re.sub(r"[^a-z0-9]+", "_", text).strip("_")
        return text

    @classmethod
    def _is_wind(cls, text: str) -> bool:
        lowered = cls._clean(text).lower()
        return any(term in lowered for term in WIND_TERMS)

    @classmethod
    def _power_mw(cls, text: str) -> float | None:
        for match in re.finditer(
            r"(?<![\d.,])([0-9]+(?:[.\s][0-9]{3})*(?:,[0-9]+)?|[0-9]+(?:\.[0-9]+)?)\s*MW\b",
            text,
            flags=re.I,
        ):
            raw = match.group(1).replace(" ", "")
            if "," in raw:
                raw = raw.replace(".", "").replace(",", ".")
            try:
                value = float(raw)
            except ValueError:
                continue
            if 0 < value < 5000:
                return value
        return None

    @classmethod
    def _province(cls, text: str) -> str | None:
        match = re.search(r"\(([A-Z]{2})\)", text)
        if match:
            return match.group(1)
        for code in ("AG", "CL", "CT", "EN", "ME", "PA", "RG", "SR", "TP"):
            if re.search(rf"\b{code}\b", text):
                return code
        return None

    @classmethod
    def _municipality(cls, text: str) -> str | None:
        for pattern in (
            r"comune\s+di\s+([A-ZÀ-Ú][A-Za-zÀ-Úà-ú'’\- ]+?)(?:\s*\([A-Z]{2}\)|,|;|\.|\s+e\s+|$)",
            r"comune\s*:?\s*([A-ZÀ-Ú][A-Za-zÀ-Úà-ú'’\- ]+?)(?:\s+provincia|\s*\([A-Z]{2}\)|,|;|\||$)",
        ):
            match = re.search(pattern, text, flags=re.I)
            if match:
                value = cls._clean(match.group(1)).strip(" .,:;")
                if value:
                    return value
        return None

    @staticmethod
    def _external_id(code: str | None, title: str, detail_url: str) -> str:
        if code:
            return f"SICILIA-SIVVI-{code}"
        raw = title + "|" + detail_url
        return "SICILIA-SIVVI-" + hashlib.sha1(raw.encode("utf-8")).hexdigest()[:18]

    def _findings_from_csv(self) -> list[AgentFinding]:
        response = self.session.get(
            CSV_URL,
            timeout=CSV_TIMEOUT,
            headers={"User-Agent": "Wind-Radar-Agent/0.6"},
        )
        response.raise_for_status()
        text = response.content.decode("utf-8-sig", errors="replace")

        reader = csv.DictReader(
            io.StringIO(text),
            delimiter=";",
            quotechar='"',
            escapechar="\\",
            doublequote=True,
        )

        findings: list[AgentFinding] = []
        seen: set[str] = set()
        for raw_row in reader:
            row = {
                self._column(key): self._clean(value)
                for key, value in raw_row.items()
                if key is not None
            }
            raw_title = self._clean(
                row.get("procedura_progetto_oggetto")
                or row.get("oggetto")
                or row.get("titolo")
                or ""
            )
            title = re.sub(r"https?://\S+", "", raw_title).strip(" ;")
            procedure = self._clean(
                row.get("procedura_tipologia")
                or row.get("tipologia")
                or row.get("procedura")
                or ""
            ) or None
            sector = self._clean(row.get("settore") or "") or None
            searchable = " ".join([title, procedure or "", sector or ""])
            if not title or not self._is_wind(searchable):
                continue

            code = self._clean(row.get("procedura_codice") or row.get("codice") or "") or None
            detail_url = self._clean(row.get("procedura_url") or row.get("url") or "")
            if not detail_url.startswith("http"):
                url_match = re.search(r"https?://[^\s;\"']+", raw_title)
                detail_url = url_match.group(0) if url_match else CSV_URL

            external_id = self._external_id(code, title, detail_url)
            if external_id in seen:
                continue
            seen.add(external_id)

            proponent = self._clean(
                row.get("proponente_progetto")
                or row.get("proponente")
                or ""
            ) or None
            status = self._clean(row.get("stato") or row.get("status") or "") or None

            findings.append(
                AgentFinding(
                    external_id=external_id,
                    source_name=self.source_name,
                    source_url=detail_url,
                    title=title[:900],
                    finding_type="project_source",
                    payload={
                        "project_name": title[:900],
                        "proponent": proponent,
                        "region": "Sicilia",
                        "province": self._province(title),
                        "municipality": self._municipality(title),
                        "power_mw": self._power_mw(title),
                        "procedure": procedure,
                        "status_raw": status,
                        "source_code": code,
                        "latitude": row.get("latitudine") or None,
                        "longitude": row.get("longitudine") or None,
                        "sector": "eolico",
                        "source_grade_ceiling": "A1",
                        "project_specific": True,
                        "source_adapter_origin": "pv_agent_mvp/sicilia.py",
                        "ingestion_path": "official_csv",
                        "enrichment_note": (
                            "Official Sicilia environmental-procedure CSV; "
                            "SI-VVI GIS enrichment remains additive."
                        ),
                    },
                )
            )

        return findings

    def _gis_json(self, params: dict[str, object]) -> dict:
        response = self.session.get(
            SIVVI_LAYER_QUERY,
            params=params,
            timeout=GIS_TIMEOUT,
            headers={"User-Agent": "Wind-Radar-Agent/0.6"},
        )
        response.raise_for_status()
        data = response.json()
        if data.get("error"):
            raise RuntimeError(f"SI-VVI MapServer error: {data['error']}")
        return data

    @staticmethod
    def _gis_detail_url(id_value: object, codproc_value: object) -> str:
        where_parts: list[str] = []
        if id_value not in (None, ""):
            where_parts.append(f"id={int(id_value)}")
        if codproc_value not in (None, ""):
            where_parts.append(f"codproc={int(codproc_value)}")
        query = urlencode(
            {
                "f": "pjson",
                "where": " AND ".join(where_parts) if where_parts else "1=0",
                "outFields": "*",
                "returnGeometry": "true",
                "outSR": 4326,
            }
        )
        return f"{SIVVI_LAYER_QUERY}?{query}"

    def _findings_from_gis(self, fallback_reason: str | None) -> list[AgentFinding]:
        ids_data = self._gis_json(
            {
                "f": "json",
                "where": "1=1",
                "returnIdsOnly": "true",
            }
        )
        object_ids = ids_data.get("objectIds") or []

        findings: list[AgentFinding] = []
        seen: set[str] = set()
        for start in range(0, len(object_ids), GIS_BATCH_SIZE):
            batch = object_ids[start : start + GIS_BATCH_SIZE]
            data = self._gis_json(
                {
                    "f": "json",
                    "objectIds": ",".join(str(value) for value in batch),
                    "outFields": "id,codproc,oggetto,procedura,proponente,settore",
                    "returnGeometry": "true",
                    "outSR": 4326,
                }
            )

            for feature in data.get("features") or []:
                attrs = feature.get("attributes") or {}
                geometry = feature.get("geometry") or {}
                title = self._clean(attrs.get("oggetto"))
                procedure = self._clean(attrs.get("procedura")) or None
                sector = self._clean(attrs.get("settore")) or None
                searchable = " ".join([title, procedure or "", sector or ""])
                if not title or not self._is_wind(searchable):
                    continue

                id_value = attrs.get("id")
                codproc_value = attrs.get("codproc")
                code_value = codproc_value if codproc_value not in (None, "") else id_value
                code = str(code_value) if code_value not in (None, "") else None
                detail_url = self._gis_detail_url(id_value, codproc_value)
                external_id = self._external_id(code, title, detail_url)
                if external_id in seen:
                    continue
                seen.add(external_id)

                findings.append(
                    AgentFinding(
                        external_id=external_id,
                        source_name=self.source_name,
                        source_url=detail_url,
                        title=title[:900],
                        finding_type="project_source",
                        payload={
                            "project_name": title[:900],
                            "proponent": self._clean(attrs.get("proponente")) or None,
                            "region": "Sicilia",
                            "province": self._province(title),
                            "municipality": self._municipality(title),
                            "power_mw": self._power_mw(title),
                            "procedure": procedure,
                            "status_raw": None,
                            "source_code": code,
                            "sivvi_id": id_value,
                            "sivvi_codproc": codproc_value,
                            "latitude": geometry.get("y"),
                            "longitude": geometry.get("x"),
                            "sector": "eolico",
                            "source_grade_ceiling": "A1",
                            "project_specific": True,
                            "source_adapter_origin": "pv_agent_mvp/sicilia.py",
                            "ingestion_path": "official_sivvi_mapserver_fallback",
                            "fallback_reason": fallback_reason,
                            "enrichment_note": (
                                "Official SI-VVI ArcGIS layer fallback reused from "
                                "pv_agent_mvp when the regional CSV is unavailable."
                            ),
                        },
                    )
                )

        return findings

    def fetch(self) -> list[AgentFinding]:
        csv_error: str | None = None
        try:
            csv_findings = self._findings_from_csv()
            if csv_findings:
                return csv_findings
        except Exception as exc:
            csv_error = f"{type(exc).__name__}: {exc}"

        try:
            return self._findings_from_gis(csv_error or "CSV returned no wind findings")
        except Exception as gis_exc:
            if csv_error is None:
                raise
            raise RuntimeError(
                "Sicilia official CSV and SI-VVI MapServer fallback both failed; "
                f"csv={csv_error}; gis={type(gis_exc).__name__}: {gis_exc}"
            ) from gis_exc
