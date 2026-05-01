from __future__ import annotations

import csv
import hashlib
import io
import json
import re
import unicodedata
from pathlib import Path
from urllib.parse import urlparse, parse_qs

import requests
from bs4 import BeautifulSoup

from app.collectors.base import BaseCollector, CollectorResult


CSV_URL = (
    "https://dati.regione.sicilia.it/download/dataset/"
    "progetti-sottoposti-valutazione-ambientale/filesystem/"
    "progetti-sottoposti-valutazione-ambientale_csv.csv"
)

SOURCE_URL = "https://si-vvi.regione.sicilia.it/viavas/"

# Servizi GIS usati dalla mappa pubblica SI-VVI.
# Obiettivo: usare ID/codproc e geografia ufficiale per arricchire comuni/province,
# riducendo le inferenze fragili da titolo/testo.
SIVVI_MAPSERVER = "https://map.sitr.regione.sicilia.it/orbs/rest/services/sivvi/procedure_valutazione_ambientale/MapServer"
COMUNI_LAYER = "https://map.sitr.regione.sicilia.it/gis/rest/services/catasto/cartografia_catastale/MapServer/9"
GIS_ENRICHMENT_ENABLED = True

COMMERCIAL_PV_KEYWORDS = [
    "fotovoltaico",
    "agro-fotovoltaico",
    "agrofotovoltaico",
    "agrivoltaico",
    "agrovoltaico",
    "impianto fv",
    "parco fv",
    "fv ",
    " fv",
    "solare fotovoltaico",
]

BESS_KEYWORDS = [
    "accumulo",
    "storage",
    "bess",
]

EXCLUDE_KEYWORDS = [
    "pensilina",
    "pensiline",
    "tettoia",
    "copertura",
    "coperture",
    "fabbricato",
    "edificio",
    "capannone",
    "scuola",
    "ospedale",
    "ripristino dell'impianto fotovoltaico esistente",
    "ripristino dell’impianto fotovoltaico esistente",
]

PROVINCE_CODES = {
    "AG",
    "CL",
    "CT",
    "EN",
    "ME",
    "PA",
    "RG",
    "SR",
    "TP",
}


# Mappa prudente: serve solo come fallback quando il testo espone il comune senza sigla provincia.
# Non deve sostituire l'estrazione esplicita dal dettaglio del portale.
SICILIA_MUNICIPALITY_TO_PROVINCE = {
    # AG
    "agrigento": "AG",
    "bivona": "AG",
    "canicatti": "AG",
    "canicattini bagni": "SR",
    "castrofilippo": "AG",
    "licata": "AG",
    "menfi": "AG",
    "naro": "AG",
    "palma di montechiaro": "AG",
    "racalmuto": "AG",
    "raffadali": "AG",
    "ravanusa": "AG",
    "ribera": "AG",
    "sambuca di sicilia": "AG",
    "sciacca": "AG",

    # CL
    "acquaviva platani": "CL",
    "butera": "CL",
    "caltanissetta": "CL",
    "delia": "CL",
    "gela": "CL",
    "mazzarino": "CL",
    "mussomeli": "CL",
    "niscemi": "CL",
    "riesi": "CL",
    "san cataldo": "CL",
    "sommatino": "CL",
    "villalba": "CL",

    # CT
    "adrano": "CT",
    "belpasso": "CT",
    "biancavilla": "CT",
    "caltagirone": "CT",
    "catania": "CT",
    "licodia eubea": "CT",
    "mazzarrone": "CT",
    "mineo": "CT",
    "misterbianco": "CT",
    "motta sant anastasia": "CT",
    "motta santanastasia": "CT",
    "paterno": "CT",
    "paternò": "CT",
    "ramacca": "CT",
    "raddusa": "CT",
    "randazzo": "CT",
    "scordia": "CT",

    # EN
    "agira": "EN",
    "aidone": "EN",
    "assoro": "EN",
    "bararrafranca": "EN",
    "barotta": "EN",
    "barrafranca": "EN",
    "calascibetta": "EN",
    "centuripe": "EN",
    "enna": "EN",
    "leonforte": "EN",
    "nicosia": "EN",
    "piazza armerina": "EN",
    "regalbuto": "EN",
    "troina": "EN",
    "valguarnera caropepe": "EN",

    # ME
    "barcellona pozzo di gotto": "ME",
    "milazzo": "ME",
    "messina": "ME",
    "patti": "ME",
    "san filippo del mela": "ME",

    # PA
    "bagheria": "PA",
    "bisacquino": "PA",
    "caccamo": "PA",
    "castellana sicula": "PA",
    "cefalu": "PA",
    "corleone": "PA",
    "monreale": "PA",
    "palermo": "PA",
    "petralia sottana": "PA",
    "termini imerese": "PA",
    "ventimiglia di sicilia": "PA",

    # RG
    "acate": "RG",
    "chiaramonte gulfi": "RG",
    "comiso": "RG",
    "ispica": "RG",
    "modica": "RG",
    "pozzallo": "RG",
    "ragusa": "RG",
    "scicli": "RG",
    "vittoria": "RG",

    # SR
    "augusta": "SR",
    "avola": "SR",
    "carlentini": "SR",
    "floridia": "SR",
    "francofonte": "SR",
    "lentini": "SR",
    "melilli": "SR",
    "noto": "SR",
    "pachino": "SR",
    "priolo gargallo": "SR",
    "rosolini": "SR",
    "siracusa": "SR",
    "solarino": "SR",
    "sortino": "SR",

    # TP
    "alcamo": "TP",
    "calatafimi segesta": "TP",
    "campobello di mazara": "TP",
    "campofelice di fitalia": "PA",
    "ciminna": "PA",
    "mezzojuso": "PA",
    "castelvetrano": "TP",
    "castel di iudica": "CT",
    "marsala": "TP",
    "mazara del vallo": "TP",
    "partanna": "TP",
    "salemi": "TP",
    "trapani": "TP",
}


def _gis_clean_text(value) -> str:
    return re.sub(r"\s+", " ", str(value or "")).strip()


def _gis_norm(value) -> str:
    value = _gis_clean_text(value).lower()
    value = unicodedata.normalize("NFKD", value)
    value = "".join(ch for ch in value if not unicodedata.combining(ch))
    value = re.sub(r"[^a-z0-9]+", " ", value)
    return re.sub(r"\s+", " ", value).strip()


class SiciliaGISEnricher:
    """
    Arricchimento Sicilia basato sulla mappa pubblica SI-VVI.

    Strategia:
    - match record collector -> layer Procedure tramite id/codproc;
    - se disponibile, punto layer 0 -> intersezione col layer Comuni;
    - se il punto non basta, fallback su poligoni Area_Progetto / Ingombro_Completo.

    Nota: l'arricchimento NON deve bloccare il collector. In caso di errore ritorna vuoto.
    """

    def __init__(self, session: requests.Session, debug_base: Path | None = None):
        self.session = session
        self.debug_base = debug_base or Path("/app/reports/debug_sicilia")
        self.by_pair: dict[tuple[int, int], dict] = {}
        self.by_id: dict[int, dict] = {}
        self.by_codproc: dict[int, list[dict]] = {}
        self.comune_field: str | None = None
        self.provincia_field: str | None = None
        self.loaded = False
        self.disabled = False
        self.point_cache: dict[tuple[float, float], tuple[list[str], list[str]]] = {}
        self.polygon_cache: dict[tuple[int, int], tuple[list[str], list[str]]] = {}

    def enrich(self, id_value: int | None, codproc_value: int | None, title: str) -> dict:
        if self.disabled or (id_value is None and codproc_value is None):
            return {}

        try:
            self._load_once()
            match, match_mode = self._find_layer0_match(id_value, codproc_value, title)

            result = {
                "match_mode": match_mode,
                "map_source": "",
                "proponent": "",
                "province": "",
                "municipalities": [],
                "x": None,
                "y": None,
                "oggetto": "",
            }

            if match:
                result["proponent"] = _gis_clean_text(match.get("proponente"))
                result["oggetto"] = _gis_clean_text(match.get("oggetto"))
                result["x"] = match.get("x")
                result["y"] = match.get("y")

                comuni, province = self._location_from_point(match.get("x"), match.get("y"))
                if comuni or province:
                    result["municipalities"] = comuni
                    result["province"] = ", ".join(province)
                    result["map_source"] = "layer0_point"
                    return result

            polygon_codproc_candidates: list[int] = []
            for value in [codproc_value, id_value]:
                if value is not None and value not in polygon_codproc_candidates:
                    polygon_codproc_candidates.append(value)

            for candidate_codproc in polygon_codproc_candidates:
                for layer_id in [8, 9]:
                    comuni, province = self._location_from_polygons(layer_id, candidate_codproc)
                    if comuni or province:
                        result["municipalities"] = comuni
                        result["province"] = ", ".join(province)
                        result["map_source"] = f"layer{layer_id}_polygon"
                        result["polygon_codproc_used"] = candidate_codproc
                        return result

            return result

        except Exception as exc:
            self.disabled = True
            try:
                self.debug_base.mkdir(parents=True, exist_ok=True)
                (self.debug_base / "gis_enrichment_error.txt").write_text(str(exc), encoding="utf-8")
            except Exception:
                pass
            return {}

    def _load_once(self) -> None:
        if self.loaded:
            return

        self._fetch_layer0_index()
        self.comune_field, self.provincia_field = self._get_comuni_field_names()
        self.loaded = True

    def _get_json(self, url: str, params: dict, timeout: int = 60) -> dict:
        response = self.session.get(
            url,
            params=params,
            timeout=timeout,
            headers={"User-Agent": "Mozilla/5.0 pv-agent"},
        )
        response.raise_for_status()
        data = response.json()
        if "error" in data:
            raise RuntimeError(json.dumps(data["error"], ensure_ascii=False))
        return data

    def _query_arcgis(self, url: str, params: dict) -> list[dict]:
        data = self._get_json(url, params)
        return data.get("features") or []

    def _fetch_layer0_index(self) -> None:
        ids_data = self._get_json(
            f"{SIVVI_MAPSERVER}/0/query",
            {"f": "json", "where": "1=1", "returnIdsOnly": "true"},
        )

        object_ids = ids_data.get("objectIds") or []
        for batch_start in range(0, len(object_ids), 200):
            batch = object_ids[batch_start:batch_start + 200]
            data = self._get_json(
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
                    "oggetto": _gis_clean_text(attrs.get("oggetto")),
                    "procedura": _gis_clean_text(attrs.get("procedura")),
                    "proponente": _gis_clean_text(attrs.get("proponente")),
                    "settore": _gis_clean_text(attrs.get("settore")),
                }

                self.by_pair[(row["id"], row["codproc"])] = row
                self.by_id[row["id"]] = row
                self.by_codproc.setdefault(row["codproc"], []).append(row)

    def _get_comuni_field_names(self) -> tuple[str | None, str | None]:
        meta = self._get_json(COMUNI_LAYER, {"f": "json"})
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

    def _rows_to_location(self, features: list[dict]) -> tuple[list[str], list[str]]:
        comuni: list[str] = []
        province: list[str] = []

        for feature in features:
            attrs = feature.get("attributes") or {}
            comune = _gis_clean_text(attrs.get(self.comune_field)) if self.comune_field else ""
            provincia = _gis_clean_text(attrs.get(self.provincia_field)) if self.provincia_field else ""

            if comune and comune not in comuni:
                comuni.append(comune)
            if provincia and provincia not in province:
                province.append(provincia)

        return comuni, province

    def _location_from_point(self, x, y) -> tuple[list[str], list[str]]:
        if x is None or y is None:
            return [], []

        key = (round(float(x), 7), round(float(y), 7))
        if key in self.point_cache:
            return self.point_cache[key]

        features = self._query_arcgis(
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

        location = self._rows_to_location(features)
        self.point_cache[key] = location
        return location

    def _location_from_polygons(self, layer_id: int, codproc: int) -> tuple[list[str], list[str]]:
        cache_key = (layer_id, int(codproc))
        if cache_key in self.polygon_cache:
            return self.polygon_cache[cache_key]

        comuni_all: list[str] = []
        province_all: list[str] = []

        polygons = self._query_arcgis(
            f"{SIVVI_MAPSERVER}/{layer_id}/query",
            {
                "f": "json",
                "where": f"codproc = {int(codproc)}",
                "outFields": "*",
                "returnGeometry": "true",
                "outSR": 4326,
            },
        )

        for feature in polygons:
            geometry = feature.get("geometry") or {}
            if not geometry:
                continue

            features = self._query_arcgis(
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
            comuni, province = self._rows_to_location(features)
            for comune in comuni:
                if comune not in comuni_all:
                    comuni_all.append(comune)
            for provincia in province:
                if provincia not in province_all:
                    province_all.append(provincia)

        location = (comuni_all, province_all)
        self.polygon_cache[cache_key] = location
        return location

    def _find_layer0_match(self, id_value: int | None, codproc_value: int | None, title: str) -> tuple[dict | None, str]:
        """
        Match robusto tra CSV e layer Procedure.

        Nei dati Sicilia ci sono due numeri:
        - procedura_codice dal CSV;
        - procedura___oggetto_raw dalla URL.

        L'audit GIS ha dimostrato che la combinazione funziona, ma per evitare
        errori di verso proviamo entrambe le coppie prima dei fallback singoli.
        """
        title_norm = _gis_norm(title)

        # 1) Coppie esatte: prima verso normale, poi verso invertito.
        pair_candidates: list[tuple[int, int, str]] = []
        if id_value is not None and codproc_value is not None:
            pair_candidates.append((id_value, codproc_value, "id+codproc"))
            pair_candidates.append((codproc_value, id_value, "id+codproc_reversed"))

        for a, b, mode in pair_candidates:
            if (a, b) in self.by_pair:
                return self.by_pair[(a, b)], mode

        # 2) Fallback su id layer0: prova entrambi i numeri, con titolo se possibile.
        id_candidates: list[tuple[int, str]] = []
        for value, mode in [(id_value, "id"), (codproc_value, "id_from_second_number")]:
            if value is not None:
                id_candidates.append((value, mode))

        for value, mode in id_candidates:
            candidate = self.by_id.get(value)
            if not candidate:
                continue

            oggetto_norm = _gis_norm(candidate.get("oggetto"))
            if title_norm and oggetto_norm and (title_norm in oggetto_norm or oggetto_norm in title_norm):
                return candidate, mode + "+title"

        for value, mode in id_candidates:
            candidate = self.by_id.get(value)
            if candidate:
                return candidate, mode

        # 3) Fallback su codproc layer0: prova entrambi i numeri.
        codproc_candidates: list[tuple[int, str]] = []
        for value, mode in [(codproc_value, "codproc"), (id_value, "codproc_from_first_number")]:
            if value is not None:
                codproc_candidates.append((value, mode))

        for value, mode in codproc_candidates:
            candidates = self.by_codproc.get(value) or []
            if len(candidates) == 1:
                return candidates[0], mode + "_unique"

            for candidate in candidates:
                oggetto_norm = _gis_norm(candidate.get("oggetto"))
                if title_norm and oggetto_norm and (title_norm in oggetto_norm or oggetto_norm in title_norm):
                    return candidate, mode + "+title"

        return None, ""



class SiciliaCollector(BaseCollector):
    source_name = "sicilia"
    base_url = SOURCE_URL

    def _to_int_or_none(self, value) -> int | None:
        if value is None:
            return None
        match = re.search(r"\d+", str(value))
        if not match:
            return None
        try:
            return int(match.group(0))
        except Exception:
            return None

    def _normalize_sicilia_province_value(self, value: str | None) -> str:
        value = self._clean_text(value)
        if not value:
            return ""

        upper = value.upper()
        if upper in PROVINCE_CODES:
            return upper

        mapping = {
            "AGRIGENTO": "AG",
            "CALTANISSETTA": "CL",
            "CATANIA": "CT",
            "ENNA": "EN",
            "MESSINA": "ME",
            "PALERMO": "PA",
            "RAGUSA": "RG",
            "SIRACUSA": "SR",
            "TRAPANI": "TP",
        }

        parts = []
        for item in re.split(r",|;", value):
            key = self._normalize_for_match(item).upper()
            key = key.replace(" ", "")
            mapped = None
            for name, code in mapping.items():
                if key == name.replace(" ", ""):
                    mapped = code
                    break
            if mapped:
                if mapped not in parts:
                    parts.append(mapped)
            else:
                cleaned = item.strip()
                if cleaned and cleaned not in parts:
                    parts.append(cleaned)

        return ", ".join(parts)

    def _apply_gis_enrichment(self, normalized: dict, gis_info: dict) -> dict:
        if not gis_info:
            return normalized

        merged = dict(normalized)
        current_municipalities = merged.get("municipalities") or []
        gis_municipalities = gis_info.get("municipalities") or []
        gis_province = self._normalize_sicilia_province_value(gis_info.get("province"))
        gis_proponent = self._clean_text(gis_info.get("proponent"))
        map_source = self._clean_text(gis_info.get("map_source"))

        # Regola prudente: la mappa completa i vuoti; non sovrascrive localizzazioni
        # già presenti, salvo poligoni ufficiali, che possono aggiungere comuni mancanti.
        if not current_municipalities and gis_municipalities:
            merged["municipalities"] = gis_municipalities
            merged["location_source"] = map_source or "gis"
        elif current_municipalities and gis_municipalities and map_source.endswith("_polygon"):
            merged["municipalities"] = self._merge_lists(current_municipalities, gis_municipalities)
            merged["location_source"] = map_source or "gis"

        if not merged.get("province") and gis_province:
            # Se l'intersezione ritorna più province, manteniamo la stringa separata da virgola.
            # La dashboard la mostra comunque come testo; meglio non buttare informazione.
            merged["province"] = gis_province
            merged["location_source"] = map_source or "gis"

        if not merged.get("proponent") and gis_proponent:
            merged["proponent"] = gis_proponent
            merged["proponent_source"] = "gis_layer0"

        if gis_info.get("x") is not None and not merged.get("longitudine"):
            merged["longitudine"] = gis_info.get("x")
        if gis_info.get("y") is not None and not merged.get("latitudine"):
            merged["latitudine"] = gis_info.get("y")

        if gis_info.get("match_mode"):
            merged["gis_match_mode"] = gis_info.get("match_mode")
        if map_source:
            merged["gis_map_source"] = map_source

        return merged

    def fetch(self) -> list[CollectorResult]:
        debug_base = Path("/app/reports/debug_sicilia")
        debug_base.mkdir(parents=True, exist_ok=True)

        try:
            response = self.session.get(
                CSV_URL,
                timeout=120,
                headers={"User-Agent": "Mozilla/5.0 pv-agent"},
            )
            response.raise_for_status()
            text = response.content.decode("utf-8-sig", errors="replace")
        except Exception as exc:
            self._write_text(debug_base / "download_error.txt", str(exc))
            return []

        self._write_text(debug_base / "sicilia_raw.csv", text[:800000])

        rows = self._read_csv(text, debug_base)
        if not rows:
            self._write_json(
                debug_base / "rows_empty.json",
                {"note": "Nessuna riga letta dal CSV Sicilia"},
            )
            return []

        self._write_json(debug_base / "sample_rows.json", rows[:20])
        self._write_json(
            debug_base / "columns.json",
            {"columns": list(rows[0].keys()) if rows else []},
        )

        gis_enricher = SiciliaGISEnricher(self.session, debug_base) if GIS_ENRICHMENT_ENABLED else None
        gis_stats = {
            "enabled": bool(gis_enricher),
            "matched": 0,
            "location_completed": 0,
            "proponent_completed": 0,
            "errors_disabled": 0,
            "by_source": {},
        }

        results: list[CollectorResult] = []
        matched_rows: list[dict] = []
        excluded_rows: list[dict] = []
        seen_keys: set[str] = set()

        for row in rows:
            normalized = self._normalize_row(row)
            if not normalized:
                continue

            title = normalized["title"]

            if not self._is_commercial_pv_project(title):
                if self._contains_any(title, COMMERCIAL_PV_KEYWORDS + BESS_KEYWORDS):
                    excluded_rows.append(row)
                continue

            detail_url = normalized.get("detail_url") or CSV_URL
            detail_info = self._fetch_detail_info(detail_url, debug_base)
            normalized = self._merge_detail_info(normalized, detail_info)

            if gis_enricher:
                before_has_location = bool(normalized.get("province")) and bool(normalized.get("municipalities"))
                before_has_proponent = bool(normalized.get("proponent"))
                id_value = self._to_int_or_none(normalized.get("codice"))
                codproc_value = self._to_int_or_none(self._extract_raw_id_from_url(normalized.get("detail_url") or ""))
                gis_info = gis_enricher.enrich(id_value, codproc_value, normalized.get("title") or "")
                if gis_info.get("match_mode"):
                    gis_stats["matched"] += 1
                source = gis_info.get("map_source") or "none"
                gis_stats["by_source"][source] = gis_stats["by_source"].get(source, 0) + 1
                normalized = self._apply_gis_enrichment(normalized, gis_info)
                after_has_location = bool(normalized.get("province")) and bool(normalized.get("municipalities"))
                after_has_proponent = bool(normalized.get("proponent"))
                if after_has_location and not before_has_location:
                    gis_stats["location_completed"] += 1
                if after_has_proponent and not before_has_proponent:
                    gis_stats["proponent_completed"] += 1
                if getattr(gis_enricher, "disabled", False):
                    gis_stats["errors_disabled"] += 1

            # Dopo l'arricchimento dal dettaglio/GIS, il titolo può essere più completo.
            title = normalized["title"]

            status_raw = (
                detail_info.get("status_raw")
                or normalized.get("status_raw")
                or "Conclusa"
            )

            external_id = self._build_external_id(normalized)
            if external_id in seen_keys:
                continue
            seen_keys.add(external_id)

            matched_rows.append(row)

            results.append(
                CollectorResult(
                    external_id=external_id,
                    source_url=detail_url,
                    title=title[:250],
                    payload={
                        "title": title[:900],
                        "project_name": title[:900],
                        "proponent": normalized.get("proponent"),
                        "status_raw": status_raw,
                        "region": "Sicilia",
                        "province": normalized.get("province"),
                        "municipalities": normalized.get("municipalities") or [],
                        "power": normalized.get("power"),
                        "project_type_hint": normalized.get("procedure") or "Sicilia VIA/VAS",
                        "procedure": normalized.get("procedure"),
                        "latitudine": normalized.get("latitudine"),
                        "longitudine": normalized.get("longitudine"),
                        "location_source": normalized.get("location_source"),
                        "gis_match_mode": normalized.get("gis_match_mode"),
                        "gis_map_source": normalized.get("gis_map_source"),
                        "proponent_source": normalized.get("proponent_source"),
                    },
                )
            )

        self._write_json(debug_base / "matched_rows_sample.json", matched_rows[:100])
        self._write_json(debug_base / "excluded_rows_sample.json", excluded_rows[:100])
        self._write_json(
            debug_base / "summary.json",
            {
                "used_url": CSV_URL,
                "rows_total": len(rows),
                "matched_rows": len(matched_rows),
                "excluded_pv_like_rows": len(excluded_rows),
                "results": len(results),
                "gis_enrichment": gis_stats,
            },
        )

        return results

    def _read_csv(self, text: str, debug_base: Path) -> list[dict]:
        """
        Il CSV Sicilia contiene almeno una riga formalmente sporca:
        BARRAFRANCA\\"
        Senza escapechar='\\', Python sposta le colonne e manda l'URL nel titolo.
        """
        try:
            reader = csv.DictReader(
                io.StringIO(text),
                delimiter=";",
                quotechar='"',
                escapechar="\\",
                doublequote=True,
            )

            rows: list[dict] = []

            for row in reader:
                clean_row = {}
                for key, value in row.items():
                    if key is None:
                        continue

                    clean_key = self._normalize_column_name(key)
                    clean_value = self._clean_text(value)

                    clean_row[clean_key] = clean_value

                if clean_row:
                    rows.append(clean_row)

            return rows

        except Exception as exc:
            self._write_text(debug_base / "csv_parse_error.txt", str(exc))
            return []

    def _normalize_row(self, row: dict) -> dict | None:
        title = self._clean_text(
            row.get("procedura_progetto_oggetto")
            or row.get("oggetto")
            or row.get("titolo")
            or ""
        )

        if not title:
            return None

        title = self._repair_title(title)

        codice = self._clean_text(
            row.get("procedura_codice")
            or row.get("codice")
            or ""
        )

        detail_url = self._clean_text(
            row.get("procedura_url")
            or row.get("url")
            or ""
        )

        detail_url = self._repair_url(detail_url, title)

        procedure = self._clean_text(
            row.get("procedura_tipologia")
            or row.get("tipologia")
            or row.get("procedura")
            or ""
        )

        proponent = self._clean_text(
            row.get("proponente_progetto")
            or row.get("proponente")
            or ""
        )

        province = self._extract_province(title)
        municipalities = self._extract_municipalities(title)
        power = self._extract_power_text(title)

        return {
            "codice": codice,
            "title": title,
            "detail_url": detail_url,
            "procedure": procedure,
            "proponent": proponent,
            "municipalities": municipalities,
            "province": province,
            "power": power,
            "latitudine": row.get("latitudine"),
            "longitudine": row.get("longitudine"),
            "status_raw": row.get("stato") or row.get("status"),
        }

    def _repair_title(self, title: str) -> str:
        title = self._clean_text(title)

        # Caso CSV sporco: URL finito dentro il titolo.
        title = re.sub(r"https?://\S+", "", title)

        # Se resta un separatore finale sporco.
        title = title.strip(" ;")

        # Quote sporche residue.
        title = title.replace('\\"', '"')
        title = title.replace('""', '"')

        return self._clean_text(title)

    def _repair_url(self, url: str, title: str) -> str:
        url = self._clean_text(url)

        if self._is_valid_url(url):
            return url

        # Caso CSV sporco: URL finito dentro il titolo.
        match = re.search(r"https?://[^\s;\"']+", title or "")
        if match:
            candidate = match.group(0).strip()
            if self._is_valid_url(candidate):
                return candidate

        return CSV_URL

    def _fetch_detail_status(self, url: str, debug_base: Path) -> str | None:
        """
        Compatibilità con vecchie chiamate: ora lo stato viene letto da _fetch_detail_info.
        """
        return self._fetch_detail_info(url, debug_base).get("status_raw")

    def _fetch_detail_info(self, url: str, debug_base: Path) -> dict:
        """
        Legge la pagina di dettaglio Sicilia.
        La vecchia versione usava il dettaglio quasi solo per lo stato; qui lo usiamo anche
        per recuperare titolo completo, comune, provincia, potenza e proponente quando il CSV
        è troncato o povero.
        """
        if not self._is_valid_url(url) or url == CSV_URL:
            return {}

        try:
            response = self.session.get(
                url,
                timeout=45,
                headers={"User-Agent": "Mozilla/5.0 pv-agent"},
            )

            if response.status_code != 200:
                return {}

            html = response.text or ""
            soup = BeautifulSoup(html, "html.parser")
            plain = self._clean_text(soup.get_text(" ", strip=True))
            line_text = soup.get_text("\n", strip=True)
            lines = [self._clean_text(x) for x in line_text.splitlines() if self._clean_text(x)]
            relevant_text = self._extract_relevant_detail_text(lines, plain)

            title = self._extract_detail_title(soup, relevant_text or plain)
            status_raw = self._extract_status_from_lines(lines, plain)
            proponent = self._extract_proponent_from_text(relevant_text or plain)
            province = self._extract_province(relevant_text or plain)
            municipalities = self._extract_municipalities(relevant_text or plain)
            power = self._extract_power_text(relevant_text or plain)

            return {
                "title": title,
                "plain_text_sample": (relevant_text or plain)[:5000],
                "status_raw": status_raw,
                "proponent": proponent,
                "province": province,
                "municipalities": municipalities,
                "power": power,
            }

        except Exception as exc:
            safe_name = self._safe_filename(url)
            self._write_text(debug_base / f"detail_error_{safe_name}.txt", str(exc))
            return {}

    def _merge_detail_info(self, normalized: dict, detail_info: dict) -> dict:
        if not detail_info:
            return normalized

        merged = dict(normalized)

        current_title = self._clean_text(merged.get("title"))
        detail_title = self._clean_text(detail_info.get("title"))
        detail_plain = self._clean_text(detail_info.get("plain_text_sample"))

        # Usa il titolo dettaglio solo se è davvero più informativo, non è solo una ripetizione
        # del titolo CSV e non contiene code tecniche del portale.
        if (
            detail_title
            and len(detail_title) > len(current_title) + 25
            and self._contains_any(detail_title, COMMERCIAL_PV_KEYWORDS + BESS_KEYWORDS)
            and not self._looks_like_page_chrome(detail_title)
            and not self._same_title_core(current_title, detail_title)
        ):
            merged["title"] = detail_title

        combined = self._clean_text(" ".join([
            current_title,
            detail_title,
            detail_plain[:4000],
        ]))

        municipalities = self._merge_lists(
            merged.get("municipalities") or [],
            detail_info.get("municipalities") or [],
            self._extract_municipalities(combined),
        )

        municipalities = self._finalize_municipalities(municipalities)

        merged["municipalities"] = municipalities
        merged["province"] = (
            merged.get("province")
            or detail_info.get("province")
            or self._extract_province(combined)
            or self._infer_province_from_municipalities(municipalities)
        )

        merged["power"] = merged.get("power") or detail_info.get("power") or self._extract_power_text(combined)
        merged["proponent"] = merged.get("proponent") or detail_info.get("proponent")
        merged["status_raw"] = detail_info.get("status_raw") or merged.get("status_raw")

        return merged

    def _extract_relevant_detail_text(self, lines: list[str], plain: str) -> str:
        """
        Riduce il testo della pagina ai soli blocchi utili.
        Evita che footer/header del portale finiscano nei comuni.
        """
        useful: list[str] = []
        seen: set[str] = set()

        keywords = [
            "oggetto",
            "progetto",
            "impianto",
            "fotovoltaico",
            "agrivoltaico",
            "agrovoltaico",
            "comune",
            "comuni",
            "territorio",
            "localizzazione",
            "ubicazione",
            "località",
            "localita",
            "contrada",
            "c.da",
            "proponente",
            "potenza",
            "provincia",
        ]

        for idx, line in enumerate(lines):
            norm = self._normalize_for_match(line)
            if not norm:
                continue

            if any(k in norm for k in [self._normalize_for_match(x) for x in keywords]):
                # Prende anche la riga successiva, utile quando il portale usa label/valore su righe separate.
                chunk = " ".join(lines[idx:idx + 2])
                chunk = self._clean_text(chunk)

                if self._looks_like_page_chrome(chunk):
                    continue

                key = self._normalize_for_match(chunk)
                if key not in seen:
                    seen.add(key)
                    useful.append(chunk)

        if useful:
            return self._clean_text(" ".join(useful[:30]))

        return plain[:3000]

    def _extract_detail_title(self, soup: BeautifulSoup, plain: str) -> str | None:
        candidates: list[str] = []

        for selector in ["h1", "h2", "h3", ".title", ".titolo", ".page-title"]:
            for node in soup.select(selector):
                text = self._clean_text(node.get_text(" ", strip=True))
                if text:
                    candidates.append(text)

        # Fallback: cerca frasi lunghe contenenti fotovoltaico/agrivoltaico.
        for match in re.finditer(
            r"((?:progetto|realizzazione|impianto|parco|centrale)[^.]{40,900}(?:fotovoltaic|agrivoltaic|agrovoltaic)[^.]{0,500})",
            plain,
            flags=re.IGNORECASE,
        ):
            candidates.append(self._clean_text(match.group(1)))

        for candidate in candidates:
            if self._looks_like_page_chrome(candidate):
                continue
            if len(candidate) >= 35 and self._contains_any(candidate, COMMERCIAL_PV_KEYWORDS + BESS_KEYWORDS):
                return candidate

        return None

    def _extract_status_from_lines(self, lines: list[str], plain: str) -> str | None:
        for line in lines:
            normalized = self._normalize_for_match(line)

            if normalized in {"conclusa", "concluso"}:
                return "Conclusa"

            if "conclusa |" in normalized or "concluso |" in normalized:
                return "Conclusa"

            if normalized in {"in corso", "avviata", "avviato"}:
                return "In corso"

            if "archiviata" in normalized or "archiviato" in normalized:
                return "Archiviata"

        if "Conclusa |" in plain or "Concluso |" in plain:
            return "Conclusa"

        return None

    def _extract_proponent_from_text(self, text: str) -> str | None:
        if not text:
            return None

        patterns = [
            r"\bProponente\s*[:\-]\s*(.+?)(?:\s+(?:Oggetto|Procedura|Localizzazione|Comune|Data|Stato)\b|$)",
            r"\bDitta\s+proponente\s*[:\-]\s*(.+?)(?:\s+(?:Oggetto|Procedura|Localizzazione|Comune|Data|Stato)\b|$)",
            r"\bSociet[aà]\s+proponente\s*[:\-]\s*(.+?)(?:\s+(?:Oggetto|Procedura|Localizzazione|Comune|Data|Stato)\b|$)",
        ]

        for pattern in patterns:
            match = re.search(pattern, text, flags=re.IGNORECASE)
            if match:
                value = self._clean_proponent(match.group(1))
                if value:
                    return value

        return None

    def _clean_proponent(self, value: str) -> str | None:
        value = self._clean_text(value)
        value = re.split(
            r"\s+(?:Oggetto|Procedura|Localizzazione|Comune|Data|Stato|Documentazione)\b",
            value,
            maxsplit=1,
            flags=re.IGNORECASE,
        )[0]
        value = value.strip(" .,:;-")

        if not value or len(value) > 180:
            return None

        bad = ["sistema", "regione siciliana", "valutazione ambientale", "procedura"]
        norm = self._normalize_for_match(value)
        if any(x in norm for x in bad):
            return None

        return value

    def _looks_like_page_chrome(self, value: str) -> bool:
        norm = self._normalize_for_match(value)
        bad = [
            "regione siciliana",
            "valutazione ambientale",
            "assessorato",
            "dipartimento",
            "procedura elenco",
            "homepage",
            "accesso",
            "privacy",
        ]
        return any(x in norm for x in bad)

    def _merge_lists(self, *lists: list[str]) -> list[str]:
        found: list[str] = []
        seen: set[str] = set()

        for values in lists:
            for value in values or []:
                clean = self._clean_municipality(value)
                if not clean:
                    continue
                key = self._normalize_for_match(clean)
                if key in seen:
                    continue
                seen.add(key)
                found.append(clean)

        return found[:10]

    def _infer_province_from_municipalities(self, municipalities: list[str]) -> str | None:
        for municipality in municipalities or []:
            key = self._normalize_for_match(municipality)
            if key in SICILIA_MUNICIPALITY_TO_PROVINCE:
                return SICILIA_MUNICIPALITY_TO_PROVINCE[key]
        return None

    def _is_commercial_pv_project(self, title: str) -> bool:
        lowered = f" {self._normalize_for_match(title)} "

        has_core_pv = any(k in lowered for k in COMMERCIAL_PV_KEYWORDS)
        has_bess = any(k in lowered for k in BESS_KEYWORDS)

        if not has_core_pv and not has_bess:
            return False

        if any(k in lowered for k in EXCLUDE_KEYWORDS):
            return False

        # Tiene solo impianti/parchi/progetti energetici, evita citazioni marginali.
        strong_terms = [
            "impianto",
            "parco",
            "centrale",
            "produzione di energia",
            "agro",
            "agrivoltaico",
            "agrovoltaico",
            "revamping",
        ]

        return any(term in lowered for term in strong_terms)

    def _extract_power_text(self, text: str) -> str | None:
        if not text:
            return None

        value = self._clean_text(text)

        patterns = [
            r"potenza\s+(?:complessiva\s+)?(?:nominale\s+)?(?:di\s+picco\s+)?(?:pari\s+a\s+|di\s+)?([0-9][0-9\.\,]*)\s*(mw[p]?|kw[p]?)",
            r"da\s+([0-9][0-9\.\,]*)\s*(mw[p]?|kw[p]?)",
            r"([0-9][0-9\.\,]*)\s*(mw[p]?|kw[p]?)",
        ]

        for pattern in patterns:
            match = re.search(pattern, value, flags=re.IGNORECASE)
            if match:
                number = match.group(1)
                unit = match.group(2).upper()
                return f"{number} {unit}"

        return None

    def _extract_province(self, text: str) -> str | None:
        if not text:
            return None

        matches = re.findall(r"\(([A-Z]{2})\)", text.upper())
        for match in matches:
            if match in PROVINCE_CODES:
                return match

        matches = re.findall(r"\b(AG|CL|CT|EN|ME|PA|RG|SR|TP)\b", text.upper())
        for match in matches:
            if match in PROVINCE_CODES:
                return match

        return None

    def _same_title_core(self, a: str, b: str) -> bool:
        na = self._normalize_for_match(a)
        nb = self._normalize_for_match(b)
        if not na or not nb:
            return False

        # Se uno contiene l'altro, il dettaglio spesso ha solo duplicato il titolo CSV.
        return na in nb or nb in na

    def _finalize_municipalities(self, values: list[str]) -> list[str]:
        cleaned: list[str] = []
        seen: set[str] = set()

        # Primo passaggio: pulizia forte e deduplica.
        for value in values or []:
            comune = self._clean_municipality(value)
            if not comune:
                continue

            key = self._normalize_for_match(comune)
            if key in seen:
                continue

            seen.add(key)
            cleaned.append(comune)

        # Secondo passaggio: se esiste "Augusta" e "Melilli", elimina "Augusta E Melilli".
        single_keys = {self._normalize_for_match(x) for x in cleaned}
        final: list[str] = []

        for comune in cleaned:
            norm = self._normalize_for_match(comune)

            if " e " in f" {norm} ":
                parts = [p.strip() for p in norm.split(" e ") if p.strip()]
                if parts and all(part in single_keys for part in parts):
                    continue

            # Scarta località/contrade sfuggite alla prima pulizia.
            if norm.startswith(("da ", "c da ", "contrada ", "localita ")):
                continue

            final.append(comune)

        return final[:10]

    def _scan_known_municipalities(self, text: str) -> list[str]:
        """
        Fallback prudente: cerca comuni siciliani noti solo quando nel testo ci sono
        marcatori territoriali. Prima elimina le frasi "provincia di X", altrimenti
        capoluoghi come Palermo/Ragusa/Siracusa vengono falsamente aggiunti come comuni.
        """
        norm = self._normalize_for_match(text)
        if not norm:
            return []

        location_markers = [
            "comune",
            "comuni",
            "territorio",
            "territori",
            "sito in",
            "sita in",
            "ubicato",
            "localizzato",
            "da realizzarsi",
            "da realizzare",
        ]

        if not any(marker in norm for marker in location_markers):
            return []

        province_names = [
            "agrigento",
            "caltanissetta",
            "catania",
            "enna",
            "messina",
            "palermo",
            "ragusa",
            "siracusa",
            "trapani",
        ]

        scan_norm = norm

        # Rimuove riferimenti alla provincia: non sono comuni di progetto.
        # Esempi reali da evitare:
        # "Scicli, provincia di Ragusa" -> non aggiungere Ragusa
        # "Francofonte (SR), provincia regionale di Siracusa" -> non aggiungere Siracusa
        # "Mezzojuso e Ciminna, provincia di Palermo" -> non aggiungere Palermo
        # "Libero Consorzio Comunale di Ragusa" -> non aggiungere Ragusa
        # "Città Metropolitana di Palermo" -> non aggiungere Palermo
        for province in province_names:
            province_re = re.escape(province)

            removal_patterns = [
                rf"\bprovincia\s+(?:regionale\s+)?(?:di|del|della)?\s*{province_re}\b",
                rf"\bprov\.\s*(?:regionale\s+)?(?:di|del|della)?\s*{province_re}\b",
                rf"\blibero\s+consorzio\s+comunale\s+(?:di|del|della)?\s*{province_re}\b",
                rf"\bcitta\s+metropolitana\s+(?:di|del|della)?\s*{province_re}\b",
                rf"\bcittà\s+metropolitana\s+(?:di|del|della)?\s*{province_re}\b",
            ]

            for pattern in removal_patterns:
                scan_norm = re.sub(pattern, " ", scan_norm)

        scan_norm = re.sub(r"\s+", " ", scan_norm).strip()

        found: list[str] = []
        for municipality_norm in sorted(SICILIA_MUNICIPALITY_TO_PROVINCE, key=len, reverse=True):
            if not re.search(rf"\b{re.escape(municipality_norm)}\b", scan_norm):
                continue

            # I capoluoghi di provincia sono ambigui: spesso compaiono come "provincia di X",
            # non come comune dell'impianto. Li accettiamo nel fallback solo se sono dentro
            # una frase territoriale forte senza "provincia/consorzio/città metropolitana" in mezzo.
            if municipality_norm in province_names and not self._has_strong_municipality_context(scan_norm, municipality_norm):
                continue

            title = self._title_case_location(municipality_norm)
            if title not in found:
                found.append(title)

        return found[:10]

    def _has_strong_municipality_context(self, norm_text: str, municipality_norm: str) -> bool:
        """
        Serve per capoluoghi ambigui nel fallback.
        Esempio:
        - "nei comuni di Canicattini Bagni, Siracusa e Noto" => Siracusa è comune.
        - "in comune di Francofonte, provincia regionale di Siracusa" => Siracusa è provincia.
        """
        if not norm_text or not municipality_norm:
            return False

        context_markers = [
            "comune",
            "comuni",
            "territorio",
            "territori",
            "sito in",
            "sita in",
            "ubicato",
            "localizzato",
            "da realizzarsi",
            "da realizzare",
        ]

        municipal_re = re.escape(municipality_norm)

        for marker in context_markers:
            marker_re = re.escape(marker)
            for match in re.finditer(rf"\b{marker_re}\b", norm_text):
                start = match.start()
                window = norm_text[start:start + 180]

                if not re.search(rf"\b{municipal_re}\b", window):
                    continue

                before_municipality = window[:re.search(rf"\b{municipal_re}\b", window).start()]
                if any(bad in before_municipality for bad in ["provincia", "prov ", "consorzio", "citta metropolitana", "città metropolitana"]):
                    continue

                return True

        return False

    def _extract_municipalities(self, text: str) -> list[str]:
        if not text:
            return []

        value = self._clean_text(text)
        found: list[str] = []

        def add_exact(value: str) -> None:
            comune = self._clean_municipality(value)
            if comune and comune not in found:
                found.append(comune)

        # Casi espliciti ad alta affidabilità: "Comune di X (SR)", "in comune di X (SR)".
        # Vanno trattati prima dei pattern generici, per non catturare località successive.
        explicit_patterns = [
            r"\b(?:nel\s+|in\s+|sito\s+nel\s+|sito\s+in\s+|sita\s+nel\s+|sita\s+in\s+)?comune\s+di\s+([A-ZÀ-Ý][A-Za-zÀ-ÿ'’\-\s]{2,55}?)\s*\((AG|CL|CT|EN|ME|PA|RG|SR|TP)\)",
            r"\b(?:nei\s+|in\s+|siti\s+nei\s+|siti\s+in\s+)?comuni\s+di\s+(.{3,180}?)\s*\((AG|CL|CT|EN|ME|PA|RG|SR|TP)\)",
        ]

        for pattern in explicit_patterns:
            for match in re.findall(pattern, value, flags=re.IGNORECASE):
                chunk = match[0] if isinstance(match, tuple) else match
                for part in self._split_municipality_chunk(chunk):
                    add_exact(part)

        def add_chunk(chunk: str) -> None:
            for part in self._split_municipality_chunk(chunk):
                comune = self._clean_municipality(part)
                if comune and comune not in found:
                    found.append(comune)

        # Pattern esplicito: "Comune di X (XX)" / "in comune di X (XX)".
        # Va processato prima del generico "Nome (XX)" per evitare località dopo il comune.
        for match in re.finditer(
            r"\b(?:comune\s+di|comune|in\s+comune\s+di|nel\s+comune\s+di)\s+([A-ZÀ-Ý][A-Za-zÀ-ÿ'’\-\s]{2,55}?)\s*\((AG|CL|CT|EN|ME|PA|RG|SR|TP)\)",
            value,
            flags=re.IGNORECASE,
        ):
            add_chunk(match.group(1))

        # Pattern abbastanza affidabile: "Nome Comune (XX)".
        # Prima puliamo la parte prima della provincia per evitare "KWP da realizzare nel Comune di Augusta".
        for match in re.finditer(
            r"\b([A-ZÀ-Ý][A-Za-zÀ-ÿ'’\-\s]{2,90}?)\s*\((AG|CL|CT|EN|ME|PA|RG|SR|TP)\)",
            value,
            flags=re.IGNORECASE,
        ):
            add_chunk(match.group(1))

        patterns = [
            r"\bnei\s+territori\s+(?:dei\s+)?comuni\s+di\s+(.+?)(?:\s+(?:provincia|e\s+relative|e\s+delle\s+relative|con\s+opere|opere\s+di|c\.da|contrada|localit[aà]|foglio|particella|particelle|potenza|denominato|denominata|codice|societ[aà]\s+proponente)|[.;]|$)",
            r"\bnel\s+territorio\s+(?:del\s+)?comune\s+di\s+(.+?)(?:\s+(?:provincia|e\s+relative|e\s+delle\s+relative|con\s+opere|opere\s+di|c\.da|contrada|localit[aà]|foglio|particella|particelle|potenza|denominato|denominata|codice|societ[aà]\s+proponente)|[.;]|$)",
            r"\bda\s+realizzarsi\s+nei\s+comuni\s+di\s+(.+?)(?:\s+(?:provincia|e\s+relative|e\s+delle\s+relative|con\s+opere|opere\s+di|c\.da|contrada|localit[aà]|foglio|particella|particelle|potenza|denominato|denominata|codice|societ[aà]\s+proponente)|[.;]|$)",
            r"\bda\s+realizzarsi\s+nel\s+comune\s+di\s+(.+?)(?:\s+(?:provincia|e\s+relative|e\s+delle\s+relative|con\s+opere|opere\s+di|c\.da|contrada|localit[aà]|foglio|particella|particelle|potenza|denominato|denominata|codice|societ[aà]\s+proponente)|[.;]|$)",
            r"\bda\s+realizzare\s+nel\s+comune\s+di\s+(.+?)(?:\s+(?:provincia|e\s+relative|e\s+delle\s+relative|con\s+opere|opere\s+di|c\.da|contrada|localit[aà]|foglio|particella|particelle|potenza|denominato|denominata|codice|societ[aà]\s+proponente)|[.;]|$)",
            r"\bsito\s+in\s+comune\s+di\s+(.+?)(?:\s+(?:provincia|e\s+relative|e\s+delle\s+relative|con\s+opere|opere\s+di|c\.da|contrada|localit[aà]|foglio|particella|particelle|potenza|denominato|denominata|codice|societ[aà]\s+proponente)|[.;]|$)",
            r"\bsito\s+nel\s+comune\s+di\s+(.+?)(?:\s+(?:provincia|e\s+relative|e\s+delle\s+relative|con\s+opere|opere\s+di|c\.da|contrada|localit[aà]|foglio|particella|particelle|potenza|denominato|denominata|codice|societ[aà]\s+proponente)|[.;]|$)",
            r"\bin\s+comune\s+di\s+(.+?)(?:\s+(?:provincia|e\s+relative|e\s+delle\s+relative|con\s+opere|opere\s+di|c\.da|contrada|localit[aà]|foglio|particella|particelle|potenza|denominato|denominata|codice|societ[aà]\s+proponente)|[.;]|$)",
            r"\bnei\s+comuni\s+di\s+(.+?)(?:\s+(?:provincia|e\s+relative|e\s+delle\s+relative|con\s+opere|opere\s+di|c\.da|contrada|localit[aà]|foglio|particella|particelle|potenza|denominato|denominata|codice|societ[aà]\s+proponente)|[.;]|$)",
            r"\bnel\s+comune\s+di\s+(.+?)(?:\s+(?:provincia|e\s+relative|e\s+delle\s+relative|con\s+opere|opere\s+di|c\.da|contrada|localit[aà]|foglio|particella|particelle|potenza|denominato|denominata|codice|societ[aà]\s+proponente)|[.;]|$)",
            r"\bcomuni\s+di\s+(.+?)(?:\s+(?:provincia|e\s+relative|e\s+delle\s+relative|con\s+opere|opere\s+di|c\.da|contrada|localit[aà]|foglio|particella|particelle|potenza|denominato|denominata|codice|societ[aà]\s+proponente)|[.;]|$)",
            r"\bcomune\s+di\s+(.+?)(?:\s+(?:provincia|e\s+relative|e\s+delle\s+relative|con\s+opere|opere\s+di|c\.da|contrada|localit[aà]|foglio|particella|particelle|potenza|denominato|denominata|codice|societ[aà]\s+proponente)|[.;]|$)",
        ]

        for pattern in patterns:
            for match in re.findall(pattern, value, flags=re.IGNORECASE):
                add_chunk(match)

        # Fallback finale: se il testo contiene marcatori territoriali, prova a riconoscere
        # comuni siciliani noti rimasti nel titolo/dettaglio.
        for comune in self._scan_known_municipalities(value):
            if comune not in found:
                found.append(comune)

        return found[:10]

    def _split_municipality_chunk(self, chunk: str) -> list[str]:
        chunk = self._clean_text(chunk)

        # Elimina sigle provincia e code del portale.
        chunk = re.sub(r"\((AG|CL|CT|EN|ME|PA|RG|SR|TP)\)", " ", chunk, flags=re.IGNORECASE)
        chunk = re.split(
            r"\b(?:cod\.?|codice|regione\s+siciliana|portale\s+valutazioni|urbanistiche|societ[aà]\s+proponente|proponente|c\.da|contrada|localit[aà]|distinto|distin|foglio|particella|particelle|snc|elettrodotto|cavidotto|rtn|stazione|cabina)\b",
            chunk,
            maxsplit=1,
            flags=re.IGNORECASE,
        )[0]
        chunk = self._strip_location_prefixes(chunk)

        parts = re.split(r",|;|/|\s+-\s+|\s+ e\s+|\s+ ed\s+", chunk, flags=re.IGNORECASE)

        # Secondo giro: se una parte contiene ancora "Comune di X", tiene solo X.
        cleaned_parts = []
        for part in parts:
            part = self._strip_location_prefixes(part)

            # Caso residuo: "Mezzojuso E Ciminna" o "Siracusa E Noto" non splittato al primo giro.
            subparts = re.split(r"\s+e\s+|\s+ed\s+", part, flags=re.IGNORECASE)
            for subpart in subparts:
                subpart = self._strip_location_prefixes(subpart)
                if subpart:
                    cleaned_parts.append(subpart)

        return cleaned_parts

    def _strip_location_prefixes(self, value: str) -> str:
        value = self._clean_text(value)

        patterns = [
            r"^.*?\bterritori\s+(?:dei\s+)?comuni\s+di\s+",
            r"^.*?\bterritorio\s+(?:del\s+)?comune\s+di\s+",
            r"^.*?\bda\s+realizzarsi\s+nei\s+comuni\s+di\s+",
            r"^.*?\bda\s+realizzarsi\s+nel\s+comune\s+di\s+",
            r"^.*?\bda\s+realizzare\s+nel\s+comune\s+di\s+",
            r"^.*?\bsito\s+in\s+comune\s+di\s+",
            r"^.*?\bsito\s+nel\s+comune\s+di\s+",
            r"^.*?\bsito\s+in\s+",
            r"^.*?\bsita\s+in\s+",
            r"^.*?\bin\s+comune\s+di\s+",
            r"^.*?\blocalizzato\s+nel\s+comune\s+di\s+",
            r"^.*?\bubicato\s+nel\s+comune\s+di\s+",
            r"^.*?\bricadente\s+nel\s+comune\s+di\s+",
            r"^.*?\bnei\s+comuni\s+di\s+",
            r"^.*?\bnel\s+comune\s+di\s+",
            r"^.*?\bcomuni\s+di\s+",
            r"^.*?\bcomune\s+di\s+",
        ]

        for pattern in patterns:
            value = re.sub(pattern, "", value, flags=re.IGNORECASE).strip()

        return value

    def _clean_municipality(self, value: str) -> str | None:
        value = self._clean_text(value)
        value = self._strip_location_prefixes(value)

        if not value:
            return None

        value = re.sub(r"\((AG|CL|CT|EN|ME|PA|RG|SR|TP)\)", "", value, flags=re.IGNORECASE)
        value = re.sub(r"^(?:e|ed|di|del|della|dello|dei|degli|in|nel|nella|da|c\.?\s*da|contrada|localit[aà])\s+", "", value, flags=re.IGNORECASE)
        value = re.sub(r"\b(?:di|del|della|dello|dei|degli|in|provincia)\b$", "", value, flags=re.IGNORECASE)
        value = value.strip(" ,.;:-()[]\"'")
        value = re.sub(r"\s+['’]?$", "", value).strip()
        value = re.sub(r"\s+\b[cC]\b$", "", value).strip()

        # Evita località e codici rimasti agganciati al comune.
        if re.search(r"\bin\s+via\b", value, flags=re.IGNORECASE):
            return None
        if re.search(r"-[a-z]$", value, flags=re.IGNORECASE):
            return None

        if not value:
            return None

        if len(value) < 3 or len(value) > 55:
            return None

        if re.search(r"\d", value):
            return None

        bad_fragments = [
            "potenza",
            "impianto",
            "fotovoltaico",
            "agrivoltaico",
            "agrovoltaico",
            "opere",
            "connessione",
            "rete",
            "rtn",
            "catasto",
            "foglio",
            "particelle",
            "particella",
            "progetto",
            "realizzazione",
            "produzione",
            "energia",
            "denominato",
            "denominata",
            "contrada",
            "localita",
            "località",
            "cavidotto",
            "elettrodotto",
            "stazione",
            "cabina",
            "procedura",
            "valutazione",
            "ambientale",
            "portale",
            "urbanistiche",
            "kwp",
            "mwp",
            "mw",
            "centrale",
            "internamente",
            "esterna",
            "nco",
            "sottostazione",
            "cod",
            "siciliana",
            "passaneto",
            "contado",
            "settefarine",
            "camemi",
            "pozzocamino",
            "pozzo camino",
        ]

        normalized = self._normalize_for_match(value)

        # Se è esattamente un comune noto, accettalo prima dei filtri anti-frammento.
        if normalized in SICILIA_MUNICIPALITY_TO_PROVINCE:
            return self._title_case_location(normalized)

        if any(fragment in normalized for fragment in bad_fragments):
            return None

        if normalized in {"sicilia", "provincia", "comune", "comuni", "sito", "siti", "maz"}:
            return None

        if " in " in f" {normalized} " and normalized not in SICILIA_MUNICIPALITY_TO_PROVINCE:
            return None

        # Se contiene ancora frasi troppo amministrative, meglio non inventare.
        if len(normalized.split()) > 5:
            return None

        return self._title_case_location(value)

    def _title_case_location(self, value: str) -> str:
        minor = {"di", "del", "della", "dello", "dei", "degli", "delle", "da", "de", "la", "lo", "il", "l"}
        words = []
        for word in self._clean_text(value).split():
            lower = word.lower()
            if lower in minor:
                words.append(lower)
            else:
                words.append(word[:1].upper() + word[1:].lower())
        return " ".join(words)

    def _build_external_id(self, normalized: dict) -> str:
        codice = normalized.get("codice") or ""
        url = normalized.get("detail_url") or ""
        title = normalized.get("title") or ""
        proponent = normalized.get("proponent") or ""

        raw_id = self._extract_raw_id_from_url(url)

        stable = "|".join(
            [
                str(codice).strip(),
                str(raw_id).strip(),
                self._slugify(title)[:120],
                self._slugify(proponent)[:80],
            ]
        )

        if stable.strip("|"):
            return stable[:240]

        digest = hashlib.sha1(f"{title}|{proponent}|{url}".encode("utf-8")).hexdigest()
        return f"sicilia-{digest}"

    def _extract_raw_id_from_url(self, url: str) -> str:
        if not url:
            return ""

        try:
            parsed = urlparse(url)
            query = parse_qs(parsed.query)
            values = query.get("procedura___oggetto_raw")
            if values:
                return values[0]
        except Exception:
            return ""

        match = re.search(r"procedura___oggetto_raw=([0-9]+)", url)
        if match:
            return match.group(1)

        return ""

    def _contains_any(self, text: str, needles: list[str]) -> bool:
        value = self._normalize_for_match(text)
        return any(self._normalize_for_match(needle) in value for needle in needles)

    def _is_valid_url(self, value: str | None) -> bool:
        if not value:
            return False

        value = str(value).strip()
        return value.startswith("http://") or value.startswith("https://")

    def _normalize_column_name(self, value: str) -> str:
        value = self._clean_text(value)
        value = value.replace("\ufeff", "")
        value = value.strip().lower()

        replacements = {
            "aoo_nome": "aoo_nome",
            "aoo_codiceipa": "aoo_codiceipa",
            "aoo_codiceipa": "aoo_codiceipa",
        }

        return replacements.get(value, value)

    def _normalize_for_match(self, text: str) -> str:
        text = self._clean_text(text).lower()
        text = unicodedata.normalize("NFKD", text)
        text = "".join(ch for ch in text if not unicodedata.combining(ch))
        text = text.replace("’", "'")
        text = re.sub(r"[^a-z0-9àèéìòù'\s\.-]", " ", text)
        text = re.sub(r"\s+", " ", text)
        return text.strip()

    def _slugify(self, text: str) -> str:
        text = self._normalize_for_match(text)
        text = re.sub(r"[^a-z0-9]+", "-", text)
        text = re.sub(r"-+", "-", text)
        return text.strip("-")

    def _clean_text(self, value) -> str:
        if value is None:
            return ""

        value = str(value)
        value = value.replace("\ufeff", "")
        value = value.replace("\xa0", " ")
        value = value.replace("\r", " ")
        value = value.replace("\n", " ")
        value = value.replace("\\u2019", "’")
        value = value.strip()

        value = re.sub(r"\s+", " ", value)

        return value.strip()

    def _safe_filename(self, value: str) -> str:
        digest = hashlib.sha1(value.encode("utf-8", errors="ignore")).hexdigest()
        return digest[:16]

    def _write_json(self, path: Path, data) -> None:
        try:
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text(
                json.dumps(data, ensure_ascii=False, indent=2),
                encoding="utf-8",
            )
        except Exception:
            pass

    def _write_text(self, path: Path, text: str) -> None:
        try:
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text(str(text), encoding="utf-8")
        except Exception:
            pass


if __name__ == "__main__":
    collector = SiciliaCollector()
    items = collector.fetch()
    print("items:", len(items))
    missing_province = sum(1 for item in items if not item.payload.get("province"))
    missing_municipalities = sum(1 for item in items if not item.payload.get("municipalities"))
    print("missing_province:", missing_province)
    print("missing_municipalities:", missing_municipalities)

    try:
        summary_path = Path("/app/reports/debug_sicilia/summary.json")
        if summary_path.exists():
            debug_summary = json.loads(summary_path.read_text(encoding="utf-8"))
            print("gis_enrichment:", json.dumps(debug_summary.get("gis_enrichment", {}), ensure_ascii=False))
    except Exception as exc:
        print("gis_enrichment_summary_error:", exc)

    for item in items[:80]:
        print(
            str(item.external_id)[:80],
            "|",
            str(item.title)[:120],
            "|",
            item.payload.get("province"),
            "|",
            item.payload.get("municipalities"),
            "|",
            item.payload.get("power"),
            "|",
            item.payload.get("status_raw"),
        )