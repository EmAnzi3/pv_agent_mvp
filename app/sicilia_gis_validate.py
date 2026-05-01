from __future__ import annotations

import csv
import json
import re
from collections import Counter
from pathlib import Path
from typing import Any

from app.collectors.sicilia import SiciliaCollector


OUT_DIR = Path("/app/reports")


PROVINCE_CODES = {"AG", "CL", "CT", "EN", "ME", "PA", "RG", "SR", "TP"}

ADMIN_CAPITALS = {
    "AG": "Agrigento",
    "CL": "Caltanissetta",
    "CT": "Catania",
    "EN": "Enna",
    "ME": "Messina",
    "PA": "Palermo",
    "RG": "Ragusa",
    "SR": "Siracusa",
    "TP": "Trapani",
}


def clean(value: Any) -> str:
    return re.sub(r"\s+", " ", str(value or "")).strip()


def norm(value: Any) -> str:
    text = clean(value).lower()
    repl = {
        "à": "a", "è": "e", "é": "e", "ì": "i", "ò": "o", "ù": "u",
        "’": "'", "‘": "'", "“": '"', "”": '"',
    }
    for a, b in repl.items():
        text = text.replace(a, b)
    text = re.sub(r"[^a-z0-9]+", " ", text)
    return re.sub(r"\s+", " ", text).strip()


def as_list(value: Any) -> list[str]:
    if value is None:
        return []
    if isinstance(value, list):
        return [clean(x) for x in value if clean(x)]
    return [clean(x) for x in str(value).split(",") if clean(x)]


def extract_explicit_comuni_from_title(title: str) -> list[str]:
    """
    Estrazione volutamente prudente, solo per audit.
    Non pretende di essere esaustiva.
    """
    found: list[str] = []

    patterns = [
        r"\bcomune\s+di\s+([A-ZÀ-Ý][A-Za-zÀ-ÿ'’\-\s]{2,60}?)(?:\s*\((AG|CL|CT|EN|ME|PA|RG|SR|TP)\)|,|\s+localit|\s+provincia|$)",
        r"\bnel\s+comune\s+di\s+([A-ZÀ-Ý][A-Za-zÀ-ÿ'’\-\s]{2,60}?)(?:\s*\((AG|CL|CT|EN|ME|PA|RG|SR|TP)\)|,|\s+localit|\s+provincia|$)",
        r"\bin\s+comune\s+di\s+([A-ZÀ-Ý][A-Za-zÀ-ÿ'’\-\s]{2,60}?)(?:\s*\((AG|CL|CT|EN|ME|PA|RG|SR|TP)\)|,|\s+localit|\s+provincia|$)",
    ]

    for pattern in patterns:
        for match in re.finditer(pattern, title, flags=re.IGNORECASE):
            comune = clean(match.group(1)).strip(" ,.;:-()[]\"'")
            comune = re.sub(r"\s+(SR|CT|PA|RG|TP|AG|CL|EN|ME)$", "", comune, flags=re.I).strip()
            if comune and comune not in found:
                found.append(comune)

    return found


def has_admin_false_positive(title: str, municipalities: list[str]) -> bool:
    """
    Segnala casi dove un capoluogo/provincia potrebbe essere stato infilato come comune.
    È un warning, non una sentenza.
    """
    title_norm = norm(title)
    municipal_norms = {norm(x) for x in municipalities}

    for capital in ADMIN_CAPITALS.values():
        cap_norm = norm(capital)
        if cap_norm not in municipal_norms:
            continue

        # Se il titolo parla chiaramente di più comuni e include il capoluogo, è plausibile.
        strong_municipal_context = re.search(
            rf"\b(comune|comuni|territori)\b.{0,120}\b{re.escape(cap_norm)}\b",
            title_norm,
        )

        # Se invece appare solo in contesto provincia/ente, warning.
        admin_context = re.search(
            rf"\b(provincia|prov|libero consorzio|citta metropolitana|città metropolitana)\b.{0,80}\b{re.escape(cap_norm)}\b",
            title_norm,
        )

        if admin_context and not strong_municipal_context:
            return True

    return False


def main() -> None:
    OUT_DIR.mkdir(parents=True, exist_ok=True)

    items = SiciliaCollector().fetch()

    rows: list[dict[str, Any]] = []
    warnings: list[dict[str, Any]] = []

    mode_counter = Counter()
    source_counter = Counter()
    province_counter = Counter()

    for item in items:
        payload = item.payload or {}

        title = clean(payload.get("title") or item.title)
        municipalities = as_list(payload.get("municipalities"))
        province = clean(payload.get("province"))

        mode = clean(payload.get("gis_match_mode"))
        source = clean(payload.get("gis_map_source"))

        mode_counter[mode or "none"] += 1
        source_counter[source or "none"] += 1
        province_counter[province or "none"] += 1

        explicit_comuni = extract_explicit_comuni_from_title(title)
        municipality_norms = {norm(x) for x in municipalities}

        issues: list[str] = []

        if not province:
            issues.append("missing_province")
        if not municipalities:
            issues.append("missing_municipalities")

        if mode and mode not in {"id+codproc", "id+codproc_reversed"}:
            issues.append(f"non_exact_match_mode:{mode}")

        for explicit in explicit_comuni:
            if norm(explicit) not in municipality_norms:
                issues.append(f"explicit_municipality_not_in_output:{explicit}")

        if has_admin_false_positive(title, municipalities):
            issues.append("possible_admin_capital_false_positive")

        row = {
            "external_id": item.external_id,
            "title": title,
            "province": province,
            "municipalities": ", ".join(municipalities),
            "power": clean(payload.get("power")),
            "status_raw": clean(payload.get("status_raw")),
            "proponent": clean(payload.get("proponent")),
            "gis_match_mode": mode,
            "gis_map_source": source,
            "location_source": clean(payload.get("location_source")),
            "latitudine": clean(payload.get("latitudine")),
            "longitudine": clean(payload.get("longitudine")),
            "explicit_comuni_in_title": ", ".join(explicit_comuni),
            "issues": "; ".join(issues),
            "url": item.source_url,
        }

        rows.append(row)

        if issues:
            warnings.append(row)

    fieldnames = list(rows[0].keys()) if rows else []

    all_path = OUT_DIR / "sicilia_gis_validate_all.csv"
    warn_path = OUT_DIR / "sicilia_gis_validate_warnings.csv"
    summary_path = OUT_DIR / "sicilia_gis_validate_summary.txt"

    for path, data in [(all_path, rows), (warn_path, warnings)]:
        with path.open("w", newline="", encoding="utf-8-sig") as handle:
            writer = csv.DictWriter(handle, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(data)

    suspicious_non_exact = sum(1 for r in rows if r["gis_match_mode"] and r["gis_match_mode"] not in {"id+codproc", "id+codproc_reversed"})
    missing_province = sum(1 for r in rows if not r["province"])
    missing_municipalities = sum(1 for r in rows if not r["municipalities"])
    admin_fp = sum(1 for r in rows if "possible_admin_capital_false_positive" in r["issues"])
    explicit_mismatch = sum(1 for r in rows if "explicit_municipality_not_in_output" in r["issues"])

    summary = [
        "Sicilia GIS validation",
        "======================",
        "",
        f"records: {len(rows)}",
        f"missing_province: {missing_province}",
        f"missing_municipalities: {missing_municipalities}",
        f"warnings: {len(warnings)}",
        f"non_exact_match_warnings: {suspicious_non_exact}",
        f"explicit_title_municipality_mismatches: {explicit_mismatch}",
        f"possible_admin_capital_false_positives: {admin_fp}",
        "",
        "gis_match_mode:",
        json.dumps(dict(mode_counter), ensure_ascii=False, indent=2),
        "",
        "gis_map_source:",
        json.dumps(dict(source_counter), ensure_ascii=False, indent=2),
        "",
        "province distribution:",
        json.dumps(dict(province_counter), ensure_ascii=False, indent=2),
        "",
        f"all_csv: {all_path}",
        f"warnings_csv: {warn_path}",
    ]

    summary_path.write_text("\n".join(summary), encoding="utf-8")

    print("\n".join(summary))


if __name__ == "__main__":
    main()
