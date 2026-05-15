from __future__ import annotations

import csv
import json
import sys
from datetime import datetime
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))


OUT_DIR = Path("reports/staging_basilicata")
OUT_JSON = OUT_DIR / "basilicata_staging.json"
OUT_CSV = OUT_DIR / "basilicata_staging.csv"
OUT_SUMMARY = OUT_DIR / "basilicata_staging_summary.json"


def _as_text(value) -> str:
    if value is None:
        return ""
    if isinstance(value, list):
        return ", ".join(str(x) for x in value)
    return str(value)


def _validate_record(r: dict) -> list[str]:
    errors = []

    required = [
        "source",
        "source_label",
        "source_group",
        "region",
        "province",
        "municipalities",
        "power_mw",
        "proponent",
        "title",
        "url",
    ]

    for field in required:
        value = r.get(field)
        if value is None or value == "" or value == []:
            errors.append(f"campo mancante: {field}")

    if r.get("source") != "Basilicata":
        errors.append(f"source non valido: {r.get('source')!r}")

    if r.get("source_label") != "Basilicata":
        errors.append(f"source_label non valido: {r.get('source_label')!r}")

    if r.get("source_group") != "Basilicata":
        errors.append(f"source_group non valido: {r.get('source_group')!r}")

    if r.get("region") != "Basilicata":
        errors.append(f"region non valida: {r.get('region')!r}")

    if r.get("province") not in {"PZ", "MT"}:
        errors.append(f"province non valida: {r.get('province')!r}")

    try:
        mw = float(r.get("power_mw"))
    except Exception:
        mw = None

    if mw is None:
        errors.append("power_mw non numerico")
    elif mw < 5:
        errors.append(f"power_mw sotto soglia: {mw}")

    title_blob = f"{r.get('title', '')} {r.get('plain_text_sample', '')}".lower()
    forbidden = [
        "eolico",
        "eolica",
        "rifiuti",
        "r.a.e.e",
        "raee",
        "discarica",
        "cava",
        "amianto",
        "i.p.p.c",
        "ippc",
        "a.i.a",
    ]

    for word in forbidden:
        if word in title_blob:
            errors.append(f"keyword esclusa presente: {word}")

    return errors


def main() -> int:
    from app.collectors.basilicata import BasilicataCollector, START_URLS

    OUT_DIR.mkdir(parents=True, exist_ok=True)

    collector = BasilicataCollector()

    records: list[dict] = []
    seen_urls: set[str] = set()
    debug = []

    print("[stage-basilicata] Avvio staging Basilicata via funzioni interne")

    for source in START_URLS:
        url = source["url"]
        procedure = source.get("procedure", "")

        html = collector._get_html(url)

        if not html:
            print(f"[stage-basilicata] SKIP, HTML assente: {url}")
            continue

        rows = collector._parse_list_page(
            html_page=html,
            page_url=url,
            procedure=procedure,
        )

        print(f"[stage-basilicata] {procedure}: rows={len(rows)}")

        for row in rows:
            normalized = collector._normalize_row(row)

            if not normalized:
                continue

            is_relevant = collector._is_relevant(normalized)

            debug.append({
                "source_url": normalized.get("source_url"),
                "title": normalized.get("title"),
                "proponent": normalized.get("proponent"),
                "province": normalized.get("province"),
                "municipalities": normalized.get("municipalities"),
                "power_mw": normalized.get("power_mw"),
                "is_relevant": is_relevant,
            })

            if not is_relevant:
                continue

            source_url = normalized.get("source_url")

            if not source_url or source_url in seen_urls:
                continue

            seen_urls.add(source_url)

            municipalities = normalized.get("municipalities") or []

            record = {
                "source": "Basilicata",
                "source_label": "Basilicata",
                "source_group": "Basilicata",
                "_merged_sources": ["Basilicata"],
                "region": "Basilicata",
                "province": normalized.get("province"),
                "municipalities": ", ".join(municipalities),
                "municipalities_list": municipalities,
                "power": normalized.get("power"),
                "power_mw": normalized.get("power_mw"),
                "proponent": normalized.get("proponent"),
                "title": normalized.get("title"),
                "url": source_url,
                "status": normalized.get("status_raw"),
                "project_type": normalized.get("project_type_hint"),
                "procedure": normalized.get("procedure"),
                "external_id": collector._build_external_id(source_url),
                "plain_text_sample": normalized.get("plain_text_sample"),
                "updated_at": datetime.now().isoformat(timespec="seconds"),
                "is_terna": False,
                "is_punctual": True,
            }

            records.append(record)

    validation_errors = []

    for idx, r in enumerate(records, 1):
        errors = _validate_record(r)
        if errors:
            validation_errors.append({
                "index": idx,
                "title": r.get("title"),
                "url": r.get("url"),
                "errors": errors,
            })

    summary = {
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "record_count": len(records),
        "total_mw": round(sum(float(r["power_mw"]) for r in records if r.get("power_mw") is not None), 6),
        "validation_errors_count": len(validation_errors),
        "validation_errors": validation_errors,
        "province_counts": {},
        "proponents": sorted({r.get("proponent") for r in records if r.get("proponent")}),
    }

    for r in records:
        province = r.get("province") or "n/d"
        summary["province_counts"][province] = summary["province_counts"].get(province, 0) + 1

    OUT_JSON.write_text(
        json.dumps(
            {
                "summary": summary,
                "records": records,
                "debug": debug,
            },
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )

    fieldnames = [
        "source",
        "source_label",
        "source_group",
        "region",
        "province",
        "municipalities",
        "power_mw",
        "proponent",
        "title",
        "url",
        "procedure",
        "project_type",
        "external_id",
    ]

    with OUT_CSV.open("w", newline="", encoding="utf-8-sig") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()

        for r in records:
            writer.writerow({k: r.get(k) for k in fieldnames})

    OUT_SUMMARY.write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")

    print(f"[stage-basilicata] JSON: {OUT_JSON}")
    print(f"[stage-basilicata] CSV : {OUT_CSV}")
    print(f"[stage-basilicata] Summary: {OUT_SUMMARY}")
    print(f"[stage-basilicata] Record: {summary['record_count']}")
    print(f"[stage-basilicata] MW totali: {summary['total_mw']}")
    print(f"[stage-basilicata] Errori validazione: {summary['validation_errors_count']}")

    if len(records) == 0:
        print("[stage-basilicata] ERRORE: 0 record raccolti.")
        return 2

    if len(records) < 10:
        print(f"[stage-basilicata] ERRORE: record troppo pochi: {len(records)}")
        return 2

    if validation_errors:
        print()
        print("[stage-basilicata] ERRORI:")
        for err in validation_errors:
            print("-" * 80)
            print("index:", err.get("index"))
            print("title:", err.get("title"))
            print("url:", err.get("url"))
            for e in err.get("errors", []):
                print("  -", e)
        return 2

    print("[stage-basilicata] OK: staging Basilicata valido.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
