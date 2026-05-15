from __future__ import annotations

import argparse
import json
from pathlib import Path
from collections import Counter, defaultdict
from datetime import datetime


def _load_json(path: Path):
    if not path.exists():
        raise SystemExit(f"[merge-basilicata] ERRORE: file non trovato: {path}")
    return json.loads(path.read_text(encoding="utf-8"))


def _records_container(data):
    if isinstance(data, dict) and isinstance(data.get("records"), list):
        return data["records"]
    if isinstance(data, list):
        return data
    raise ValueError("Formato data.json non riconosciuto")


def _norm_url(value) -> str:
    return str(value or "").strip().lower()


def _as_float(value) -> float:
    try:
        return float(value or 0)
    except Exception:
        return 0.0


def _normalize_basilicata_record(r: dict) -> dict:
    out = dict(r)

    out["source"] = "Basilicata"
    out["source_label"] = "Basilicata"
    out["source_group"] = "Basilicata"
    out["_merged_sources"] = ["Basilicata"]
    out["region"] = "Basilicata"

    municipalities = out.get("municipalities")
    if isinstance(municipalities, list):
        out["municipalities"] = ", ".join(str(x).strip() for x in municipalities if str(x).strip())

    out["province"] = str(out.get("province") or "").strip()
    out["proponent"] = str(out.get("proponent") or "").strip()
    out["title"] = str(out.get("title") or "").strip()
    out["url"] = str(out.get("url") or out.get("source_url") or "").strip()

    out["is_terna"] = False
    out["is_punctual"] = True
    out["numero_pratiche"] = 0
    out["province_deduced"] = False
    out["municipalities_deduced"] = False

    if "project_type" not in out or not out.get("project_type"):
        out["project_type"] = out.get("procedure") or "Basilicata VIA/Screening"

    if "status" not in out:
        out["status"] = out.get("procedure") or ""

    if "updated_at" not in out or not out.get("updated_at"):
        out["updated_at"] = datetime.now().isoformat(timespec="seconds")

    return out


def _validate_basilicata_record(r: dict) -> list[str]:
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

    if _as_float(r.get("power_mw")) < 5:
        errors.append(f"power_mw sotto soglia: {r.get('power_mw')!r}")

    if not str(r.get("url") or "").startswith("http"):
        errors.append(f"url non valido: {r.get('url')!r}")

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

    blob = json.dumps(r, ensure_ascii=False).lower()
    for word in forbidden:
        if word in blob:
            errors.append(f"keyword esclusa presente: {word}")

    return errors


def _recompute_summary(data: dict) -> None:
    records = _records_container(data)

    summary = data.setdefault("summary", {})

    punctual = [r for r in records if not bool(r.get("is_terna"))]
    terna = [r for r in records if bool(r.get("is_terna"))]

    summary["total_records"] = len(records)
    summary["punctual_records"] = len(punctual)
    summary["terna_records"] = len(terna)
    summary["total_mw_punctual"] = round(sum(_as_float(r.get("power_mw")) for r in punctual), 3)
    summary["total_mw_terna"] = round(sum(_as_float(r.get("power_mw")) for r in terna), 3)

    source_counts = Counter(str(r.get("source") or "n/d") for r in records)

    def label_for_source(source: str) -> str:
        labels = [
            str(r.get("source_label"))
            for r in records
            if str(r.get("source") or "") == source and r.get("source_label")
        ]
        return labels[0] if labels else source

    summary["source_counts"] = [
        {
            "source": source,
            "label": label_for_source(source),
            "count": count,
        }
        for source, count in source_counts.most_common()
    ]

    # Aggiorna anche le regioni in modo semplice.
    region_bucket = defaultdict(lambda: {
        "punctual_count": 0,
        "punctual_mw": 0.0,
        "terna_count": 0,
        "terna_mw": 0.0,
        "terna_practices": 0,
    })

    for r in records:
        region = str(r.get("region") or "n/d").strip()
        if not region:
            region = "n/d"

        if bool(r.get("is_terna")):
            region_bucket[region]["terna_count"] += 1
            region_bucket[region]["terna_mw"] += _as_float(r.get("power_mw"))
            region_bucket[region]["terna_practices"] += int(r.get("numero_pratiche") or 0)
        else:
            region_bucket[region]["punctual_count"] += 1
            region_bucket[region]["punctual_mw"] += _as_float(r.get("power_mw"))

    regions = []
    for region, values in region_bucket.items():
        punctual_mw = round(values["punctual_mw"], 3)
        terna_mw = round(values["terna_mw"], 3)
        total_mw = round(punctual_mw + terna_mw, 3)

        # Score leggero coerente con dashboard: conta soprattutto dati puntuali.
        score = round((values["punctual_count"] * 0.06) + (punctual_mw / 1000.0), 1)

        regions.append({
            "region": region,
            "punctual_count": values["punctual_count"],
            "punctual_mw": punctual_mw,
            "terna_count": values["terna_count"],
            "terna_mw": terna_mw,
            "terna_practices": values["terna_practices"],
            "total_mw": total_mw,
            "priority_score": score,
        })

    summary["regions"] = sorted(
        regions,
        key=lambda x: (x["priority_score"], x["punctual_count"], x["punctual_mw"]),
        reverse=True,
    )

    # Top progetti operativi: ricostruzione semplice.
    top_projects = sorted(
        punctual,
        key=lambda r: _as_float(r.get("power_mw")),
        reverse=True,
    )[:50]

    summary["top_projects"] = top_projects


def _validate_clean_sources(data: dict) -> list[str]:
    records = _records_container(data)
    errors = []

    allowed_sources = {
        "lazio",
        "sicilia",
        "mase",
        "mase_provvedimenti",
        "terna_econnextion",
        "sistema_puglia_energia",
        "lombardia",
        "Calabria",
        "Basilicata",
        "emilia_romagna",
        "toscana",
        "veneto",
        "campania",
        "Umbria",
        "Sardegna",
        "piemonte",
    }

    sources = Counter(str(r.get("source") or "") for r in records)

    bad_sources = sorted(s for s in sources if s not in allowed_sources)
    if bad_sources:
        errors.append(f"fonti non ammesse: {bad_sources}")

    if sources.get("puglia", 0):
        errors.append(f"trovati ancora record source='puglia': {sources['puglia']}")

    return errors


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--data", default="reports/site/data.json")
    parser.add_argument("--staging", default="reports/staging_basilicata/basilicata_staging.json")
    parser.add_argument("--out", default=None)
    args = parser.parse_args()

    data_path = Path(args.data)
    staging_path = Path(args.staging)
    out_path = Path(args.out) if args.out else data_path

    data = _load_json(data_path)
    site_records = _records_container(data)

    staging = _load_json(staging_path)
    staging_records = staging.get("records", [])

    if not staging_records:
        raise SystemExit("[merge-basilicata] ERRORE: staging Basilicata vuoto")

    normalized = [_normalize_basilicata_record(r) for r in staging_records]

    validation_errors = []
    for idx, r in enumerate(normalized, 1):
        errs = _validate_basilicata_record(r)
        if errs:
            validation_errors.append({
                "index": idx,
                "title": r.get("title"),
                "url": r.get("url"),
                "errors": errs,
            })

    if validation_errors:
        print("[merge-basilicata] ERRORE: staging non valido")
        for e in validation_errors:
            print("-" * 80)
            print("index:", e["index"])
            print("title:", e["title"])
            print("url:", e["url"])
            for err in e["errors"]:
                print("  -", err)
        return 2

    before = len(site_records)

    # Rimuove eventuali precedenti record Basilicata regionali e poi reinserisce lo staging.
    cleaned = [
        r for r in site_records
        if str(r.get("source") or "") != "Basilicata"
    ]

    existing_urls = {_norm_url(r.get("url") or r.get("source_url")) for r in cleaned}
    inserted = []

    for r in normalized:
        url = _norm_url(r.get("url"))
        if url in existing_urls:
            continue
        inserted.append(r)
        existing_urls.add(url)

    cleaned.extend(inserted)

    if isinstance(data, dict) and isinstance(data.get("records"), list):
        data["records"] = cleaned
    else:
        data = cleaned

    if isinstance(data, dict):
        _recompute_summary(data)

    clean_errors = _validate_clean_sources(data if isinstance(data, dict) else {"records": data})
    if clean_errors:
        print("[merge-basilicata] ERRORE: validazione fonti fallita")
        for e in clean_errors:
            print("-", e)
        return 3

    out_path.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")

    print(f"[merge-basilicata] data: {data_path}")
    print(f"[merge-basilicata] staging: {staging_path}")
    print(f"[merge-basilicata] record iniziali: {before}")
    print(f"[merge-basilicata] record Basilicata staging: {len(normalized)}")
    print(f"[merge-basilicata] record inseriti: {len(inserted)}")
    print(f"[merge-basilicata] record finali: {len(cleaned)}")
    print(f"[merge-basilicata] output: {out_path}")
    print("[merge-basilicata] OK")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
