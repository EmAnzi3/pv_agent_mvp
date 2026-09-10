from __future__ import annotations

import argparse
import csv
import json
import re
import unicodedata
from datetime import datetime
from pathlib import Path


OVERRIDES = {
    "https://va.mite.gov.it/it-IT/Oggetti/Info/8018": {
        "region": "Piemonte",
        "province": "TO",
        "municipalities": "Lombardore, San Benigno Canavese",
        "power_mw": 18.77382,
        "reason": "manual_mase_8018_lombardore_location_power_fix",
    },
    "https://va.mite.gov.it/it-IT/Comunicazione/DettaglioUltimiProvvedimenti/6160": {
        "region": "Emilia-Romagna",
        "province": "BO",
        "municipalities": "Malalbergo, Baricella",
        "reason": "manual_mase_6160_location_fix",
    },
    "https://va.mite.gov.it/it-IT/Oggetti/Info/9004": {
        "province": "VS",
        "reason": "manual_mase_9004_villacidro_province_fix",
    },
    "https://va.mite.gov.it/it-IT/Comunicazione/DettaglioUltimiProvvedimenti/6417": {
        "region": "Friuli-Venezia Giulia",
        "province": "PN",
        "reason": "manual_mase_6417_sesto_al_reghena_province_fix",
    },
    "https://va.mite.gov.it/it-IT/Oggetti/Info/10476": {
        "region": "Friuli-Venezia Giulia",
        "province": "PN",
        "reason": "manual_mase_10476_sesto_al_reghena_province_fix",
    },
}


# Protezioni indipendenti dall'URL: servono quando MASE ripubblica lo stesso
# progetto con un nuovo identificativo o con sigle territoriali incoerenti.
PROTECTED_LOCATION_RULES = {
    "sesto al reghena": {
        "region": "Friuli-Venezia Giulia",
        "province": "PN",
        "reason": "protected_municipality_sesto_al_reghena",
    },
}


SEARCH_FIELDS = [
    "title",
    "project_name",
    "municipalities",
    "municipality",
    "comuni",
    "comune",
    "localizzazione",
    "location",
]


def _records_container(data):
    if isinstance(data, dict) and isinstance(data.get("records"), list):
        return data["records"]
    if isinstance(data, list):
        return data
    raise ValueError("Formato data.json non riconosciuto")


def _norm(value) -> str:
    text = str(value or "").strip().lower()
    text = unicodedata.normalize("NFKD", text)
    text = "".join(ch for ch in text if not unicodedata.combining(ch))
    text = re.sub(r"[^a-z0-9]+", " ", text)
    return re.sub(r"\s+", " ", text).strip()


def _record_search_text(record: dict) -> str:
    return _norm(" ".join(str(record.get(field) or "") for field in SEARCH_FIELDS))


def _record_matches_rule(record: dict, municipality_norm: str) -> bool:
    haystack = _record_search_text(record)
    if not haystack:
        return False
    return bool(re.search(rf"\b{re.escape(municipality_norm)}\b", haystack))


def _snapshot(record: dict) -> dict:
    return {
        "region": record.get("region", ""),
        "province": record.get("province", ""),
        "municipalities": record.get("municipalities", ""),
    }


def _apply_override(record: dict, override: dict) -> int:
    changed = 0
    for field in ["region", "province", "municipalities", "power_mw"]:
        if field not in override:
            continue
        if record.get(field) != override[field]:
            record[field] = override[field]
            changed += 1
    return changed


def _audit_row(record: dict, before: dict, reason: str, ts: str) -> dict:
    return {
        "timestamp": ts,
        "url": str(record.get("url") or record.get("source_url") or "").strip(),
        "title": record.get("title", ""),
        "proponent": record.get("proponent", ""),
        "old_region": before["region"],
        "new_region": record.get("region", ""),
        "old_province": before["province"],
        "new_province": record.get("province", ""),
        "old_municipalities": before["municipalities"],
        "new_municipalities": record.get("municipalities", ""),
        "reason": reason,
    }


def _protected_violations(records: list[dict]) -> list[dict]:
    violations = []

    for record in records:
        for municipality_norm, rule in PROTECTED_LOCATION_RULES.items():
            if not _record_matches_rule(record, municipality_norm):
                continue

            province_ok = str(record.get("province") or "").strip().upper() == rule["province"]
            region_ok = _norm(record.get("region")) == _norm(rule["region"])

            if not province_ok or not region_ok:
                violations.append(
                    {
                        "municipality": municipality_norm,
                        "title": record.get("title", ""),
                        "url": record.get("url") or record.get("source_url") or "",
                        "province": record.get("province", ""),
                        "region": record.get("region", ""),
                        "expected_province": rule["province"],
                        "expected_region": rule["region"],
                    }
                )

    return violations


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--data", required=True)
    parser.add_argument("--audit", default="reports/manual_mase_location_overrides_audit.csv")
    args = parser.parse_args()

    data_path = Path(args.data)
    audit_path = Path(args.audit)

    if not data_path.exists():
        raise SystemExit(f"[manual-mase-location-overrides] file non trovato: {data_path}")

    data = json.loads(data_path.read_text(encoding="utf-8"))
    records = _records_container(data)

    rows = []
    changed = 0
    protected_matches = 0
    ts = datetime.now().isoformat(timespec="seconds")

    # Primo livello: override puntuali per gli URL già noti.
    for record in records:
        url = str(record.get("url") or record.get("source_url") or "").strip()

        if url not in OVERRIDES:
            continue

        override = OVERRIDES[url]
        before = _snapshot(record)
        changed_here = _apply_override(record, override)
        changed += changed_here

        rows.append(_audit_row(record, before, override["reason"], ts))

    # Secondo livello: protezione semantica indipendente da URL e ID MASE.
    # Se compare Sesto al Reghena in un campo territoriale/titolo, la coppia
    # provincia-regione viene sempre riallineata a PN / Friuli-Venezia Giulia.
    for record in records:
        for municipality_norm, rule in PROTECTED_LOCATION_RULES.items():
            if not _record_matches_rule(record, municipality_norm):
                continue

            protected_matches += 1
            before = _snapshot(record)
            changed_here = _apply_override(record, rule)
            changed += changed_here

            # Evita righe audit duplicate quando l'override URL aveva già prodotto
            # esattamente lo stesso risultato e la regola protetta non cambia nulla.
            if changed_here:
                rows.append(_audit_row(record, before, rule["reason"], ts))

    data_path.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")

    if rows:
        audit_path.parent.mkdir(parents=True, exist_ok=True)
        with audit_path.open("w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
            writer.writeheader()
            writer.writerows(rows)

    # Terzo livello: rilettura dal disco e verifica fail-closed.
    # Se la correzione non è realmente persistita, il run termina con errore.
    persisted = json.loads(data_path.read_text(encoding="utf-8"))
    persisted_records = _records_container(persisted)
    violations = _protected_violations(persisted_records)

    print(f"[manual-mase-location-overrides] record URL intercettati: {sum(1 for r in records if str(r.get('url') or r.get('source_url') or '').strip() in OVERRIDES)}")
    print(f"[manual-mase-location-overrides] record protetti intercettati: {protected_matches}")
    print(f"[manual-mase-location-overrides] campi corretti: {changed}")
    print(f"[manual-mase-location-overrides] audit: {audit_path}")

    if violations:
        sample = violations[0]
        raise SystemExit(
            "[manual-mase-location-overrides] ERRORE GEO BLOCCANTE: "
            f"{sample['municipality']} risulta ancora "
            f"{sample['province']} / {sample['region']} invece di "
            f"{sample['expected_province']} / {sample['expected_region']}."
        )

    print("[manual-mase-location-overrides] OK: regole geografiche protette verificate.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())