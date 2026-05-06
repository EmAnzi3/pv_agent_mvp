from __future__ import annotations

import argparse
import csv
import json
from copy import deepcopy
from datetime import datetime
from pathlib import Path


CANALICCHI_URL = "https://www.regione.calabria.it/provvedimenti-regionali/provvedimento-autorizzatorio-unico-regionale-paur-ai-sensi-dellart-27bis-del-d-lgs-152-2006-e-s-m-i-relativo-al-progetto-di-costruzione-ed-esercizio-di-un-impianto-agrivoltaico/"

CANALICCHI_RECORD = {
    "source": "Calabria",
    "region": "Calabria",
    "province": "KR",
    "municipalities": ["Crotone", "Scandale"],
    "power_mw": 19.52,
    "proponent": "SOLUX srl",
    "title": (
        "Provvedimento Autorizzatorio Unico regionale (PAUR) ai sensi dell’art. 27bis "
        "del d.lgs. 152/2006 e s.m.i., relativo al progetto di “Costruzione ed esercizio "
        "di un impianto agrivoltaico denominato “Canalicchi”, con potenza di picco pari "
        "a 19,52 MW e potenza in immissione pari a 16 MW, da realizzarsi nel Comune di "
        "Crotone (KR), in località Canalicchi, e delle relative opere connesse, che "
        "interessano anche il Comune di Scandale (KR)” – proponente: SOLUX srl"
    ),
    "url": CANALICCHI_URL,
    "source_url": CANALICCHI_URL,
    "status": "PAUR",
    "procedure": "PAUR",
    "project_type_hint": "Agrivoltaico",
    "technology": "agrivoltaico",
    "project_type": "agrivoltaico",
    "is_agrivoltaico": True,
}


KNOWN_CALABRIA_OVERRIDES = {
    "progetto-di-costruzione-ed-esercizio-di-impianto-fotovoltaico-della-potenza-complessiva-pari-a-189865-mw": {
        "province": "KR",
        "municipalities": ["Crotone", "Scandale"],
        "power_mw": 18.9865,
        "proponent": "Cargo S.r.l.",
    },
    "impianto-fotovoltaico-variante-cargosrl": {
        "province": "KR",
        "municipalities": ["Crotone", "Scandale"],
        "power_mw": 18.9865,
        "proponent": "Cargo S.r.l.",
    },
    "progetto-di-un-impianto-fotovoltaico-flottante-nonche-delle-relative-opere-strettamente-connesse-di-potenza-pari-a-9-98-mwp-denominato-dit040-calusia": {
        "province": "KR",
        "municipalities": ["Caccuri"],
        "power_mw": 9.98,
        "proponent": "RESOL 1 S.r.l.",
    },
    "progetto-costruzione-e-desercizio-di-un-impianto-fotovoltaico-della-potenza-complessiva-pari-a997920mwp": {
        "province": "CZ",
        "municipalities": ["Badolato"],
        "power_mw": 9.9792,
        "proponent": "ENERSPV2 S.r.l.",
    },
    "costruzione-ed-esercizio-di-un-impianto-agrivoltaico-dalla-potenza-nominale-di-3151-mwp": {
        "province": "KR",
        "municipalities": ["Scandale", "Cutro"],
        "power_mw": 31.51,
        "proponent": "GO MANDORLO S.r.l",
    },
    "748_cs": {
        "province": "CS",
        "municipalities": ["Altomonte", "San Lorenzo del Vallo", "Spezzano Albanese", "Castrovillari"],
        "power_mw": 19.857,
        "proponent": "ALTOMONTE SOLAR ENERGY S.r.l",
    },
    "pratica-n-459-kr-_calabria-suap-sportello-ambiente": {
        "province": "KR",
        "municipalities": ["Crotone", "Scandale"],
        "power_mw": 10.0,
        "proponent": "FRI-EL S.p.A",
    },
    "avviso-paur-realizzazione-di-un-impianto-agrivoltaico-denominato-crotone": {
        "province": "KR",
        "municipalities": ["Crotone", "Cutro", "San Mauro Marchesato", "Scandale"],
        "power_mw": 17.72624,
        "proponent": "Habemus s.r.l",
    },
    "impianto-agrivoltaico-denominato-colli-crotonesi": {
        "province": "KR",
        "municipalities": ["Crotone", "Scandale"],
        "power_mw": 16.736,
        "proponent": "RWE RENEWABLES ITALIA S.r.l",
    },
    "provvedimento-autorizzatorio-unico-regionale-paur-ai-sensi-dellart-27bis-del-d-lgs-152-2006-e-s-m-i-relativo-al-progetto-di-costruzione-ed-esercizio-di-un-impianto-agrivoltaico": {
        "province": "KR",
        "municipalities": ["Crotone", "Scandale"],
        "power_mw": 19.52,
        "proponent": "SOLUX srl",
    },
}


def _records_container(data):
    if isinstance(data, dict) and isinstance(data.get("records"), list):
        return data["records"]
    if isinstance(data, list):
        return data
    raise ValueError("Formato data.json non riconosciuto: atteso dict con records oppure lista")


def _normalize_calabria_source(obj) -> int:
    changed = 0

    if isinstance(obj, dict):
        items = list(obj.items())
        obj.clear()

        for k, v in items:
            new_k = "Calabria" if k == "calabria" else k
            if new_k != k:
                changed += 1

            if isinstance(v, str) and v.strip() == "calabria":
                v = "Calabria"
                changed += 1
            else:
                changed += _normalize_calabria_source(v)

            obj[new_k] = v

    elif isinstance(obj, list):
        for i, v in enumerate(obj):
            if isinstance(v, str) and v.strip() == "calabria":
                obj[i] = "Calabria"
                changed += 1
            else:
                changed += _normalize_calabria_source(v)

    return changed


def _apply_known_calabria_overrides(records: list[dict]) -> int:
    changed = 0

    for r in records:
        url = str(r.get("url") or r.get("source_url") or "").lower()
        source = str(r.get("source", "")).strip().lower()

        if source == "calabria":
            r["source"] = "Calabria"
            changed += 1

        for needle, patch in KNOWN_CALABRIA_OVERRIDES.items():
            if needle not in url:
                continue

            r["source"] = "Calabria"
            r["region"] = "Calabria"

            for key, value in patch.items():
                if r.get(key) != value:
                    r[key] = value
                    changed += 1

    return changed


def _already_exists(records: list[dict]) -> bool:
    for r in records:
        blob = json.dumps(r, ensure_ascii=False).lower()

        if "canalicchi" in blob and "solux" in blob:
            return True

        if str(r.get("url", "")).strip() == CANALICCHI_URL:
            return True

        if str(r.get("source_url", "")).strip() == CANALICCHI_URL:
            return True

    return False


def _build_record(records: list[dict]) -> dict:
    template = None

    for r in records:
        if str(r.get("source", "")).strip().lower() == "calabria":
            template = deepcopy(r)
            break

    if template is None:
        template = {}

    template.update(CANALICCHI_RECORD)
    return template


def _write_audit(path: Path, rows: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)

    fieldnames = [
        "timestamp",
        "action",
        "data_path",
        "details",
    ]

    write_header = not path.exists()

    with path.open("a", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        if write_header:
            writer.writeheader()
        writer.writerows(rows)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--data", default="reports/site/data.json")
    parser.add_argument("--audit", default="reports/manual_calabria_overrides_audit.csv")
    args = parser.parse_args()

    data_path = Path(args.data)
    audit_path = Path(args.audit)

    if not data_path.exists():
        raise SystemExit(f"[manual-calabria-overrides] ERRORE: file non trovato: {data_path}")

    data = json.loads(data_path.read_text(encoding="utf-8"))
    records = _records_container(data)

    audit_rows = []
    timestamp = datetime.now().isoformat(timespec="seconds")

    normalized_count = _normalize_calabria_source(data)
    patched_count = _apply_known_calabria_overrides(records)

    if _already_exists(records):
        print(f"[manual-calabria-overrides] OK: Canalicchi / SOLUX già presente in {data_path}")
        added = False
    else:
        records.append(_build_record(records))
        print(f"[manual-calabria-overrides] AGGIUNTO: Canalicchi / SOLUX in {data_path}")
        added = True

    data_path.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")

    audit_rows.extend([
        {
            "timestamp": timestamp,
            "action": "normalize_calabria_source",
            "data_path": str(data_path),
            "details": f"token_normalizzati={normalized_count}",
        },
        {
            "timestamp": timestamp,
            "action": "known_calabria_overrides",
            "data_path": str(data_path),
            "details": f"campi_corretti={patched_count}",
        },
        {
            "timestamp": timestamp,
            "action": "canalicchi_solux",
            "data_path": str(data_path),
            "details": "added=1" if added else "already_present=1",
        },
    ])

    _write_audit(audit_path, audit_rows)

    print(f"[manual-calabria-overrides] source Calabria normalizzati: {normalized_count}")
    print(f"[manual-calabria-overrides] override Calabria applicati: {patched_count}")
    print(f"[manual-calabria-overrides] audit: {audit_path}")


if __name__ == "__main__":
    main()
