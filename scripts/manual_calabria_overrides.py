from __future__ import annotations

import argparse
import csv
import json
from datetime import datetime
from pathlib import Path
from typing import Any

CANALICCHI_URL = (
    "https://www.regione.calabria.it/provvedimenti-regionali/"
    "provvedimento-autorizzatorio-unico-regionale-paur-ai-sensi-dellart-27bis-del-d-lgs-152-2006-e-s-m-i-"
    "relativo-al-progetto-di-costruzione-ed-esercizio-di-un-impianto-agrivoltaico/"
)

CANALICCHI_TITLE = (
    "Provvedimento Autorizzatorio Unico regionale (PAUR) ai sensi dellâ€™art. 27bis "
    "del d.lgs. 152/2006 e s.m.i., relativo al progetto di â€œCostruzione ed esercizio "
    "di un impianto agrivoltaico denominato â€œCanalicchiâ€, con potenza di picco pari "
    "a 19,52 MW e potenza in immissione pari a 16 MW, da realizzarsi nel Comune di "
    "Crotone (KR), in localitÃ  Canalicchi, e delle relative opere connesse, che "
    "interessano anche il Comune di Scandale (KR)â€ â€“ proponente: SOLUX srl"
)

CANALICCHI_KEY = "calabria_canalicchi_solux_kr_1952"


def _load_records(data: Any) -> list[dict[str, Any]]:
    if isinstance(data, dict) and isinstance(data.get("records"), list):
        return data["records"]
    if isinstance(data, list):
        return data
    raise ValueError("Formato data.json non riconosciuto: atteso dict con records oppure lista")


def _norm(value: Any) -> str:
    return str(value or "").strip().lower()


def _already_exists(records: list[dict[str, Any]]) -> bool:
    for record in records:
        blob = json.dumps(record, ensure_ascii=False).lower()
        url = _norm(record.get("url") or record.get("source_url"))

        if url == CANALICCHI_URL.lower():
            return True
        if "canalicchi" in blob and "solux" in blob:
            return True
    return False


def _template_from_records(records: list[dict[str, Any]]) -> dict[str, Any]:
    """Build an empty record with the same keys used by Calabria records when possible.

    This avoids carrying over unrelated values from another project while preserving
    fields expected by the dashboard, if present in the current JSON schema.
    """
    template: dict[str, Any] | None = None

    for record in records:
        if _norm(record.get("source")) == "calabria":
            template = record
            break

    if template is None and records:
        template = records[0]

    if template is None:
        return {}

    return {key: None for key in template.keys()}


def _set_if_present(record: dict[str, Any], key: str, value: Any) -> None:
    if key in record:
        record[key] = value


def _build_record(records: list[dict[str, Any]]) -> dict[str, Any]:
    record = _template_from_records(records)

    # Campi minimi usati dalla dashboard attuale.
    record.update(
        {
            "source": "Calabria",
            "region": "Calabria",
            "province": "KR",
            "municipalities": ["Crotone", "Scandale"],
            "power_mw": 19.52,
            "proponent": "SOLUX srl",
            "title": CANALICCHI_TITLE,
            "url": CANALICCHI_URL,
            "source_url": CANALICCHI_URL,
            "status": "PAUR",
        }
    )

    # CompatibilitÃ  con eventuali chiavi alternative presenti nello schema.
    _set_if_present(record, "id", CANALICCHI_KEY)
    _set_if_present(record, "external_id", CANALICCHI_KEY)
    _set_if_present(record, "project_key", CANALICCHI_KEY)
    _set_if_present(record, "source_id", CANALICCHI_KEY)

    _set_if_present(record, "project_name", "Canalicchi")
    _set_if_present(record, "name", "Canalicchi")
    _set_if_present(record, "municipality", "Crotone")
    _set_if_present(record, "municipality_raw", "Crotone; Scandale")
    _set_if_present(record, "city", "Crotone")
    _set_if_present(record, "location", "Crotone (KR), localitÃ  Canalicchi; opere connesse in Scandale (KR)")

    _set_if_present(record, "power_mwp", 19.52)
    _set_if_present(record, "capacity_mw", 19.52)
    _set_if_present(record, "peak_power_mw", 19.52)
    _set_if_present(record, "grid_power_mw", 16.0)
    _set_if_present(record, "power_injection_mw", 16.0)

    _set_if_present(record, "company", "SOLUX srl")
    _set_if_present(record, "applicant", "SOLUX srl")
    _set_if_present(record, "holder", "SOLUX srl")

    _set_if_present(record, "technology", "agrivoltaico")
    _set_if_present(record, "project_type", "agrivoltaico")
    _set_if_present(record, "is_agrivoltaico", True)
    _set_if_present(record, "procedure", "PAUR")
    _set_if_present(record, "authorization_type", "PAUR")

    return record


def _write_audit(path: Path | None, action: str, data_path: Path, record: dict[str, Any] | None) -> None:
    if path is None:
        return

    path.parent.mkdir(parents=True, exist_ok=True)
    exists = path.exists()

    with path.open("a", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(
            handle,
            fieldnames=[
                "timestamp",
                "action",
                "data_path",
                "source",
                "province",
                "municipalities",
                "power_mw",
                "proponent",
                "url",
            ],
        )
        if not exists:
            writer.writeheader()
        writer.writerow(
            {
                "timestamp": datetime.now().isoformat(timespec="seconds"),
                "action": action,
                "data_path": str(data_path),
                "source": (record or {}).get("source"),
                "province": (record or {}).get("province"),
                "municipalities": "; ".join((record or {}).get("municipalities") or []),
                "power_mw": (record or {}).get("power_mw"),
                "proponent": (record or {}).get("proponent"),
                "url": (record or {}).get("url") or (record or {}).get("source_url"),
            }
        )


def apply_override(data_path: Path, audit_path: Path | None = None) -> int:
    if not data_path.exists():
        raise FileNotFoundError(f"Data file non trovato: {data_path}")

    data = json.loads(data_path.read_text(encoding="utf-8"))
    records = _load_records(data)

    if _already_exists(records):
        print(f"[manual-calabria-overrides] OK: Canalicchi / SOLUX giÃ  presente in {data_path}")
        _write_audit(audit_path, "already_present", data_path, None)
        return 0

    record = _build_record(records)
    records.append(record)
    data_path.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")

    print(f"[manual-calabria-overrides] AGGIUNTO: Canalicchi / SOLUX in {data_path}")
    _write_audit(audit_path, "added", data_path, record)
    return 1


def main() -> None:
    parser = argparse.ArgumentParser(description="Override manuali per progetti Calabria non intercettati dal crawler.")
    parser.add_argument("--data", default="reports/site/data.json", help="Percorso del data.json da aggiornare")
    parser.add_argument("--audit", default=None, help="Percorso CSV audit override")
    args = parser.parse_args()

    data_path = Path(args.data)
    audit_path = Path(args.audit) if args.audit else None

    added = apply_override(data_path, audit_path)
    print(f"[manual-calabria-overrides] record aggiunti: {added}")


if __name__ == "__main__":
    main()
