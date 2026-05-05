from __future__ import annotations

import argparse
import csv
import json
import re
import unicodedata
from pathlib import Path
from typing import Any


PROVINCE_NAME_TO_CODE = {
    "AGRIGENTO": "AG",
    "ALESSANDRIA": "AL",
    "ANCONA": "AN",
    "AOSTA": "AO",
    "AREZZO": "AR",
    "ASCOLI PICENO": "AP",
    "ASTI": "AT",
    "AVELLINO": "AV",
    "BARI": "BA",
    "BARLETTA ANDRIA TRANI": "BT",
    "BARLETTA-ANDRIA-TRANI": "BT",
    "BELLUNO": "BL",
    "BENEVENTO": "BN",
    "BERGAMO": "BG",
    "BIELLA": "BI",
    "BOLOGNA": "BO",
    "BOLZANO": "BZ",
    "BRESCIA": "BS",
    "BRINDISI": "BR",
    "CAGLIARI": "CA",
    "CALTANISSETTA": "CL",
    "CAMPOBASSO": "CB",
    "CARBONIA IGLESIAS": "CI",
    "CASERTA": "CE",
    "CATANIA": "CT",
    "CATANZARO": "CZ",
    "CHIETI": "CH",
    "COMO": "CO",
    "COSENZA": "CS",
    "CREMONA": "CR",
    "CROTONE": "KR",
    "CUNEO": "CN",
    "ENNA": "EN",
    "FERMO": "FM",
    "FERRARA": "FE",
    "FIRENZE": "FI",
    "FOGGIA": "FG",
    "FORLI CESENA": "FC",
    "FORLI-CESENA": "FC",
    "FROSINONE": "FR",
    "GENOVA": "GE",
    "GORIZIA": "GO",
    "GROSSETO": "GR",
    "IMPERIA": "IM",
    "ISERNIA": "IS",
    "LA SPEZIA": "SP",
    "L AQUILA": "AQ",
    "L'AQUILA": "AQ",
    "LATINA": "LT",
    "LECCE": "LE",
    "LECCO": "LC",
    "LIVORNO": "LI",
    "LODI": "LO",
    "LUCCA": "LU",
    "MACERATA": "MC",
    "MANTOVA": "MN",
    "MASSA CARRARA": "MS",
    "MASSA-CARRARA": "MS",
    "MATERA": "MT",
    "MESSINA": "ME",
    "MILANO": "MI",
    "MODENA": "MO",
    "MONZA BRIANZA": "MB",
    "MONZA E BRIANZA": "MB",
    "NAPOLI": "NA",
    "NOVARA": "NO",
    "NUORO": "NU",
    "ORISTANO": "OR",
    "PADOVA": "PD",
    "PALERMO": "PA",
    "PARMA": "PR",
    "PAVIA": "PV",
    "PERUGIA": "PG",
    "PESARO URBINO": "PU",
    "PESARO E URBINO": "PU",
    "PESCARA": "PE",
    "PIACENZA": "PC",
    "PISA": "PI",
    "PISTOIA": "PT",
    "PORDENONE": "PN",
    "POTENZA": "PZ",
    "PRATO": "PO",
    "RAGUSA": "RG",
    "RAVENNA": "RA",
    "REGGIO CALABRIA": "RC",
    "REGGIO EMILIA": "RE",
    "RIETI": "RI",
    "RIMINI": "RN",
    "ROMA": "RM",
    "ROVIGO": "RO",
    "SALERNO": "SA",
    "SASSARI": "SS",
    "SAVONA": "SV",
    "SIENA": "SI",
    "SIRACUSA": "SR",
    "SONDRIO": "SO",
    "SUD SARDEGNA": "SU",
    "TARANTO": "TA",
    "TERAMO": "TE",
    "TERNI": "TR",
    "TORINO": "TO",
    "TRAPANI": "TP",
    "TRENTO": "TN",
    "TREVISO": "TV",
    "TRIESTE": "TS",
    "UDINE": "UD",
    "VARESE": "VA",
    "VENEZIA": "VE",
    "VERBANO CUSIO OSSOLA": "VB",
    "VERBANIA": "VB",
    "VERCELLI": "VC",
    "VERONA": "VR",
    "VIBO VALENTIA": "VV",
    "VICENZA": "VI",
    "VITERBO": "VT",
}


VALID_CODES = set(PROVINCE_NAME_TO_CODE.values())


def clean(value: Any) -> str:
    if value is None:
        return ""
    return re.sub(r"\s+", " ", str(value)).strip()


def normalize_key(value: str) -> str:
    text = clean(value).upper()
    text = unicodedata.normalize("NFKD", text)
    text = "".join(ch for ch in text if not unicodedata.combining(ch))
    text = text.replace("’", "'")
    text = re.sub(r"[^A-Z0-9']+", " ", text)
    text = re.sub(r"\s+", " ", text).strip()
    return text


def normalize_province(value: Any) -> str:
    raw = clean(value)
    if not raw:
        return ""

    # Already valid province code.
    if len(raw) == 2 and raw.upper() in VALID_CODES:
        return raw.upper()

    # Multi-code values: "LE BR", "LE, BR", "LE/BR"
    parts = re.split(r"[,/;\s]+", raw.strip())
    if len(parts) > 1 and all(len(part) == 2 and part.upper() in VALID_CODES for part in parts if part):
        return " ".join(part.upper() for part in parts if part)

    key = normalize_key(raw)
    if key in PROVINCE_NAME_TO_CODE:
        return PROVINCE_NAME_TO_CODE[key]

    # Case: "Provincia di Grosseto"
    key = re.sub(r"^(PROVINCIA|PROV)\s+(DI\s+)?", "", key).strip()
    if key in PROVINCE_NAME_TO_CODE:
        return PROVINCE_NAME_TO_CODE[key]

    return raw


def normalize_record(record: dict[str, Any], idx: int) -> dict[str, Any] | None:
    old = clean(record.get("province"))
    new = normalize_province(old)

    if old != new:
        record["province"] = new
        return {
            "idx": idx,
            "source": record.get("source", ""),
            "source_label": record.get("source_label", ""),
            "title": record.get("title", ""),
            "old_province": old,
            "new_province": new,
            "url": record.get("url", ""),
        }

    return None


def main() -> int:
    parser = argparse.ArgumentParser(description="Normalizza nomi provincia in sigle provincia nei data.json.")
    parser.add_argument("--data", required=True, help="Percorso data.json da normalizzare")
    parser.add_argument("--audit", default="reports/province_normalization_audit.csv", help="CSV audit")
    args = parser.parse_args()

    data_path = Path(args.data)
    audit_path = Path(args.audit)

    if not data_path.exists():
        raise FileNotFoundError(f"File non trovato: {data_path}")

    data = json.loads(data_path.read_text(encoding="utf-8"))

    records = data.get("records", [])
    if not isinstance(records, list):
        raise ValueError("data.json non contiene una lista records valida")

    changes = []
    for idx, record in enumerate(records):
        if isinstance(record, dict):
            change = normalize_record(record, idx)
            if change:
                changes.append(change)

    # Normalizza anche summary.top_projects, se presente.
    top_projects = data.get("summary", {}).get("top_projects", [])
    if isinstance(top_projects, list):
        for idx, record in enumerate(top_projects):
            if isinstance(record, dict):
                change = normalize_record(record, idx)
                if change:
                    change["idx"] = f"summary.top_projects[{idx}]"
                    changes.append(change)

    data_path.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")

    audit_path.parent.mkdir(parents=True, exist_ok=True)
    with audit_path.open("w", newline="", encoding="utf-8-sig") as handle:
        writer = csv.DictWriter(
            handle,
            fieldnames=["idx", "source", "source_label", "title", "old_province", "new_province", "url"],
        )
        writer.writeheader()
        writer.writerows(changes)

    print(f"[province-normalization] data: {data_path}")
    print(f"[province-normalization] province normalizzate: {len(changes)}")
    print(f"[province-normalization] audit: {audit_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
