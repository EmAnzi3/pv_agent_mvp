from __future__ import annotations

import argparse
import csv
import json
from pathlib import Path


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
    "FORLI-CESENA": "FC",
    "FROSINONE": "FR",
    "GENOVA": "GE",
    "GORIZIA": "GO",
    "GROSSETO": "GR",
    "IMPERIA": "IM",
    "ISERNIA": "IS",
    "LA SPEZIA": "SP",
    "L'AQUILA": "AQ",
    "LATINA": "LT",
    "LECCE": "LE",
    "LECCO": "LC",
    "LIVORNO": "LI",
    "LODI": "LO",
    "LUCCA": "LU",
    "MACERATA": "MC",
    "MANTOVA": "MN",
    "MASSA-CARRARA": "MS",
    "MATERA": "MT",
    "MESSINA": "ME",
    "MILANO": "MI",
    "MODENA": "MO",
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
    "VERBANIA": "VB",
    "VERCELLI": "VC",
    "VERONA": "VR",
    "VIBO VALENTIA": "VV",
    "VICENZA": "VI",
    "VITERBO": "VT",
}


def normalize_province(value: str | None) -> str:
    if value is None:
        return ""

    raw = str(value).strip()
    if not raw:
        return ""

    # Se è già sigla provincia, la manteniamo.
    if len(raw) == 2 and raw.isalpha():
        return raw.upper()

    # Gestione multi-provincia tipo "LE BR" o "LE, BR"
    parts = raw.replace(",", " ").replace("/", " ").replace("-", " ").split()
    if len(parts) > 1 and all(len(p) == 2 and p.isalpha() for p in parts):
        return " ".join(p.upper() for p in parts)

    key = raw.upper().strip()
    return PROVINCE_NAME_TO_CODE.get(key, raw)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--data", required=True, help="Percorso data.json da normalizzare")
    parser.add_argument("--audit", default="reports/province_normalization_audit.csv")
    args = parser.parse_args()

    data_path = Path(args.data)
    audit_path = Path(args.audit)

    if not data_path.exists():
        raise FileNotFoundError(f"File non trovato: {data_path}")

    data = json.loads(data_path.read_text(encoding="utf-8"))
    records = data.get("records", [])

    changes = []

    for idx, row in enumerate(records):
        old = str(row.get("province") or "").strip()
        new = normalize_province(old)

        if old != new:
            row["province"] = new
            changes.append({
                "idx": idx,
                "source": row.get("source", ""),
                "title": row.get("title", ""),
                "old_province": old,
                "new_province": new,
                "url": row.get("url", ""),
            })

    data_path.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")

    audit_path.parent.mkdir(parents=True, exist_ok=True)

    with audit_path.open("w", newline="", encoding="utf-8-sig") as handle:
        writer = csv.DictWriter(
            handle,
            fieldnames=["idx", "source", "title", "old_province", "new_province", "url"],
        )
        writer.writeheader()
        writer.writerows(changes)

    print(f"[province-normalization] file: {data_path}")
    print(f"[province-normalization] province normalizzate: {len(changes)}")
    print(f"[province-normalization] audit: {audit_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
