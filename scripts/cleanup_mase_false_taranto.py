from __future__ import annotations

import argparse
import csv
import json
import re
from datetime import datetime
from pathlib import Path


PROVINCE_TO_REGION = {
    "AG": "Sicilia", "CL": "Sicilia", "CT": "Sicilia", "EN": "Sicilia", "ME": "Sicilia",
    "PA": "Sicilia", "RG": "Sicilia", "SR": "Sicilia", "TP": "Sicilia",
    "FG": "Puglia", "BA": "Puglia", "BR": "Puglia", "LE": "Puglia", "TA": "Puglia",
    "MO": "Emilia-Romagna", "BO": "Emilia-Romagna", "RA": "Emilia-Romagna",
    "OR": "Sardegna", "SS": "Sardegna", "SU": "Sardegna", "CI": "Sardegna", "VS": "Sardegna",
    "VE": "Veneto", "TO": "Piemonte", "MT": "Basilicata", "PZ": "Basilicata",
}

PROVINCE_ALIASES = {
    "CI": "SU",
    "VS": "SU",
}

PROVINCE_CAPITAL_MUNICIPALITY = {
    "EN": "Enna",
    "MT": "Matera",
}

MUNICIPALITY_TO_PROVINCE = {
    "CATANIA": "CT",
    "FOGGIA": "FG",
    "RAVENNA": "RA",
    "MODENA": "MO",
    "VENEZIA": "VE",
    "SERRAMANNA": "SU",
    "GRAVINA IN PUGLIA": "BA",
    "ORISTANO": "OR",
    "MARRUBIU": "OR",
    "PALMAS ARBOREA": "OR",
    "SANTA GIUSTA": "OR",
    "SIAMANNA": "OR",
    "SIMAXIS": "OR",
    "VILLAMASSARGIA": "SU",
    "MUSEI": "SU",
    "MIGLIONICO": "MT",
    "ENNA": "EN",
    "MINEO": "CT",
    "RAMACCA": "CT",
    "AIDONE": "EN",
    "APRICÉNA": "FG",
    "APRICENA": "FG",
    "SAN SEVERO": "FG",
}

SPLIT_RE = re.compile(r"\s*,\s*|\s+e\s+|\s+ed\s+", flags=re.IGNORECASE)


def normalize_province(code: str | None) -> str:
    c = str(code or "").strip().upper()
    return PROVINCE_ALIASES.get(c, c)


def region_from_province(code: str | None) -> str:
    c = normalize_province(code)
    return PROVINCE_TO_REGION.get(c, "")


def as_municipality_list(value) -> list[str]:
    if isinstance(value, list):
        raw = value
    else:
        raw = SPLIT_RE.split(str(value or ""))

    out = []
    seen = set()

    for item in raw:
        x = str(item or "").strip()
        if not x:
            continue
        key = x.upper()
        if key not in seen:
            out.append(x)
            seen.add(key)

    return out


def clean_title_municipality(value: str) -> str:
    x = str(value or "").strip()

    # Rimuove sigle provincia tra parentesi: "Musei (CI)" -> "Musei"
    x = re.sub(r"\s*\([A-Z]{2}\)", "", x)

    # Rimuove residui tipo "Simaxis OR)" o "Simaxis OR"
    x = re.sub(r"\s+[A-Z]{2}\)?\s*$", "", x)

    # Taglia rumore testuale dopo il nome del comune
    x = re.sub(
        r"\b(?:della|di|con|e|ed|relative|opere|potenza|nel|nella|territorio|localit[a?]).*$",
        "",
        x,
        flags=re.IGNORECASE,
    )

    # Secondo passaggio dopo il taglio del rumore
    x = re.sub(r"\s+[A-Z]{2}\)?\s*$", "", x)

    return x.strip(" ,.;:-)")


def extract_municipalities_from_title(title: str) -> list[str]:
    patterns = [
        r"nei comuni di\s+(.+?)(?:\s+e\s+relative|\s+della|\s+con|\s+nel territorio|\s*$)",
        r"nei Comuni\s+(.+?)(?:\s+e\s+relative|\s+della|\s+con|\s+nel territorio|\s*$)",
        r"nei Comuni di\s+(.+?)(?:\s+e\s+relative|\s+della|\s+con|\s+nel territorio|\s*$)",
        r"nel Comune di\s+(.+?)(?:\s*\([A-Z]{2}\)|,|\.|$)",
        r"territorio comunale di\s+(.+?)(?:\s*\([A-Z]{2}\)|,|\.|$)",
        r"territorio del Comune di\s+(.+?)(?:\s*\([A-Z]{2}\)|,|\.|$)",
    ]

    found = []

    for pattern in patterns:
        m = re.search(pattern, title or "", flags=re.IGNORECASE)
        if not m:
            continue

        chunk = m.group(1)
        parts = SPLIT_RE.split(chunk)

        for part in parts:
            name = clean_title_municipality(part)
            if name and len(name) > 2:
                found.append(name)

        if found:
            break

    out = []
    seen = set()

    for item in found:
        key = item.upper()
        if key not in seen and key != "TARANTO":
            out.append(item)
            seen.add(key)

    return out


def extract_province_codes_from_title(title: str) -> list[str]:
    codes = []

    for match in re.findall(r"\(([A-Z]{2})\)", title or ""):
        code = normalize_province(match)
        if code and code in PROVINCE_TO_REGION and code not in codes:
            codes.append(code)

    return codes


def has_explicit_taranto_in_title(title: str) -> bool:
    t = str(title or "").lower()
    return "taranto" in t or re.search(r"\(\s*ta\s*\)", t, flags=re.IGNORECASE) is not None


def infer_province(title: str, municipalities: list[str], current_province: str) -> str:
    codes = [c for c in extract_province_codes_from_title(title) if c != "TA"]

    if codes:
        return codes[0]

    for m in municipalities:
        code = MUNICIPALITY_TO_PROVINCE.get(str(m).strip().upper())
        if code:
            return normalize_province(code)

    current = normalize_province(current_province)
    if current != "TA":
        return current

    return ""


def _records_container(data):
    if isinstance(data, dict) and isinstance(data.get("records"), list):
        return data["records"]
    if isinstance(data, list):
        return data
    raise ValueError("Formato data.json non riconosciuto")


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--data", required=True)
    parser.add_argument("--audit", default="reports/mase_false_taranto_cleanup_audit.csv")
    parser.add_argument("--apply", action="store_true")
    args = parser.parse_args()

    data_path = Path(args.data)
    audit_path = Path(args.audit)

    data = json.loads(data_path.read_text(encoding="utf-8"))
    records = _records_container(data)

    rows = []
    ts = datetime.now().isoformat(timespec="seconds")

    for r in records:
        source = str(r.get("source") or "").lower()

        if "mase" not in source:
            continue

        title = str(r.get("title") or "")
        old_region = str(r.get("region") or "")
        old_province = str(r.get("province") or "")
        old_municipalities = r.get("municipalities") or ""

        municipalities = as_municipality_list(old_municipalities)
        has_taranto_muni = any(str(m).strip().upper() == "TARANTO" for m in municipalities)
        has_ta_province = old_province.strip().upper() == "TA"

        if not (has_taranto_muni or has_ta_province):
            continue

        # Se il titolo parla esplicitamente di Taranto / (TA), non toccare.
        if has_explicit_taranto_in_title(title):
            continue

        new_municipalities = [
            m for m in municipalities
            if str(m).strip().upper() != "TARANTO"
        ]

        if not new_municipalities:
            new_municipalities = extract_municipalities_from_title(title)

        # Pulizia finale dei comuni: rimuove residui tipo "Simaxis OR)" / "Simaxis OR"
        new_municipalities = [
            re.sub(r"\s+[A-Z]{2}\)?\s*$", "", str(m).strip()).strip(" ,.;:-)")
            for m in new_municipalities
        ]
        new_municipalities = [m for m in new_municipalities if m]

        new_province = infer_province(title, new_municipalities, old_province)

        if not new_municipalities and new_province in PROVINCE_CAPITAL_MUNICIPALITY:
            new_municipalities = [PROVINCE_CAPITAL_MUNICIPALITY[new_province]]

        new_region = region_from_province(new_province) or old_region

        # Se non abbiamo abbastanza informazioni, meglio togliere il falso TA/Taranto
        # e lasciare vuoto piuttosto che inventare.
        if has_ta_province and not new_province:
            new_region = ""
            new_province = ""

        changed = (
            ", ".join(new_municipalities) != str(old_municipalities)
            or new_province != old_province
            or new_region != old_region
        )

        if not changed:
            continue

        row = {
            "timestamp": ts,
            "url": r.get("url", ""),
            "title": title,
            "proponent": r.get("proponent", ""),
            "old_region": old_region,
            "new_region": new_region,
            "old_province": old_province,
            "new_province": new_province,
            "old_municipalities": old_municipalities,
            "new_municipalities": ", ".join(new_municipalities),
            "reason": "remove_false_taranto_from_mase_location",
        }
        rows.append(row)

        if args.apply:
            r["region"] = new_region
            r["province"] = new_province
            r["municipalities"] = ", ".join(new_municipalities)

    audit_path.parent.mkdir(parents=True, exist_ok=True)

    fields = [
        "timestamp", "url", "title", "proponent",
        "old_region", "new_region",
        "old_province", "new_province",
        "old_municipalities", "new_municipalities",
        "reason",
    ]

    with audit_path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fields)
        writer.writeheader()
        writer.writerows(rows)

    if args.apply:
        data_path.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")

    print(f"[mase-false-taranto-cleanup] candidati/corretti: {len(rows)}")
    print(f"[mase-false-taranto-cleanup] apply: {args.apply}")
    print(f"[mase-false-taranto-cleanup] audit: {audit_path}")

    for row in rows:
        print("-" * 80)
        print("title:", row["title"][:160])
        print("old:", row["old_region"], row["old_province"], row["old_municipalities"])
        print("new:", row["new_region"], row["new_province"], row["new_municipalities"])
        print("url:", row["url"])

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
