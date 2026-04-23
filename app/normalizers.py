from __future__ import annotations

import re
from typing import Any

STATUS_MAP = {
    "in verifica amministrativa": "VERIFICA_AMMINISTRATIVA",
    "verifica amministrativa": "VERIFICA_AMMINISTRATIVA",
    "in corso": "ISTRUTTORIA",
    "in itinere": "ISTRUTTORIA",
    "istruttoria in corso": "ISTRUTTORIA",
    "archiviata": "ARCHIVIATO",
    "archiviato": "ARCHIVIATO",
    "positivo": "PROVVEDIMENTO_POSITIVO",
    "negativo": "PROVVEDIMENTO_NEGATIVO",
}

TYPE_KEYWORDS = {
    "agrivolta": "AGRIVOLTAICO",
    "agrovolta": "AGRIVOLTAICO",
    "fotovolta": "FOTOVOLTAICO",
    "bess": "BESS",
    "accumulo": "BESS",
    "storage": "BESS",
}


def normalize_text(value: str | None) -> str:
    if not value:
        return ""
    return " ".join(value.strip().split())



def normalize_status(value: str | None) -> str:
    raw = normalize_text(value).lower()
    return STATUS_MAP.get(raw, raw.upper().replace(" ", "_") if raw else "UNKNOWN")



def normalize_project_type(title: str | None, description: str | None = None) -> str:
    text = f"{title or ''} {description or ''}".lower()
    matches = {normalized for keyword, normalized in TYPE_KEYWORDS.items() if keyword in text}
    if "AGRIVOLTAICO" in matches:
        return "AGRIVOLTAICO"
    if "FOTOVOLTAICO" in matches and "BESS" in matches:
        return "FOTOVOLTAICO+BESS"
    if matches:
        return sorted(matches)[0]
    return "NON_CLASSIFICATO"



def normalize_power_to_mw(value: Any) -> str | None:
    if value is None:
        return None
    text = normalize_text(str(value)).lower()
    text = text.replace("mw", " mw ").replace("mwp", " mwp ").replace("kwp", " kwp ")
    match = re.search(r"([0-9]+(?:[\.,][0-9]+)?)", text)
    if not match:
        return None
    num = match.group(1).replace(".", "").replace(",", ".") if "," in match.group(1) else match.group(1)
    try:
        value_num = float(num)
    except ValueError:
        return None
    if "kw" in text and "mw" not in text:
        value_num = value_num / 1000
    return f"{value_num:.3f}"



def as_list(value: Any) -> list[str]:
    if value is None:
        return []
    if isinstance(value, list):
        return [normalize_text(str(v)) for v in value if normalize_text(str(v))]
    if isinstance(value, str):
        parts = re.split(r"[,;/]", value)
        return [normalize_text(p) for p in parts if normalize_text(p)]
    return [normalize_text(str(value))]
