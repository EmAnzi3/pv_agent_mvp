from __future__ import annotations

import argparse
import csv
import hashlib
import html
import json
import math
import re
import shutil
import unicodedata
from collections import Counter, defaultdict
from datetime import datetime
from difflib import SequenceMatcher
from pathlib import Path
from typing import Any
from urllib.parse import parse_qsl, urlparse


TITLE_FIELDS = [
    "title", "titolo", "project", "progetto", "project_name", "nome_progetto",
    "name", "nome", "denominazione", "impianto", "descrizione"
]
SOURCE_FIELDS = ["source", "fonte", "dataset", "origine", "source_name"]
URL_FIELDS = ["url", "link", "source_url", "detail_url", "href", "pagina", "page_url"]
REGION_FIELDS = ["region", "regione"]
PROVINCE_FIELDS = ["province", "provincia", "prov", "sigla_provincia", "sigla_prov"]
MUNICIPALITY_FIELDS = ["municipality", "comune", "city", "localita", "località", "locality"]
MW_FIELDS = [
    "mw", "mwp", "potenza_mw", "potenza_mwp", "capacity_mw", "power_mw",
    "potenza", "power", "capacita", "capacità"
]
PROPONENT_FIELDS = [
    "proponent", "proponente", "societa", "società", "azienda", "richiedente",
    "developer", "soggetto", "soggetto_proponente"
]

SPECIFIC_QUERY_KEYS = {
    "id", "idprocedimento", "idprocedura", "idpratica", "idprogetto",
    "idistanza", "articleid", "articlegroupid", "codice", "uuid",
    "key", "n", "numero", "pk", "iddocumento"
}

GENERIC_PATH_WORDS = {
    "search", "ricerca", "lista", "elenco", "progetti", "procedimenti",
    "archivio", "home", "index", "page", "portal", "download", "center"
}

STOPWORDS = {
    "impianto", "fotovoltaico", "agrivoltaico", "agrovoltaico", "realizzazione",
    "costruzione", "esercizio", "progetto", "procedura", "valutazione", "verifica",
    "assoggettabilita", "assoggettabilità", "via", "vas", "pnrr", "pniec",
    "autorizzazione", "unica", "determina", "determinazione", "provvedimento",
    "comune", "provincia", "localita", "località", "sito", "territorio",
    "potenza", "nominale", "complessiva", "mw", "mwp", "kw", "kwp"
}

# Province italiane: serve per non bloccare falsi duplicati tipo FR vs FROSINONE.
PROVINCE_NAME_TO_CODE = {
    "agrigento": "AG", "alessandria": "AL", "ancona": "AN", "aosta": "AO", "arezzo": "AR",
    "ascoli piceno": "AP", "asti": "AT", "avellino": "AV", "bari": "BA", "barletta andria trani": "BT",
    "belluno": "BL", "benevento": "BN", "bergamo": "BG", "biella": "BI", "bologna": "BO",
    "bolzano": "BZ", "brescia": "BS", "brindisi": "BR", "cagliari": "CA", "caltanissetta": "CL",
    "campobasso": "CB", "caserta": "CE", "catania": "CT", "catanzaro": "CZ", "chieti": "CH",
    "como": "CO", "cosenza": "CS", "cremona": "CR", "crotone": "KR", "cuneo": "CN",
    "enna": "EN", "fermo": "FM", "ferrara": "FE", "firenze": "FI", "foggia": "FG",
    "forli cesena": "FC", "forli": "FC", "frosinone": "FR", "genova": "GE", "gorizia": "GO",
    "grosseto": "GR", "imperia": "IM", "isernia": "IS", "la spezia": "SP", "laquila": "AQ",
    "l aquila": "AQ", "latina": "LT", "lecce": "LE", "lecco": "LC", "livorno": "LI",
    "lodi": "LO", "lucca": "LU", "macerata": "MC", "mantova": "MN", "massa carrara": "MS",
    "matera": "MT", "messina": "ME", "milano": "MI", "modena": "MO", "monza brianza": "MB",
    "monza e brianza": "MB", "napoli": "NA", "novara": "NO", "nuoro": "NU", "oristano": "OR",
    "padova": "PD", "palermo": "PA", "parma": "PR", "pavia": "PV", "perugia": "PG",
    "pesaro urbino": "PU", "pescara": "PE", "piacenza": "PC", "pisa": "PI", "pistoia": "PT",
    "pordenone": "PN", "potenza": "PZ", "prato": "PO", "ragusa": "RG", "ravenna": "RA",
    "reggio calabria": "RC", "reggio emilia": "RE", "rieti": "RI", "rimini": "RN", "roma": "RM",
    "rovigo": "RO", "salerno": "SA", "sassari": "SS", "savona": "SV", "siena": "SI",
    "siracusa": "SR", "sondrio": "SO", "sud sardegna": "SU", "taranto": "TA", "teramo": "TE",
    "terni": "TR", "torino": "TO", "trapani": "TP", "trento": "TN", "treviso": "TV",
    "trieste": "TS", "udine": "UD", "varese": "VA", "venezia": "VE", "verbano cusio ossola": "VB",
    "vercelli": "VC", "verona": "VR", "vibo valentia": "VV", "vicenza": "VI", "viterbo": "VT",
}
PROVINCE_CODES = set(PROVINCE_NAME_TO_CODE.values())


def now_stamp() -> str:
    return datetime.now().strftime("%Y%m%d_%H%M%S")


def norm_key(s: str) -> str:
    s = str(s).strip().lower()
    s = unicodedata.normalize("NFKD", s)
    s = "".join(ch for ch in s if not unicodedata.combining(ch))
    return re.sub(r"[^a-z0-9]+", "", s)


def norm_text(value: Any) -> str:
    if value is None:
        return ""
    s = html.unescape(str(value))
    s = unicodedata.normalize("NFKD", s)
    s = "".join(ch for ch in s if not unicodedata.combining(ch))
    s = s.lower().replace("&", " e ")
    s = re.sub(r"\b(s\.?\s*r\.?\s*l\.?|srl)\b", " srl ", s)
    s = re.sub(r"\b(s\.?\s*p\.?\s*a\.?|spa)\b", " spa ", s)
    s = re.sub(r"\b(s\.?\s*r\.?\s*l\.?\s*s\.?|srls)\b", " srls ", s)
    s = re.sub(r"[^a-z0-9]+", " ", s)
    return re.sub(r"\s+", " ", s).strip()


def normalize_province(value: Any) -> str:
    s_raw = str(value or "").strip()
    if not s_raw:
        return ""
    s_up = re.sub(r"[^A-Za-z]", "", s_raw).upper()
    if len(s_up) == 2 and s_up in PROVINCE_CODES:
        return s_up
    s = norm_text(s_raw)
    if s in PROVINCE_NAME_TO_CODE:
        return PROVINCE_NAME_TO_CODE[s]
    s2 = s.replace("provincia di ", "").replace("provincia ", "").strip()
    if s2 in PROVINCE_NAME_TO_CODE:
        return PROVINCE_NAME_TO_CODE[s2]
    return s_up or s.upper()
def province_codes_from_value(value: Any) -> set[str]:
    """Restituisce tutte le sigle provincia presenti in un campo.

    Serve per campi multi-provincia tipo "LE BR" o "LE/BR".
    """
    raw = str(value or "").strip()
    if not raw:
        return set()

    single = normalize_province(raw)
    if single in PROVINCE_CODES:
        return {single}

    codes: set[str] = set()
    for token in re.findall(r"[A-Za-z]{2}", raw.upper()):
        if token in PROVINCE_CODES:
            codes.add(token)

    # Anche eventuali nomi estesi nel campo.
    n = norm_text(raw)
    for name, code in PROVINCE_NAME_TO_CODE.items():
        if re.search(rf"\b{re.escape(name)}\b", n):
            codes.add(code)

    return codes




def title_tokens(title: str) -> set[str]:
    return {x for x in norm_text(title).split() if len(x) >= 4 and x not in STOPWORDS}


def short_hash(value: str, length: int = 16) -> str:
    return hashlib.sha1(value.encode("utf-8", errors="ignore")).hexdigest()[:length]


def field_lookup(row: dict[str, Any]) -> dict[str, str]:
    return {norm_key(k): k for k in row.keys()}


def get_field(row: dict[str, Any], candidates: list[str]) -> Any:
    lookup = field_lookup(row)
    for c in candidates:
        original = lookup.get(norm_key(c))
        if original is not None:
            value = row.get(original)
            if value not in (None, ""):
                return value
    return ""


def set_field_if_present(row: dict[str, Any], candidates: list[str], value: Any) -> None:
    lookup = field_lookup(row)
    for c in candidates:
        original = lookup.get(norm_key(c))
        if original is not None:
            row[original] = value
            return
    row[candidates[0]] = value


def parse_number_locale(raw: str) -> float | None:
    raw = str(raw or "").strip()
    if not raw:
        return None

    # Normalizza separatori frequenti nei portali italiani:
    # 19.994,88  -> 19994.88
    # 6'093.36   -> 6093.36
    # 6’093.36   -> 6093.36
    raw = (
        raw.replace("\u00a0", " ")
        .replace(" ", "")
        .replace("’", "'")
        .replace("`", "'")
    )

    match = re.search(r"[-+]?\d+(?:[.,']\d+)*(?:[.,]\d+)?", raw)
    if not match:
        return None

    x = match.group(0).replace("'", "")

    if "." in x and "," in x:
        if x.rfind(",") > x.rfind("."):
            x = x.replace(".", "").replace(",", ".")
        else:
            x = x.replace(",", "")
    elif "," in x:
        x = x.replace(",", ".")
    elif "." in x and len(x.split(".")) > 2:
        parts = x.split(".")
        x = "".join(parts[:-1]) + "." + parts[-1]

    try:
        return float(x)
    except ValueError:
        return None


def parse_mw(value: Any) -> float | None:
    if value is None or value == "":
        return None
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        if math.isnan(float(value)):
            return None
        return float(value)
    n = parse_number_locale(str(value))
    return n


def title_power_to_mw(raw_number: str, unit: str) -> float | None:
    """Converte una potenza trovata nel titolo in MW.

    Regola pratica:
    - MW/MWp restano MW.
    - kW/kWp vengono convertiti in MW.
    - Notazione italiana ambigua con punto:
      130.000 kWp -> 130 MW
      6.000 kWp   -> 6 MW
      4.495 kWp   -> 4.495 MW
      980.20 kWp  -> 0.9802 MW
    - Notazione con apostrofo:
      6'093.36 kWp -> 6.09336 MW
    """
    raw = (
        str(raw_number or "")
        .strip()
        .replace("\u00a0", " ")
        .replace(" ", "")
        .replace("’", "'")
        .replace("`", "'")
    )
    unit = str(unit or "").lower()
    n = parse_number_locale(raw)
    if n is None:
        return None

    if unit.startswith("mw"):
        return n

    if unit.startswith("kw"):
        if "'" in raw:
            return n / 1000.0
        if "." in raw and "," in raw:
            return n / 1000.0
        if "." in raw and "," not in raw:
            parts = raw.split(".")
            if len(parts) == 2:
                integer, frac = parts
                if len(frac) == 2 and n >= 100:
                    return n / 1000.0
                if len(frac) >= 3 and n < 1000:
                    return n
            return n / 1000.0 if n >= 1000 else n
        return n / 1000.0

    return n


_POWER_NUMBER_RE = r"\d+(?:[\.,'’]\d+)*(?:[\.,]\d+)?"

# V10: fonti per cui il titolo procedurale è considerato più affidabile
# del campo estratto quando c'è una discordanza esplicita e verificabile.
TRUST_TITLE_MW_SOURCES = {"emilia_romagna", "emilia romagna"}
TRUST_TITLE_PROVINCE_SOURCES = {"lazio"}


def extract_title_mws(title: str) -> list[float]:
    """Estrae tutte le potenze leggibili dal titolo.

    Serve per evitare falsi allarmi quando il titolo contiene sia potenza di picco
    sia potenza in immissione/AC. Esempio: 10,9326 MWp e 9,635 MW.
    """
    t = html.unescape(str(title or ""))
    values: list[float] = []
    seen: set[float] = set()

    patterns = [
        rf"({_POWER_NUMBER_RE})\s*(mwp|mw|kwp|kw)\b",
        rf"potenza[^\d]{{0,60}}({_POWER_NUMBER_RE})\s*(mwp|mw|kwp|kw)\b",
    ]
    for pat in patterns:
        for m in re.finditer(pat, t, flags=re.IGNORECASE):
            mw = title_power_to_mw(m.group(1), m.group(2))
            if mw is None:
                continue
            key = round(float(mw), 6)
            if key not in seen:
                seen.add(key)
                values.append(float(mw))
    return values


def extract_title_mw(title: str) -> float | None:
    values = extract_title_mws(title)
    return values[0] if values else None


def title_has_close_mw(title: str, field_mw: float | None, tolerance: float = 0.05) -> bool:
    if field_mw is None:
        return True
    values = extract_title_mws(title)
    if not values:
        return True
    return any(mw_close(v, field_mw, tolerance=tolerance) for v in values)


def best_title_mw_for_repair(title: str, field_mw: float | None) -> float | None:
    """Sceglie la potenza del titolo più coerente con un campo palesemente sottoscala."""
    values = extract_title_mws(title)
    if not values:
        return None
    if field_mw is None or field_mw == 0:
        return values[0]

    candidates = []
    for v in values:
        if should_repair_mw_from_title(field_mw, v):
            ratio = abs((v / max(field_mw, 1e-9)) - round(v / max(field_mw, 1e-9)))
            candidates.append((ratio, v))
    if candidates:
        candidates.sort(key=lambda x: x[0])
        return candidates[0][1]
    return values[0]

def mw_close(a: float | None, b: float | None, tolerance: float = 0.025) -> bool:
    if a is None or b is None:
        return True
    denom = max(abs(a), abs(b), 1.0)
    return abs(a - b) / denom <= tolerance


def mw_conflict(a: float | None, b: float | None, tolerance: float = 0.035) -> bool:
    if a is None or b is None:
        return False
    denom = max(abs(a), abs(b), 1.0)
    return abs(a - b) / denom > tolerance


def normalize_municipality(value: Any) -> str:
    s = norm_text(value)
    s = re.sub(r"\bcomune di\b", " ", s)
    s = re.sub(r"\bin\b", " ", s)
    return re.sub(r"\s+", " ", s).strip()


def is_terna_aggregate(row: dict[str, Any]) -> bool:
    # V2: non basta trovare la stringa "terna" nel titolo, perché appare in parole come "esterna".
    source = norm_text(get_field(row, SOURCE_FIELDS))
    title = norm_text(get_field(row, TITLE_FIELDS))
    url = str(get_field(row, URL_FIELDS) or "").lower()
    kind = norm_text(row.get("type", "") or row.get("record_type", "") or row.get("categoria", ""))

    if source in {"terna", "terna econnextion", "terna_econnextion"}:
        return True
    if source.startswith("terna ") or source.startswith("terna_"):
        return True
    if title.startswith("terna econnextion"):
        return True
    if "dati.terna.it" in url or "download-center" in url and source.startswith("terna"):
        return True
    if "aggreg" in kind and source.startswith("terna"):
        return True
    return False


def canonical_record(row: dict[str, Any]) -> dict[str, Any]:
    title = get_field(row, TITLE_FIELDS)
    source = get_field(row, SOURCE_FIELDS)
    url = get_field(row, URL_FIELDS)
    region = get_field(row, REGION_FIELDS)
    province = get_field(row, PROVINCE_FIELDS)
    municipality = get_field(row, MUNICIPALITY_FIELDS)
    mw = parse_mw(get_field(row, MW_FIELDS))
    proponent = get_field(row, PROPONENT_FIELDS)
    province_norm = normalize_province(province)
    return {
        "title": str(title).strip(),
        "title_norm": norm_text(title),
        "source": str(source).strip(),
        "source_norm": norm_text(source),
        "url": str(url).strip(),
        "region": str(region).strip(),
        "region_norm": norm_text(region),
        "province": str(province).strip().upper(),
        "province_norm": province_norm,
        "municipality": str(municipality).strip(),
        "municipality_norm": normalize_municipality(municipality),
        "mw": mw,
        "proponent": str(proponent).strip(),
        "proponent_norm": norm_text(proponent),
        "is_terna_aggregate": is_terna_aggregate(row),
    }


def specific_url_key(url: str) -> str:
    if not url:
        return ""
    try:
        parsed = urlparse(url)
    except Exception:
        return ""
    domain = parsed.netloc.lower().replace("www.", "")
    path = re.sub(r"/+", "/", parsed.path.strip("/").lower())
    query = dict(parse_qsl(parsed.query, keep_blank_values=False))
    specific_items = []
    for k, v in query.items():
        nk = norm_key(k)
        if nk in SPECIFIC_QUERY_KEYS and str(v).strip():
            specific_items.append((nk, norm_text(v)))
    if specific_items:
        specific_items.sort()
        q = "&".join(f"{k}={v}" for k, v in specific_items)
        return f"{domain}/{path}?{q}"
    path_parts = [p for p in path.split("/") if p]
    if not path_parts:
        return ""
    last = path_parts[-1]
    has_numeric_or_slug = bool(re.search(r"\d{4,}|[a-f0-9]{8,}", last))
    genericish = any(word in last for word in GENERIC_PATH_WORDS)
    if has_numeric_or_slug and not genericish:
        return f"{domain}/{path}"
    return ""


def generic_url_key(url: str) -> str:
    if not url or specific_url_key(url):
        return ""
    try:
        p = urlparse(url)
    except Exception:
        return ""
    if not p.netloc:
        return ""
    return f"{p.netloc.lower().replace('www.', '')}/{p.path.strip('/').lower()}"


def extract_source_identifier(row: dict[str, Any]) -> str:
    direct_fields = [
        "id", "project_id", "procedure_id", "id_procedura", "id_procedimento",
        "codice", "codice_procedura", "codice_pratica", "numero_procedura",
        "id_pratica", "mase_id"
    ]
    lookup = field_lookup(row)
    for f in direct_fields:
        original = lookup.get(norm_key(f))
        if original is not None:
            value = row.get(original)
            if value not in (None, ""):
                return f"{norm_key(f)}:{norm_text(value)}"
    title = str(get_field(row, TITLE_FIELDS))
    source = norm_text(get_field(row, SOURCE_FIELDS))
    if "mase" in source:
        for pat in [
            r"\b(?:id|codice)\s*(?:procedura|progetto|pratica)?\s*[:#]?\s*([0-9]{3,})\b",
            r"\b\[?id[_\s:-]*([0-9]{3,})\]?\b",
        ]:
            m = re.search(pat, norm_text(title))
            if m:
                return f"mase:{m.group(1)}"
    return ""


def extract_title_municipality_hint(title: str) -> str:
    if not title:
        return ""
    t = norm_text(title)
    # Solo pattern con "comune". Non uso "località" perché spesso è contrada/via, non comune.
    patterns = [
        r"\bcomune di ([a-z0-9 ]{3,80}?)(?: provincia| prov | in provincia| \([a-z]{2}\)| e |,|$)",
        r"\bcomuni di ([a-z0-9 ]{3,80}?)(?: provincia| prov | in provincia| \([a-z]{2}\)| e |,|$)",
        r"\bnel territorio comunale di ([a-z0-9 ]{3,80}?)(?: provincia| prov | in provincia| \([a-z]{2}\)| e |,|$)",
    ]
    for pat in patterns:
        m = re.search(pat, t)
        if m:
            value = re.sub(r"\b(provincia|prov|regione|potenza|mw|mwp)\b.*$", "", m.group(1)).strip()
            return normalize_municipality(value)
    return ""


def _looks_like_electrical_acronym_context(raw: str, start: int, end: int, code: str) -> bool:
    """Evita di leggere AT/MT come province quando significano Alta/Media Tensione."""
    before = norm_text(raw[max(0, start - 80):start])
    after = norm_text(raw[end:end + 80])
    ctx = f"{before} {after}"

    if code == "AT":
        electrical_words = [
            "alta tensione", "trifase in alta tensione", "stallo", "cabina primaria",
            "rete at", "connessione at", "linea at"
        ]
        if any(w in ctx for w in electrical_words):
            return True

    if code == "MT":
        electrical_words = [
            "media tensione", "rete mt", "connessione mt", "cavidotto mt",
            "linea mt", "cabina primaria", "15 000 v", "15000 v"
        ]
        if any(w in ctx for w in electrical_words):
            return True

    return False


def extract_title_province_codes(title: str) -> list[str]:
    """Estrae tutte le province esplicite dal titolo, senza scegliere a caso.

    Regole:
    - accetta sigle tra parentesi solo se non sono chiaramente sigle elettriche;
    - accetta "provincia/prov. di X";
    - conserva più province quando il progetto o la connessione attraversano più territori.
    """
    if not title:
        return []

    raw = str(title)
    found: list[str] = []
    seen: set[str] = set()

    def add(code: str) -> None:
        code = str(code or "").upper()
        if code in PROVINCE_CODES and code not in seen:
            seen.add(code)
            found.append(code)

    # Sigle esplicite tra parentesi: Gravina in Puglia (BA), Craco (MT).
    for m in re.finditer(r"\(([A-Za-z]{2})\)", raw):
        code = m.group(1).upper()
        if code in PROVINCE_CODES and not _looks_like_electrical_acronym_context(raw, m.start(), m.end(), code):
            add(code)

    t = norm_text(raw)

    # Sigla dopo contesto provinciale: prov FG, provincia FG.
    for m in re.finditer(r"\b(?:prov|provincia)\s+(?:di\s+)?([a-z]{2})\b", t):
        code = m.group(1).upper()
        add(code)

    # Nome provincia solo se preceduto da contesto provinciale.
    province_names = sorted(PROVINCE_NAME_TO_CODE.keys(), key=len, reverse=True)
    for name in province_names:
        pat = rf"\b(?:in provincia di|provincia di|provincia|prov)\s+(?:di\s+)?{re.escape(name)}\b"
        if re.search(pat, t):
            add(PROVINCE_NAME_TO_CODE[name])

    return found


def extract_title_province_code(title: str) -> str:
    codes = extract_title_province_codes(title)
    return codes[0] if codes else ""


def location_conflict(a: dict[str, Any], b: dict[str, Any]) -> tuple[bool, str]:
    ca = canonical_record(a)
    cb = canonical_record(b)
    if ca["is_terna_aggregate"] != cb["is_terna_aggregate"]:
        return True, "terna_aggregate_vs_project"
    province_codes_a = province_codes_from_value(ca["province"])
    province_codes_b = province_codes_from_value(cb["province"])
    if province_codes_a and province_codes_b and province_codes_a.isdisjoint(province_codes_b):
        return True, f"province_conflict:{ca['province']}!={cb['province']}"
    ma, mb = ca["municipality_norm"], cb["municipality_norm"]
    if ma and mb and ma != mb and ma not in mb and mb not in ma:
        return True, f"municipality_conflict:{ca['municipality']}!={cb['municipality']}"
    title_hint_a = extract_title_municipality_hint(ca["title"])
    title_hint_b = extract_title_municipality_hint(cb["title"])
    if title_hint_a and cb["municipality_norm"]:
        if title_hint_a != cb["municipality_norm"] and title_hint_a not in cb["municipality_norm"] and cb["municipality_norm"] not in title_hint_a:
            return True, f"title_location_conflict:{title_hint_a}!={cb['municipality']}"
    if title_hint_b and ca["municipality_norm"]:
        if title_hint_b != ca["municipality_norm"] and title_hint_b not in ca["municipality_norm"] and ca["municipality_norm"] not in title_hint_b:
            return True, f"title_location_conflict:{title_hint_b}!={ca['municipality']}"
    if mw_conflict(ca["mw"], cb["mw"]):
        return True, f"mw_conflict:{ca['mw']}!={cb['mw']}"
    return False, ""


def title_similarity(a: str, b: str) -> float:
    if not a or not b:
        return 0.0
    return SequenceMatcher(None, a, b).ratio()


def should_merge(a: dict[str, Any], b: dict[str, Any]) -> tuple[bool, str, int]:
    ca = canonical_record(a)
    cb = canonical_record(b)
    conflict, reason = location_conflict(a, b)
    if conflict:
        return False, reason, 0

    url_a, url_b = specific_url_key(ca["url"]), specific_url_key(cb["url"])
    sid_a, sid_b = extract_source_identifier(a), extract_source_identifier(b)
    score = 0
    reasons: list[str] = []

    if url_a and url_b and url_a == url_b:
        score += 85
        reasons.append("same_specific_url")
    if sid_a and sid_b and sid_a == sid_b:
        score += 90
        reasons.append("same_source_identifier")
    if ca["title_norm"] and ca["title_norm"] == cb["title_norm"]:
        score += 60
        reasons.append("same_title")

    sim = title_similarity(ca["title_norm"], cb["title_norm"])
    if sim >= 0.92:
        score += 45
        reasons.append(f"title_similarity:{sim:.2f}")
    elif sim >= 0.82:
        score += 25
        reasons.append(f"title_similarity:{sim:.2f}")

    if ca["proponent_norm"] and cb["proponent_norm"] and ca["proponent_norm"] == cb["proponent_norm"]:
        score += 20
        reasons.append("same_proponent")
    if ca["municipality_norm"] and cb["municipality_norm"] and ca["municipality_norm"] == cb["municipality_norm"]:
        score += 15
        reasons.append("same_municipality")
    elif ca["province_norm"] and cb["province_norm"] and ca["province_norm"] == cb["province_norm"]:
        score += 8
        reasons.append("same_province")
    elif ca["region_norm"] and cb["region_norm"] and ca["region_norm"] == cb["region_norm"]:
        score += 4
        reasons.append("same_region")
    if ca["mw"] is not None and cb["mw"] is not None and mw_close(ca["mw"], cb["mw"]):
        score += 15
        reasons.append("same_mw")

    tokens_a, tokens_b = title_tokens(ca["title"]), title_tokens(cb["title"])
    token_union_overlap = 0.0
    token_containment = 0.0
    if tokens_a and tokens_b:
        shared_tokens = tokens_a & tokens_b
        token_union_overlap = len(shared_tokens) / max(len(tokens_a | tokens_b), 1)
        token_containment = len(shared_tokens) / max(min(len(tokens_a), len(tokens_b)), 1)
        if token_union_overlap >= 0.65:
            score += 15
            reasons.append(f"title_token_overlap:{token_union_overlap:.2f}")
        elif token_containment >= 0.85:
            score += 15
            reasons.append(f"title_token_containment:{token_containment:.2f}")

    is_mase_cross_source = (
        ca["source_norm"] != cb["source_norm"]
        and "mase" in ca["source_norm"]
        and "mase" in cb["source_norm"]
    )

    if any("mase" in s for s in {ca["source_norm"], cb["source_norm"]}):
        if ca["title_norm"] and cb["title_norm"] and sim >= 0.82 and mw_close(ca["mw"], cb["mw"]):
            score += 15
            reasons.append("mase_bonus")

    if (
        is_mase_cross_source
        and ca["proponent_norm"] and ca["proponent_norm"] == cb["proponent_norm"]
        and ca["province_norm"] and ca["province_norm"] == cb["province_norm"]
        and mw_close(ca["mw"], cb["mw"])
        and (sim >= 0.75 or token_containment >= 0.85)
    ):
        score += 25
        reasons.append("mase_cross_source_same_proponent_province_mw")

    if ca["is_terna_aggregate"] and cb["is_terna_aggregate"]:
        if ca["title_norm"] == cb["title_norm"] and ca["source_norm"] == cb["source_norm"]:
            return True, "terna_exact_aggregate_duplicate", 100
        return False, "terna_aggregate_not_exact", score

    if (
        is_mase_cross_source
        and ca["proponent_norm"] and ca["proponent_norm"] == cb["proponent_norm"]
        and ca["province_norm"] and ca["province_norm"] == cb["province_norm"]
        and mw_close(ca["mw"], cb["mw"])
        and (sim >= 0.75 or token_containment >= 0.85)
        and score >= 75
    ):
        return True, "+".join(reasons), score

    if score >= 90:
        return True, "+".join(reasons), score
    if ca["title_norm"] and ca["title_norm"] == cb["title_norm"] and score >= 75:
        return True, "+".join(reasons), score
    if (
        ca["proponent_norm"] and ca["proponent_norm"] == cb["proponent_norm"]
        and ca["municipality_norm"] and ca["municipality_norm"] == cb["municipality_norm"]
        and mw_close(ca["mw"], cb["mw"])
        and sim >= 0.75
    ):
        return True, "+".join(reasons), score
    return False, "+".join(reasons) or "insufficient_evidence", score


class UnionFind:
    def __init__(self, n: int):
        self.parent = list(range(n))
    def find(self, x: int) -> int:
        while self.parent[x] != x:
            self.parent[x] = self.parent[self.parent[x]]
            x = self.parent[x]
        return x
    def union(self, a: int, b: int) -> None:
        ra, rb = self.find(a), self.find(b)
        if ra != rb:
            self.parent[rb] = ra


def completeness_score(row: dict[str, Any]) -> int:
    score = 0
    for fields in [TITLE_FIELDS, SOURCE_FIELDS, URL_FIELDS, REGION_FIELDS, PROVINCE_FIELDS, MUNICIPALITY_FIELDS, MW_FIELDS, PROPONENT_FIELDS]:
        if get_field(row, fields) not in ("", None):
            score += 3
    score += sum(1 for _, v in row.items() if v not in ("", None, [], {}))
    return score


def stable_project_key(row: dict[str, Any]) -> str:
    c = canonical_record(row)
    if c["is_terna_aggregate"]:
        material = "|".join(["terna_aggregate", c["source_norm"], c["region_norm"], c["province_norm"], c["title_norm"]])
        return "agg:terna:" + short_hash(material)
    sid = extract_source_identifier(row)
    if sid:
        return "src:" + short_hash(sid)
    u = specific_url_key(c["url"])
    if u:
        return "url:" + short_hash(u)
    mw_bucket = f"{round(c['mw'], 2):.2f}" if c["mw"] is not None else ""
    material = "|".join([c["title_norm"], c["proponent_norm"], c["region_norm"], c["province_norm"], c["municipality_norm"], mw_bucket])
    return "prj:" + short_hash(material)



def enforce_unique_project_keys(rows: list[dict[str, Any]]) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    """Ensure project_key is unique after conservative dedupe.

    V8 fix: a URL may be specific enough to help identify a source record, but
    when two rows share the same URL and were *not* merged because of MW/province
    conflicts, they must not keep the same project_key. Otherwise downstream
    dashboards can treat different records as the same object.
    """
    buckets: dict[str, list[int]] = defaultdict(list)
    for i, row in enumerate(rows):
        key = str(row.get("project_key") or stable_project_key(row))
        row["project_key"] = key
        buckets[key].append(i)

    used: set[str] = set()
    report: list[dict[str, Any]] = []

    for old_key, ids in sorted(buckets.items()):
        if len(ids) == 1:
            used.add(old_key)
            continue

        for ordinal, idx in enumerate(ids, start=1):
            row = rows[idx]
            c = canonical_record(row)

            mw_material = ""
            if c["mw"] is not None:
                mw_material = f"{round(c['mw'], 6):.6f}"

            material = "|".join([
                old_key,
                c["source_norm"],
                c["title_norm"],
                c["region_norm"],
                c["province_norm"],
                c["municipality_norm"],
                mw_material,
                specific_url_key(c["url"]) or norm_text(c["url"]),
                str(ordinal),
            ])

            new_key = f"{old_key}:split:{short_hash(material, 8)}"
            while new_key in used:
                material += f"|{idx}|{len(used)}"
                new_key = f"{old_key}:split:{short_hash(material, 8)}"

            used.add(new_key)
            row["_project_key_original"] = old_key
            row["project_key"] = new_key

            report.append({
                "old_project_key": old_key,
                "new_project_key": new_key,
                "idx": idx,
                "title": c["title"],
                "source": c["source"],
                "region": c["region"],
                "province": c["province"],
                "municipality": c["municipality"],
                "mw": c["mw"],
                "url": c["url"],
                "reason": "project_key_split_after_blocked_merge_or_conflict",
            })

    return rows, report



def merge_group(rows: list[dict[str, Any]]) -> dict[str, Any]:
    base = max(rows, key=completeness_score).copy()
    for row in sorted(rows, key=completeness_score, reverse=True):
        for k, v in row.items():
            if base.get(k) in ("", None, [], {}) and v not in ("", None, [], {}):
                base[k] = v
    canonicals = [canonical_record(r) for r in rows]

    def most_common_nonempty(key: str) -> str:
        values = [c[key] for c in canonicals if c[key] not in ("", None)]
        return Counter(values).most_common(1)[0][0] if values else ""

    for fields, key in [(REGION_FIELDS, "region"), (PROVINCE_FIELDS, "province"), (MUNICIPALITY_FIELDS, "municipality")]:
        value = most_common_nonempty(key)
        if value:
            set_field_if_present(base, fields, value)
    titles = [c["title"] for c in canonicals if c["title"]]
    if titles:
        set_field_if_present(base, TITLE_FIELDS, max(titles, key=len))
    urls = [c["url"] for c in canonicals if c["url"]]
    specific_urls = [u for u in urls if specific_url_key(u)]
    if specific_urls:
        set_field_if_present(base, URL_FIELDS, specific_urls[0])
    elif urls:
        set_field_if_present(base, URL_FIELDS, urls[0])
    mw_values = [c["mw"] for c in canonicals if c["mw"] is not None]
    if mw_values:
        set_field_if_present(base, MW_FIELDS, Counter(round(x, 3) for x in mw_values).most_common(1)[0][0])
    base["_merged_sources"] = list(dict.fromkeys(c["source"] for c in canonicals if c["source"]))
    base["_dedupe_group_size"] = len(rows)
    base["_dedupe_titles"] = sorted({c["title"] for c in canonicals if c["title"]})[:10]
    base["_dedupe_urls"] = sorted({c["url"] for c in canonicals if c["url"]})[:10]
    base["project_key"] = stable_project_key(base)
    return base


def build_candidate_pairs(rows: list[dict[str, Any]]) -> set[tuple[int, int]]:
    blocks: dict[str, list[int]] = defaultdict(list)
    for i, row in enumerate(rows):
        c = canonical_record(row)
        u = specific_url_key(c["url"])
        if u:
            blocks[f"url:{u}"].append(i)
        sid = extract_source_identifier(row)
        if sid:
            blocks[f"sid:{sid}"].append(i)
        if c["title_norm"]:
            blocks[f"title:{c['title_norm']}"].append(i)
        if c["title_norm"] and c["mw"] is not None:
            blocks[f"title_mw:{c['title_norm']}:{round(c['mw'], 1)}"].append(i)
        if c["proponent_norm"] and c["municipality_norm"] and c["mw"] is not None:
            blocks[f"prop_loc_mw:{c['proponent_norm']}:{c['municipality_norm']}:{round(c['mw'], 1)}"].append(i)

        # V11: MASE and MASE Provvedimenti often describe the same project
        # using different pages/URLs: one is the project detail, the other is
        # the published measure. They can therefore miss exact-title/URL blocks.
        # Keep this blocking narrow: only MASE-family sources, same province,
        # same MW bucket. should_merge() remains conservative and rejects low
        # similarity or conflicts.
        if "mase" in c["source_norm"] and c["mw"] is not None and c["province_norm"]:
            blocks[f"mase_cross_source_mw_prov:{c['province_norm']}:{round(c['mw'], 3)}"].append(i)

        tokens = sorted(title_tokens(c["title"]))
        if len(tokens) >= 3:
            blocks[f"tokens:{' '.join(tokens[:8])}"].append(i)
    pairs: set[tuple[int, int]] = set()
    for ids in blocks.values():
        if len(ids) < 2 or len(ids) > 80:
            continue
        for ia in range(len(ids)):
            for ib in range(ia + 1, len(ids)):
                a, b = ids[ia], ids[ib]
                pairs.add((min(a, b), max(a, b)))
    return pairs


def dedupe_projects(rows: list[dict[str, Any]]) -> tuple[list[dict[str, Any]], list[dict[str, Any]], list[dict[str, Any]]]:
    pairs = build_candidate_pairs(rows)
    uf = UnionFind(len(rows))
    accepted, rejected = [], []
    for a, b in sorted(pairs):
        ok, reason, score = should_merge(rows[a], rows[b])
        ca, cb = canonical_record(rows[a]), canonical_record(rows[b])
        record = {
            "idx_a": a, "idx_b": b, "merge": ok, "score": score, "reason": reason,
            "title_a": ca["title"], "title_b": cb["title"],
            "source_a": ca["source"], "source_b": cb["source"],
            "region_a": ca["region"], "region_b": cb["region"],
            "province_a": ca["province"], "province_b": cb["province"],
            "province_norm_a": ca["province_norm"], "province_norm_b": cb["province_norm"],
            "municipality_a": ca["municipality"], "municipality_b": cb["municipality"],
            "mw_a": ca["mw"], "mw_b": cb["mw"],
            "is_terna_a": ca["is_terna_aggregate"], "is_terna_b": cb["is_terna_aggregate"],
            "url_a": ca["url"], "url_b": cb["url"],
        }
        if ok:
            uf.union(a, b)
            accepted.append(record)
        else:
            if score > 0 or "conflict" in reason:
                rejected.append(record)
    groups: dict[int, list[dict[str, Any]]] = defaultdict(list)
    for i, row in enumerate(rows):
        groups[uf.find(i)].append(row)
    merged = [merge_group(group) for group in groups.values()]
    merged.sort(key=lambda r: (canonical_record(r)["is_terna_aggregate"], -(canonical_record(r)["mw"] or 0), canonical_record(r)["title_norm"]))
    return merged, accepted, rejected


def regenerate_top_projects(
    rows: list[dict[str, Any]],
    limit: int = 20,
    excluded_project_keys: set[str] | None = None,
) -> list[dict[str, Any]]:
    """Rigenera la Top progetti da record deduplicati.

    V7: i record sospetti restano nel dataset, ma vengono esclusi dalla classifica
    per evitare che la dashboard promuova dati sporchi come Gravina/Crispiano.
    """
    excluded_project_keys = excluded_project_keys or set()
    point_rows, seen = [], set()
    for row in rows:
        c = canonical_record(row)
        if c["is_terna_aggregate"] or c["mw"] is None:
            continue
        key = str(row.get("project_key") or stable_project_key(row))
        if key in excluded_project_keys:
            continue
        if key in seen:
            continue
        seen.add(key)
        point_rows.append(row)
    point_rows.sort(key=lambda r: canonical_record(r)["mw"] or 0, reverse=True)
    return point_rows[:limit]


def suspicious_project_keys(rows: list[dict[str, Any]], suspicious: list[dict[str, Any]]) -> set[str]:
    """Restituisce i project_key dei record sospetti.

    Il report sospetti contiene l'indice della riga analizzata. V7 usa questa
    informazione per escludere solo dalla Top 20 i record sospetti, senza
    cancellarli dal dataset.
    """
    keys: set[str] = set()
    for item in suspicious:
        try:
            idx = int(item.get("idx"))
        except (TypeError, ValueError):
            continue
        if 0 <= idx < len(rows):
            row = rows[idx]
            keys.add(str(row.get("project_key") or stable_project_key(row)))
    return keys


def top_exclusion_report(rows: list[dict[str, Any]], excluded_project_keys: set[str]) -> list[dict[str, Any]]:
    report: list[dict[str, Any]] = []
    for row in rows:
        key = str(row.get("project_key") or stable_project_key(row))
        if key not in excluded_project_keys:
            continue
        c = canonical_record(row)
        if c["is_terna_aggregate"]:
            continue
        report.append({
            "project_key": key,
            "title": c["title"],
            "source": c["source"],
            "region": c["region"],
            "province": c["province"],
            "municipality": c["municipality"],
            "mw": c["mw"],
            "url": c["url"],
            "reason": "excluded_from_top_projects_because_suspicious",
        })
    report.sort(key=lambda r: r.get("mw") or 0, reverse=True)
    return report


def detect_suspicious_rows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    issues: list[dict[str, Any]] = []
    generic_groups: dict[str, list[int]] = defaultdict(list)
    for i, row in enumerate(rows):
        c = canonical_record(row)
        g = generic_url_key(c["url"])
        if g and not c["is_terna_aggregate"]:
            generic_groups[g].append(i)

    noisy_generic_idxs = set()
    for _, ids in generic_groups.items():
        if len(ids) > 1:
            titles = {canonical_record(rows[i])["title_norm"] for i in ids}
            mws = {round(canonical_record(rows[i])["mw"] or -1, 3) for i in ids}
            if len(titles) > 1 or len(mws) > 1:
                noisy_generic_idxs.update(ids)

    for i, row in enumerate(rows):
        c = canonical_record(row)
        title_hint = extract_title_municipality_hint(c["title"])
        title_prov_codes = extract_title_province_codes(c["title"])
        title_prov = " ".join(title_prov_codes)
        title_mws = extract_title_mws(c["title"])
        title_mw = title_mws[0] if title_mws else None

        field_province_codes = province_codes_from_value(c["province"])

        base = {
            "idx": i,
            "title": c["title"],
            "source": c["source"],
            "title_hint": title_hint,
            "field_municipality": c["municipality"],
            "title_province": title_prov,
            "field_province": c["province"],
            "field_province_norm": c["province_norm"],
            "field_province_codes": " ".join(sorted(field_province_codes)),
            "title_mw": title_mw,
            "title_mws": " ".join(str(round(x, 6)) for x in title_mws),
            "field_mw": c["mw"],
            "url": c["url"],
        }

        if title_hint and c["municipality_norm"]:
            if title_hint != c["municipality_norm"] and title_hint not in c["municipality_norm"] and c["municipality_norm"] not in title_hint:
                issues.append({**base, "issue": "title_municipality_mismatch"})

        # Se titolo/campo citano più province, segnalo solo se non c'è alcuna sovrapposizione.
        if title_prov_codes and field_province_codes and set(title_prov_codes).isdisjoint(field_province_codes):
            issues.append({**base, "issue": "title_province_mismatch"})

        # Se il titolo contiene più potenze, il campo è accettabile se coincide con una di esse.
        if title_mws and c["mw"] is not None and not title_has_close_mw(c["title"], c["mw"], tolerance=0.05):
            issues.append({**base, "issue": "title_mw_mismatch"})

        # Non metto più gli URL generici nel file sospetti riga-per-riga:
        # erano centinaia di righe poco utili. La regola resta attiva nella
        # deduplica: un URL generico non è mai una prova sufficiente per fondere.
        # Se serve, si può riattivare un report separato per gruppi URL.

    return issues


def find_project_list(data: Any) -> tuple[list[dict[str, Any]], str | None]:
    if isinstance(data, list):
        return [x for x in data if isinstance(x, dict)], None
    if not isinstance(data, dict):
        raise ValueError("Formato data.json non riconosciuto: root non è né lista né oggetto.")
    for key in ["projects", "all_projects", "records", "items", "rows", "data", "project_rows", "puntual_projects"]:
        value = data.get(key)
        if isinstance(value, list):
            rows = [x for x in value if isinstance(x, dict)]
            if rows:
                return rows, key
    best_key, best_rows = None, []
    for key, value in data.items():
        if not isinstance(value, list):
            continue
        rows = [x for x in value if isinstance(x, dict)]
        if len(rows) <= len(best_rows):
            continue
        sample = rows[:30]
        hits = sum(1 for r in sample if get_field(r, TITLE_FIELDS) or get_field(r, MW_FIELDS) or get_field(r, SOURCE_FIELDS))
        if hits >= max(1, len(sample) // 3):
            best_key, best_rows = key, rows
    if best_key is None:
        raise ValueError("Non trovo una lista progetti dentro data.json.")
    return best_rows, best_key


def find_top_projects(data: Any) -> tuple[list[dict[str, Any]], str | None]:
    if not isinstance(data, dict):
        return [], None
    for key in ["top_projects", "topProjects", "top", "top_10_projects"]:
        value = data.get(key)
        if isinstance(value, list):
            return [x for x in value if isinstance(x, dict)], key
    return [], None



def recompute_summary_from_rows(existing_summary: dict[str, Any] | None, rows: list[dict[str, Any]], top_projects: list[dict[str, Any]]) -> dict[str, Any]:
    """Rebuild dashboard-facing summary fields from the deduplicated records.

    V6 fix: previous versions updated root records/top_projects but left summary stale.
    This function keeps the dashboard and data.json internally coherent.
    """
    existing_summary = dict(existing_summary or {})

    def row_source(row: dict[str, Any]) -> str:
        return str(get_field(row, SOURCE_FIELDS) or row.get("source") or "ND").strip() or "ND"

    def row_label(row: dict[str, Any]) -> str:
        return str(row.get("source_label") or row_source(row)).strip() or row_source(row)

    def row_region(row: dict[str, Any]) -> str:
        return str(get_field(row, REGION_FIELDS) or row.get("region") or "ND").strip() or "ND"

    def row_province(row: dict[str, Any]) -> str:
        return str(get_field(row, PROVINCE_FIELDS) or row.get("province") or "").strip()

    def row_municipality(row: dict[str, Any]) -> str:
        return str(get_field(row, MUNICIPALITY_FIELDS) or row.get("municipalities") or "").strip()

    def row_url(row: dict[str, Any]) -> str:
        return str(get_field(row, URL_FIELDS) or row.get("url") or "").strip()

    def row_mw(row: dict[str, Any]) -> float | None:
        return parse_mw(get_field(row, MW_FIELDS))

    def is_terna_row(row: dict[str, Any]) -> bool:
        return bool(row.get("is_terna")) or canonical_record(row)["is_terna_aggregate"]

    punctual_rows = [r for r in rows if not is_terna_row(r)]
    terna_rows = [r for r in rows if is_terna_row(r)]

    source_counts_counter = Counter(row_source(r) for r in rows)
    source_labels = {}
    for r in rows:
        source_labels.setdefault(row_source(r), row_label(r))

    source_counts = [
        {
            "source": source,
            "label": source_labels.get(source, source),
            "count": count,
        }
        for source, count in source_counts_counter.most_common()
    ]

    regions = []
    region_names = sorted({row_region(r) for r in rows}, key=lambda x: (x == "ND", x))
    for region in region_names:
        region_punctual = [r for r in punctual_rows if row_region(r) == region]
        region_terna = [r for r in terna_rows if row_region(r) == region]
        punctual_mw = round(sum((row_mw(r) or 0.0) for r in region_punctual), 3)
        terna_mw = round(sum((row_mw(r) or 0.0) for r in region_terna), 3)
        terna_practices = int(sum(int(r.get("numero_pratiche") or 0) for r in region_terna))

        # Simple deterministic score for sorting/visual priority.
        # It intentionally weights real punctual projects more than Terna radar data.
        priority_score = round((punctual_mw / 1000.0) + (len(region_punctual) * 0.05) + (terna_mw / 5000.0), 1)

        regions.append({
            "region": region,
            "punctual_count": len(region_punctual),
            "punctual_mw": punctual_mw,
            "terna_count": len(region_terna),
            "terna_mw": terna_mw,
            "terna_practices": terna_practices,
            "total_mw": round(punctual_mw + terna_mw, 3),
            "priority_score": priority_score,
        })

    regions.sort(key=lambda r: (r["priority_score"], r["total_mw"], r["punctual_count"]), reverse=True)

    missing_mw = sum(1 for r in punctual_rows if row_mw(r) is None)
    missing_region = sum(1 for r in punctual_rows if not row_region(r) or row_region(r) == "ND")
    missing_province = sum(1 for r in punctual_rows if not row_province(r))
    missing_municipality = sum(1 for r in punctual_rows if not row_municipality(r))
    missing_url = sum(1 for r in punctual_rows if not row_url(r))
    province_deduced = sum(1 for r in punctual_rows if bool(r.get("province_deduced")))
    municipalities_deduced = sum(1 for r in punctual_rows if bool(r.get("municipalities_deduced")))

    quality_by_source = []
    for source, count in source_counts_counter.most_common():
        s_rows = [r for r in punctual_rows if row_source(r) == source]
        if not s_rows:
            # Keep Terna in the source table, but quality is not very meaningful for aggregate radar rows.
            s_rows = [r for r in rows if row_source(r) == source]
        s_count = len(s_rows)
        s_missing_mw = sum(1 for r in s_rows if row_mw(r) is None)
        s_missing_province = sum(1 for r in s_rows if not row_province(r))
        s_missing_municipality = sum(1 for r in s_rows if not row_municipality(r))
        s_province_deduced = sum(1 for r in s_rows if bool(r.get("province_deduced")))
        s_municipalities_deduced = sum(1 for r in s_rows if bool(r.get("municipalities_deduced")))
        denom = max(s_count * 3, 1)
        completeness_pct = round(100.0 * (denom - s_missing_mw - s_missing_province - s_missing_municipality) / denom, 1)
        quality_by_source.append({
            "source": source,
            "source_label": source_labels.get(source, source),
            "count": s_count,
            "missing_mw": s_missing_mw,
            "missing_province": s_missing_province,
            "missing_municipality": s_missing_municipality,
            "province_deduced": s_province_deduced,
            "municipalities_deduced": s_municipalities_deduced,
            "completeness_pct": completeness_pct,
        })

    terna_status_counter: dict[str, dict[str, Any]] = {}
    for r in terna_rows:
        status = str(r.get("status") or "ND").strip() or "ND"
        bucket = terna_status_counter.setdefault(status, {"status": status, "mw": 0.0, "count": 0, "practices": 0})
        bucket["mw"] += row_mw(r) or 0.0
        bucket["count"] += 1
        bucket["practices"] += int(r.get("numero_pratiche") or 0)

    terna_status_rows = []
    for item in terna_status_counter.values():
        item = dict(item)
        item["mw"] = round(item["mw"], 3)
        terna_status_rows.append(item)
    terna_status_rows.sort(key=lambda r: r["mw"], reverse=True)

    existing_summary.update({
        "generated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "total_records": len(rows),
        "punctual_records": len(punctual_rows),
        "terna_records": len(terna_rows),
        "total_mw_punctual": round(sum((row_mw(r) or 0.0) for r in punctual_rows), 3),
        "total_mw_terna": round(sum((row_mw(r) or 0.0) for r in terna_rows), 3),
        "source_counts": source_counts,
        "regions": regions,
        "top_projects": top_projects,
        "terna_summary": {
            "status_rows": terna_status_rows,
        },
        "quality": {
            "punctual_records": len(punctual_rows),
            "missing_mw": missing_mw,
            "missing_region": missing_region,
            "missing_province": missing_province,
            "missing_municipality": missing_municipality,
            "missing_url": missing_url,
            "province_deduced": province_deduced,
            "municipalities_deduced": municipalities_deduced,
        },
        "quality_by_source": quality_by_source,
    })
    return existing_summary


def write_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if not rows:
        path.write_text("", encoding="utf-8")
        return
    fields = sorted({k for row in rows for k in row.keys()})
    with path.open("w", newline="", encoding="utf-8-sig") as f:
        writer = csv.DictWriter(f, fieldnames=fields, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)


def safe_md(value: Any) -> str:
    s = re.sub(r"\s+", " ", str(value or "")).strip().replace("|", "\\|")
    return s[:137] + "..." if len(s) > 140 else s



def should_repair_mw_from_title(field_mw: float | None, title_mw: float | None) -> bool:
    """Decide se il MW del campo è palesemente sottoscala rispetto al titolo.

    Non corregge differenze ordinarie AC/DC o revisioni progetto.
    Corregge solo errori tipici da kW/kWp convertiti due volte:
    0.003 invece di 3
    0.05886 invece di 58.86
    0.99488 invece di 19.99488
    """
    if title_mw is None:
        return False
    if field_mw is None or field_mw <= 0:
        return title_mw > 0
    if field_mw < 1.0 and title_mw >= 3.0:
        return True
    if field_mw < 0.5 and title_mw >= 2.0:
        return True
    return False


def should_repair_trusted_title_mw(source: str, field_mw: float | None, title_mws: list[float]) -> float | None:
    """V10: ripara mismatch MW chiari per fonti in cui il titolo è più affidabile.

    Dopo la rimozione della fonte CKAN Puglia restano pochi mismatch reali.
    Per Emilia-Romagna il titolo ARPAE contiene spesso la potenza esplicita,
    mentre il campo può arrivare da parsing non coerente. La correzione è
    volutamente limitata a fonti allowlistate.
    """
    source_norm = norm_text(source)
    source_key = str(source or "").strip().lower()
    if source_norm not in TRUST_TITLE_MW_SOURCES and source_key not in TRUST_TITLE_MW_SOURCES:
        return None
    if field_mw is None or not title_mws:
        return None
    if any(mw_close(v, field_mw, tolerance=0.05) for v in title_mws):
        return None
    if len(title_mws) == 1:
        return title_mws[0]
    return title_mws[0]


def should_repair_trusted_title_province(source: str, field_province: str, title_prov_codes: list[str]) -> str:
    """V10: ripara provincia quando il titolo contiene una sola sigla esplicita.

    Esempio Lazio: titolo con Cisterna di Latina (LT), campo provincia RM.
    """
    source_norm = norm_text(source)
    source_key = str(source or "").strip().lower()
    if source_norm not in TRUST_TITLE_PROVINCE_SOURCES and source_key not in TRUST_TITLE_PROVINCE_SOURCES:
        return ""
    if len(title_prov_codes) != 1:
        return ""
    title_code = title_prov_codes[0]
    field_codes = province_codes_from_value(field_province)
    if not field_codes:
        return title_code
    if title_code not in field_codes:
        return title_code
    return ""


def repair_obvious_fields(rows: list[dict[str, Any]]) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    """Corregge solo anomalie oggettive prima della deduplica.

    La correzione è conservativa:
    - corregge MW quando il campo è chiaramente sottoscala rispetto al titolo;
    - V10 corregge anche mismatch MW per fonti allowlistate in cui il titolo è
      più affidabile del campo estratto;
    - V10 corregge la provincia quando il titolo contiene una sola sigla esplicita
      e la fonte è allowlistata.
    """
    repaired_rows: list[dict[str, Any]] = []
    repairs: list[dict[str, Any]] = []

    for idx, row in enumerate(rows):
        new_row = row.copy()
        c = canonical_record(new_row)
        title_mws = extract_title_mws(c["title"])
        title_mw = best_title_mw_for_repair(c["title"], c["mw"])
        mw_repair_reason = ""

        if should_repair_mw_from_title(c["mw"], title_mw):
            mw_repair_reason = "field_mw_underscaled_vs_title"
        else:
            trusted_title_mw = should_repair_trusted_title_mw(c["source"], c["mw"], title_mws)
            if trusted_title_mw is not None:
                title_mw = trusted_title_mw
                mw_repair_reason = "field_mw_replaced_by_trusted_title_mw"

        if mw_repair_reason and title_mw is not None:
            old_value = get_field(new_row, MW_FIELDS)
            set_field_if_present(new_row, MW_FIELDS, round(float(title_mw), 6))
            repairs.append({
                "idx": idx,
                "field": "mw",
                "old_value": old_value,
                "old_mw": c["mw"],
                "new_value": round(float(title_mw), 6),
                "reason": mw_repair_reason,
                "title": c["title"],
                "source": c["source"],
                "province": c["province"],
                "url": c["url"],
            })

        c2 = canonical_record(new_row)
        title_prov_codes = extract_title_province_codes(c2["title"])
        title_prov = title_prov_codes[0] if len(title_prov_codes) == 1 else ""
        province_repair_reason = ""
        province_repair_value = ""

        if title_prov and not c2["province_norm"]:
            province_repair_value = title_prov
            province_repair_reason = "province_filled_from_explicit_title_code"
        else:
            trusted_title_prov = should_repair_trusted_title_province(c2["source"], c2["province"], title_prov_codes)
            if trusted_title_prov:
                province_repair_value = trusted_title_prov
                province_repair_reason = "province_replaced_by_trusted_title_code"

        if province_repair_reason and province_repair_value:
            old_value = get_field(new_row, PROVINCE_FIELDS)
            set_field_if_present(new_row, PROVINCE_FIELDS, province_repair_value)
            repairs.append({
                "idx": idx,
                "field": "province",
                "old_value": old_value,
                "old_mw": c2["mw"],
                "new_value": province_repair_value,
                "reason": province_repair_reason,
                "title": c2["title"],
                "source": c2["source"],
                "province": c2["province"],
                "url": c2["url"],
            })

        repaired_rows.append(new_row)

    return repaired_rows, repairs


def make_audit_markdown(original_rows, deduped_rows, accepted, rejected, suspicious, top_before, top_after, repairs=None) -> str:
    original_point = [r for r in original_rows if not canonical_record(r)["is_terna_aggregate"]]
    original_terna = [r for r in original_rows if canonical_record(r)["is_terna_aggregate"]]
    deduped_point = [r for r in deduped_rows if not canonical_record(r)["is_terna_aggregate"]]
    deduped_terna = [r for r in deduped_rows if canonical_record(r)["is_terna_aggregate"]]
    mw_original_point = sum((canonical_record(r)["mw"] or 0) for r in original_point)
    mw_deduped_point = sum((canonical_record(r)["mw"] or 0) for r in deduped_point)
    mw_original_terna = sum((canonical_record(r)["mw"] or 0) for r in original_terna)
    mw_deduped_terna = sum((canonical_record(r)["mw"] or 0) for r in deduped_terna)
    merged_groups = [r for r in deduped_rows if int(r.get("_dedupe_group_size", 1) or 1) > 1]
    source_counter = Counter(canonical_record(r)["source"] or "N/D" for r in deduped_rows)
    rejected_family = Counter(str(r.get("reason", "")).split(":")[0] for r in rejected)
    suspicious_family = Counter(r.get("issue", "") for r in suspicious)
    repairs = repairs or []
    repair_family = Counter(r.get("reason", "") for r in repairs)

    lines = [
        "# Data quality audit - pv_agent_mvp", "",
        f"Generato: `{datetime.now().isoformat(timespec='seconds')}`", "",
        "## Sintesi", "",
        f"- Record iniziali: **{len(original_rows)}**",
        f"- Record dopo deduplica: **{len(deduped_rows)}**",
        f"- Record rimossi/fusi: **{len(original_rows) - len(deduped_rows)}**",
        f"- Gruppi realmente fusi: **{len(merged_groups)}**",
        f"- Coppie accettate in deduplica: **{len(accepted)}**",
        f"- Coppie respinte per conflitto o prove insufficienti: **{len(rejected)}**",
        f"- Righe corrette automaticamente prima della deduplica: **{len(repairs)}**",
        f"- Righe sospette da verificare: **{len(suspicious)}**", "",
        "## Puntuali vs Terna", "",
        "| Categoria | Prima | Dopo | MW prima | MW dopo |",
        "|---|---:|---:|---:|---:|",
        f"| Progetti puntuali | {len(original_point)} | {len(deduped_point)} | {mw_original_point:.2f} | {mw_deduped_point:.2f} |",
        f"| Record Terna aggregati | {len(original_terna)} | {len(deduped_terna)} | {mw_original_terna:.2f} | {mw_deduped_terna:.2f} |", "",
        "## Fonti principali dopo deduplica", "",
        "| Fonte | Record |", "|---|---:|",
    ]
    for source, count in source_counter.most_common(20):
        lines.append(f"| {safe_md(source)} | {count} |")
    lines += ["", "## Motivi principali di rigetto", "", "| Motivo | Count |", "|---|---:|"]
    for reason, count in rejected_family.most_common(20):
        lines.append(f"| {safe_md(reason)} | {count} |")
    lines += ["", "## Righe sospette per tipologia", "", "| Issue | Count |", "|---|---:|"]
    for issue, count in suspicious_family.most_common(20):
        lines.append(f"| {safe_md(issue)} | {count} |")
    lines += ["", "## Correzioni automatiche pre-deduplica", "", "| Correzione | Count |", "|---|---:|"]
    if repair_family:
        for reason, count in repair_family.most_common(20):
            lines.append(f"| {safe_md(reason)} | {count} |")
    else:
        lines.append("| Nessuna | 0 |")
    lines += ["", "## Top projects", "", f"- Top projects prima: **{len(top_before)}**", f"- Top projects dopo rigenerazione: **{len(top_after)}**", ""]
    lines += ["## Prime fusioni accettate", ""]
    if accepted:
        lines += ["| Score | Motivo | Titolo A | Titolo B | MW A | MW B | Prov A | Prov B |", "|---:|---|---|---|---:|---:|---|---|"]
        for r in accepted[:30]:
            lines.append(f"| {r['score']} | {safe_md(r['reason'])} | {safe_md(r['title_a'])} | {safe_md(r['title_b'])} | {r['mw_a']} | {r['mw_b']} | {safe_md(r['province_a'])} | {safe_md(r['province_b'])} |")
    else:
        lines.append("Nessuna fusione accettata.")
    lines += ["", "## Prime fusioni respinte", ""]
    if rejected:
        lines += ["| Score | Motivo | Titolo A | Titolo B | MW A | MW B | Prov A | Prov B |", "|---:|---|---|---|---:|---:|---|---|"]
        for r in rejected[:30]:
            lines.append(f"| {r['score']} | {safe_md(r['reason'])} | {safe_md(r['title_a'])} | {safe_md(r['title_b'])} | {r['mw_a']} | {r['mw_b']} | {safe_md(r['province_a'])} | {safe_md(r['province_b'])} |")
    else:
        lines.append("Nessuna fusione respinta rilevante.")
    lines += ["", "## Prime righe sospette", ""]
    if suspicious:
        lines += ["| Problema | Titolo | Fonte | Prov titolo | Prov campo | MW titolo | MW campo |", "|---|---|---|---|---|---:|---:|"]
        for r in suspicious[:50]:
            lines.append(f"| {safe_md(r['issue'])} | {safe_md(r['title'])} | {safe_md(r['source'])} | {safe_md(r['title_province'])} | {safe_md(r['field_province'])} | {r['title_mw']} | {r['field_mw']} |")
    else:
        lines.append("Nessuna riga sospetta rilevata.")
    lines += ["", "## File generati", "", "- `reports/data_deduped.json`", "- `reports/dedupe_audit.md`", "- `reports/dedupe_accepted.csv`", "- `reports/dedupe_rejected.csv`", "- `reports/dedupe_suspicious_rows.csv`", "- `reports/dedupe_field_repairs.csv`", "- `reports/dedupe_top_excluded.csv`", "- `reports/dedupe_project_key_splits.csv`", ""]
    return "\n".join(lines)


def locate_input(path_arg: str | None) -> Path:
    if path_arg:
        p = Path(path_arg)
        if p.exists():
            return p
        raise FileNotFoundError(f"File non trovato: {p}")
    for p in [Path("/app/reports/data.json"), Path("/app/data.json"), Path("reports/data.json"), Path("data.json")]:
        if p.exists():
            return p
    found = list(Path(".").rglob("data.json"))
    if found:
        found.sort(key=lambda p: p.stat().st_mtime, reverse=True)
        return found[0]
    raise FileNotFoundError("Non trovo data.json. Usa --input percorso/file.json")


def main() -> int:
    parser = argparse.ArgumentParser(description="Audit, correzioni conservative e deduplica sicura V10 per pv_agent_mvp data.json")
    parser.add_argument("--input", default=None, help="Percorso data.json. Default: ricerca automatica.")
    parser.add_argument("--output", default="/app/reports/data_deduped.json", help="Output JSON deduplicato.")
    parser.add_argument("--reports-dir", default="/app/reports", help="Cartella report.")
    parser.add_argument("--top-limit", type=int, default=20, help="Numero top_projects da rigenerare.")
    parser.add_argument("--in-place", action="store_true", help="Sovrascrive data.json dopo backup.")
    parser.add_argument("--no-field-repair", action="store_true", help="Disattiva le correzioni conservative MW/provincia prima della deduplica.")
    args = parser.parse_args()

    input_path = locate_input(args.input)
    output_path = Path(args.output)
    reports_dir = Path(args.reports_dir)
    reports_dir.mkdir(parents=True, exist_ok=True)

    with input_path.open("r", encoding="utf-8") as f:
        data = json.load(f)
    rows, project_key = find_project_list(data)
    top_before, top_key = find_top_projects(data)

    print(f"[data-quality-v12] input: {input_path}")
    print(f"[data-quality-v12] project list key: {project_key or '<root-list>'}")
    print(f"[data-quality-v12] record iniziali: {len(rows)}")

    if args.no_field_repair:
        working_rows = rows
        repairs = []
    else:
        working_rows, repairs = repair_obvious_fields(rows)

    deduped_rows, accepted, rejected = dedupe_projects(working_rows)

    # V8: se due record non vengono fusi perché confliggono, ma avevano la stessa
    # project_key derivata da URL o aggregato, la chiave viene "splittata" per
    # renderla univoca nel dataset finale.
    deduped_rows, project_key_splits = enforce_unique_project_keys(deduped_rows)

    # V7/V8: valuta i sospetti sui record finali deduplicati, poi escludili solo
    # dalle classifiche Top, non dal dataset.
    suspicious = detect_suspicious_rows(deduped_rows)
    excluded_keys = suspicious_project_keys(deduped_rows, suspicious)
    excluded_top_rows = top_exclusion_report(deduped_rows, excluded_keys)
    top_after = regenerate_top_projects(
        deduped_rows,
        limit=args.top_limit,
        excluded_project_keys=excluded_keys,
    )

    if isinstance(data, list):
        new_data: Any = deduped_rows
    else:
        new_data = dict(data)
        if project_key is None:
            new_data = deduped_rows
        else:
            new_data[project_key] = deduped_rows
        new_data[top_key or "top_projects"] = top_after
        if isinstance(new_data, dict):
            new_data["summary"] = recompute_summary_from_rows(
                new_data.get("summary") if isinstance(new_data.get("summary"), dict) else {},
                deduped_rows,
                top_after,
            )
        new_data["data_quality"] = {
            "generated_at": datetime.now().isoformat(timespec="seconds"),
            "input_file": str(input_path),
            "records_before": len(rows),
            "records_after": len(deduped_rows),
            "merged_or_removed": len(rows) - len(deduped_rows),
            "accepted_pairs": len(accepted),
            "rejected_pairs": len(rejected),
            "field_repairs": len(repairs),
            "suspicious_rows": len(suspicious),
            "top_projects_excluded_suspicious": len(excluded_keys),
            "project_key_splits": len(project_key_splits),
            "project_key_conflict_groups": len(set(r.get("old_project_key") for r in project_key_splits)),
            "version": "v12",
            "rules": {
                "generic_urls_are_not_strong_keys": True,
                "terna_detection_requires_source_not_substring": True,
                "province_names_are_normalized_to_codes": True,
                "title_province_and_title_mw_mismatches_are_flagged_with_multi_value_checks": True,
                "terna_aggregate_never_merged_with_point_projects": True,
                "municipality_province_mw_conflicts_block_merge": True,
                "top_projects_regenerated_from_deduped_projects": True,
                "top_projects_exclude_suspicious_records": True,
                "project_keys_are_unique_after_conflict_splits": True,
                "trusted_title_repairs_for_known_source_mismatches": True,
            },
        }

    output_path.parent.mkdir(parents=True, exist_ok=True)
    with output_path.open("w", encoding="utf-8") as f:
        json.dump(new_data, f, ensure_ascii=False, indent=2)

    write_csv(reports_dir / "dedupe_accepted.csv", accepted)
    write_csv(reports_dir / "dedupe_rejected.csv", rejected)
    write_csv(reports_dir / "dedupe_suspicious_rows.csv", suspicious)
    write_csv(reports_dir / "dedupe_field_repairs.csv", repairs)
    write_csv(reports_dir / "dedupe_top_excluded.csv", excluded_top_rows)
    write_csv(reports_dir / "dedupe_project_key_splits.csv", project_key_splits)
    audit_md = make_audit_markdown(working_rows, deduped_rows, accepted, rejected, suspicious, top_before, top_after, repairs=repairs)
    (reports_dir / "dedupe_audit.md").write_text(audit_md, encoding="utf-8")

    print(f"[data-quality-v12] record dopo deduplica: {len(deduped_rows)}")
    print(f"[data-quality-v12] fusi/rimossi: {len(rows) - len(deduped_rows)}")
    print(f"[data-quality-v12] correzioni automatiche: {len(repairs)}")
    print(f"[data-quality-v12] coppie accettate: {len(accepted)}")
    print(f"[data-quality-v12] coppie respinte: {len(rejected)}")
    print(f"[data-quality-v12] righe sospette: {len(suspicious)}")
    print(f"[data-quality-v12] esclusi dalla top_projects perché sospetti: {len(excluded_keys)}")
    print(f"[data-quality-v12] project_key splittate per conflitto: {len(project_key_splits)}")
    print(f"[data-quality-v12] output: {output_path}")
    print(f"[data-quality-v12] audit: {reports_dir / 'dedupe_audit.md'}")

    if args.in_place:
        backup = input_path.with_name(input_path.stem + f"_backup_before_dedupe_v12_{now_stamp()}" + input_path.suffix)
        shutil.copy2(input_path, backup)
        shutil.copy2(output_path, input_path)
        print(f"[data-quality-v12] backup creato: {backup}")
        print(f"[data-quality-v12] data.json sovrascritto: {input_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
