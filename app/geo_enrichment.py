from __future__ import annotations

import csv
import re
from dataclasses import dataclass
from functools import lru_cache
from pathlib import Path
from typing import Any


BASE_DIR = Path(__file__).resolve().parent
COMUNI_CSV_PATH = BASE_DIR / "data" / "comuni_italiani.csv"


PROVINCE_NAME_TO_CODE = {
    "agrigento": "AG",
    "alessandria": "AL",
    "ancona": "AN",
    "aosta": "AO",
    "arezzo": "AR",
    "ascoli piceno": "AP",
    "asti": "AT",
    "avellino": "AV",
    "bari": "BA",
    "barletta-andria-trani": "BT",
    "belluno": "BL",
    "benevento": "BN",
    "bergamo": "BG",
    "biella": "BI",
    "bologna": "BO",
    "bolzano": "BZ",
    "brescia": "BS",
    "brindisi": "BR",
    "cagliari": "CA",
    "caltanissetta": "CL",
    "campobasso": "CB",
    "caserta": "CE",
    "catania": "CT",
    "catanzaro": "CZ",
    "chieti": "CH",
    "como": "CO",
    "cosenza": "CS",
    "cremona": "CR",
    "crotone": "KR",
    "cuneo": "CN",
    "enna": "EN",
    "fermo": "FM",
    "ferrara": "FE",
    "firenze": "FI",
    "foggia": "FG",
    "forli-cesena": "FC",
    "forlì-cesena": "FC",
    "frosinone": "FR",
    "genova": "GE",
    "gorizia": "GO",
    "grosseto": "GR",
    "imperia": "IM",
    "isernia": "IS",
    "la spezia": "SP",
    "l'aquila": "AQ",
    "latina": "LT",
    "lecce": "LE",
    "lecco": "LC",
    "livorno": "LI",
    "lodi": "LO",
    "lucca": "LU",
    "macerata": "MC",
    "mantova": "MN",
    "massa-carrara": "MS",
    "matera": "MT",
    "messina": "ME",
    "milano": "MI",
    "modena": "MO",
    "monza e brianza": "MB",
    "napoli": "NA",
    "novara": "NO",
    "nuoro": "NU",
    "oristano": "OR",
    "padova": "PD",
    "palermo": "PA",
    "parma": "PR",
    "pavia": "PV",
    "perugia": "PG",
    "pesaro e urbino": "PU",
    "pescara": "PE",
    "piacenza": "PC",
    "pisa": "PI",
    "pistoia": "PT",
    "pordenone": "PN",
    "potenza": "PZ",
    "prato": "PO",
    "ragusa": "RG",
    "ravenna": "RA",
    "reggio calabria": "RC",
    "reggio emilia": "RE",
    "rieti": "RI",
    "rimini": "RN",
    "roma": "RM",
    "rovigo": "RO",
    "salerno": "SA",
    "sassari": "SS",
    "savona": "SV",
    "siena": "SI",
    "siracusa": "SR",
    "sondrio": "SO",
    "sud sardegna": "SU",
    "carbonia-iglesias": "CI",
    "taranto": "TA",
    "teramo": "TE",
    "terni": "TR",
    "torino": "TO",
    "trapani": "TP",
    "trento": "TN",
    "treviso": "TV",
    "trieste": "TS",
    "udine": "UD",
    "varese": "VA",
    "venezia": "VE",
    "verbano-cusio-ossola": "VB",
    "vercelli": "VC",
    "verona": "VR",
    "vibo valentia": "VV",
    "vicenza": "VI",
    "viterbo": "VT",
}


PROVINCE_TO_REGION = {
    "AG": "Sicilia",
    "AL": "Piemonte",
    "AN": "Marche",
    "AO": "Valle d'Aosta",
    "AR": "Toscana",
    "AP": "Marche",
    "AT": "Piemonte",
    "AV": "Campania",
    "BA": "Puglia",
    "BT": "Puglia",
    "BL": "Veneto",
    "BN": "Campania",
    "BG": "Lombardia",
    "BI": "Piemonte",
    "BO": "Emilia-Romagna",
    "BZ": "Trentino-Alto Adige",
    "BS": "Lombardia",
    "BR": "Puglia",
    "CA": "Sardegna",
    "CL": "Sicilia",
    "CB": "Molise",
    "CE": "Campania",
    "CT": "Sicilia",
    "CZ": "Calabria",
    "CH": "Abruzzo",
    "CO": "Lombardia",
    "CS": "Calabria",
    "CR": "Lombardia",
    "KR": "Calabria",
    "CN": "Piemonte",
    "EN": "Sicilia",
    "FM": "Marche",
    "FE": "Emilia-Romagna",
    "FI": "Toscana",
    "FG": "Puglia",
    "FC": "Emilia-Romagna",
    "FR": "Lazio",
    "GE": "Liguria",
    "GO": "Friuli-Venezia Giulia",
    "GR": "Toscana",
    "IM": "Liguria",
    "IS": "Molise",
    "SP": "Liguria",
    "AQ": "Abruzzo",
    "LT": "Lazio",
    "LE": "Puglia",
    "LC": "Lombardia",
    "LI": "Toscana",
    "LO": "Lombardia",
    "LU": "Toscana",
    "MC": "Marche",
    "MN": "Lombardia",
    "MS": "Toscana",
    "MT": "Basilicata",
    "ME": "Sicilia",
    "MI": "Lombardia",
    "MO": "Emilia-Romagna",
    "MB": "Lombardia",
    "NA": "Campania",
    "NO": "Piemonte",
    "NU": "Sardegna",
    "OR": "Sardegna",
    "PD": "Veneto",
    "PA": "Sicilia",
    "PR": "Emilia-Romagna",
    "PV": "Lombardia",
    "PG": "Umbria",
    "PU": "Marche",
    "PE": "Abruzzo",
    "PC": "Emilia-Romagna",
    "PI": "Toscana",
    "PT": "Toscana",
    "PN": "Friuli-Venezia Giulia",
    "PZ": "Basilicata",
    "PO": "Toscana",
    "RG": "Sicilia",
    "RA": "Emilia-Romagna",
    "RC": "Calabria",
    "RE": "Emilia-Romagna",
    "RI": "Lazio",
    "RN": "Emilia-Romagna",
    "RM": "Lazio",
    "RO": "Veneto",
    "SA": "Campania",
    "SS": "Sardegna",
    "SV": "Liguria",
    "SI": "Toscana",
    "SR": "Sicilia",
    "SO": "Lombardia",
    "SU": "Sardegna",
    "CI": "Sardegna",
    "TA": "Puglia",
    "TE": "Abruzzo",
    "TR": "Umbria",
    "TO": "Piemonte",
    "TP": "Sicilia",
    "TN": "Trentino-Alto Adige",
    "TV": "Veneto",
    "TS": "Friuli-Venezia Giulia",
    "UD": "Friuli-Venezia Giulia",
    "VA": "Lombardia",
    "VE": "Veneto",
    "VB": "Piemonte",
    "VC": "Piemonte",
    "VR": "Veneto",
    "VV": "Calabria",
    "VI": "Veneto",
    "VT": "Lazio",
}


FALLBACK_MUNICIPALITY_TO_PROVINCE = {
    "voghera": "PV",
    "pizzale": "PV",
    "dorno": "PV",
    "cergnago": "PV",
    "olevano di lomellina": "PV",
    "pralboino": "BS",
    "medole": "MN",
    "marmirolo": "MN",
    "marmiolo": "MN",
    "guidizzolo": "MN",
    "pavia": "PV",
    "mantova": "MN",
    "taranto": "TA",
    "gonnesa": "SU",
    "venezia": "VE",
    "viterbo": "VT",
    "montalto di castro": "VT",
    "canino": "VT",
    "tuscania": "VT",
    "tarquinia": "VT",
    "roma": "RM",
    "latina": "LT",
    "aprilia": "LT",
    "foggia": "FG",
    "cerignola": "FG",
    "orta nova": "FG",
    "brindisi": "BR",
    "nardo": "LE",
    "nardò": "LE",
    "erchie": "BR",
    "ragusa": "RG",
    "giarratana": "RG",
    "modica": "RG",
    "siracusa": "SR",
    "lentini": "SR",
    "carlentini": "SR",
    "catania": "CT",
    "vizzini": "CT",
    "bronte": "CT",
    "palermo": "PA",
    "monreale": "PA",
    "trapani": "TP",
    "marsala": "TP",
    "mazara del vallo": "TP",
    "salemi": "TP",
    "gela": "CL",
    "licata": "AG",
    "agrigento": "AG",
    "pisa": "PI",
    "bientina": "PI",
    "grosseto": "GR",
    "manciano": "GR",
    "scansano": "GR",
    "orbetello": "GR",
    "sinalunga": "SI",
    "argenta": "FE",
    "ferrara": "FE",
    "ravenna": "RA",
    "faenza": "RA",
    "forli": "FC",
    "forlì": "FC",
    "cesena": "FC",
    "bologna": "BO",
    "modena": "MO",
    "reggio emilia": "RE",
    "brescia": "BS",
    "bergamo": "BG",
    "alessandria": "AL",
    "asti": "AT",
    "cuneo": "CN",
    "novara": "NO",
    "torino": "TO",
    "vercelli": "VC",
    "rovigo": "RO",
    "padova": "PD",
    "verona": "VR",
    "vicenza": "VI",
    "treviso": "TV",
    "udine": "UD",
    "santa maria la fossa": "CE",
    "buonabitacolo": "SA",
    "bisaccia": "AV",
    "morcone": "BN",
    "scampitella": "AV",
    "montecalvo irpino": "AV",
    "giugliano in campania": "NA",
}


DIRTY_MUNICIPALITY_FRAGMENTS = [
    "dettaglio",
    "valutazioni",
    "autorizzazioni",
    "ambientali",
    "vas",
    "via",
    "aia",
    "conclusa",
    "concluso",
    "successivamente",
    "ridotta",
    "ridotto",
    "determinazione",
    "determina",
    "decreto",
    "ministeriale",
    "direttoriale",
    "provvedimento",
    "d.m.",
    "dm_",
    "dm ",
    "data pubblicazione",
    "pubblicazione",
    "esito",
    "positivo",
    "negativo",
    "scarica",
    "progetto",
    "documentazione",
    "procedura",
    "pagina",
    "home",
    "menu",
    "mw",
    "mwp",
    "kw",
    "kwp",
    "potenza",
    "impianto",
    "impianti",
    "fotovoltaico",
    "fotovoltaici",
    "agrivoltaico",
    "agrivoltaici",
    "agrovoltaico",
    "agrofotovoltaico",
    "fonte rinnovabile",
    "energia elettrica",
    "connessione",
    "elettrodotto",
    "sottostazione",
    "stazione elettrica",
    "cabina",
    "tracker",
    "moduli",
    "pannelli",
    "area",
    "aree",
    "catasto",
    "foglio",
    "particella",
    "particelle",
    "cavidotto",
    "strada",
    "provincia",
    "città metropolitana",
    "citta metropolitana",
]


TRAILING_NOISE_WORDS = {
    "in",
    "nel",
    "nella",
    "nei",
    "nelle",
    "del",
    "della",
    "delle",
    "dei",
    "con",
    "da",
    "di",
    "e",
    "ed",
}


@dataclass
class MunicipalityInfo:
    name: str
    province_code: str
    province_name: str | None = None
    region: str | None = None


@dataclass
class GeoEnrichmentResult:
    province: str | None = None
    region: str | None = None
    municipalities: list[str] | None = None
    province_deduced: bool = False
    municipalities_deduced: bool = False


def enrich_geo_from_text(
    text: str | None,
    existing_region: str | None = None,
    existing_province: str | None = None,
    existing_municipalities: str | list[str] | None = None,
) -> GeoEnrichmentResult:
    full_text = clean_text(text or "") or ""

    current_municipalities = normalize_existing_municipalities(existing_municipalities)

    province = normalize_province_code(existing_province)
    region = clean_text(existing_region)
    municipalities = current_municipalities[:]

    province_deduced = False
    municipalities_deduced = False

    if not province:
        province = extract_province_code(full_text)
        province_deduced = bool(province)

    explicit_municipalities = extract_municipalities_by_regex(full_text)

    if not municipalities and explicit_municipalities:
        municipalities = explicit_municipalities
        municipalities_deduced = True

    known_municipalities: list[str] = []

    # Con il CSV nazionale attivo NON facciamo scansione libera dei comuni nel testo:
    # genererebbe falsi positivi tipo "potenza" -> Comune di Potenza.
    # La scansione libera resta solo come fallback quando il CSV non esiste.
    if not csv_available():
        known_municipalities = municipalities_from_known_map(full_text)

    if not municipalities and known_municipalities:
        municipalities = known_municipalities
        municipalities_deduced = True

    if not province and municipalities:
        inferred = infer_province_from_municipalities(municipalities)
        if inferred:
            province = inferred
            province_deduced = True

    if not region and province:
        region = PROVINCE_TO_REGION.get(province.upper())

    return GeoEnrichmentResult(
        province=province,
        region=region,
        municipalities=municipalities,
        province_deduced=province_deduced,
        municipalities_deduced=municipalities_deduced,
    )


def extract_province_code(text: str | None) -> str | None:
    if not text:
        return None

    context_patterns = [
        r"\b(?:provincia|prov\.|comune|comuni|località|localita)\b[^.;\n]{0,160}\(([A-Z]{2})\)",
        r"\(([A-Z]{2})\)[^.;\n]{0,160}\b(?:provincia|prov\.|comune|comuni|località|localita)\b",
    ]

    for pattern in context_patterns:
        for match in re.findall(pattern, text, flags=re.IGNORECASE):
            code = normalize_province_code(match)
            if code:
                return code

    province_name_patterns = [
        r"\bprovincia\s+di\s+([A-ZÀ-Ú][A-Za-zÀ-Úà-ú'\- ]{2,80})(?:\s*\(([A-Z]{2})\)|,|\.|;|$)",
        r"\bin\s+provincia\s+di\s+([A-ZÀ-Ú][A-Za-zÀ-Úà-ú'\- ]{2,80})(?:\s*\(([A-Z]{2})\)|,|\.|;|$)",
        r"\bprov\.\s+di\s+([A-ZÀ-Ú][A-Za-zÀ-Úà-ú'\- ]{2,80})(?:\s*\(([A-Z]{2})\)|,|\.|;|$)",
        r"\bcittà metropolitana di\s+([A-ZÀ-Ú][A-Za-zÀ-Úà-ú'\- ]{2,80})(?:,|\.|;|$)",
        r"\bcitta metropolitana di\s+([A-ZÀ-Ú][A-Za-zÀ-Úà-ú'\- ]{2,80})(?:,|\.|;|$)",
    ]

    for pattern in province_name_patterns:
        for match in re.finditer(pattern, text, flags=re.IGNORECASE):
            province_name = clean_text(match.group(1))
            code_in_parentheses = (
                normalize_province_code(match.group(2))
                if match.lastindex and match.lastindex >= 2 and match.group(2)
                else None
            )

            if code_in_parentheses:
                return code_in_parentheses

            if province_name:
                code = province_name_to_code(province_name)
                if code:
                    return code

    for match in re.finditer(r"\(([A-Z]{2})\)", text):
        code = normalize_province_code(match.group(1))

        if not code:
            continue

        start = max(0, match.start() - 120)
        end = min(len(text), match.end() + 120)
        context = text[start:end].lower()

        if any(word in context for word in ["comune", "comuni", "provincia", "prov.", "località", "localita"]):
            return code

    return None


def extract_municipalities_by_regex(text: str | None) -> list[str]:
    if not text:
        return []

    results: list[str] = []

    patterns = [
        r"\bComune\s+di\s+(.+?)(?:\s*\([A-Z]{2}\)|,?\s+in\s+provincia|,?\s+provincia|,?\s+città metropolitana|,?\s+citta metropolitana|,?\s+denominato|,?\s+avente|,?\s+con\s+una|,?\s+di\s+potenza|\.|;|$)",
        r"\bComuni\s+di\s+(.+?)(?:\s*\([A-Z]{2}\)|,?\s+in\s+provincia|,?\s+provincia|,?\s+città metropolitana|,?\s+citta metropolitana|,?\s+denominato|,?\s+avente|,?\s+con\s+una|,?\s+di\s+potenza|\.|;|$)",
        r"\bnel\s+Comune\s+di\s+(.+?)(?:\s*\([A-Z]{2}\)|,?\s+in\s+provincia|,?\s+provincia|,?\s+città metropolitana|,?\s+citta metropolitana|,?\s+denominato|,?\s+avente|,?\s+con\s+una|,?\s+di\s+potenza|\.|;|$)",
        r"\bnei\s+Comuni\s+di\s+(.+?)(?:\s*\([A-Z]{2}\)|,?\s+in\s+provincia|,?\s+provincia|,?\s+città metropolitana|,?\s+citta metropolitana|,?\s+denominato|,?\s+avente|,?\s+con\s+una|,?\s+di\s+potenza|\.|;|$)",
        r"\bnel\s+comune\s+di\s+(.+?)(?:\s*\([A-Z]{2}\)|,?\s+in\s+provincia|,?\s+provincia|,?\s+città metropolitana|,?\s+citta metropolitana|,?\s+denominato|,?\s+avente|,?\s+con\s+una|,?\s+di\s+potenza|\.|;|$)",
        r"\bnei\s+comuni\s+di\s+(.+?)(?:\s*\([A-Z]{2}\)|,?\s+in\s+provincia|,?\s+provincia|,?\s+città metropolitana|,?\s+citta metropolitana|,?\s+denominato|,?\s+avente|,?\s+con\s+una|,?\s+di\s+potenza|\.|;|$)",
        r"\bterritorio\s+comunale\s+di\s+(.+?)(?:\s*\([A-Z]{2}\)|,?\s+in\s+provincia|,?\s+provincia|,?\s+città metropolitana|,?\s+citta metropolitana|,?\s+denominato|,?\s+avente|,?\s+con\s+una|,?\s+di\s+potenza|\.|;|$)",
        r"\bcentro\s+abitato\s+di\s+(.+?)(?:\s*\([A-Z]{2}\)|,?\s+in\s+provincia|,?\s+provincia|,?\s+città metropolitana|,?\s+citta metropolitana|,?\s+denominato|,?\s+avente|,?\s+con\s+una|,?\s+di\s+potenza|\.|;|$)",
        r"\b([A-ZÀ-Ú][A-Za-zÀ-Úà-ú'\- ]{2,80}),\s+in\s+provincia\s+di\s+[A-ZÀ-Ú][A-Za-zÀ-Úà-ú'\- ]{2,80}(?:\s*\([A-Z]{2}\))?",
        r"\b([A-ZÀ-Ú][A-Za-zÀ-Úà-ú'\- ]{2,80}),\s+città metropolitana di\s+[A-ZÀ-Ú][A-Za-zÀ-Úà-ú'\- ]{2,80}",
        r"\b([A-ZÀ-Ú][A-Za-zÀ-Úà-ú'\- ]{2,80}),\s+citta metropolitana di\s+[A-ZÀ-Ú][A-Za-zÀ-Úà-ú'\- ]{2,80}",
    ]

    for pattern in patterns:
        for match in re.finditer(pattern, text, flags=re.IGNORECASE):
            raw = clean_text(match.group(1))

            if not raw:
                continue

            for part in split_municipality_list(raw):
                municipality = clean_municipality_name(part)

                if not municipality:
                    continue

                resolved = resolve_known_municipality_name(municipality)

                # Se è presente il CSV nazionale, accetta solo comuni esistenti.
                if csv_available() and not resolved:
                    continue

                municipality = resolved or municipality

                if municipality not in results:
                    results.append(municipality)

    return results[:12]


def municipalities_from_known_map(text: str | None) -> list[str]:
    if not text:
        return []

    norm_text = normalize_for_match(text)
    municipalities_map = get_municipality_map()
    results: list[str] = []

    for municipality_norm in sorted(municipalities_map.keys(), key=len, reverse=True):
        if len(municipality_norm) < 4:
            continue

        for match in re.finditer(rf"\b{re.escape(municipality_norm)}\b", norm_text):
            context = get_normalized_context(norm_text, match.start(), match.end(), radius=55)

            if is_bad_municipality_context(municipality_norm, context):
                continue

            info = municipalities_map[municipality_norm]
            formatted = info.name

            if formatted not in results:
                results.append(formatted)

    return results[:12]


def infer_province_from_municipalities(municipalities: list[str]) -> str | None:
    municipalities_map = get_municipality_map()
    codes: list[str] = []

    for municipality in municipalities:
        key = normalize_for_match(municipality)
        info = municipalities_map.get(key)

        if info and info.province_code and info.province_code not in codes:
            codes.append(info.province_code)

    if len(codes) == 1:
        return codes[0]

    return None


def resolve_known_municipality_name(value: str) -> str | None:
    key = normalize_for_match(value)
    info = get_municipality_map().get(key)

    if info:
        return info.name

    return None


@lru_cache(maxsize=1)
def get_municipality_map() -> dict[str, MunicipalityInfo]:
    loaded = load_municipalities_from_csv(COMUNI_CSV_PATH)

    if loaded:
        return loaded

    fallback: dict[str, MunicipalityInfo] = {}

    for municipality, province_code in FALLBACK_MUNICIPALITY_TO_PROVINCE.items():
        province_code = province_code.upper()
        name = format_municipality(municipality)

        fallback[normalize_for_match(municipality)] = MunicipalityInfo(
            name=name,
            province_code=province_code,
            province_name=None,
            region=PROVINCE_TO_REGION.get(province_code),
        )

    return fallback


@lru_cache(maxsize=1)
def csv_available() -> bool:
    return bool(load_municipalities_from_csv(COMUNI_CSV_PATH))


def load_municipalities_from_csv(path: Path) -> dict[str, MunicipalityInfo]:
    if not path.exists():
        return {}

    raw = read_text_with_fallback(path)

    try:
        dialect = csv.Sniffer().sniff(raw[:4096], delimiters=",;|\t")
    except csv.Error:
        dialect = csv.excel

    reader = csv.DictReader(raw.splitlines(), dialect=dialect)

    if not reader.fieldnames:
        return {}

    field_map = {normalize_header(field): field for field in reader.fieldnames}

    comune_field = first_existing_field(
        field_map,
        [
            "comune",
            "denominazione_ita",
            "denominazione_italiana",
            "denominazione_comune",
            "nome_comune",
            "denominazione",
        ],
    )

    province_code_field = first_existing_field(
        field_map,
        [
            "sigla",
            "sigla_provincia",
            "codice_provincia",
            "provincia_sigla",
            "targa",
            "sigla_automobilistica",
            "automobilistica",
        ],
    )

    province_name_field = first_existing_field(
        field_map,
        [
            "provincia",
            "denominazione_provincia",
            "nome_provincia",
            "denominazione_dell_unita_territoriale_sovracomunale",
            "denominazione_unita_territoriale_sovracomunale",
        ],
    )

    region_field = first_existing_field(
        field_map,
        [
            "regione",
            "denominazione_regione",
            "nome_regione",
        ],
    )

    if not comune_field:
        return {}

    result: dict[str, MunicipalityInfo] = {}

    for row in reader:
        comune = clean_text(row.get(comune_field))

        if not comune:
            continue

        province_code = normalize_province_code(row.get(province_code_field)) if province_code_field else None
        province_name = clean_text(row.get(province_name_field)) if province_name_field else None
        region = clean_text(row.get(region_field)) if region_field else None

        if not province_code and province_name:
            province_code = province_name_to_code(province_name)

        if not province_code:
            continue

        if not region:
            region = PROVINCE_TO_REGION.get(province_code)

        name = format_municipality(comune)
        key = normalize_for_match(comune)

        result[key] = MunicipalityInfo(
            name=name,
            province_code=province_code,
            province_name=province_name,
            region=region,
        )

    return result


def read_text_with_fallback(path: Path) -> str:
    for encoding in ["utf-8-sig", "utf-8", "cp1252", "latin-1"]:
        try:
            return path.read_text(encoding=encoding)
        except UnicodeDecodeError:
            continue

    return path.read_text(encoding="utf-8", errors="replace")


def first_existing_field(field_map: dict[str, str], aliases: list[str]) -> str | None:
    for alias in aliases:
        key = normalize_header(alias)
        if key in field_map:
            return field_map[key]

    return None


def province_name_to_code(value: str) -> str | None:
    value_norm = normalize_for_match(value)

    for name, code in sorted(PROVINCE_NAME_TO_CODE.items(), key=lambda item: len(item[0]), reverse=True):
        if normalize_for_match(name) == value_norm:
            return code

    for name, code in sorted(PROVINCE_NAME_TO_CODE.items(), key=lambda item: len(item[0]), reverse=True):
        name_norm = normalize_for_match(name)
        if re.search(rf"\b{re.escape(name_norm)}\b", value_norm):
            return code

    return None


def normalize_province_code(value: Any) -> str | None:
    if value is None:
        return None

    text = clean_text(str(value))

    if not text:
        return None

    text = text.strip().upper()

    if text in PROVINCE_TO_REGION:
        return text

    match = re.search(r"\(([A-Z]{2})\)", text)
    if match:
        code = match.group(1).upper()
        if code in PROVINCE_TO_REGION:
            return code

    code = province_name_to_code(text)
    if code:
        return code

    return None


def split_municipality_list(value: str) -> list[str]:
    value = clean_text(value) or ""

    value = re.sub(r"\bnei territori comunali di\b", "", value, flags=re.IGNORECASE)
    value = re.sub(r"\bnel territorio comunale di\b", "", value, flags=re.IGNORECASE)
    value = re.sub(r"\bterritorio comunale di\b", "", value, flags=re.IGNORECASE)
    value = re.sub(r"\bnel comune di\b", "", value, flags=re.IGNORECASE)
    value = re.sub(r"\bnei comuni di\b", "", value, flags=re.IGNORECASE)
    value = re.sub(r"\bnel Comune di\b", "", value, flags=re.IGNORECASE)
    value = re.sub(r"\bnei Comuni di\b", "", value, flags=re.IGNORECASE)
    value = re.sub(r"\bcomune di\b", "", value, flags=re.IGNORECASE)
    value = re.sub(r"\bcomuni di\b", "", value, flags=re.IGNORECASE)

    # Separa "X e Y", "X ed Y", virgole, slash, punto e virgola.
    parts = re.split(
        r"\s*,\s*|\s*;\s*|\s*/\s*|\s+(?:e|ed)\s+",
        value,
        flags=re.IGNORECASE,
    )

    cleaned: list[str] = []

    for part in parts:
        part = clean_text(part)

        if not part:
            continue

        cleaned.append(part)

    return cleaned


def clean_municipality_name(value: str | None) -> str | None:
    value = clean_text(value)

    if not value:
        return None

    value = value.strip(" .:-,;()[]")

    value = re.sub(r"\s*\([A-Za-z]{2}\b.*$", "", value).strip()
    value = re.sub(r"\s*-\s*dettaglio\b.*$", "", value, flags=re.IGNORECASE).strip()
    value = re.sub(r"\s*-\s*valutazioni\b.*$", "", value, flags=re.IGNORECASE).strip()
    value = re.sub(r"\s+Dettaglio\b.*$", "", value, flags=re.IGNORECASE).strip()
    value = re.sub(r"\s+Valutazioni\b.*$", "", value, flags=re.IGNORECASE).strip()

    value = re.sub(r"\b(provincia|prov\.|località|localita)\b.*$", "", value, flags=re.IGNORECASE).strip()
    value = re.sub(r"\bcittà metropolitana\b.*$", "", value, flags=re.IGNORECASE).strip()
    value = re.sub(r"\bcitta metropolitana\b.*$", "", value, flags=re.IGNORECASE).strip()
    value = re.sub(r"\bdenominat[oa]\b.*$", "", value, flags=re.IGNORECASE).strip()
    value = re.sub(r"\bavente\b.*$", "", value, flags=re.IGNORECASE).strip()
    value = re.sub(r"\bcon\s+potenza\b.*$", "", value, flags=re.IGNORECASE).strip()
    value = re.sub(r"\bdi\s+potenza\b.*$", "", value, flags=re.IGNORECASE).strip()
    value = re.sub(r"\bsuccessivamente\b.*$", "", value, flags=re.IGNORECASE).strip()

    value = strip_trailing_noise_words(value)

    if not value:
        return None

    if len(value) > 80:
        return None

    if not is_valid_municipality_candidate(value):
        return None

    return format_municipality(value)


def strip_trailing_noise_words(value: str) -> str:
    words = value.split()

    while words and normalize_for_match(words[-1]) in TRAILING_NOISE_WORDS:
        words.pop()

    return " ".join(words).strip()


def is_valid_municipality_candidate(value: str | None) -> bool:
    value = clean_text(value)

    if not value:
        return False

    lowered = value.lower()

    if any(fragment in lowered for fragment in DIRTY_MUNICIPALITY_FRAGMENTS):
        return False

    if len(value) < 3:
        return False

    if re.search(r"\d", value):
        return False

    if len(value.split()) > 6:
        return False

    if len(value) == 1:
        return False

    return True


def is_bad_municipality_context(municipality_norm: str, context: str) -> bool:
    bad_context_patterns = [
        rf"provincia di {re.escape(municipality_norm)}",
        rf"in provincia di {re.escape(municipality_norm)}",
        rf"prov {re.escape(municipality_norm)}",
        rf"provincia {re.escape(municipality_norm)}",
        rf"citta metropolitana di {re.escape(municipality_norm)}",
        rf"città metropolitana di {re.escape(municipality_norm)}",
    ]

    if any(re.search(pattern, context) for pattern in bad_context_patterns):
        return True

    if any(fragment in context for fragment in ["dettaglio valutazioni", "vas via aia", "scarica il provvedimento"]):
        return True

    tokens = context.split()
    municipality_tokens = municipality_norm.split()

    if not municipality_tokens:
        return False

    for idx in range(0, len(tokens) - len(municipality_tokens) + 1):
        if tokens[idx : idx + len(municipality_tokens)] == municipality_tokens:
            before = tokens[max(0, idx - 4) : idx]
            after = tokens[idx + len(municipality_tokens) : idx + len(municipality_tokens) + 4]

            if any(token in before for token in ["provincia", "prov", "metropolitana"]):
                return True

            if any(token in after for token in ["dettaglio", "valutazioni", "ambientali"]):
                return True

    return False


def get_normalized_context(norm_text: str, start: int, end: int, radius: int = 50) -> str:
    left = max(0, start - radius)
    right = min(len(norm_text), end + radius)
    return norm_text[left:right]


def format_municipality(value: str) -> str:
    value = clean_text(value) or ""
    value = value.strip(" .:-,;()[]")

    words = []
    for word in value.split():
        low = word.lower()
        if low in {"di", "del", "della", "delle", "dei", "da", "in", "sul", "sulla", "l", "la", "le"}:
            words.append(low)
        else:
            words.append(word[:1].upper() + word[1:].lower())

    return " ".join(words)


def normalize_existing_municipalities(value: str | list[str] | None) -> list[str]:
    if value is None:
        return []

    if isinstance(value, list):
        raw_parts = value
    else:
        raw = clean_text(value)

        if not raw:
            return []

        raw_parts = re.split(r",|;|\|", raw)

    results: list[str] = []

    for part in raw_parts:
        municipality = clean_municipality_name(str(part))

        if not municipality:
            continue

        resolved = resolve_known_municipality_name(municipality)

        if csv_available() and not resolved:
            continue

        municipality = resolved or municipality

        if municipality not in results:
            results.append(municipality)

    return results


def clean_text(value: str | None) -> str | None:
    if value is None:
        return None

    text = str(value).replace("\xa0", " ")
    text = " ".join(text.split()).strip()

    if text.lower() in {"none", "nan", "null"}:
        return None

    return text or None


def normalize_header(value: str | None) -> str:
    value = clean_text(value or "") or ""
    value = value.lower()
    value = value.replace("à", "a")
    value = value.replace("è", "e")
    value = value.replace("é", "e")
    value = value.replace("ì", "i")
    value = value.replace("ò", "o")
    value = value.replace("ù", "u")
    value = re.sub(r"[^a-z0-9]+", "_", value)
    return value.strip("_")


def normalize_for_match(value: str | None) -> str:
    value = clean_text(value or "") or ""
    value = value.lower()
    value = value.replace("à", "a")
    value = value.replace("è", "e")
    value = value.replace("é", "e")
    value = value.replace("ì", "i")
    value = value.replace("ò", "o")
    value = value.replace("ù", "u")
    value = re.sub(r"[^a-z0-9]+", " ", value)
    return " ".join(value.split())