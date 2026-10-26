import re
from datetime import datetime
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup

from app.wind_agents.legacy.models import ProjectRecord


BASE_URL = "https://va.mite.gov.it"
SEARCH_URL = f"{BASE_URL}/it-IT/Ricerca/Via"
SEARCH_FREE_URL = f"{BASE_URL}/it-IT/Ricerca/ViaLibera"

DEFAULT_KEYWORDS = [
    "fotovoltaico",
    "agrivoltaico",
    "eolico",
    "bess",
    "accumulo",
    "biometano",
    "rifiuti",
    "depuratore",
]


HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/126.0 Safari/537.36"
    ),
    "Accept-Language": "it-IT,it;q=0.9,en;q=0.8",
}


def _clean(text: str | None) -> str | None:
    if not text:
        return None
    text = re.sub(r"\s+", " ", text).strip()
    return text or None


def _extract_first(pattern: str, text: str) -> str | None:
    match = re.search(pattern, text, flags=re.IGNORECASE)
    if not match:
        return None
    return _clean(match.group(1))


def _extract_power_mw(text: str) -> float | None:
    """
    Estrae potenza in MW quando il testo contiene forme tipo:
    - 34,62 MW
    - 160 MW
    - 41247 kWp
    - 8200 kW
    """
    text_norm = text.replace(".", "").replace(",", ".")

    mw_match = re.search(r"(\d+(?:\.\d+)?)\s*MWp?\b", text_norm, flags=re.IGNORECASE)
    if mw_match:
        try:
            return float(mw_match.group(1))
        except ValueError:
            pass

    kw_match = re.search(r"(\d+(?:\.\d+)?)\s*kWp?\b", text_norm, flags=re.IGNORECASE)
    if kw_match:
        try:
            return round(float(kw_match.group(1)) / 1000, 3)
        except ValueError:
            pass

    return None


def _extract_region_province_municipality(text: str) -> tuple[str | None, str | None, str | None]:
    """
    Estrazione volutamente prudente.
    Per ora prova a riconoscere alcune forme ricorrenti:
    - comune di X
    - comuni di X, Y
    La provincia/regione verranno migliorate in step successivo.
    """
    municipality = _extract_first(r"\bcomune di ([A-ZÀ-ÖØ-Ýa-zà-öø-ÿ'’\-\s]+?)(?:[,.;)]|\s+e\s+|\s+in\s+provincia|\s+nel\s+territorio|$)", text)
    if not municipality:
        municipality = _extract_first(r"\bcomuni di ([A-ZÀ-ÖØ-Ýa-zà-öø-ÿ'’\-\s,]+?)(?:[.;)]|\s+in\s+provincia|\s+nel\s+territorio|$)", text)

    province = _extract_first(r"\(([A-Z]{2})\)", text)

    return None, province, municipality


def _infer_sector(text: str) -> str | None:
    t = text.lower()
    if "agrivoltaic" in t or "agrovoltaic" in t or "agri-voltaic" in t:
        return "agrivoltaico"
    if "fotovoltaic" in t:
        return "fotovoltaico"
    if "eolico" in t or "aerogenerator" in t:
        return "eolico"
    if "bess" in t or "accumulo" in t or "storage" in t:
        return "energia"
    if "biometano" in t:
        return "biometano"
    if "rifiuti" in t:
        return "rifiuti"
    if "depurator" in t:
        return "depurazione"
    return "energia"


def _infer_phase(text: str) -> str | None:
    t = text.lower()
    if "verifica di assoggettabilità" in t:
        return "verifica assoggettabilità VIA"
    if "valutazione di impatto ambientale" in t or "procedura di via" in t or "via" in t:
        return "VIA"
    if "provvedimento" in t and "positivo" in t:
        return "provvedimento positivo"
    if "osservazioni" in t:
        return "osservazioni"
    if "integrazioni" in t:
        return "richiesta integrazioni"
    return "procedura ambientale"


def _get_token(session: requests.Session) -> str | None:
    response = session.get(SEARCH_URL, headers=HEADERS, timeout=30)
    response.raise_for_status()

    soup = BeautifulSoup(response.text, "lxml")
    token_input = soup.find("input", {"name": "__RequestVerificationToken"})
    if token_input and token_input.get("value"):
        return token_input["value"]

    return None


def _search_keyword(session: requests.Session, keyword: str, max_pages: int = 2) -> list[str]:
    """
    Cerca i link alle schede progetto per una keyword.
    Ritorna URL assoluti di tipo /Oggetti/Info/{id}.
    """
    links: list[str] = []

    token = None
    try:
        token = _get_token(session)
    except Exception as exc:
        print(f"[MASE] Token non recuperato: {exc}")

    for page in range(1, max_pages + 1):
        params = {
            "T": "o",
            "Testo": keyword,
            "pagina": page,
            "x": "15",
            "y": "15",
        }
        if token:
            params["__RequestVerificationToken"] = token

        try:
            response = session.get(
                SEARCH_FREE_URL,
                params=params,
                headers=HEADERS,
                timeout=45,
            )
            response.raise_for_status()
        except Exception as exc:
            print(f"[MASE] Errore ricerca keyword '{keyword}' pagina {page}: {exc}")
            continue

        soup = BeautifulSoup(response.text, "lxml")

        page_links = []
        for a in soup.find_all("a", href=True):
            href = a["href"]
            if "/Oggetti/Info/" in href:
                page_links.append(urljoin(BASE_URL, href))

        page_links = sorted(set(page_links))

        if not page_links:
            break

        links.extend(page_links)

    return sorted(set(links))


def _parse_detail(session: requests.Session, url: str) -> ProjectRecord | None:
    try:
        response = session.get(url, headers=HEADERS, timeout=45)
        response.raise_for_status()
    except Exception as exc:
        print(f"[MASE] Errore dettaglio {url}: {exc}")
        return None

    soup = BeautifulSoup(response.text, "lxml")
    full_text = _clean(soup.get_text(" ", strip=True)) or ""

    external_id = None
    id_match = re.search(r"/Oggetti/Info/(\d+)", url)
    if id_match:
        external_id = f"MASE-{id_match.group(1)}"

    h1 = soup.find("h1")
    title = _clean(h1.get_text(" ", strip=True)) if h1 else None

    if not title:
        # fallback: primo blocco testuale sensato
        title = _extract_first(r"(Progetto[^.]{20,250})", full_text)

    if not title:
        title = f"Progetto MASE {external_id or ''}".strip()

    description = _clean(full_text[:900])

    proponent = (
        _extract_first(r"Proponente\s+(.+?)(?:Procedura|Localizzazione|Documentazione|$)", full_text)
        or _extract_first(r"Società proponente\s+(.+?)(?:Procedura|Localizzazione|Documentazione|$)", full_text)
    )

    region, province, municipality = _extract_region_province_municipality(full_text)

    record = ProjectRecord(
        source="MASE VIA",
        source_url=url,
        external_id=external_id,
        title=title,
        description=description,
        region=region,
        province=province,
        municipality=municipality,
        sector=_infer_sector(full_text),
        category="energia / ambiente",
        intervention_type="nuova costruzione" if "realizzazione" in full_text.lower() else None,
        phase=_infer_phase(full_text),
        client=proponent,
        client_type="privato" if proponent else None,
        power_mw=_extract_power_mw(full_text),
        last_seen=datetime.now().isoformat(timespec="seconds"),
    )

    return record


def collect_mase_via(
    keywords: list[str] | None = None,
    max_pages_per_keyword: int = 1,
    max_details: int = 30,
) -> list[ProjectRecord]:
    keywords = keywords or DEFAULT_KEYWORDS

    session = requests.Session()

    all_links: list[str] = []

    for keyword in keywords:
        print(f"[MASE] Cerco keyword: {keyword}")
        links = _search_keyword(session, keyword, max_pages=max_pages_per_keyword)
        print(f"[MASE] Link trovati per '{keyword}': {len(links)}")
        all_links.extend(links)

    unique_links = sorted(set(all_links))[:max_details]
    print(f"[MASE] Totale link unici da leggere: {len(unique_links)}")

    records: list[ProjectRecord] = []
    seen_ids: set[str] = set()

    for url in unique_links:
        record = _parse_detail(session, url)
        if not record:
            continue

        key = record.external_id or record.source_url or record.title
        if key in seen_ids:
            continue

        seen_ids.add(key)
        records.append(record)

    return records
