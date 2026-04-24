from __future__ import annotations

import re


POWER_RE = re.compile(
    r"(?<![\d.,'’])"
    r"(?P<value>"
    r"(?:\d{1,3}(?:[.\s'’]\d{3})+(?:[,.]\d+)?)"
    r"|"
    r"(?:\d+[.,]\d+)"
    r"|"
    r"(?:\d+)"
    r")"
    r"\s*"
    r"(?P<unit>MWp|MW|kWp|KWp|kW|KW)"
    r"\b",
    flags=re.IGNORECASE,
)


def parse_power_to_mw(text: str | None) -> float | None:
    """
    Estrae la prima potenza trovata in un testo e la converte in MW.

    Gestisce:
    - 48.491,52 kWp -> 48.49152 MW
    - 19.056,42 kW  -> 19.05642 MW
    - 12’701,52 kWp -> 12.70152 MW
    - 24'995,52 kWp -> 24.99552 MW
    - 8951.00 kWp   -> 8.951 MW
    - 99 MWp        -> 99 MW
    - 47,01 MWp     -> 47.01 MW
    - 19.305 MWp    -> 19.305 MW
    - 118.07 MW     -> 118.07 MW
    - 29.0752 kWp   -> 29.0752 MW, caso sporco fonte ER
    """
    if not text:
        return None

    match = POWER_RE.search(text)
    if not match:
        return None

    raw_value = match.group("value")
    unit = match.group("unit").lower()

    value = _parse_number(raw_value, unit)
    if value is None:
        return None

    if unit in {"kw", "kwp"}:
        return round(value / 1000, 6)

    if unit in {"mw", "mwp"}:
        return round(value, 6)

    return None


def extract_power_text(text: str | None) -> str | None:
    if not text:
        return None

    match = POWER_RE.search(text)
    if not match:
        return None

    return f"{match.group('value')} {match.group('unit')}"


def _parse_number(raw: str, unit: str | None = None) -> float | None:
    if raw is None:
        return None

    s = raw.strip()
    s = s.replace(" ", "")
    s = s.replace("’", "'")

    unit = (unit or "").lower()

    # Apostrofo come separatore migliaia: 12'701,52
    if "'" in s:
        s = s.replace("'", "")
        if "," in s:
            s = s.replace(",", ".")
        try:
            return float(s)
        except ValueError:
            return None

    # Formato italiano classico: 48.491,52
    if "." in s and "," in s:
        s = s.replace(".", "").replace(",", ".")
        try:
            return float(s)
        except ValueError:
            return None

    # Solo virgola: 47,01
    if "," in s:
        s = s.replace(",", ".")
        try:
            return float(s)
        except ValueError:
            return None

    # Solo punto: caso ambiguo
    if "." in s:
        parts = s.split(".")

        # MW/MWp: il punto è quasi sempre decimale.
        # 118.07 MW -> 118.07
        # 19.305 MWp -> 19.305
        if unit in {"mw", "mwp"}:
            try:
                return float(s)
            except ValueError:
                return None

        # kW/kWp con forma sporca tipo 29.0752 kWp.
        # Interpretazione pratica:
        # 29.0752 kWp -> 29.075,2 kWp -> 29075.2 kWp -> 29.0752 MW
        if (
            unit in {"kw", "kwp"}
            and len(parts) == 2
            and len(parts[0]) <= 3
            and len(parts[1]) == 4
        ):
            candidate = f"{parts[0]}{parts[1][:3]}.{parts[1][3:]}"
            try:
                return float(candidate)
            except ValueError:
                return None

        # kW/kWp con punto migliaia: 48.491 kWp -> 48491 kWp
        if (
            unit in {"kw", "kwp"}
            and len(parts) == 2
            and len(parts[1]) == 3
            and len(parts[0]) <= 3
        ):
            s = s.replace(".", "")
            try:
                return float(s)
            except ValueError:
                return None

        # kW/kWp con punto decimale tecnico: 8951.00 kWp -> 8951.00 kWp
        try:
            return float(s)
        except ValueError:
            return None

    try:
        return float(s)
    except ValueError:
        return None