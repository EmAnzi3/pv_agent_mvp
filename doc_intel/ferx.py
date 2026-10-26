"""GSE FER X rankings (photovoltaic): which radar projects were awarded the incentive. Information only.

Table A of each competitive procedure lists the plants admitted in a useful position. A plant awarded
an incentive has a guaranteed revenue and a deadline to enter operation: the strongest public signal
that it will be built. This module downloads the tables, reads their rows and matches them to the radar.
"""
from __future__ import annotations

import io
import json
import re
from pathlib import Path
from typing import Any, Callable

BASE = "https://www.gse.it/documenti_site/Documenti%20GSE/Servizi%20per%20te/FERX/Graduatorie%20bando%201/"
TABLES = [
    {"procedura": "FER X transitorio – fotovoltaico (PC_FTV_2025_01)", "pubblicata": "2025-12-01",
     "url": BASE + "Graduatoria_DM_FERXtransitorio_PC_FTV_2025_TabA.pdf"},
    {"procedura": "FER X transitorio – fotovoltaico NZIA (PC_FTVNZIA_2025_01)", "pubblicata": "2025-12-10",
     "url": BASE + "Graduatoria_DM_FERXtransitorio_PC_FTVNZIA_2025_TabA.pdf"},
]
UA = {"User-Agent": "Mozilla/5.0 (PV Agent; GSE rankings reader)"}


def pdf_lines(raw: bytes) -> list[str]:
    import fitz
    doc = fitz.open(stream=raw, filetype="pdf")
    out = []
    for page in doc:
        out += [ln.strip() for ln in page.get_text("text").splitlines() if ln.strip()]
    return out


def dump(get: Callable[[str], Any] | None = None) -> list[str]:
    import requests
    get = get or (lambda u: requests.get(u, headers=UA, timeout=60))
    notes = []
    for t in TABLES:
        r = get(t["url"])
        lines = pdf_lines(r.content) if r.status_code == 200 else []
        notes.append(f"{t['procedura']}: HTTP {r.status_code}, {len(lines)} righe :: " + " | ".join(lines[:60]))
    return notes
