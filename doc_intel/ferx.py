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
UA = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0 Safari/537.36",
      "Accept": "application/pdf,text/html;q=0.9,*/*;q=0.8", "Accept-Language": "it-IT,it;q=0.9",
      "Referer": "https://www.gse.it/servizi-per-te/fonti-rinnovabili/ferx/graduatorie"}


def pdf_lines(raw: bytes) -> list[str]:
    import fitz
    doc = fitz.open(stream=raw, filetype="pdf")
    out = []
    for page in doc:
        out += [ln.strip() for ln in page.get_text("text").splitlines() if ln.strip()]
    return out


def dump(get: Callable[[str], Any] | None = None) -> list[str]:
    import requests
    sess = requests.Session()
    sess.headers.update(UA)
    try:
        home = sess.get("https://www.gse.it/servizi-per-te/fonti-rinnovabili/ferx/graduatorie", timeout=60)
        notes = [f"pagina graduatorie: HTTP {home.status_code}"]
    except Exception as exc:
        notes = [f"pagina graduatorie: {exc}"]
    get = get or (lambda u: sess.get(u, timeout=60))
    for t in TABLES:
        r = get(t["url"])
        lines = pdf_lines(r.content) if r.status_code == 200 else []
        notes.append(f"{t['procedura']}: HTTP {r.status_code}, {len(lines)} righe :: " + " | ".join(lines[:60]))
    return notes
