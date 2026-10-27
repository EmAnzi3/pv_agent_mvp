"""MASE VIA portal, wind projects (onshore and offshore): the same reader as the PV collector, with the
wind typologies of the 'Ricerca per tipologia' page and a wind filter instead of the PV one.

Used only by the wind census (scripts/wind_census.py): it never writes to the PV radar.
"""
from __future__ import annotations

import re
import time
from urllib.parse import parse_qs, urljoin, urlparse

from bs4 import BeautifulSoup

from app.collectors.mase import SEARCH_URL, VIA_TIPOLOGIA_URL, MaseCollector

WIND_RE = re.compile(r"(?i)\beolic[oaih]\w*|aerogenerator\w*|\bwind\b|\bWTG\b|parco\s+eolico|offshore")
NOT_WIND_RE = re.compile(r"(?i)fotovoltaic|agrivoltaic|agrovoltaic")
WIND_TYPOLOGY_RE = re.compile(r"(?i)eolic")
# every MASE status table row: '<procedure code> <start date> <status> Dettagli procedura'
STATUS_RE = re.compile(r"\b(\d{3,6})\s+(\d{2}/\d{2}/\d{4})\s+([^\d:]{3,60}?)\s+Dettagli procedura")


class MaseWindCollector(MaseCollector):
    source_name = "mase_wind"
    max_pages = None
    type_hints = {"Eolico", "Eolico offshore"}
    request_sleep = 1.0            # gentler than the PV collector: the census reads every project page

    def __init__(self) -> None:
        super().__init__()
        self.tipologie = []
        self.info_cache: dict[str, dict] = {}

    # the wind typologies are read from the portal (ids are not hard-coded)
    def discover_tipologie(self) -> list[dict]:
        r = self.session.get(SEARCH_URL, headers=self._headers(), timeout=90)
        r.raise_for_status()
        soup = BeautifulSoup(r.text, "html.parser")
        found: dict[str, dict] = {}
        for a in soup.find_all("a", href=True):
            url = urljoin(SEARCH_URL, a["href"])
            if "/Ricerca/ViaTipologia" not in url:
                continue
            tid = (parse_qs(urlparse(url).query).get("tipologiaID") or [None])[0]
            label = self._clean_text(a.get_text(" ", strip=True))
            if tid and label:
                found[tid] = {"id": tid, "label": label}
        for opt in soup.find_all("option"):
            tid, label = opt.get("value"), self._clean_text(opt.get_text(" ", strip=True))
            if tid and tid.isdigit() and label:
                found.setdefault(tid, {"id": tid, "label": label})
        return list(found.values())

    def fetch(self):
        if not self.tipologie:
            all_t = self.discover_tipologie()
            self.tipologie = [{"id": t["id"], "label": t["label"],
                               "project_type_hint": "Eolico offshore" if re.search(r"(?i)mare|offshore", t["label"]) else "Eolico"}
                              for t in all_t if WIND_TYPOLOGY_RE.search(t["label"])]
            print(f"::notice title=mase-wind tipologie::{self.tipologie} (su {len(all_t)}: "
                  + "; ".join(f"{t['id']}={t['label'][:30]}" for t in all_t)[:900] + ")")
        return super().fetch()

    def _fetch_info_page(self, url: str) -> str | None:
        html_page = super()._fetch_info_page(url)
        time.sleep(self.request_sleep)
        if html_page:
            text = re.sub(r"\s+", " ", BeautifulSoup(html_page, "html.parser").get_text(" ", strip=True))
            i = text.find("Scegli la procedura")
            self.info_cache[url] = {
                "statuses": [m.group(3).strip() for m in STATUS_RE.finditer(text[i:])] if i >= 0 else [],
                "procedures": [{"codice": m.group(1), "avvio": m.group(2), "stato": m.group(3).strip()}
                               for m in STATUS_RE.finditer(text[i:])] if i >= 0 else [],
            }
        return html_page

    def _is_pv_related(self, text: str) -> bool:          # here: 'is this a wind project'
        return bool(WIND_RE.search(text or "")) and not (NOT_WIND_RE.search(text or "") and not re.search(r"(?i)eolic", text or ""))

    def _infer_project_type(self, text: str | None) -> str | None:
        if re.search(r"(?i)offshore|galleggiant|\bin\s+mare\b|marin", text or ""):
            return "Eolico offshore"
        return "Eolico" if WIND_RE.search(text or "") else None
