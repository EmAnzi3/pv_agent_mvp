"""MASE (va.mite.gov.it) adapter — generic, no hard-coded project or document IDs.

Entry points accepted: ``/Oggetti/Info/<id>`` and
``/Comunicazione/DettaglioUltimiProvvedimenti/<n>`` (resolved to its Info page).
"""
from __future__ import annotations

import re
import time
from urllib.parse import urljoin, urlparse

from bs4 import BeautifulSoup

from ..schema import DocumentRef
from .base import AdapterCapabilities, Discovery, DocumentAdapter, ProjectIdentity

HOSTS = {"va.mite.gov.it", "va.mase.gov.it", "va.minambiente.it"}
ROOT = "https://va.mite.gov.it"
DECLARED_RE = re.compile(r"\(\s*n\.?\s*(\d+)\s*\)\s*Document", re.I)
DOC_RE = re.compile(r"/File/(?:Documento|Provvedimento)/\d+", re.I)
MUNI_TITLE_RE = re.compile(r"(?i)\bComun[ei]\s+di\s+(?P<list>.+?\([A-Z]{2}\)|[^,.(]+?)(?=\s*[.;]|\s*,?\s*in\s+localit|\s*,?\s*e\s+relativ|\s*,\s*(?:con|comprensiv|per|nonch|ove)|\s+-\s|\s*$)")
NAME_RE = re.compile(r"^[A-ZÀ-Ýa-zà-ÿ][A-Za-zÀ-ÿ'’\- ]{1,40}$")
REGIONS = ["Abruzzo", "Basilicata", "Calabria", "Campania", "Emilia-Romagna", "Friuli-Venezia Giulia", "Lazio",
           "Liguria", "Lombardia", "Marche", "Molise", "Piemonte", "Puglia", "Sardegna", "Sicilia", "Toscana",
           "Trentino-Alto Adige", "Umbria", "Valle d'Aosta", "Veneto"]
REGION_RE = re.compile(r"\bRegione\s*:?\s*(" + "|".join(re.escape(r) for r in REGIONS) + r")\b", re.I)
PROV_RE = re.compile(r"\(([A-Z]{2})\)")


def _host_ok(url: str) -> bool:
    return urlparse(url).hostname in HOSTS


def _clean(url: str) -> str:
    return url.split("#")[0].rstrip("/")


class MaseAdapter(DocumentAdapter):
    source = "mase"
    capabilities = AdapterCapabilities(
        source="mase", project_identity=True, procedure_ids=True, document_index=True,
        pagination="query_param", download="http", declared_totals=True,
        obstacles=["index pages may require browser rendering if HTTP is blocked",
                   "large drawings (>100 MB) need byte cap tuning"],
        status="supported")

    @staticmethod
    def handles(url: str) -> bool:
        return _host_ok(url)

    # ------------------------------------------------------------ identity
    def identify(self, url: str) -> ProjectIdentity:
        url = _clean(url)
        if "/DettaglioUltimiProvvedimenti/" in url:
            html = self.get_html(url)
            soup = BeautifulSoup(html, "html.parser")
            info = None
            for a in soup.find_all("a", href=True):
                href = urljoin(url, a["href"])
                if re.search(r"/Oggetti/Info/\d+", href) and _host_ok(href):
                    info = _clean(href)
                    break
            if not info:
                raise LookupError(f"provvedimento page without project link: {url}")
            ident = self.identify(info)
            ident.raw["entry_url"] = url
            ident.raw["provvedimento_documents"] = sorted({_clean(urljoin(url, a["href"])) for a in soup.find_all("a", href=True)
                                                            if DOC_RE.search(a["href"])})
            return ident
        m = re.search(r"/Oggetti/Info/(\d+)", url)
        if not m:
            raise LookupError(f"not a MASE project URL: {url}")
        pid = m.group(1)
        ident = None
        for attempt in range(3):
            html = self.get_html(url)
            ident = self.parse_info(html, url, pid)
            if ident.procedures or (ident.proponent and ident.title and "VAS - VIA - AIA" not in ident.title):
                break
            # the portal sometimes answers with its generic home page: wait and ask again
            self.log(f"project page {url} returned no project data (attempt {attempt + 1}/3)")
            time.sleep(20 * (attempt + 1))
        if not ident.procedures and not ident.proponent:
            snippet = re.sub(r"\s+", " ", BeautifulSoup(html, "html.parser").get_text(" ", strip=True))[:600]
            print(f"::warning title=mase-page::{url} returned a page without project data: {snippet}")
            raise LookupError(f"MASE project page not available (generic page returned): {url}")
        if not ident.procedures:
            soup = BeautifulSoup(html, "html.parser")
            ident.raw["info_links"] = [(a.get_text(" ", strip=True)[:60], a["href"][:160])
                                       for a in soup.find_all("a", href=True)
                                       if re.search(r"(?i)document|procedur|oggetti|provvedim", a["href"])][:40]
        return ident

    def parse_info(self, html: str, url: str, pid: str) -> ProjectIdentity:
        soup = BeautifulSoup(html, "html.parser")
        text = soup.get_text(" ", strip=True)
        title = None
        for sel in ("h1", "h2", ".titolo", "title"):
            node = soup.select_one(sel)
            if node and len(node.get_text(strip=True)) > 25:
                title = node.get_text(" ", strip=True)
                break
        om = re.search(r"Oggetto\s*:?\s*(.+?)(?:\s+Proponente\s*:|\s+Tipologia|\s+-\s+Prescrizion|\s+Data\s+presentazione|$)", text)
        if om and len(om.group(1)) > 25:
            title = om.group(1).strip()
        if title:
            title = re.split(r"\s+-\s+Prescrizion|\s+Data\s+presentazione", title)[0].strip()
        prop = None
        pm = re.search(r"\bProponente\s*:\s*(.+?)(?:\s+Tipologia\s+di\s+opera|\s+Scadenza|\s+Territori|\s+Procedura|\s+Data\s+presentazione|\s+Oggetto\s*:|$)", text)
        if pm:
            prop = pm.group(1).strip()
        munis, provs = [], []
        if title:
            mm = MUNI_TITLE_RE.search(title)
            if mm:
                for part in re.split(r",|\s+e\s+", mm.group("list")):
                    pr = PROV_RE.search(part)
                    name = PROV_RE.sub("", part).strip(" .;")
                    if name and NAME_RE.match(name):
                        munis.append(name.title())
                    if pr:
                        provs.append(pr.group(1))
        procedures = []
        seen = set()
        for a in soup.find_all("a", href=True):
            href = _clean(urljoin(url, a["href"]))
            pm2 = re.search(r"/Oggetti/Documentazione/(\d+)/(\d+)$", href)
            # menu links to other objects are ignored: object id must be this project
            if pm2 and pm2.group(1) == pid and href not in seen:
                seen.add(href)
                row = a.find_parent("tr")
                label = (row.get_text(" ", strip=True) if row else a.get_text(" ", strip=True))[:200]
                procedures.append({"id": pm2.group(2), "url": href, "label": label})
        rm = {m.group(1).lower() for m in REGION_RE.finditer(text)}
        region = next((r for r in REGIONS if r.lower() in rm), None) if len(rm) == 1 else None
        ident = ProjectIdentity(source="mase", project_id=pid, info_url=url, title=title, proponent=prop,
                                region=region,
                                municipalities=munis, province=provs[0] if len(set(provs)) == 1 else None,
                                procedures=procedures)
        ident.raw["municipalities_citation"] = {"url": url, "excerpt": (title or "")[:300]}
        return ident

    # ------------------------------------------------------------ documents
    def discover(self, identity: ProjectIdentity, max_pages: int = 80) -> Discovery:
        docs: list[DocumentRef] = []
        procs = []
        errors: list[str] = []
        seen_urls: set[str] = set()
        for proc in identity.procedures:
            info = {"id": proc["id"], "url": proc["url"], "label": proc.get("label"), "declared": None,
                    "enumerated": 0, "index_pages": 0, "errors": [], "duplicates_across_procedures": 0}
            found: dict[str, DocumentRef] = {}
            last_page = None
            for n in range(1, max_pages + 1):
                page_url = proc["url"] + (f"?pagina={n}" if n > 1 else "")
                try:
                    html = self.get_html(page_url)
                except Exception as exc:
                    info["errors"].append(f"page {n}: {exc}"[:200])
                    break
                page_docs, declared, pager_max = self.parse_index(html, page_url, proc["id"], n)
                if declared is not None:
                    info["declared"] = declared
                if pager_max:
                    last_page = max(last_page or 0, pager_max)
                new = [d for d in page_docs if d.url not in found]
                if not page_docs or (not new and n > 1):
                    break
                for d in new:
                    found[d.url] = d
                info["index_pages"] = n
                if info["declared"] and len(found) >= info["declared"]:
                    break
                if last_page and n >= last_page:
                    break
            info["enumerated"] = len(found)
            info["pager_last_page"] = last_page
            for d in found.values():
                if d.url in seen_urls:
                    info["duplicates_across_procedures"] += 1
                    d.extra["also_in_other_procedure"] = True
                seen_urls.add(d.url)
                docs.append(d)
            procs.append(info)
        for u in identity.raw.get("provvedimento_documents", []):
            if u not in seen_urls:
                docs.append(DocumentRef(url=u, procedure_id="provvedimento", title="Provvedimento",
                                        declared_in_index=False))
                seen_urls.add(u)
        if not identity.procedures:
            errors.append("no documentation procedures linked from the project page")
        return Discovery(identity, docs, procs, errors)

    def parse_index(self, html: str, page_url: str, proc_id: str, page_no: int):
        soup = BeautifulSoup(html, "html.parser")
        text = soup.get_text(" ", strip=True)
        dm = DECLARED_RE.search(text)
        declared = int(dm.group(1)) if dm else None
        pages = [int(x) for x in re.findall(r"[?&]pagina=(\d+)", html)]
        out = []
        for a in soup.find_all("a", href=True):
            href = _clean(urljoin(page_url, a["href"]))
            if not (DOC_RE.search(href) and _host_ok(href)):
                continue
            row = a.find_parent("tr")
            cells = [c.get_text(" ", strip=True) for c in row.find_all("td")] if row else []
            title = next((c for c in cells if len(c) > 8 and not re.fullmatch(r"[\d/.\-\s]+", c)), None) \
                or a.get_text(" ", strip=True) or a.get("title")
            date = next((c for c in cells if re.fullmatch(r"\d{2}/\d{2}/\d{4}", c)), None)
            iso = None
            if date:
                d, m, y = date.split("/")
                iso = f"{y}-{m}-{d}"
            out.append(DocumentRef(url=href, procedure_id=proc_id, title=(title or "")[:200], index_page=page_no,
                                   extra={"index_date": iso, "cells": cells[:8]}))
        # one document may have several anchors in the same row
        uniq = {}
        for d in out:
            uniq.setdefault(d.url, d)
        return list(uniq.values()), declared, (max(pages) if pages else None)
