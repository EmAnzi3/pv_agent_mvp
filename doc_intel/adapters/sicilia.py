"""Regione Siciliana SI-VVI (si-vvi.regione.sicilia.it/viavas), Joomla + Fabrik lists. Reachable from Italian IPs only.

Record page: list 30 filtered by ``procedura___oggetto_raw=<id>``: one table row with procedure code, proponent,
type, procedure, status and a link to the documents (list 28, ``integrazioni___id_integrazioni_raw=<code>``).
Documents page: one row per file (Tipo documento, Descrizione, Download link
``index.php?option=com_fabrik&task=plugin.pluginAjax&plugin=fileupload&method=ajax_download...&rowid=N``).
Structure read from the portal on 21/10/2026 (local exploration report).
"""
from __future__ import annotations

import re
from urllib.parse import urljoin

from bs4 import BeautifulSoup

from ..schema import DocumentRef
from .base import AdapterCapabilities, Discovery, DocumentAdapter, ProjectIdentity

HOST = "si-vvi.regione.sicilia.it"
DOCS_LINK = re.compile(r"/component/fabrik/list/28/.*integrazioni___id_integrazioni_raw=(\d+)")
DOWNLOAD = re.compile(r"method=ajax_download.*rowid=(\d+)")


class SiciliaViaAdapter(DocumentAdapter):
    source = "sicilia"
    capabilities = AdapterCapabilities(source="sicilia", project_identity=True, procedure_ids=True, document_index=True,
                                       pagination="query_param", download="http", declared_totals=False,
                                       obstacles=["portal refuses foreign IPs: run from an Italian network (local runner)"],
                                       status="supported")

    @staticmethod
    def handles(url: str) -> bool:
        return HOST in (url or "") and "procedura___oggetto_raw=" in url

    def identify(self, url: str) -> ProjectIdentity:
        html = self.get_html(url)
        soup = BeautifulSoup(html, "html.parser")
        text = soup.get_text(" ", strip=True)
        pid = re.search(r"procedura___oggetto_raw=(\d+)", url).group(1)
        tm = re.search(r"Oggetto\s+(.+?)\s+Cod\.\s*Proc\.", text)
        title = tm.group(1).strip() if tm else None
        a = soup.find("a", href=DOCS_LINK)
        cells, code, docs_url = [], None, None
        if a:
            docs_url = urljoin(url, a["href"])
            code = DOCS_LINK.search(docs_url).group(1)
            row = a.find_parent("tr")
            cells = [td.get_text(" ", strip=True) for td in row.find_all("td")] if row else []
        proponent = cells[1] if len(cells) > 1 else None
        munis, prov = [], None
        mm = re.search(r"(?i)\bcomun[ei]\s+d[ie]\s+([A-ZÀ-Ýa-zà-ÿ' ]+?)\s*\(([A-Z]{2})\)", title or "")
        if mm:
            munis, prov = [mm.group(1).strip().title()], mm.group(2)
        ident = ProjectIdentity(source="sicilia", project_id=pid, info_url=url, title=title, proponent=proponent,
                                municipalities=munis, province=prov, region="Sicilia",
                                procedures=[{"id": code or pid, "url": docs_url, "label": " · ".join(cells[2:5])}] if docs_url else [])
        ident.raw.update({"procedure_code": code, "row": cells,
                          "municipalities_citation": {"url": url, "excerpt": (title or "")[:300]}})
        return ident

    def discover(self, identity: ProjectIdentity, max_pages: int = 40) -> Discovery:
        docs, procs, errors = [], [], []
        for proc in identity.procedures:
            found: dict[str, DocumentRef] = {}
            info = {"id": proc["id"], "url": proc["url"], "label": proc.get("label"), "declared": None,
                    "enumerated": 0, "index_pages": 0, "errors": []}
            base = re.sub(r"[&?]limitstart28=\d+", "", proc["url"])
            step, start = None, 0
            for n in range(max_pages):
                # Fabrik honours limit28 on most lists; if not, pages advance by the size of the first page
                page_url = base + ("&" if "?" in base else "?") + f"limit28=200&limitstart28={start}"
                try:
                    soup = BeautifulSoup(self.get_html(page_url), "html.parser")
                except Exception as exc:
                    info["errors"].append(f"page {n + 1}: {exc}"[:200])
                    break
                new = 0
                for a in soup.find_all("a", href=DOWNLOAD):
                    href = urljoin(page_url, a["href"])
                    if href in found:
                        continue
                    row = a.find_parent("tr")
                    cells = [td.get_text(" ", strip=True) for td in row.find_all("td")] if row else []
                    title = " - ".join(c for c in cells if c and c.lower() != "download")[:200] or "documento"
                    found[href] = DocumentRef(url=href, procedure_id=proc["id"], title=title, source="sicilia",
                                              extra={"kind": cells[0] if cells else None})
                    new += 1
                info["index_pages"] = n + 1
                step = step or new
                if not new or new < step or step >= 200:
                    break
                start += step
            info["enumerated"] = len(found)
            docs.extend(found.values())
            procs.append(info)
        if not identity.procedures:
            errors.append("no documents link on the SI-VVI record page")
        return Discovery(identity, docs, procs, errors)
