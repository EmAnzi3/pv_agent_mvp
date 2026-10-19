"""Regional adapters, one per acquisition *method* (not per region).

* ``HtmlIndexAdapter``: the record page itself lists downloadable attachments
  (Emilia-Romagna VIAVASweb). Host rules say which links are documents.
* ``NextcloudShareAdapter``: the record URL is a public Nextcloud share
  (Veneto ``sharing.regione.veneto.it/index.php/s/<token>``); the full file tree
  is listed through the public WebDAV endpoint, each file downloaded individually.

Sources without a declared document count are marked ``declared_available: False``:
the acquisition gate then reports completeness against the listing itself.
"""
from __future__ import annotations

import re
import xml.etree.ElementTree as ET
from dataclasses import dataclass
from typing import Callable
from urllib.parse import quote, unquote, urljoin, urlparse

from bs4 import BeautifulSoup

from ..schema import DocumentRef
from .base import AdapterCapabilities, Discovery, DocumentAdapter, ProjectIdentity


@dataclass
class HostRule:
    host: str
    source: str
    doc_link: re.Pattern
    project_id: re.Pattern


HTML_RULES = [
    HostRule("serviziambiente.regione.emilia-romagna.it", "emilia_romagna",
             re.compile(r"/viavasweb/[a-zA-Z]+/scaricaAllegato/\d+/\d+"), re.compile(r"/dettaglio/(\d+)")),
]


class HtmlIndexAdapter(DocumentAdapter):
    source = "html_index"
    capabilities = AdapterCapabilities(
        source="html_index", project_identity=True, procedure_ids=False, document_index=True,
        pagination="none", download="http", declared_totals=False,
        obstacles=["no declared document total: completeness is measured against the page listing"],
        status="supported")

    @staticmethod
    def handles(url: str) -> bool:
        return any(urlparse(url).hostname == r.host for r in HTML_RULES)

    def _rule(self, url: str) -> HostRule:
        return next(r for r in HTML_RULES if urlparse(url).hostname == r.host)

    def identify(self, url: str) -> ProjectIdentity:
        rule = self._rule(url)
        html = self.get_html(url)
        soup = BeautifulSoup(html, "html.parser")
        m = rule.project_id.search(url)
        title = None
        for sel in ("h1", "h2", "h3", "title"):
            n = soup.select_one(sel)
            if n and len(n.get_text(strip=True)) > 15:
                title = n.get_text(" ", strip=True)[:300]
                break
        ident = ProjectIdentity(source=rule.source, project_id=m.group(1) if m else url.rsplit("/", 1)[-1],
                                info_url=url, title=title)
        ident.raw["html"] = html
        return ident

    def discover(self, identity: ProjectIdentity) -> Discovery:
        rule = self._rule(identity.info_url)
        soup = BeautifulSoup(identity.raw.pop("html", "") or self.get_html(identity.info_url), "html.parser")
        docs: dict[str, DocumentRef] = {}
        for a in soup.find_all("a", href=True):
            href = urljoin(identity.info_url, a["href"]).split("#")[0]
            if not rule.doc_link.search(href):
                continue
            row = a.find_parent(["tr", "li", "div"])
            section = a.find_previous(["h2", "h3", "h4", "legend", "caption"])
            title = a.get_text(" ", strip=True) or (row.get_text(" ", strip=True)[:150] if row else "")
            docs.setdefault(href, DocumentRef(url=href, procedure_id=(section.get_text(" ", strip=True)[:80] if section else "record"),
                                              title=title[:200], source=rule.source))
        procs = {}
        for d in docs.values():
            procs.setdefault(d.procedure_id, 0)
            procs[d.procedure_id] += 1
        procedures = [{"id": k, "url": identity.info_url, "label": k, "declared": None, "declared_available": False,
                       "enumerated": v, "index_pages": 1, "errors": []} for k, v in procs.items()]
        return Discovery(identity, list(docs.values()), procedures,
                         [] if docs else ["record page lists no attachment matching the host rule"])


DAV = "{DAV:}"


class NextcloudShareAdapter(DocumentAdapter):
    source = "nextcloud_share"
    capabilities = AdapterCapabilities(
        source="nextcloud_share", project_identity=True, procedure_ids=False, document_index=True,
        pagination="none", download="http", declared_totals=False,
        obstacles=["public WebDAV must be enabled on the share"], status="supported")

    SHARE_RE = re.compile(r"^(?P<base>https?://[^/]+(?:/[^/]+)*?)/index\.php/s/(?P<token>[A-Za-z0-9]+)")

    def __init__(self, get_html: Callable[[str], str] | None = None,
                 propfind: Callable[[str, str], str] | None = None):
        super().__init__(get_html)
        self.propfind = propfind or self._propfind

    @classmethod
    def handles(cls, url: str) -> bool:
        return bool(cls.SHARE_RE.match(url))

    def _propfind(self, url: str, token: str) -> str:
        import requests
        r = requests.request("PROPFIND", url, auth=(token, ""), headers={"Depth": "infinity"}, timeout=90)
        if r.status_code in (400, 403, 405, 501):  # some servers refuse infinity: caller falls back to depth 1
            r = requests.request("PROPFIND", url, auth=(token, ""), headers={"Depth": "1"}, timeout=90)
        r.raise_for_status()
        return r.text

    def identify(self, url: str) -> ProjectIdentity:
        m = self.SHARE_RE.match(url)
        title = None
        try:
            soup = BeautifulSoup(self.get_html(url), "html.parser")
            title = soup.title.get_text(strip=True) if soup.title else None
        except Exception:
            pass
        ident = ProjectIdentity(source="veneto", project_id=m.group("token"), info_url=url, title=title)
        ident.raw.update({"dav_base": f"{m.group('base')}/public.php/webdav", "token": m.group("token")})
        return ident

    def list_tree(self, dav_base: str, token: str) -> list[tuple[str, int | None]]:
        files, todo, seen = [], ["/"], set()
        while todo:
            path = todo.pop()
            if path in seen:
                continue
            seen.add(path)
            xml = self.propfind(dav_base + quote(path), token)
            root = ET.fromstring(xml)
            for resp in root.findall(f"{DAV}response"):
                href = unquote(resp.findtext(f"{DAV}href") or "")
                rel = href.split("/public.php/webdav", 1)[-1] or "/"
                is_dir = resp.find(f".//{DAV}collection") is not None
                size = resp.findtext(f".//{DAV}getcontentlength")
                if is_dir:
                    if rel.rstrip("/") != path.rstrip("/"):
                        todo.append(rel if rel.endswith("/") else rel + "/")
                else:
                    files.append((rel, int(size) if size and size.isdigit() else None))
        return sorted(set(files))

    def discover(self, identity: ProjectIdentity) -> Discovery:
        base, token = identity.raw["dav_base"], identity.raw["token"]
        try:
            files = self.list_tree(base, token)
        except Exception as exc:
            return Discovery(identity, [], [], [f"WebDAV listing failed: {exc}"[:200]])
        docs = []
        for rel, size in files:
            folder = rel.rsplit("/", 1)[0] or "/"
            docs.append(DocumentRef(url=base + quote(rel), procedure_id=folder, title=rel.rsplit("/", 1)[-1],
                                    source="veneto", extra={"size": size, "auth": [token, ""]}))
        folders = {}
        for d in docs:
            folders[d.procedure_id] = folders.get(d.procedure_id, 0) + 1
        procedures = [{"id": k, "url": identity.info_url, "label": k, "declared": None, "declared_available": False,
                       "enumerated": v, "index_pages": 1, "errors": []} for k, v in sorted(folders.items())]
        return Discovery(identity, docs, procedures, [])
