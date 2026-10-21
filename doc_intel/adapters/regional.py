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
             re.compile(r"/viavasweb/[a-zA-Z]+/scaricaAllegato\w*/\d+/\d+"), re.compile(r"/dettaglio/(\d+)")),
    # Basilicata: 'Allegati' list of PDF/ZIP on the project page (HTTP only, reachable from Italian networks)
    HostRule("valutazioneambientale.regione.basilicata.it", "Basilicata",
             re.compile(r"(?i)\.(?:pdf|zip|p7m|rar|7z)(?:$|[?#])|/files/|download|getfile|allegat"),
             re.compile(r"[?&]id=(\d+)")),
    # Sistema Puglia: one determination per record, '[Scarica .pdf]' link
    HostRule("www.sistema.puglia.it", "sistema_puglia_energia",
             re.compile(r"(?i)\.pdf(?:$|[?#])|/pls/portal/docs/|download|getfile"), re.compile(r"[?&]id=(\d+)")),
]


COUNT_ONLY = re.compile(r"^\s*[↴⤵↓]?\s*(\d{1,4})\s+document[io]\s*$", re.I)


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

    SECTION_COUNT = re.compile(r"^\s*(?P<name>[^\d↴]{3,80}?)\s*[↴⤵↓]?\s*(?P<n>\d{1,4})\s+document[io]\s*$", re.I)

    def discover(self, identity: ProjectIdentity) -> Discovery:
        """Sections are the blocks announced as '<Name> ↴ N documenti' (declared totals);
        each attachment belongs to the nearest preceding section header."""
        rule = self._rule(identity.info_url)
        soup = BeautifulSoup(identity.raw.pop("html", "") or self.get_html(identity.info_url), "html.parser")
        headers = []
        for el in soup.find_all(["h1", "h2", "h3", "h4", "h5", "div", "p", "span", "a", "legend"]):
            txt = el.get_text(" ", strip=True)
            m = self.SECTION_COUNT.match(txt)
            if m and not any(h[0] in el.parents for h in headers):
                headers.append((el, " ".join(m.group("name").split()).strip(" :"), int(m.group("n"))))
        if not headers:
            # layout where the count is a separate text node: '<a>Progetto Iniziale</a> <i/> 95 documenti'
            for node in soup.find_all(string=COUNT_ONLY):
                m = COUNT_ONLY.search(node)
                name_node = node.find_previous(string=lambda t: t and t.strip() and not COUNT_ONLY.search(t)
                                               and t.strip() not in ("↴", "⤵", "↓"))
                if name_node is not None and node.parent is not None:
                    headers.append((node.parent, " ".join(name_node.split())[:80], int(m.group(1))))
        header_names = {id(h[0]): (h[1], h[2]) for h in headers}
        docs: dict[str, DocumentRef] = {}
        declared: dict[str, int] = {name: n for _, name, n in headers}
        for a in soup.find_all("a", href=True):
            href = urljoin(identity.info_url, a["href"]).split("#")[0]
            if not rule.doc_link.search(href):
                continue
            section = "record"
            for prev in a.find_all_previous():
                if id(prev) in header_names:
                    section = header_names[id(prev)][0]
                    break
            row = a.find_parent("tr")
            cells = [c.get_text(" ", strip=True) for c in row.find_all("td")] if row else []
            date = next((c for c in cells if re.fullmatch(r"\d{2}/\d{2}/\d{4}", c)), None)
            iso = "-".join(reversed(date.split("/"))) if date else None
            docs.setdefault(href, DocumentRef(url=href, procedure_id=section, title=(a.get_text(" ", strip=True) or "")[:200],
                                              source=rule.source, extra={"index_date": iso, "cells": cells[:5]}))
        counts: dict[str, int] = {}
        for d in docs.values():
            counts[d.procedure_id] = counts.get(d.procedure_id, 0) + 1
        names = list(dict.fromkeys(list(declared) + list(counts)))
        procedures = [{"id": k, "url": identity.info_url, "label": k, "declared": declared.get(k),
                       "declared_available": k in declared, "enumerated": counts.get(k, 0), "index_pages": 1,
                       "errors": []} for k in names]
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
                mod = resp.findtext(f".//{DAV}getlastmodified")
                if is_dir:
                    if rel.rstrip("/") != path.rstrip("/"):
                        todo.append(rel if rel.endswith("/") else rel + "/")
                else:
                    files.append((rel, int(size) if size and size.isdigit() else None, _http_date(mod)))
        return sorted(set(files))

    def discover(self, identity: ProjectIdentity) -> Discovery:
        base, token = identity.raw["dav_base"], identity.raw["token"]
        try:
            files = self.list_tree(base, token)
        except Exception as exc:
            return Discovery(identity, [], [], [f"WebDAV listing failed: {exc}"[:200]])
        docs = []
        for rel, size, mod in files:
            folder = rel.rsplit("/", 1)[0] or "/"
            docs.append(DocumentRef(url=base + quote(rel), procedure_id=folder, title=rel.rsplit("/", 1)[-1],
                                    source="veneto", extra={"size": size, "auth": [token, ""], "index_date": mod}))
        folders = {}
        for d in docs:
            folders[d.procedure_id] = folders.get(d.procedure_id, 0) + 1
        procedures = [{"id": k, "url": identity.info_url, "label": k, "declared": None, "declared_available": False,
                       "enumerated": v, "index_pages": 1, "errors": []} for k, v in sorted(folders.items())]
        return Discovery(identity, docs, procedures, [])


class BoxSharedFolderAdapter(DocumentAdapter):
    """Public Box shared folders (Regione Lazio ``regionelazio.app.box.com/v/<vanity>``).

    The folder page embeds its listing as JSON (``Box.postStreamData``); items are
    files (``f_<id>``) or sub-folders (``d_<id>``). Files are downloaded through the
    public shared-file endpoint. Pages beyond the first are read with ``?page=N``.
    """
    source = "box_shared"
    capabilities = AdapterCapabilities(
        source="box_shared", project_identity=True, procedure_ids=False, document_index=True,
        pagination="query_param", download="http", declared_totals=True,
        obstacles=["listing embedded as JSON in the page; Box may change the page format"], status="partial")

    VANITY_RE = re.compile(r"^(?P<root>https?://[^/]+\.box\.com)/v/(?P<vanity>[^/?#]+)")
    STREAM_RE = re.compile(r"Box\.postStreamData\s*=\s*(\{.*?\});\s*</script>", re.S)

    @classmethod
    def handles(cls, url: str) -> bool:
        return bool(cls.VANITY_RE.match(url))

    def identify(self, url: str) -> ProjectIdentity:
        m = self.VANITY_RE.match(url)
        ident = ProjectIdentity(source="lazio", project_id=m.group("vanity"), info_url=url, title=m.group("vanity"))
        ident.raw.update({"root": m.group("root"), "vanity": m.group("vanity")})
        return ident

    @classmethod
    def parse_stream(cls, html: str) -> dict:
        import json
        m = cls.STREAM_RE.search(html)
        if not m:
            return {}
        try:
            return json.loads(m.group(1))
        except ValueError:
            return {}

    @staticmethod
    def _items(data) -> tuple[list[dict], int | None, int | None]:
        """Walk the stream JSON; return items with typedID plus pageCount/totalCount when present."""
        items, page_count, total = [], None, None
        stack = [data]
        while stack:
            cur = stack.pop()
            if isinstance(cur, dict):
                if isinstance(cur.get("typedID"), str) and cur.get("name"):
                    items.append(cur)
                    continue
                page_count = page_count or cur.get("pageCount")
                total = total or cur.get("totalCount") or cur.get("itemCount")
                stack.extend(cur.values())
            elif isinstance(cur, list):
                stack.extend(cur)
        return items, page_count, total

    MAX_REQUESTS = 400
    MAX_SECONDS = 600

    def _list_folder(self, url: str, path: str, docs: list, procs: list, root: str, vanity: str,
                     state: dict, depth: int = 0, parent_ids: tuple = ()):
        import time
        seen_pages, page, total = 0, 1, None
        found = listed = 0
        last_ids = None
        errors = []
        while True:
            if state["requests"] >= self.MAX_REQUESTS or time.time() - state["t0"] > self.MAX_SECONDS:
                errors.append("listing budget exhausted (requests/time): folder partially listed")
                state["truncated"] = True
                break
            state["requests"] += 1
            html = self.get_html(url + (f"?page={page}" if page > 1 else ""))
            items, page_count, tot = self._items(self.parse_stream(html))
            ids = tuple(sorted(it["typedID"] for it in items))
            self.log(f"[box] {path} page {page}/{page_count or '?'}: {len(items)} items "
                     f"(requests {state['requests']}, {time.time() - state['t0']:.0f}s)")
            if ids and ids == last_ids:
                break  # the server ignores ?page=N: stop instead of looping
            if ids and ids == parent_ids:
                # either the item is the folder itself (Box lists the current folder in the stream)
                # or the sub-folder URL format is not honoured: after 3 such cases, stop recursing
                state["parent_echo"] = state.get("parent_echo", 0) + 1
                if state["parent_echo"] >= 3:
                    state["bad_folder_url"] = True
                return
            first_page_ids = ids if page == 1 else None
            last_ids = ids
            total = total or tot
            seen_pages += 1
            for it in items:
                tid = it["typedID"]
                listed += 1
                if tid.startswith("f_"):
                    docs.append(DocumentRef(
                        url=f"{root}/index.php?rm=box_download_shared_file&vanity_name={vanity}&file_id={tid}",
                        procedure_id=path, title=it["name"], source="lazio",
                        extra={"size": it.get("itemSize") or it.get("size"), "box_id": tid,
                               "index_date": _epoch_date(it.get("contentUpdated") or it.get("date") or it.get("modified"))}))
                    found += 1
                elif tid.startswith("d_") and depth < 4 and tid not in state["visited"] and not state.get("bad_folder_url"):
                    state["visited"].add(tid)
                    self._list_folder(f"{root}/v/{vanity}/folder/{tid[2:]}", f"{path.rstrip('/')}/{it['name']}",
                                      docs, procs, root, vanity, state, depth + 1, first_page_ids or ids)
            if not page_count or page >= page_count or not items or page >= 200:
                break
            page += 1
        procs.append({"id": path, "url": url, "label": path, "declared": total, "declared_available": total is not None,
                      "enumerated": listed, "files": found, "index_pages": seen_pages, "errors": errors})

    def discover(self, identity: ProjectIdentity) -> Discovery:
        import time
        docs: list[DocumentRef] = []
        procs: list[dict] = []
        root, vanity = identity.raw["root"], identity.raw["vanity"]
        state = {"requests": 0, "t0": time.time(), "visited": set()}
        try:
            self._list_folder(identity.info_url, "/", docs, procs, root, vanity, state)
        except Exception as exc:
            return Discovery(identity, docs, procs, [f"Box listing failed: {exc}"[:200]])
        errors = [e for p in procs for e in p["errors"]]
        if state.get("bad_folder_url"):
            errors.append("sub-folder URLs return the parent listing: folder URL format not supported, "
                          "sub-folders not listed")
        if not docs:
            errors.append("no files found in the embedded listing (page format may have changed)")
        return Discovery(identity, docs, procs, errors)


def _http_date(v: str | None) -> str | None:
    """'Tue, 20 Oct 2026 08:00:00 GMT' -> '2026-10-20' (upload date in the regional repository)."""
    if not v:
        return None
    from email.utils import parsedate_to_datetime
    try:
        return parsedate_to_datetime(v).date().isoformat()
    except (TypeError, ValueError):
        return None


def _epoch_date(v) -> str | None:
    from datetime import datetime, timezone
    try:
        x = float(v)
    except (TypeError, ValueError):
        return None
    if x > 1e11:
        x /= 1000                                    # milliseconds
    if not 9.0e8 < x < 4.0e9:
        return None
    return datetime.fromtimestamp(x, timezone.utc).date().isoformat()
