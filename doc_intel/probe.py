"""Phase 5 probe: one real sample per regional source, to choose the acquisition method.

For each source present in the canonical dataset the probe picks one record whose
URL identifies a single procedure, fetches it with plain HTTP and (optionally) a
rendered browser, and reports: reachability, page type, candidate document links,
pagination, file signatures of the first downloads and obstacles. It never writes
to the canonical dataset. The result decides which adapter pattern fits the source.
"""
from __future__ import annotations

import json
import re
from collections import Counter, defaultdict
from typing import Any, Callable
from urllib.parse import urljoin, urlparse

from .fetch import DEFAULT_UA, sniff

EXCLUDED_SOURCES = {"mase", "mase_provvedimenti", "terna_econnextion"}
DOC_HINT = re.compile(r"(?i)\.(pdf|zip|p7m|rar|7z|kmz|dwg|docx?|xlsx?)(?:$|[?#])|/download|scarica|allegat|/File/|"
                      r"getfile|downloadfile|attachment|documento|elaborat|/s/[A-Za-z0-9]{8,}|box\.com/(?:s|v)/")
PAGER_HINT = re.compile(r"(?i)[?&](?:page|pagina|start|limitstart\d*|offset|p)=\d+|pag(?:ina)?\s*\d+\s*di\s*\d+|›|»|next|successiv")


def pick_samples(records: list[dict]) -> dict[str, dict]:
    """One record per source, preferring a URL that is not shared with other records of the same source."""
    by_src: dict[str, list[dict]] = defaultdict(list)
    for r in records:
        src = r.get("source") or ""
        if src and src not in EXCLUDED_SOURCES and isinstance(r.get("url"), str):
            by_src[src].append(r)
    out = {}
    for src, rows in sorted(by_src.items()):
        counts = Counter(r["url"] for r in rows)
        unique = [r for r in rows if counts[r["url"]] == 1]
        pick = sorted(unique or rows, key=lambda r: (-(r.get("power_mw") or 0), r["url"]))[0]
        out[src] = {"source": src, "records": len(rows), "distinct_urls": len(counts),
                    "url_identifies_procedure": bool(unique), "url": pick["url"],
                    "title": (pick.get("title") or "")[:160], "project_key": pick.get("project_key")}
    return out


def classify_links(html: str, base: str) -> dict[str, Any]:
    from bs4 import BeautifulSoup
    soup = BeautifulSoup(html, "html.parser")
    host = urlparse(base).hostname
    docs, pagers, external = [], [], Counter()
    for a in soup.find_all("a", href=True):
        href = urljoin(base, a["href"]).split("#")[0]
        if not href.startswith("http"):
            continue
        text = a.get_text(" ", strip=True)[:120]
        h = urlparse(href).hostname
        if h != host:
            external[h] += 1
        if DOC_HINT.search(href) or DOC_HINT.search(text):
            docs.append({"url": href, "text": text})
        elif PAGER_HINT.search(href) or PAGER_HINT.search(text):
            pagers.append({"url": href, "text": text})
    seen, uniq = set(), []
    for d in docs:
        if d["url"] not in seen:
            seen.add(d["url"])
            uniq.append(d)
    text = soup.get_text(" ", strip=True)
    return {"title": (soup.title.get_text(strip=True) if soup.title else "")[:150],
            "text_chars": len(text), "anchors": len(soup.find_all("a")),
            "doc_links": uniq[:60], "doc_link_count": len(uniq), "pager_links": pagers[:10],
            "external_hosts": external.most_common(8), "forms": len(soup.find_all("form")),
            "iframes": [urljoin(base, f.get("src", "")) for f in soup.find_all("iframe")][:5],
            "js_app_hint": bool(re.search(r"(?i)<app-root|ng-version|__NEXT_DATA__|id=\"root\"|id=\"app\"", html)),
            "snippet": text[:400]}


FOLLOW_HINT = re.compile(r"(?i)allegat|document|elaborat|procediment|pratica|progetto|dettaglio|scheda|istanza|va\.mite|"
                         r"valutazion|sharing|box\.com|download")


def _robust_get(session):
    import time
    def get(u):
        last = None
        for attempt in range(3):
            try:
                return session.get(u, timeout=90, allow_redirects=True)
            except Exception as exc:  # classify later; retry transient errors
                last = exc
                time.sleep(5 * (attempt + 1))
        raise last
    return get


def error_class(msg: str) -> str:
    m = msg.lower()
    if "name or service not known" in m or "nodename" in m or "getaddrinfo" in m:
        return "dns"
    if "timed out" in m or "timeout" in m:
        return "timeout"
    if "refused" in m or "reset" in m or "max retries" in m:
        return "connection_refused_or_filtered"
    if "ssl" in m or "certificate" in m:
        return "tls"
    return "other"


def probe_url(url: str, get: Callable | None = None, render: Callable[[str], Any] | None = None,
              sample_downloads: int = 3, follow: int = 4) -> dict[str, Any]:
    import requests
    session = requests.Session()
    session.headers["User-Agent"] = DEFAULT_UA
    get = get or _robust_get(session)
    out: dict[str, Any] = {"url": url, "http": None, "rendered": None, "downloads": [], "obstacles": []}
    html = None
    try:
        r = get(url)
        out["http"] = {"status": r.status_code, "final_url": r.url, "content_type": r.headers.get("Content-Type"),
                       "bytes": len(r.content)}
        if r.status_code < 400:
            kind = sniff(r.content)
            out["http"]["kind"] = kind
            if kind == "html" or "html" in (r.headers.get("Content-Type") or ""):
                html = r.text
                out["http"].update(classify_links(html, r.url))
    except Exception as exc:
        out["http"] = {"error": str(exc)[:300], "error_class": error_class(str(exc))}
        out["obstacles"].append(f"HTTP failed after 3 attempts ({error_class(str(exc))}): " + str(exc)[:120])
    if render:
        try:
            rendered = render(url)
            rhtml, xhr = (rendered if isinstance(rendered, tuple) else (rendered, []))
            out["rendered"] = classify_links(rhtml, url)
            out["rendered"]["xhr"] = xhr[:25]
        except Exception as exc:
            out["rendered"] = {"error": str(exc)[:200], "error_class": error_class(str(exc))}
    # one hop: when the record page lists no documents, follow the most promising links
    base_cls = out["rendered"] if out["rendered"] and "doc_links" in out["rendered"] else out["http"]
    if base_cls and "doc_links" in base_cls and base_cls.get("doc_link_count", 0) == 0 and html:
        from bs4 import BeautifulSoup
        cands = []
        for a in BeautifulSoup(html, "html.parser").find_all("a", href=True):
            href = urljoin(url, a["href"]).split("#")[0]
            text = a.get_text(" ", strip=True)
            if href.startswith("http") and href != url and (FOLLOW_HINT.search(href) or FOLLOW_HINT.search(text)):
                cands.append((href, text[:80]))
        out["followed"] = []
        for href, text in list(dict.fromkeys(cands))[:follow]:
            try:
                r = get(href)
                kind = sniff(r.content)
                info = {"url": href, "text": text, "status": r.status_code, "kind": kind}
                if kind == "html":
                    c = classify_links(r.text, r.url)
                    info.update({"doc_link_count": c["doc_link_count"], "doc_links": c["doc_links"][:5],
                                 "title": c["title"]})
                out["followed"].append(info)
            except Exception as exc:
                out["followed"].append({"url": href, "error": str(exc)[:120]})
    best = out["rendered"] if (out["rendered"] and out["rendered"].get("doc_link_count", 0) >
                               (out["http"] or {}).get("doc_link_count", 0)) else out["http"]
    for d in (best or {}).get("doc_links", [])[:sample_downloads]:
        try:
            r = get(d["url"])
            raw = r.content[:2_000_000]
            out["downloads"].append({"url": d["url"], "status": r.status_code, "kind": sniff(raw),
                                     "content_type": r.headers.get("Content-Type"), "bytes": len(r.content),
                                     "disposition": r.headers.get("Content-Disposition")})
        except Exception as exc:
            out["downloads"].append({"url": d["url"], "error": str(exc)[:160]})
    http_docs = (out["http"] or {}).get("doc_link_count", 0)
    ren_docs = (out["rendered"] or {}).get("doc_link_count", 0) if out["rendered"] else 0
    if out["http"] and out["http"].get("js_app_hint") and http_docs == 0:
        out["obstacles"].append("single-page app: documents only after browser rendering")
    if ren_docs > http_docs:
        out["obstacles"].append(f"rendering reveals {ren_docs - http_docs} more document links than HTTP")
    out["verdict"] = verdict(out)
    return out


def verdict(p: dict[str, Any]) -> dict[str, str]:
    kinds = [d.get("kind") for d in p["downloads"] if d.get("kind")]
    http = p.get("http") or {}
    ren = p.get("rendered") or {}
    if http.get("error") and (not ren or ren.get("error")):
        return {"status": "unreachable_from_ci", "method": "none",
                "why": f"HTTP {http.get('error_class')} and browser {ren.get('error_class', 'n/a')}: retry from an Italian network before calling it blocked"}
    if ren.get("xhr") and not ren.get("doc_link_count"):
        return {"status": "partial", "method": "spa_api", "why": f"page loads data from {len(ren['xhr'])} API calls: adapter must call the API"}
    followed_docs = sum(f.get("doc_link_count", 0) for f in p.get("followed", []) or [])
    if followed_docs:
        return {"status": "partial", "method": "one_hop_to_document_page", "why": f"{followed_docs} document links one click away"}
    if any(k in ("pdf", "zip") for k in kinds):
        method = "browser_index+http_download" if "single-page app" in " ".join(p["obstacles"]) else "http_index+http_download"
        return {"status": "supported_candidate", "method": method, "why": f"downloads verified: {Counter(kinds)}"}
    if http.get("kind") in ("pdf", "zip"):
        return {"status": "supported_candidate", "method": "direct_file", "why": "record URL is itself a document"}
    if (http.get("doc_link_count") or 0) or ((p.get("rendered") or {}).get("doc_link_count") or 0):
        return {"status": "partial", "method": "links_found_downloads_not_verified", "why": f"kinds={kinds}"}
    return {"status": "partial", "method": "no_document_links_on_record_page",
            "why": "record page lists no documents: needs a source-specific navigation step"}


def summary_line(src: str, sample: dict, res: dict) -> dict:
    http = res.get("http") or {}
    ren = res.get("rendered") or {}
    return {"source": src, "records": sample["records"], "url_identifies_procedure": sample["url_identifies_procedure"],
            "url": sample["url"], "http_status": http.get("status"), "http_kind": http.get("kind"),
            "doc_links_http": http.get("doc_link_count"), "doc_links_rendered": ren.get("doc_link_count"),
            "downloads": [(d.get("kind"), d.get("status"), d.get("bytes")) for d in res["downloads"]],
            "obstacles": res["obstacles"], "verdict": res["verdict"],
            "example_links": [d["url"] for d in (ren.get("doc_links") or http.get("doc_links") or [])[:4]],
            "pager": [p["url"] for p in (ren.get("pager_links") or http.get("pager_links") or [])[:3]],
            "external_hosts": http.get("external_hosts"), "title": http.get("title") or ren.get("title"),
            "http_error": http.get("error"), "xhr": ren.get("xhr"),
            "followed": [{k: f.get(k) for k in ("url", "status", "kind", "doc_link_count", "title", "error")}
                         | {"doc_links": [d["url"] for d in f.get("doc_links", [])[:3]]} for f in res.get("followed", []) or []]}


def to_json(obj: Any) -> str:
    return json.dumps(obj, ensure_ascii=False, default=str)
