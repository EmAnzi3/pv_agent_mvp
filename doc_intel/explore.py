"""Portal exploration for writing new adapters: what a record page links to and which APIs it calls.

For each URL: HTTP status and title, the links with their text, and (with a browser) every XHR/fetch
response with the start of its body, so the document listing endpoint of a single-page app can be
found without guessing. Optional one-hop follow of links whose text suggests documents.
Output is a JSON report; nothing is downloaded beyond the pages themselves.
"""
from __future__ import annotations

import json
import re
from typing import Any
from urllib.parse import urljoin, urlparse

DOC_TEXT = re.compile(r"(?i)document|allegat|elaborat|download|scarica|file|deposit|integrazion|istanza|progetto|pdf|zip")


def _links(html: str, base: str) -> list[dict]:
    from bs4 import BeautifulSoup
    soup = BeautifulSoup(html, "html.parser")
    out = []
    for a in soup.find_all("a", href=True):
        href = urljoin(base, a["href"])
        if href.startswith(("javascript:", "mailto:")):
            continue
        out.append({"text": a.get_text(" ", strip=True)[:80], "href": href[:300]})
    for f in soup.find_all(["iframe", "form"]):
        src = f.get("src") or f.get("action")
        if src:
            out.append({"text": f"<{f.name}>", "href": urljoin(base, src)[:300]})
    return out


def explore(url: str, render: bool = True, follow: int = 4, timeout: int = 45) -> dict[str, Any]:
    import requests
    rep: dict[str, Any] = {"url": url}
    try:
        r = requests.get(url, timeout=(15, timeout), headers={"User-Agent": "Mozilla/5.0 (PV Agent explorer)"})
        rep.update({"status": r.status_code, "final_url": r.url, "content_type": r.headers.get("content-type")})
        html = r.text
        m = re.search(r"(?is)<title>(.*?)</title>", html)
        rep["title"] = (m.group(1).strip() if m else None)
        links = _links(html, r.url)
        rep["links"] = links[:250]
        rep["text_head"] = re.sub(r"\s+", " ", re.sub(r"(?is)<script.*?</script>|<style.*?</style>|<[^>]+>", " ", html))[:8000]
        # template lines that build document links (Angular ng-href/ng-click, onclick, form actions)
        rep["html_hits"] = [ln.strip()[:300] for ln in html.splitlines()
                            if re.search(r"(?i)(ng-href|ng-click|onclick|href|action)=.*(download|file|allegat|document|getDoc|\.pdf)", ln)][:120]
        if html[:1].strip() in ("{", "[") and len(html) < 600_000:
            rep["json_body"] = html
        # map / GIS hooks (viewer links, WMS/WFS/ArcGIS endpoints, layer configs)
        rep["map_hits"] = sorted({m.strip()[:300] for m in re.findall(
            r"[^\n]{0,160}(?:mappa|viewer|wfs|wms|MapServer|FeatureServer|geoserver|config_[a-z]+\.json|geometr)[^\n]{0,160}",
            html, flags=re.I)})[:150]
    except Exception as exc:
        rep["error"] = str(exc)[:300]
        links = []
    if render:
        rep["xhr"] = _render(url, timeout)
    hops = [l for l in links if DOC_TEXT.search(l["text"] + " " + l["href"]) and urlparse(l["href"]).netloc][:follow]
    rep["followed"] = []
    for l in hops:
        try:
            rr = requests.get(l["href"], timeout=(15, timeout), headers={"User-Agent": "Mozilla/5.0 (PV Agent explorer)"})
            ct = rr.headers.get("content-type", "")
            item = {"from_text": l["text"], "url": l["href"], "status": rr.status_code, "content_type": ct}
            if "html" in ct:
                item["links"] = _links(rr.text, rr.url)[:120]
            else:
                item["bytes"] = len(rr.content)
            rep["followed"].append(item)
        except Exception as exc:
            rep["followed"].append({"url": l["href"], "error": str(exc)[:200]})
    return rep


def _render(url: str, timeout: int) -> list[dict] | dict:
    try:
        from playwright.sync_api import sync_playwright
    except ImportError:
        return {"error": "playwright not installed"}
    calls: list[dict] = []
    try:
        with sync_playwright() as p:
            b = p.chromium.launch()
            page = b.new_page()

            def on_response(resp):
                rt = resp.request.resource_type
                if rt == "script":
                    # lines of the app code that build download / document URLs
                    try:
                        body = resp.text()
                    except Exception:
                        return
                    hits = [ln.strip()[:300] for ln in body.splitlines()
                            if re.search(r"(?i)download|scarica|edma|allegat|document|getFile|\.html\?", ln)]
                    hits += sorted({m[:300] for m in re.findall(
                        r".{0,140}(?:wfs|wms|MapServer|FeatureServer|geoserver|config_read|getFeature|idProgetto|codProcedura).{0,140}", body, flags=re.I)})[:80]
                    if hits:
                        calls.append({"script": resp.url[:300], "lines": hits[:150]})
                    return
                if rt not in ("xhr", "fetch", "document"):
                    return
                item = {"url": resp.url[:400], "status": resp.status, "type": resp.headers.get("content-type", ""),
                        "method": resp.request.method, "post": (resp.request.post_data or "")[:300]}
                try:
                    body = resp.text()
                    item["body_head"] = body[:2500]
                    if body[:1].strip() in ("{", "[") and len(body) < 600_000:
                        item["json_body"] = body           # full API answers: the document lists live here
                    item["body_len"] = len(body)
                except Exception:
                    pass
                calls.append(item)
            page.on("response", on_response)
            page.goto(url, wait_until="networkidle", timeout=timeout * 1000)
            page.wait_for_timeout(3000)
            # links that exist only after rendering (single-page apps)
            rendered = page.eval_on_selector_all("a[href], button", "els => els.slice(0, 300).map(e => [e.innerText.slice(0, 80), e.href || e.getAttribute('ng-click') || e.getAttribute('onclick') || ''])")
            calls.append({"rendered_links": [r for r in rendered if r[0].strip() or r[1]][:300]})
            b.close()
    except Exception as exc:
        calls.append({"error": str(exc)[:300]})
    return calls


def main(urls: list[str], out: str, render: bool = True) -> None:
    reports = [explore(u, render=render) for u in urls]
    with open(out, "w", encoding="utf-8") as fh:
        json.dump(reports, fh, ensure_ascii=False, indent=1)
