"""Review-only Crawl4AI experiment on an EXISTING MASE record from docs/data.json.

Does not alter the canonical dataset or write database records.
Install separately: pip install crawl4ai pypdf requests; crawl4ai-setup.
"""
from __future__ import annotations
import argparse
import asyncio
import hashlib
import io
import json
import re
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import urljoin, urlparse, urldefrag

FIELDS = {
    "power": r"\b\d{1,4}(?:[.,]\d+)?\s*MW[p]?\b",
    "proponent": r"\bproponent[ei]\b",
    "designer": r"\bprogettist[ai]\b",
    "grid_connection": r"\b(?:connessione|cabina primaria|stazione elettrica|RTN)\b",
    "storage": r"\b(?:BESS|accumulo|storage)\b",
    "authorization": r"\b(?:VIA|provvedimento|autorizzazione|decreto)\b",
    "works": r"\b(?:cronoprogramma|inizio lavori|fine lavori|cantiere)\b",
}
MASE_HOSTS = ("va.mite.gov.it", "va.minambiente.it", "va.mase.gov.it")
URL_KEYS = ("source_url", "url", "detail_url", "project_url", "link")
SOURCE_KEYS = ("source", "source_name", "source_id", "fonte", "authority")

def choose_existing_mase_record(data, title_filter="", exact_url=""):
    records = data.get("records")
    if not isinstance(records, list):
        raise ValueError("docs/data.json: records must be an array")
    matches = []
    exact_url = exact_url.rstrip("/")
    for row in records:
        if not isinstance(row, dict):
            continue
        urls = [str(row.get(k) or "").strip() for k in URL_KEYS]
        # Other datasets may use a list of source links.
        if isinstance(row.get("sources"), list):
            for source in row["sources"]:
                if isinstance(source, dict):
                    urls.extend(str(source.get(k) or "").strip() for k in URL_KEYS)
        url = next((u for u in urls if urlparse(u).hostname in MASE_HOSTS and urlparse(u).scheme == "https"), None)
        if exact_url:
            candidate_urls = set(re.findall(r"https?://[^\s\\\"'<>]+", json.dumps(row, ensure_ascii=False)))
            candidate_urls.update(urls)
            if not any(u.rstrip("/").rstrip(".,)") == exact_url for u in candidate_urls):
                continue
            url = exact_url
        if not url:
            continue
        title = str(row.get("title") or row.get("project_name") or "")
        if title_filter and title_filter.casefold() not in title.casefold():
            continue
        matches.append((title.casefold(), url, row))
    if not matches:
        raise ValueError("No existing record with a direct HTTPS MASE URL; no fallback to unrelated projects")
    _, url, row = sorted(matches, key=lambda x: (x[0], x[1]))[0]
    return row, url

def snippets(text, url, kind, page=None):
    result = []
    for n, line in enumerate(text.splitlines(), 1):
        for field, regex in FIELDS.items():
            if re.search(regex, line, flags=re.I):
                result.append({"field": field, "excerpt": line.strip()[:450], "url": url,
                               "source_kind": kind, "extracted_line": n, "pdf_page": page,
                               "verification": "unverified_candidate"})
    return result

def safe_url(value, base):
    url, _ = urldefrag(urljoin(base, value or ""))
    p = urlparse(url)
    return url if p.scheme == "https" and p.hostname in MASE_HOSTS else None

async def analyze(record, url, max_pdfs, out):
    from crawl4ai import AsyncWebCrawler, CrawlerRunConfig
    report = {
        "status": "review_only", "generated_utc": datetime.now(timezone.utc).isoformat(),
        "existing_record": {k:record.get(k) for k in ("id", "title", "project_name", "source", "external_id", "power_mw")},
        "mase_url": url, "html": {}, "pdfs": [], "unprocessed_pdf_links": [],
        "findings": [], "errors": [],
        "warning": "No canonical updates. Matches are not validated facts or verified corporate roles."
    }
    discovered = []
    project_links = []
    report["link_inventory"] = []
    try:
        async with AsyncWebCrawler() as crawler:
            to_visit = [(url, "provvedimento")]
            visited = set()
            while to_visit and len(visited) < 3:
                page_url, kind = to_visit.pop(0)
                if page_url in visited:
                    continue
                visited.add(page_url)
                result = await crawler.arun(url=page_url, config=CrawlerRunConfig(page_timeout=45000))
                if not result.success:
                    raise RuntimeError(str(result.error_message))
                text = str(result.markdown or "")
                report["html"][kind] = {"url": page_url, "sha256_extracted_text": hashlib.sha256(text.encode()).hexdigest(),
                                        "length": len(text)}
                report["findings"].extend(snippets(text, page_url, "html"))
                for group in (result.links or {}).values():
                    for item in group:
                        href = safe_url(item.get("href"), page_url)
                        if not href:
                            continue
                        label = str(item.get("text") or item.get("title") or "").strip()
                        report["link_inventory"].append({"from":page_url, "text":label[:120], "url":href})
                        path = urlparse(href).path.lower()
                        if ("vai al progetto" in label.lower() or "/oggetti/info/" in path) and href not in project_links:
                            project_links.append(href)
                        if (path.endswith(".pdf") or "scarica il provvedimento" in label.lower() or
                            "/documenti/" in path or "/download/" in path or
                            "/file/" in path) and href not in discovered:
                            discovered.append(href)
                if kind == "provvedimento" and project_links:
                    to_visit.append((project_links[0], "progetto"))
                elif kind == "progetto":
                    docs = [item["url"] for item in report["link_inventory"]
                            if item["from"] == page_url and "/oggetti/documentazione/" in item["url"].lower()]
                    if docs:
                        to_visit.append((docs[0], "documentazione"))
    except Exception as exc:
        report["errors"].append({"url": url, "stage": "crawl", "error": str(exc)[:400]})
    report["project_links"] = project_links
    report["discovered_attachments"] = discovered
    # PDF extraction is separate from Crawl4AI, with original bytes hashed and source page references.
    import requests
    from pypdf import PdfReader
    for pdf_url in discovered[:max_pdfs]:
        entry = {"url": pdf_url, "status": "failed"}
        try:
            resp = requests.get(pdf_url, timeout=35, headers={"User-Agent": "PV-Agent-Research/1.0"})
            resp.raise_for_status()
            content = resp.content
            entry["http_status"] = resp.status_code
            entry["content_type"] = resp.headers.get("Content-Type", "")
            entry["content_length"] = len(content)
            if not content.startswith(b"%PDF") or len(content) > 30_000_000:
                raise ValueError(f"Response is not a PDF or exceeds 30 MB; status={resp.status_code}, type={entry[\'content_type\']}, bytes={len(content)}")
            reader = PdfReader(io.BytesIO(content))
            entry.update({"status": "parsed", "sha256_original_pdf": hashlib.sha256(content).hexdigest(),
                          "page_count": len(reader.pages)})
            for page_num, page in enumerate(reader.pages[:40], 1):
                page_text = page.extract_text() or ""
                report["findings"].extend(snippets(page_text, pdf_url, "pdf", page_num))
            if len(reader.pages) > 40:
                entry["pages_not_read"] = len(reader.pages) - 40
        except Exception as exc:
            entry["error"] = str(exc)[:350]
            report["errors"].append({"url": pdf_url, "stage": "pdf", "error": str(exc)[:350]})
        report["pdfs"].append(entry)
    report["unprocessed_pdf_links"] = discovered[max_pdfs:]
    dest = Path(out)
    dest.parent.mkdir(parents=True, exist_ok=True)
    dest.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
    print("PILOT_DIAGNOSTICS=" + json.dumps({"project_links":report["project_links"], "discovered_attachments":report["discovered_attachments"], "pdfs":report["pdfs"], "errors":report["errors"], "link_inventory":report["link_inventory"][:80]}, ensure_ascii=False))
    print(json.dumps({"record": report["existing_record"], "url":url, "pdfs":len(report["pdfs"]),
                      "findings":len(report["findings"]), "errors":len(report["errors"]), "report":str(dest)},
                     ensure_ascii=False))

def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--data", default="docs/data.json")
    parser.add_argument("--title-contains", default="", help="Optional substring to choose a specific existing MASE record")
    parser.add_argument("--max-pdfs", type=int, default=3)
    parser.add_argument("--url", default="", help="Require an exact URL linked in an existing record")
    parser.add_argument("--out", default="reports/crawl4ai/mase-record-pilot.json")
    args = parser.parse_args()
    if args.max_pdfs < 0 or args.max_pdfs > 10:
        parser.error("--max-pdfs must be between 0 and 10")
    data = json.loads(Path(args.data).read_text(encoding="utf-8-sig"))
    record, url = choose_existing_mase_record(data, args.title_contains, args.url)
    asyncio.run(analyze(record, url, args.max_pdfs, args.out))

if __name__ == "__main__":
    main()
