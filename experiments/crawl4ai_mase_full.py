"""MASE 6479 full-index/ZIP/document field benchmark, isolated and review-only."""
import argparse
import asyncio
import hashlib
import io
import json
import re
import zipfile
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import urljoin, urlparse

import requests
from bs4 import BeautifulSoup
from pypdf import PdfReader
from playwright.async_api import async_playwright
from crawl4ai import AsyncWebCrawler, CrawlerRunConfig

ROOT = "https://va.mite.gov.it"
PROCEDURE = ROOT + "/it-IT/Comunicazione/DettaglioUltimiProvvedimenti/6479"
INDEX = ROOT + "/it-IT/Oggetti/Documentazione/11109/16669"
FIELDS = {
 "vat": r"(?i)(?:P\.?\s*IVA|partita\s+iva)\s*[:n°.\-]*\s*(\d{11})",
 "email": r"(?i)\b[A-Z0-9._%+\-]+@[A-Z0-9.\-]+\.[A-Z]{2,}\b",
 "pec": r"(?i)\b[A-Z0-9._%+\-]+@[A-Z0-9.\-]*pec[A-Z0-9.\-]*\.[A-Z]{2,}\b",
 "phone": r"(?i)(?:tel(?:efono)?\.?|cell(?:ulare)?\.?)\s*[:.]?\s*(?:\+39\s*)?[0-9 ()/-]{7,18}",
 "company": r"(?i)\b[\wÀ-ÿ][\wÀ-ÿ '.&-]{3,70}\s+(?:S\.?\s*R\.?\s*L\.?|S\.?\s*P\.?\s*A\.?)\b",
 "power": r"(?i)\b\d{1,5}(?:[.,]\d+)?\s*(?:MWp|MWac|MW|kWp|kW)\b",
 "coordinates": r"(?i)(?:latitudine|longitudine|coordinate|UTM|WGS\s*84|EPSG|est|nord)\b",
 "schedule": r"(?i)(?:cronoprogramma|inizio lavori|fine lavori|durata lavori|mesi|entrata in esercizio|COD)\b",
 "technology": r"(?i)\b(?:agrivoltaic[oa]|fotovoltaic[oa]|BESS|accumulo|eolic[oa]|onshore|offshore)\b",
 "role": r"(?i)\b(?:proponente|progettista|committente|EPC|BoP|costruttore|direttore lavori|impresa esecutrice)\b",
}
def clean_url(href, base):
    url = urljoin(base, href or "").split("#")[0]
    p = urlparse(url)
    return url if p.scheme == "https" and p.hostname in {"va.mite.gov.it", "va.mase.gov.it"} else None

def pdf_records(data, url, label, limit=6000):
    """Only cited candidates; avoid inventing company-role joins or dates."""
    reader = PdfReader(io.BytesIO(data))
    findings = []
    blank = 0
    for page_no, page in enumerate(reader.pages, 1):
        raw = page.extract_text() or ""
        if not raw.strip():
            blank += 1
        for line_no, line in enumerate(raw.splitlines(), 1):
            if not line.strip():
                continue
            for field, pattern in FIELDS.items():
                if re.search(pattern, line):
                    if len(findings) < limit:
                        findings.append({"field":field,"text":line.strip()[:320], "page":page_no,
                                         "line":line_no,"source_url":url,"member":label,
                                         "status":"candidate_unverified"})
    return {"pages":len(reader.pages), "blank_pages":blank, "findings":findings}

async def inventory(max_index_pages):
    """Browser keeps site state for numbered pagination; inventory is collected BEFORE downloads."""
    result = {"pages":[], "documents":{}, "errors":[]}
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        page = await browser.new_page()
        try:
            await page.goto(INDEX, wait_until="domcontentloaded", timeout=60000)
            await page.wait_for_timeout(800)
            # Page 1 and subsequent numbered pages. Never assume JS table is static.
            for number in range(1, max_index_pages + 1):
                if number > 1:
                    possible = page.locator("a").filter(has_text=re.compile(r"^\s*"+str(number)+r"\s*$"))
                    if await possible.count():
                        await possible.first.click(timeout=12000)
                        await page.wait_for_timeout(950)
                    else:
                        anchors = await page.locator("a").all()
                        navigation = []
                        for anchor in anchors:
                            title = (await anchor.inner_text()).strip()
                            href = await anchor.get_attribute("href")
                            if title in ("»", "›", ">", ">>", "ultima", "Ultima") or (title.isdigit() and 7 <= int(title) <= 15):
                                navigation.append({"text":title,"href":href})
                        result["errors"].append({"index_page":number,"error":"numbered link unavailable", "pager_links":navigation[-25:]})
                        break
                html = await page.content()
                soup = BeautifulSoup(html, "html.parser")
                visible_text = soup.get_text(" ", strip=True)
                match = re.search(r"\(\s*n\.?\s*(\d+)\s*\)\s*Documenti", visible_text, re.I)
                if match:
                    result["declared_total"] = int(match.group(1))
                links = []
                for anchor in soup.find_all("a", href=True):
                    link = clean_url(anchor["href"], INDEX)
                    if link and re.search(r"/File/(?:Documento|Provvedimento)/\d+", link, re.I):
                        links.append({"url":link,"title":anchor.get_text(" ",strip=True)[:140]})
                        result["documents"].setdefault(link, {"url":link, "first_index_page":number,
                                                               "title":anchor.get_text(" ",strip=True)[:140]})
                result["pages"].append({"number":number,"count":len(set(x["url"] for x in links)),
                                        "document_ids":sorted(set(x["url"] for x in links))})
                if number>1 and result["pages"][-1]["document_ids"] == result["pages"][-2]["document_ids"]:
                    result["errors"].append({"index_page":number,"error":"pagination did not change document list"})
                    break
        finally:
            await browser.close()
    return result

def classify_download(data, headers):
    if data.startswith(b"%PDF"):
        return "pdf"
    if zipfile.is_zipfile(io.BytesIO(data)):
        return "zip"
    return "other"

def extract_docs(index, limit, max_bytes):
    report = {"processed":[], "failures":[], "candidates":[], "skipped":[]}
    session = requests.Session()
    docs = [{"url":ROOT+"/File/Provvedimento/13107","title":"Decreto VIA ZIP", "first_index_page":0}] + list(index["documents"].values())
    # Regulatory documents and technical reports before incidental low-value material.
    docs.sort(key=lambda r:(0 if re.search(r"decreto|provvedimento|relazione|tecnic|cronoprogramma|connessione|elaborat",r["title"],re.I) else 1,r["first_index_page"]))
    for item in docs[:limit]:
        url = item["url"]
        try:
            response = session.get(url, timeout=60)
            response.raise_for_status()
            raw = response.content
            if len(raw)>max_bytes:
                raise ValueError("download exceeds max bytes")
            kind = classify_download(raw,response.headers)
            entry = {"url":url,"kind":kind,"bytes":len(raw),"sha256":hashlib.sha256(raw).hexdigest()}
            if kind == "pdf":
                parts = [(url.rsplit("/",1)[-1]+".pdf",raw)]
            elif kind == "zip":
                parts=[]
                with zipfile.ZipFile(io.BytesIO(raw)) as archive:
                    for z in archive.infolist():
                        if z.is_dir(): continue
                        if z.file_size>max_bytes or len(parts)>=50: continue
                        if z.filename.lower().endswith(".pdf"):
                            content=archive.read(z)
                            if content.startswith(b"%PDF"): parts.append((z.filename,content))
                entry["zip_pdf_members"]=len(parts)
            else:
                raise ValueError("unsupported content signature; mime="+response.headers.get("Content-Type",""))
            entry["files"]=[]
            for label,content in parts:
                parsed=pdf_records(content,url,label)
                entry["files"].append({"name":label,"pages":parsed["pages"],"blank_pages":parsed["blank_pages"],
                                       "sha256":hashlib.sha256(content).hexdigest(),"hits":len(parsed["findings"])})
                report["candidates"].extend(parsed["findings"])
            report["processed"].append(entry)
        except Exception as exc:
            report["failures"].append({"url":url,"error":str(exc)[:350]})
    report["skipped"]= [r["url"] for r in docs[limit:]]
    return report

async def main():
    parser=argparse.ArgumentParser()
    parser.add_argument("--max-index-pages",type=int,default=14)
    parser.add_argument("--max-docs",type=int,default=15)
    parser.add_argument("--max-bytes",type=int,default=30000000)
    parser.add_argument("--output",default="reports/crawl4ai/mase-6479-full-benchmark.json")
    args=parser.parse_args()
    if not 1<=args.max_index_pages<=30 or not 1<=args.max_docs<=120: parser.error("invalid limits")
    # Explicitly exercise Crawl4AI on the landing page, alongside browser-driven pagination.
    async with AsyncWebCrawler() as crawler:
        crawled=await crawler.arun(url=PROCEDURE,config=CrawlerRunConfig(page_timeout=45000))
    baseline={"ok":bool(crawled.success),"chars":len(str(crawled.markdown or ""))}
    index=await inventory(args.max_index_pages)
    detail=extract_docs(index,args.max_docs,args.max_bytes)
    declared=index.get("declared_total")
    report={"generated":datetime.now(timezone.utc).isoformat(),"record":"Troia Sun / 6479 / 11109",
            "crawl4ai_landing":baseline,"index":index,"documents":detail,
            "coverage":{"declared":declared,"unique_index_urls":len(index["documents"]),
                        "index_pages":len(index["pages"]),"processed":len(detail["processed"]),
                        "failures":len(detail["failures"]),"skipped":len(detail["skipped"]),
                        "candidates_unverified":len(detail["candidates"])},
            "decision":"incomplete" if declared!=len(index["documents"]) or detail["failures"] or detail["skipped"] else "needs_evidence_review"}
    path=Path(args.output);path.parent.mkdir(parents=True,exist_ok=True)
    path.write_text(json.dumps(report,indent=2,ensure_ascii=False),encoding="utf-8")
    print("COVERAGE_REPORT="+json.dumps(report["coverage"],ensure_ascii=False))
    print("FAILURES="+json.dumps(detail["failures"][:8],ensure_ascii=False))
    print("INDEX_PAGES="+json.dumps([{"page":p["number"],"links":p["count"]} for p in index["pages"]]))
    print("INDEX_ERRORS="+json.dumps(index["errors"],ensure_ascii=False))
    print("FIELD_HITS="+json.dumps(dict(Counter(x["field"] for x in detail["candidates"]))))
    print("DOCUMENT_TYPES="+json.dumps(dict(Counter(x["kind"] for x in detail["processed"]))))
    print("EVIDENCE_SAMPLES="+json.dumps([x for x in detail["candidates"] if x["field"] in ("vat","email","phone","coordinates","schedule")][:12],ensure_ascii=False))
    print("DECISION="+report["decision"])

if __name__=="__main__":
    asyncio.run(main())
