#!/usr/bin/env python3
"""Enumerate an official MASE project's attachments without pretending to read them."""
import argparse
import csv
from datetime import datetime, timezone
from html.parser import HTMLParser
import json
import re
from pathlib import Path
from urllib.parse import urljoin, urlencode, urlsplit, parse_qsl, urlunsplit
import requests

def clean(x):
    return re.sub(r"\s+", " ", x).strip()

class Parser(HTMLParser):
    def __init__(self):
        super().__init__(convert_charrefs=True)
        self.text=[];self.links=[];self.rows=[];self.cells=None;self.cell=None;self.rowlinks=[];self.hidden=0
    def handle_starttag(self,tag,attrs):
        if tag in ("script","style"):self.hidden+=1
        if tag=="tr":self.cells=[];self.rowlinks=[]
        if tag in ("td","th") and self.cells is not None:self.cell=[]
        if tag=="a":
            href=dict(attrs).get("href")
            if href:
                self.links.append(href)
                if self.cells is not None:self.rowlinks.append(href)
    def handle_endtag(self,tag):
        if tag in ("script","style"):self.hidden=max(0,self.hidden-1)
        if tag in ("td","th") and self.cell is not None:
            self.cells.append(clean(" ".join(self.cell)));self.cell=None
        if tag=="tr" and self.cells is not None:
            self.rows.append((self.cells,self.rowlinks));self.cells=None
    def handle_data(self,s):
        if not self.hidden:
            self.text.append(s)
            if self.cell is not None:self.cell.append(s)

def validate(url):
    p=urlsplit(url)
    if p.scheme!="https" or p.hostname not in ("va.mite.gov.it","va.mase.gov.it") or p.username or p.password or p.port not in (None,443):
        raise ValueError("Non-public/non-MASE URL rejected")
    return url

def page_url(root,n):
    p=urlsplit(root)
    items=[(k,v) for k,v in parse_qsl(p.query) if k!="pagina"]
    return urlunsplit((p.scheme,p.netloc,p.path,urlencode(items+[("pagina",str(n))]),""))

def parse(body,root):
    p=Parser();p.feed(body.decode("utf-8-sig",errors="replace"))
    bodytext=clean(" ".join(p.text))
    count=re.search(r"\(n\.?\s*([\d.]+)\)\s*Documenti",bodytext,re.I)
    page=re.search(r"Pagina\s+(\d+)\s+di\s+(\d+)",bodytext,re.I)
    if not count or not page:raise ValueError("Index counters absent or access blocked")
    expected=int(count.group(1).replace(".",""))
    cur,total=int(page.group(1)),int(page.group(2))
    if not (1<=cur<=total<=300):raise ValueError("Invalid pagination counters")
    rows=[]
    for cells,links in p.rows:
        for href in links:
            if not re.search(r"/File/Documento/\d+|\.pdf(?:$|[?#])",href,re.I):continue
            if len(cells)<7:raise ValueError("Incomplete MASE document row")
            url=validate(urljoin(root,href))
            rows.append(dict(url=url,title=cells[0],filename=cells[1],section=cells[2],
                             code=cells[3],published=cells[4],scale=cells[5],size=cells[6]))
    if total>1 and not any("pagina=" in x and urlsplit(urljoin(root,x)).path==urlsplit(root).path for x in p.links):
        raise ValueError("Pagination links missing")
    return dict(expected=expected,page=cur,pages=total,rows=rows)

def get(session,url):
    url=validate(url)
    for i in range(5):
        r=session.get(url,timeout=(8,25),allow_redirects=False,headers={"User-Agent":"WindRadarMASE/1.0"})
        if r.status_code in (301,302,303,307,308):
            new=r.headers.get("Location");r.close()
            if not new:raise ValueError("Redirect without Location")
            url=validate(urljoin(url,new));continue
        r.raise_for_status()
        data=r.content;r.close()
        return data
    raise ValueError("Redirect limit")

def inventory(session,spec):
    root=validate(spec["url"])
    result=dict(id=spec["id"],url=root,expected=None,pages_declared=None,
                pages_visited=[],rows=[],complete=False,error=None)
    try:
        first=parse(get(session,root),root)
        if first["page"]!=1:raise ValueError("First index page is not 1")
        result["expected"]=first["expected"];result["pages_declared"]=first["pages"]
        result["rows"].extend(first["rows"]);result["pages_visited"].append(1)
        for n in range(2,first["pages"]+1):
            data=parse(get(session,page_url(root,n)),page_url(root,n))
            if (data["page"],data["pages"],data["expected"])!=(n,first["pages"],first["expected"]):
                raise ValueError("Inconsistent page counters on page %s"%n)
            result["rows"].extend(data["rows"]);result["pages_visited"].append(n)
        urls=[r["url"] for r in result["rows"]]
        if len(urls)!=len(set(urls)):raise ValueError("Repeated attachment URLs")
        if len(urls)!=first["expected"]:raise ValueError("Rows %s != declared %s"%(len(urls),first["expected"]))
        result["complete"]=True
    except Exception as e:
        result["error"]=str(e)[:300]
    return result

def run(config,out,offline=False):
    c=json.loads(Path(config).read_text(encoding="utf-8"))
    out=Path(out);out.mkdir(parents=True,exist_ok=True)
    f=out/"inventory.json"
    if offline:
        if not f.is_file():raise ValueError("Offline run needs prior inventory")
        report=json.loads(f.read_text(encoding="utf-8"))
        if report["project_id"]!=c["project_id"]:raise ValueError("Wrong project memory")
        identities=lambda procedures:[(str(p['id']),validate(p['url'])) for p in procedures]
        configured=identities(c['procedures']);cached=identities(report['procedures'])
        if len(set(configured))!=len(configured) or sorted(configured)!=sorted(cached):
            raise ValueError('Cached procedure IDs/URLs differ from configuration')
        if report.get('object_id')!=c['mase_object_id']:
            raise ValueError('Wrong cached MASE object')
        fetched=0
    else:
        with requests.Session() as client:
            procedures=[]
            for spec in c["procedures"]:
                procedures.append(inventory(client,spec))
                (out/"progress.json").write_text(json.dumps(procedures,ensure_ascii=False,indent=2),encoding="utf-8")
        report=dict(project_id=c["project_id"],object_id=c["mase_object_id"],
                    checked_at=datetime.now(timezone.utc).isoformat(),procedures=procedures,
                    extra_procedures_uninventoried=c["extra_procedures_uninventoried"])
        f.write_text(json.dumps(report,ensure_ascii=False,indent=2),encoding="utf-8")
        fetched=sum(len(x["pages_visited"]) for x in procedures)
    rows=[]
    for p in report["procedures"]:
        for item in p["rows"]:rows.append(dict(procedure=p["id"],**item))
    if not offline:
        with (out/"documents.csv").open("w",newline="",encoding="utf-8-sig") as dst:
            writer=csv.DictWriter(dst,fieldnames=["procedure","url","title","filename","section","code","published","scale","size"])
            writer.writeheader();writer.writerows(rows)
    summary=dict(project_id=c["project_id"],known_procedures=len(c["procedures"]),
                 indexed_procedures=sum(bool(p["complete"]) for p in report["procedures"]),
                 indexed_references=len(rows),unique_urls=len({r["url"] for r in rows}),
                 index_pages_fetched=fetched,acquired_files=0,analyzed_files=0,
                 complete_project_dossier=False,extra_procedures_uninventoried=report["extra_procedures_uninventoried"],
                 procedures=[{k:v for k,v in p.items() if k!="rows"} for p in report["procedures"]])
    (out/("summary-offline.json" if offline else "summary.json")).write_text(json.dumps(summary,ensure_ascii=False,indent=2),encoding="utf-8")
    return summary

if __name__=="__main__":
    ap=argparse.ArgumentParser()
    ap.add_argument("--config",required=True);ap.add_argument("--output",required=True);ap.add_argument("--offline",action="store_true")
    a=ap.parse_args()
    result=run(a.config,a.output,a.offline)
    print(json.dumps(result,ensure_ascii=False,indent=2))
    if result["indexed_procedures"] != result["known_procedures"]:raise SystemExit(2)
