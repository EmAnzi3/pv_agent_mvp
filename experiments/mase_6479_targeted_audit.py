"""Targeted source-document semantic audit for ambiguous MASE facts. Read-only."""
from __future__ import annotations
import json,re,requests,fitz
from pathlib import Path
TARGETS={1126263:[4],1126166:[15,16,17],1126229:[1,2,3,4,5,6],
         1126259:[173,188],1126189:[77],1126191:[1]}
NEEDLES=r"(?i)(?:coordina|UTM|WGS|latitudine|longitudine|cabina|proponente|cronoprogramma|durata|mesi|inizio|commission|esercizio|P\.IVA|PEC|110928|12 mesi|sedici)"
report={"source":"MASE VIP 12863","audit":[],"errors":[]}
for docid,pages in TARGETS.items():
    url=f"https://va.mite.gov.it/File/Documento/{docid}"
    try:
        res=requests.get(url,timeout=80);res.raise_for_status()
        d=fitz.open(stream=res.content,filetype="pdf")
        for p in pages:
            if p>len(d):continue
            lines=(d[p-1].get_text() or "").splitlines()
            matches=[]
            for n,line in enumerate(lines):
                if re.search(NEEDLES,line):
                    matches.append({"line":n+1,"context":" ".join(lines[max(0,n-2):min(len(lines),n+3)])[:550]})
            report["audit"].append({"url":url,"page":p,"line_hits":matches[:100],
                "page_total_lines":len(lines)})
        d.close()
    except Exception as e:report["errors"].append({"url":url,"error":str(e)[:200]})
out=Path("reports/crawl4ai/mase-6479-targeted-audit.json");out.parent.mkdir(parents=True,exist_ok=True)
out.write_text(json.dumps(report,ensure_ascii=False,indent=2),encoding="utf-8")
print("AUDIT="+json.dumps({"pages":len(report["audit"]),"errors":report["errors"],
 "line_hits":sum(len(x["line_hits"]) for x in report["audit"])},ensure_ascii=False))
for entry in report["audit"]:
 print("SOURCE_CONTEXT="+json.dumps({"url":entry["url"],"page":entry["page"],
   "matches":entry["line_hits"][:12]},ensure_ascii=False))
