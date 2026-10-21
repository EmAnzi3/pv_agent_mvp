"""Recovery of MASE PDFs that defeated pypdf or exceeded 30 MB; review-only."""
from __future__ import annotations
import argparse, hashlib, json, re
from pathlib import Path
from datetime import datetime, timezone
import requests
import fitz
BAD_IDS = [1126169,1126171,1126175,1126218,1126219,1126220,1126224,1126226,
           1126236,1126237,1126249,1126255]
KEYWORDS = {
 "company":r"(?i)\b(?:s\.r\.l\.|s\.p\.a\.|società|progettista|appaltatore|proponente)\b",
 "contact":r"(?i)(?:partita\s*iva|p\.?\s*iva|PEC|telefono|@)",
 "technical":r"(?i)\b(?:MWp|MW|fotovoltaic[oa]|agrivoltaic[oa]|BESS|accumulo|RTN)\b",
 "location":r"(?i)\b(?:coordinate|WGS84|UTM|EPSG|Comune|località|latitudine|longitudine)\b",
 "schedule":r"(?i)\b(?:cronoprogramma|inizio\s+lavori|fine\s+lavori|entrata\s+in\s+esercizio|durata)\b"
}
def main():
    ap=argparse.ArgumentParser()
    ap.add_argument("--out",default="reports/crawl4ai/mase-6479-recovery.json")
    ap.add_argument("--max-mb",type=int,default=150)
    args=ap.parse_args()
    out={"generated_utc":datetime.now(timezone.utc).isoformat(),"total":len(BAD_IDS),
         "files":[],"failed":[],"evidence_candidates":[]}
    for ident in BAD_IDS:
        url=f"https://va.mite.gov.it/File/Documento/{ident}"
        try:
            with requests.get(url,timeout=90,stream=True) as resp:
                resp.raise_for_status()
                blocks=[];length=0
                for block in resp.iter_content(1024*1024):
                    if not block:continue
                    length+=len(block)
                    if length>args.max_mb*1024*1024:raise ValueError("Download exceeds configured maximum")
                    blocks.append(block)
            data=b"".join(blocks)
            if not data.startswith(b"%PDF"):raise ValueError("File signature not PDF")
            document=fitz.open(stream=data,filetype="pdf")
            pages_total=len(document); pages_text=0;hits=0
            for pageno,page in enumerate(document,1):
                raw=page.get_text("text") or ""
                if raw.strip():pages_text+=1
                for line in raw.splitlines():
                    fields=[field for field,pattern in KEYWORDS.items() if re.search(pattern,line)]
                    if not fields:continue
                    hits+=1
                    if len(out["evidence_candidates"])<25000:
                        out["evidence_candidates"].append({"url":url,"page":pageno,
                            "field_tags":fields,"excerpt":line.strip()[:330],"status":"unverified"})
            out["files"].append({"url":url,"bytes":len(data),"sha256_original":hashlib.sha256(data).hexdigest(),
                "pages":pages_total,"pages_with_text":pages_text,"hits":hits,
                "scanned_or_unextractable_pages":pages_total-pages_text})
            document.close()
            print(f"RECOVERED {ident} pages={pages_total} text={pages_text} bytes={len(data)}",flush=True)
        except Exception as exc:
            out["failed"].append({"url":url,"error":str(exc)[:400]})
            print(f"FAILED {ident}: {exc}",flush=True)
    p=Path(args.out);p.parent.mkdir(parents=True,exist_ok=True)
    p.write_text(json.dumps(out,ensure_ascii=False,indent=2),encoding="utf-8")
    print("RECOVERY_SUMMARY="+json.dumps({"recovered":len(out["files"]),"failed":len(out["failed"]),
        "pages":sum(x["pages"] for x in out["files"]),
        "text_pages":sum(x["pages_with_text"] for x in out["files"]),
        "errors":out["failed"]},ensure_ascii=False))
if __name__=="__main__":main()
