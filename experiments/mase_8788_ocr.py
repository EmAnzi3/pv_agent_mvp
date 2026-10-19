"""Review-only OCR of MASE 8788 PDFs with absent digital text."""
import argparse,requests,fitz,subprocess,json,hashlib
from datetime import datetime,timezone
from pathlib import Path
IDS=[1439048,1439049,1439050,1439051,1077136,1439052,706409,1439026,706408,
706410,756119,805855,1439025,1439037,1439004,1439017,717773,1439000,756120,
1439005,1439015,706411,1439042,1438993,805874,729740,1018317,717766,735725,
756118,756121,805845,706443,1439009,1439010]
def main():
 p=argparse.ArgumentParser();p.add_argument("--shard",type=int,required=True);p.add_argument("--shards",type=int,default=7);a=p.parse_args()
 report={"shard":a.shard,"timestamp":datetime.now(timezone.utc).isoformat(),"files":[],"errors":[],"candidates":[]}
 for ident in IDS[a.shard::a.shards]:
  url=f"https://va.mite.gov.it/File/Documento/{ident}"
  try:
   r=requests.get(url,timeout=120);r.raise_for_status()
   doc=fitz.open(stream=r.content,filetype="pdf")
   item={"url":url,"sha256":hashlib.sha256(r.content).hexdigest(),"pages":len(doc),"ocr_pages":0,"digital_pages":0,"failed_pages":[],"ocr_text_characters":0}
   for pnum,page in enumerate(doc,1):
    raw=page.get_text("text")
    if len(raw.strip())>=70:item["digital_pages"]+=1;continue
    pix=page.get_pixmap(matrix=fitz.Matrix(1.3,1.3),alpha=False)
    proc=subprocess.run(["tesseract","stdin","stdout","-l","ita+eng","--psm","3"],input=pix.tobytes("png"),capture_output=True,timeout=100)
    if proc.returncode:
     item["failed_pages"].append(pnum);continue
    txt=proc.stdout.decode(errors="replace")
    item["ocr_pages"]+=1;item["ocr_text_characters"]+=len(txt)
    if len(report["candidates"])<9000:
     for line in txt.splitlines():
      if any(x in line.lower() for x in ("coordinate","wgs","cronoprogramma","p.iva","@","partita iva","committente","progettista","mw","inizio lavori")):
       report["candidates"].append({"url":url,"page":pnum,"excerpt":line.strip()[:280],"verified":False})
   report["files"].append(item);doc.close()
   print("OCR_FILE="+json.dumps(item),flush=True)
  except Exception as e:report["errors"].append({"url":url,"error":str(e)[:250]})
 path=Path(f"reports/crawl4ai/mase-8788-ocr-{a.shard}.json");path.parent.mkdir(parents=True,exist_ok=True)
 path.write_text(json.dumps(report,ensure_ascii=False,indent=2),encoding="utf8")
 print("OCR_SHARD="+json.dumps({"shard":a.shard,"files":len(report["files"]),"errors":report["errors"],
  "ocr_pages":sum(x["ocr_pages"] for x in report["files"]),"failed_pages":sum(len(x["failed_pages"]) for x in report["files"])}))
if __name__=="__main__":main()
