"""MASE 8788 full-source PDF/ZIP acquisition, isolated batches, no canon writes."""
import argparse,asyncio,io,json,re,hashlib,zipfile
from datetime import datetime,timezone
from pathlib import Path
import requests,fitz
from bs4 import BeautifulSoup
from playwright.async_api import async_playwright
ROOT="https://va.mite.gov.it"
PROCEDURES={"19438":87,"12932":124}
PATTERNS={
 "companies":r"(?i)(?:proponente|progettista|societ[aà]|s\.r\.l\.|s\.p\.a\.|appaltat|direttore lavori)",
 "contacts":r"(?i)(?:partita iva|p\.?\s*iva|@|pec|telefono|cellulare)",
 "technology":r"(?i)(?:MWp|MWac|\bMW\b|kW|agrivolta|fotovolta|eolic|bess|offshore|onshore|accumulo|turbine|inverter)",
 "geography":r"(?i)(?:coordinate|UTM|WGS84|EPSG|latitudine|longitudine|localit[aà]|particell|fogli[oa])",
 "schedule":r"(?i)(?:cronoprogramma|inizio lavori|fine lavori|durata.{0,35}mesi|entrata in esercizio|commissioning)"
}
async def inventory():
 records=[];metadata={}
 async with async_playwright() as p:
  b=await p.chromium.launch(headless=True);page=await b.new_page()
  for procedure,expected in PROCEDURES.items():
   base=f"{ROOT}/it-IT/Oggetti/Documentazione/8788/{procedure}"
   unique={}
   for num in range(1,30):
    await page.goto(base+("?pagina="+str(num) if num>1 else ""),wait_until="domcontentloaded",timeout=60000)
    soup=BeautifulSoup(await page.content(),"html.parser")
    refs=re.findall(r"https?://va\.mite\.gov\.it/File/Documento/\d+|/File/Documento/\d+",str(soup))
    refs=[x if x.startswith("http") else ROOT+x for x in refs]
    refs=list(dict.fromkeys(refs))
    if not refs:break
    before=len(unique)
    for url in refs:unique[url]=procedure
    if len(unique)==before:break
   metadata[procedure]={"expected":expected,"discovered":len(unique)}
   records.extend({"url":url,"procedure":procedure} for url in unique)
  await b.close()
 return records,metadata
def parse_pdf(raw,url,name):
 doc=fitz.open(stream=raw,filetype="pdf")
 item={"name":name,"pages":len(doc),"digital_pages":0,"candidate_count":0};hits=[]
 for n,page in enumerate(doc,1):
  txt=page.get_text("text") or ""
  if txt.strip():item["digital_pages"]+=1
  for line in txt.splitlines():
   types=[key for key,pat in PATTERNS.items() if re.search(pat,line)]
   if types:
    item["candidate_count"]+=1
    if len(hits)<1500:hits.append({"url":url,"member":name,"page":n,"types":types,"excerpt":line.strip()[:260],"verification":"candidate"})
 doc.close()
 return item,hits
def download_and_parse(record,cap):
 url=record["url"];res=requests.get(url,timeout=75,stream=True)
 res.raise_for_status()
 blocks=[];size=0
 for chunk in res.iter_content(1024*1024):
  if not chunk:continue
  size+=len(chunk)
  if size>cap:raise ValueError("file exceeds maximum byte cap")
  blocks.append(chunk)
 raw=b"".join(blocks)
 out={"url":url,"procedure":record["procedure"],"bytes":size,"sha256":hashlib.sha256(raw).hexdigest(),"files":[]}
 if raw.startswith(b"%PDF"):parts=[(url.rsplit("/",1)[-1]+".pdf",raw)]
 elif zipfile.is_zipfile(io.BytesIO(raw)):
  parts=[]
  with zipfile.ZipFile(io.BytesIO(raw)) as z:
   for entry in z.infolist():
    if entry.is_dir() or not entry.filename.lower().endswith(".pdf") or entry.file_size>cap:continue
    candidate=z.read(entry)
    if candidate.startswith(b"%PDF"):parts.append((entry.filename,candidate))
 else:raise ValueError("unrecognized file format")
 hits=[]
 for name,content in parts:
  item,found=parse_pdf(content,url,name)
  out["files"].append(item);hits.extend(found)
 return out,hits
async def main():
 p=argparse.ArgumentParser()
 p.add_argument("--offset",type=int,default=0)
 p.add_argument("--limit",type=int,default=30)
 p.add_argument("--max-mb",type=int,default=75)
 args=p.parse_args()
 docs,meta=await inventory()
 result={"case":"MASE 8788","timestamp":datetime.now(timezone.utc).isoformat(),
         "indices":meta,"total_unique":len({x["url"] for x in docs}),
         "offset":args.offset,"limit":args.limit,"files":[],"failures":[],"candidates":[]}
 for row in docs[args.offset:args.offset+args.limit]:
  try:
   info,hits=download_and_parse(row,args.max_mb*1024*1024)
   result["files"].append(info);result["candidates"].extend(hits)
  except Exception as e:result["failures"].append({"url":row["url"],"procedure":row["procedure"],"error":str(e)[:250]})
 out=Path(f"reports/crawl4ai/mase-8788-batch-{args.offset}.json");out.parent.mkdir(parents=True,exist_ok=True)
 out.write_text(json.dumps(result,ensure_ascii=False,indent=2),encoding="utf8")
 print("MASE_8788_BATCH="+json.dumps({"offset":args.offset,"indices":meta,"processed":len(result["files"]),"failed":len(result["failures"]),
 "candidates":len(result["candidates"]),"problems":result["failures"][:8]},ensure_ascii=False))
if __name__=="__main__":asyncio.run(main())
