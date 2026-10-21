"""OCR benchmark on the two MASE documents with many image-only pages."""
import io,hashlib,json,re,subprocess
from pathlib import Path
from datetime import datetime,timezone
import requests,fitz
IDS=[1126236,1126237]
PATTERNS={
 "contact":r"(?i)(?:@|partita iva|p\.?\s*iva|telefono|pec|cellulare)",
 "technical":r"(?i)(?:MWp|\bMW\b|bess|accumulo|fotovolta|agrivolta|stazione elettrica|tracker)",
 "geometry":r"(?i)(?:coordinate|utm|wgs|epsg|latitudine|longitudine|foglio|particella)",
 "schedule":r"(?i)(?:cronoprogramma|durata|inizio lavori|fine lavori|mesi)"
}
report={"timestamp":datetime.now(timezone.utc).isoformat(),"documents":[],"errors":[],"candidates":[]}
for doc_id in IDS:
    url=f"https://va.mite.gov.it/File/Documento/{doc_id}"
    try:
        response=requests.get(url,timeout=90)
        response.raise_for_status()
        original=response.content
        document=fitz.open(stream=original,filetype="pdf")
        record={"url":url,"sha256_original":hashlib.sha256(original).hexdigest(),
                "total_pages":len(document),"digital_pages":0,"ocr_pages":0,"ocr_failed":0,
                "ocr_chars":0}
        for n,page in enumerate(document,1):
            raw=page.get_text("text") or ""
            if len(raw.strip())>=80:
                record["digital_pages"]+=1
                text=raw
            else:
                pix=page.get_pixmap(matrix=fitz.Matrix(1.6,1.6),alpha=False)
                proc=subprocess.run(["tesseract","stdin","stdout","-l","ita+eng","--psm","3"],
                                    input=pix.tobytes("png"),capture_output=True,timeout=90)
                if proc.returncode!=0:
                    record["ocr_failed"]+=1
                    report["errors"].append({"url":url,"page":n,"message":proc.stderr.decode(errors="replace")[:250]})
                    continue
                record["ocr_pages"]+=1
                text=proc.stdout.decode("utf-8",errors="replace")
                record["ocr_chars"]+=len(text)
            for line in text.splitlines():
                if len(line.strip())<14:continue
                kinds=[k for k,p in PATTERNS.items() if re.search(p,line)]
                if kinds and len(report["candidates"])<20000:
                    report["candidates"].append({"url":url,"page":n,"text":line.strip()[:300],
                                                  "types":kinds,"verified":False})
        report["documents"].append(record)
        print("OCR_DOCUMENT="+json.dumps(record),flush=True)
        document.close()
    except Exception as exc:
        report["errors"].append({"url":url,"error":str(exc)[:300]})
path=Path("reports/crawl4ai/mase-6479-ocr.json")
path.parent.mkdir(parents=True,exist_ok=True)
path.write_text(json.dumps(report,ensure_ascii=False,indent=2),encoding="utf-8")
print("OCR_SUMMARY="+json.dumps({"documents":len(report["documents"]),"errors":len(report["errors"]),
                                  "ocr_pages":sum(d["ocr_pages"] for d in report["documents"]),
                                  "ocr_chars":sum(d["ocr_chars"] for d in report["documents"]),
                                  "candidates":len(report["candidates"])},ensure_ascii=False))
