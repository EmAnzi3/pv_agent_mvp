"""MASE 8788: two independent procedure indices, source-backed inventory."""
import asyncio,json,re
from pathlib import Path
from urllib.parse import urljoin
from bs4 import BeautifulSoup
from playwright.async_api import async_playwright
ROOT="https://va.mite.gov.it/it-IT/Oggetti/Documentazione/8788/"
IDS=["19438","12932"]
async def main():
    out={"project_id":"8788","procedures":{},"errors":[]}
    async with async_playwright() as p:
        browser=await p.chromium.launch(headless=True)
        page=await browser.new_page()
        for ident in IDS:
            base=ROOT+ident
            info={"id":ident,"pages":[],"documents":{},"declared":None}
            for number in range(1,60):
                url=base+("?pagina="+str(number) if number>1 else "")
                try:
                    await page.goto(url,wait_until="domcontentloaded",timeout=60000)
                    soup=BeautifulSoup(await page.content(),"html.parser")
                    links={}
                    for a in soup.find_all("a",href=True):
                        link=urljoin(url,a["href"])
                        if re.search(r"/File/Documento/\d+",link):
                            links[link]=a.get_text(" ",strip=True)[:150]
                    content=soup.get_text(" ",strip=True)
                    m=re.search(r"\(\s*n\.?\s*(\d+)\s*\)\s*Documenti",content,re.I)
                    if m:info["declared"]=int(m.group(1))
                    if not links:
                        if number==1:out["errors"].append({"id":ident,"message":"index has no document rows"})
                        break
                    before=len(info["documents"])
                    for link,title in links.items():info["documents"].setdefault(link,{"title":title,"first_page":number})
                    info["pages"].append({"page":number,"rows":len(links),"unique_total":len(info["documents"])})
                    if len(info["documents"])==before:break
                    if info["declared"] and len(info["documents"])>=info["declared"]:break
                except Exception as e:
                    out["errors"].append({"id":ident,"page":number,"error":str(e)[:220]});break
            out["procedures"][ident]=info
        await browser.close()
    path=Path("reports/crawl4ai/MASE-8788-INVENTORY.json");path.parent.mkdir(parents=True,exist_ok=True)
    path.write_text(json.dumps(out,ensure_ascii=False,indent=2),encoding="utf8")
    print("MASE_8788_INVENTORY="+json.dumps({i:{"declared":v["declared"],"unique":len(v["documents"]),"pages":len(v["pages"])} for i,v in out["procedures"].items()}))
    print("MASE_8788_ERRORS="+json.dumps(out["errors"]))
if __name__=="__main__":asyncio.run(main())
