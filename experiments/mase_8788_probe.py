"""Read-only discovery of a new MASE case, no known project or document IDs assumed."""
import asyncio,json,re
from urllib.parse import urljoin,urlparse
from bs4 import BeautifulSoup
from crawl4ai import AsyncWebCrawler,CrawlerRunConfig
URL="https://va.mite.gov.it/it-IT/Oggetti/Info/8788"
async def main():
    result={"starting_url":URL,"pages":[],"candidate_document_indices":[],"errors":[]}
    async with AsyncWebCrawler() as crawler:
        todo=[URL];seen=set()
        while todo and len(seen)<4:
            url=todo.pop(0)
            if url in seen:continue
            seen.add(url)
            r=await crawler.arun(url=url,config=CrawlerRunConfig(page_timeout=45000))
            if not r.success:
                result["errors"].append({"url":url,"error":str(r.error_message)})
                continue
            markdown=str(r.markdown or "")
            links=[]
            for values in (r.links or {}).values():
                for a in values:
                    href=urljoin(url,str(a.get("href") or ""))
                    if urlparse(href).hostname!="va.mite.gov.it":continue
                    label=str(a.get("text") or "")
                    if re.search(r"/Oggetti/(?:Documentazione|Info)/8788|/File/(?:Documento|Provvedimento)/",href,re.I):
                        links.append({"label":label[:120],"url":href})
                        if "/Oggetti/Documentazione/8788/" in href:
                            result["candidate_document_indices"].append(href)
            result["pages"].append({"url":url,"title_snippet":markdown[:5000],"links":links})
            for link in links:
                if "/Oggetti/Documentazione/8788/" in link["url"] and link["url"] not in seen:
                    todo.append(link["url"])
    with open("mase-8788-probe.json","w",encoding="utf8") as f:json.dump(result,f,ensure_ascii=False,indent=2)
    print("MASE_8788_PROBE="+json.dumps({"pages":len(result["pages"]),"indices":list(dict.fromkeys(result["candidate_document_indices"])),"errors":result["errors"]},ensure_ascii=False))
    for page in result["pages"]:
        print("MASE_8788_PAGE="+json.dumps({"url":page["url"],"text":page["title_snippet"][:1800],"links":page["links"][:15]},ensure_ascii=False))
if __name__=="__main__":asyncio.run(main())
