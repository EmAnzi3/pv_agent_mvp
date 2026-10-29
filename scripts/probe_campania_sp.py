"""Probe: list the public SharePoint share of Campania PAUR files (anonymous guest link). Review only."""
import json
import sys

import requests

SHARE = "https://regionecampania.sharepoint.com/:f:/s/VIA-VAS/EqQCPrpSpBxGlbBR-_uW2NsBnVv8PxTb5NiUzgg_t65CMw"
SITE = "https://regionecampania.sharepoint.com/sites/VIA-VAS"
ROOT = "/sites/VIA-VAS/Documenti condivisi/02_PAUR"
out = {"steps": []}
s = requests.Session()
s.headers["User-Agent"] = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/124 Safari/537.36"
r = s.get(SHARE, timeout=60, allow_redirects=True)
out["steps"].append({"share": r.status_code, "final": r.url, "cookies": sorted(s.cookies.keys())})
H = {"Accept": "application/json;odata=nometadata"}


def get(path, **kw):
    r = s.get(SITE + path, headers=H, timeout=60, **kw)
    try:
        body = r.json()
    except ValueError:
        body = r.text[:500]
    return r.status_code, body


st, b = get(f"/_api/web/GetFolderByServerRelativeUrl('{ROOT}')/Folders?$select=Name,ItemCount,ServerRelativeUrl,TimeLastModified&$top=5000")
names = [f["Name"] for f in b.get("value", [])] if isinstance(b, dict) else []
out["steps"].append({"folders_status": st, "n": len(names), "sample": names[:400], "err": None if names else b})
st, b = get(f"/_api/web/GetFolderByServerRelativeUrl('{ROOT}')/Files?$select=Name,Length,ServerRelativeUrl")
out["steps"].append({"root_files_status": st, "body": b if not isinstance(b, dict) else b.get("value", [])[:20]})
for want in sys.argv[1:]:
    hit = [n for n in names if want in n]
    item = {"want": want, "hits": hit}
    for h in hit[:2]:
        p = f"{ROOT}/{h}"
        st, fo = get(f"/_api/web/GetFolderByServerRelativeUrl('{p}')/Folders?$select=Name,ItemCount")
        st2, fi = get(f"/_api/web/GetFolderByServerRelativeUrl('{p}')/Files?$select=Name,Length,ServerRelativeUrl")
        item[h] = {"folders": fo, "files": fi if not isinstance(fi, dict) else fi.get("value", [])[:30], "st": [st, st2]}
        files = fi.get("value", []) if isinstance(fi, dict) else []
        if files:
            u = files[0]["ServerRelativeUrl"]
            r = s.get(f"{SITE}/_api/web/GetFileByServerRelativeUrl('{u}')/$value", timeout=120, stream=True)
            item[h]["download"] = [r.status_code, r.headers.get("content-type"), r.headers.get("content-length"), r.raw.read(8).hex()]
    out["steps"].append(item)
print(json.dumps(out, ensure_ascii=False, indent=1)[:6000])
json.dump(out, open("probe_campania_sp.json", "w"), ensure_ascii=False, indent=1)
