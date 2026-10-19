"""Compare MASE 8788 to PV Agent by exact source URL, not substring anywhere."""
import json,pathlib
URL="https://va.mite.gov.it/it-IT/Oggetti/Info/8788"
data=json.loads(pathlib.Path("docs/data.json").read_text(encoding="utf-8-sig"))
records=data.get("records",[])
def exact_sources(row):
    values=[]
    for key in ("url","source_url","detail_url","project_url","link"):
        if isinstance(row.get(key),str):values.append(row[key])
    for entry in row.get("sources",[]) if isinstance(row.get("sources"),list) else []:
        if isinstance(entry,dict):
            for key in ("url","source_url","detail_url","project_url","link"):
                if isinstance(entry.get(key),str):values.append(entry[key])
    return values
matches=[x for x in records if URL.rstrip("/") in [v.rstrip("/") for v in exact_sources(x)]]
out={"url":URL,"match_count":len(matches),"matches":matches,
     "status":"unique" if len(matches)==1 else "ambiguous_or_missing",
     "match_policy":"exact canonical source URL"}
path=pathlib.Path("reports/crawl4ai/mase-8788-pv-baseline.json");path.parent.mkdir(parents=True,exist_ok=True)
path.write_text(json.dumps(out,ensure_ascii=False,indent=2),encoding="utf8")
print("PV_BASELINE_8788="+json.dumps({"count":len(matches),"status":out["status"],"project_keys":[m.get("project_key") for m in matches],"titles":[m.get("title") for m in matches]},ensure_ascii=False))
if len(matches)!=1:raise SystemExit("Cannot safely enrich: exact source URL not unique")
