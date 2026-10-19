"""Compare project 8788 identity to published PV Agent record; fail closed."""
import json, pathlib
URL="https://va.mite.gov.it/it-IT/Oggetti/Info/8788"
data=json.loads(pathlib.Path("docs/data.json").read_text(encoding="utf-8-sig"))
records=data.get("records",[])
matches=[x for x in records if "8788" in json.dumps(x,ensure_ascii=False) and
         ("va.mite.gov.it" in json.dumps(x,ensure_ascii=False) or "va.mase.gov.it" in json.dumps(x,ensure_ascii=False))]
out={"url":URL,"match_count":len(matches),"matches":matches[:3],
     "status":"unique" if len(matches)==1 else "ambiguous_or_missing"}
path=pathlib.Path("reports/crawl4ai/mase-8788-pv-baseline.json");path.parent.mkdir(parents=True,exist_ok=True)
path.write_text(json.dumps(out,ensure_ascii=False,indent=2),encoding="utf8")
print("PV_BASELINE_8788="+json.dumps({"count":len(matches),"status":out["status"],"records":matches[:1]},ensure_ascii=False))
