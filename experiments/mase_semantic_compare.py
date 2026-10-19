"""Fail-closed semantic verification and canonical record comparison for MASE 6479."""
import json,re,argparse
from pathlib import Path
from experiments.evidence_toolbox import valid_vat_it
URL="https://va.mite.gov.it/it-IT/Comunicazione/DettaglioUltimiProvvedimenti/6479"
FACTS=[
 ("proponent","Troia Sun S.r.l.","1126265",1,"La Società TROIA SUN S.r.l."),
 ("proponent_vat","10148261216","1126265",1,"partita IVA 10148261216"),
 ("proponent_pec","troiasunsrl@legalmail.it","1126265",1,"TROIASUNSRL@LEGALMAIL.IT"),
 ("pv_power_mwp",54.5,"1126265",1,"54.5 MWp"),
 ("bess_power_mw",10,"1126265",1,"sistema di accumulo di 10 MW"),
 ("designer","Arch. Antonio Manco","1126247",1,"Progettista: Arch. Antonio Manco"),
 ("geologist","Geol. Fabio Di Feo","1126247",1,"Tecnico: Geol. Fabio Di Feo"),
 ("designer_email","a.manco@iprojectsrl.com","1126229",2,"a.manco@iprojectsrl.com"),
 ("designer_phone","3384117245","1126229",2,"3384117245"),
 ("geologist_pec","geofabiodifeo@pec.epap.it","1126165",2,"geofabiodifeo@pec.epap.it"),
 ("design_company","IPROJECT S.r.l.","1126228",1,"IPROJECT S.R.L."),
 ("location_municipality","Troia (FG)","1126265",1,"COMUNE DI TROIA"),
 ("construction_duration_months",24,"1126259",188,"durata di circa 24 mesi"),
 ("inverter_count",168,"1126262",15,"Inverter da 350 kW: 168"),
 ("inverter_power_kw",350,"1126262",15,"Inverter da 350 kW: 168"),
 ("module_power_kwp",0.710,"1126259",19,"Generatori fotovoltaici da 0,710 kWp"),
 ("coordinate_reference_system","UTM-WGS84 Fuso 33","1126263",4,"Coordinate UTM-WGS 84 Fuso 33"),
 ("design_company_vat","11092870960","1126228",1,"P.IVA 11092870960"),
 ("design_company_pec","I-project@legalmail.it","1126228",1,"I-project@legalmail.it"),
 ("grid_voltage_kv",36,"1126180",12,"sezione a 36 kV"),
 ("decommission_duration_months",16,"1438131",9,"durata pari a 16 mesi"),
]
def norm(s):return re.sub(r"[^a-z0-9]+","",str(s).casefold().replace(",","."))
def main():
 p=argparse.ArgumentParser();p.add_argument("--data",default="docs/data.json");p.add_argument("--evidence-dir",required=True);p.add_argument("--out",default="reports/crawl4ai/troia-semantic-closeout.json");a=p.parse_args()
 raw=json.loads(Path(a.data).read_text(encoding="utf-8-sig"))
 matches=[r for r in raw["records"] if URL in json.dumps(r,ensure_ascii=False)]
 if len(matches)!=1:raise SystemExit(f"FAIL-CLOSED: expected one canonical record, found {len(matches)}")
 rec=matches[0];hits=[];files=[]
 for file in Path(a.evidence_dir).rglob("*.json"):
  if "batch-" not in file.name:continue
  d=json.loads(file.read_text(encoding="utf-8"));hits.extend(d["documents"].get("candidates",[]));files.append(file.name)
 if len(files)!=5:raise SystemExit(f"Expected 5 distinct batch reports: {files}")
 facts=[]
 for field,value,doc,page,needle in FACTS:
  candidates=[h for h in hits if h.get("source_url","").endswith("/"+doc) and h.get("page")==page and norm(needle) in norm(h.get("text",""))]
  ev=candidates[0] if candidates else None
  status="documented" if ev else "unverified"
  if field.endswith("_vat") and not valid_vat_it(str(value)):status="invalid_vat"
  facts.append({"field":field,"value":value if ev else None,"status":status,
    "evidence":{"url":ev["source_url"],"page":page,"excerpt":ev["text"]} if ev else None})
 # Do not assume canonical key names; locate actual matching values recursively.
 canonical=json.dumps(rec,ensure_ascii=False).casefold()
 for f in facts:
  f["in_canonical_literal"]=str(f["value"]).casefold() in canonical if f["value"] is not None else None
  f["incremental_class"]="already_present" if f["in_canonical_literal"] else ("candidate_new" if f["status"]=="documented" else "not_confirmed")
 dossier={"project":"Troia Sun / MASE 6479","baseline_record":rec,"evidence_batches":len(files),
   "facts":facts,
   "location":{"precision":"municipality/locality","coordinates":None,"status":"no validated site-coordinate evidence","warning":"Do not use coordinates for a cabin or parcel as plant centroid"},
   "schedule":{"construction_months":24,"decommission_months":16,"start":None,"cod":None,"status":"durations documented in dossier; absolute dates unknown"},
   "execution_roles":{"EPC":None,"BoP":None,"installers":None,"reason":"No direct project-specific award verified"},
   "closeout_status":"incomplete",
   "remaining_gates":["Structured coordinates table and context not yet validated",
      "Automatic extraction recall and precision not benchmarked against manual gold set",
      "Some pages/tables and corporate relationships still require human cross-check"]}
 out=Path(a.out);out.parent.mkdir(parents=True,exist_ok=True);out.write_text(json.dumps(dossier,ensure_ascii=False,indent=2),encoding="utf-8")
 print("SEMANTIC_SUMMARY="+json.dumps({"documented":sum(f["status"]=="documented" for f in facts),
 "unverified":[f["field"] for f in facts if f["status"]!="documented"],
 "already_present":sum(f["incremental_class"]=="already_present" for f in facts),
 "candidate_new":[f["field"] for f in facts if f["incremental_class"]=="candidate_new"],
 "closeout":dossier["closeout_status"]},ensure_ascii=False))
if __name__=="__main__":main()
