"""Project card ('scheda') for the dashboard, built from a dossier.

The card is written for business users: plain labels, a status for every value,
the document and page it comes from. It never shows hashes, file paths or tool
names. Values that need human confirmation are shown as such, never as facts.

Status vocabulary shown to users:
  verificato      confirmed by an independent registry (e.g. VIES) or by the authority's own project sheet
  da documenti    stated in the project documents, single consistent value
  da confermare   stated in documents but needing human confirmation (EPC, contractor, coordinates...)
  in conflitto    documents report different values (all listed, none chosen)
"""
from __future__ import annotations

import json
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from . import SCHEMA_VERSION

CARD_SCHEMA = "pv-scheda/1.0"

# Fields shown in the card (user review 20/10/2026): grid voltage, inverters, module power/technology/count
# and DC/AC from documents are not shown; power comes from the official title.
CARD_FIELDS = {"role.proponent", "role.designer", "role.geologist", "role.works_director", "role.safety_coordinator",
               "role.epc", "role.contractor", "role.bop", "role.supplier", "role.om",
               "tech.technology", "tech.mounting", "tech.area_available_ha", "tech.area_panels_ha",
               "tech.bess_power_mw", "tech.bess_energy_mwh", "tech.wind_power_mw", "tech.wtg_count",
               "schedule.duration.construction", "schedule.duration.decommissioning", "schedule.duration.useful_life",
               "schedule.date.construction_start", "schedule.date.construction_end",
               "schedule.date.commercial_operation", "schedule.date.construction_start_extension"}
LABELS = {
    "tech.area_available_ha": "Superficie a disposizione", "tech.area_panels_ha": "Superficie occupata dai pannelli",
    "role.proponent": "Proponente", "role.designer": "Progettista", "role.geologist": "Geologo",
    "role.works_director": "Direttore dei lavori", "role.safety_coordinator": "Coordinatore sicurezza",
    "role.epc": "EPC indicato nei documenti", "role.contractor": "Impresa esecutrice indicata",
    "role.bop": "BoP indicato", "role.supplier": "Fornitore indicato", "role.om": "O&M indicato",
    "tech.pv_dc_mwp": "Potenza FV (DC)", "tech.pv_ac_mw": "Potenza FV (AC)",
    "tech.nominal_power_mw": "Potenza nominale", "tech.bess_power_mw": "Accumulo (potenza)",
    "tech.bess_energy_mwh": "Accumulo (energia)", "tech.wind_power_mw": "Potenza eolica",
    "tech.module_power_wp": "Potenza modulo", "tech.module_count": "Numero moduli",
    "tech.module_technology": "Tecnologia moduli", "tech.mounting": "Strutture",
    "tech.technology": "Tipologia", "tech.inverter_count": "Inverter (numero)",
    "tech.inverter_power_kw": "Inverter (potenza unitaria)", "tech.area_ha": "Superficie",
    "tech.wtg_count": "Aerogeneratori", "grid.voltage_kv": "Tensione di connessione",
    "schedule.duration.construction": "Durata cantiere", "schedule.duration.decommissioning": "Durata dismissione",
    "schedule.duration.useful_life": "Vita utile", "schedule.date.construction_start": "Inizio lavori",
    "schedule.date.construction_end": "Fine lavori", "schedule.date.commercial_operation": "Entrata in esercizio",
    "schedule.date.construction_start_extension": "Proroga inizio lavori",
}
VALUE_TEXT = {"single_axis_tracker": "tracker monoassiali", "fixed": "strutture fisse", "bifacial": "bifacciali",
              "agrivoltaic": "agrivoltaico", "agrivoltaic_advanced": "agrivoltaico avanzato",
              "floating_pv": "fotovoltaico galleggiante", "offshore": "offshore"}
TEMPORAL_TEXT = {"event_occurred": "atto/evento documentato", "planned": "previsione di progetto",
                 "template_reference": "campo presunto di piano di sicurezza o notifica",
                 "scenario": "scenario", "conditional": "condizionato", "relative_duration": "durata",
                 "unknown": "natura non determinabile"}
DOMINANCE = 4   # a value 4x more frequent than the next one is shown as prevalent, minorities listed
EXECUTION = {"role.epc", "role.contractor", "role.bop", "role.supplier", "role.om"}
SECTIONS = [
    ("societa", "Società e ruoli", ("role.", "company.")),
    ("tecnica", "Caratteristiche tecniche", ("tech.", "grid.")),
    ("tempi", "Tempi e cantiere", ("schedule.",)),
]


def _fmt(field: str, value: Any, unit: str | None = None) -> str:
    if isinstance(value, dict) and value.get("raw"):
        raw = re.sub(r"\s*\(\s*[a-z]+\s*\)", "", value["raw"]).strip()   # '30 (trenta) giorni' -> '30 giorni'
        raw = re.sub(r"(\d)(mesi|anni|giorni|settimane)", r"\1 \2", raw)
        return re.sub(r"\s+", " ", raw)
    if isinstance(value, dict) and "min" in value:
        lo, hi = value["min"], value["max"]
        unit_txt = " mesi"
        if field.endswith("useful_life") or lo >= 36:
            lo, hi, unit_txt = round(lo / 12, 1), round(hi / 12, 1), " anni"
        elif hi < 1:
            lo, hi, unit_txt = round(lo * 30.4), round(hi * 30.4), " giorni"
        return (f"{lo:g}" if lo == hi else f"{lo:g}–{hi:g}") + unit_txt
    if isinstance(value, float):
        txt = f"{value:,.3f}".rstrip("0").rstrip(".").replace(",", "X").replace(".", ",").replace("X", ".")
    else:
        txt = VALUE_TEXT.get(str(value), str(value))
    if re.fullmatch(r"\d{4}-\d{2}(-\d{2})?", txt):
        parts = txt.split("-")
        txt = "/".join(reversed(parts))
    suffix = {"tech.pv_dc_mwp": " MWp", "tech.pv_ac_mw": " MW", "tech.nominal_power_mw": " MW",
              "tech.bess_power_mw": " MW", "tech.bess_energy_mwh": " MWh", "tech.wind_power_mw": " MW",
              "tech.module_power_wp": " Wp", "tech.inverter_power_kw": " kW", "tech.area_ha": " ha",
              "tech.area_available_ha": " ha", "tech.area_panels_ha": " ha",
              "grid.voltage_kv": " kV"}.get(field, "")
    return txt + suffix


def _source(example: dict | None, inventory_titles: dict[str, str]) -> dict | None:
    if not example:
        return None
    url = example.get("url")
    return {"url": url, "page": example.get("page"), "document": inventory_titles.get(url) or "documento",
            "excerpt": (example.get("excerpt") or "")[:220]}


def build_card(d: dict[str, Any]) -> dict[str, Any]:
    p = d["project"]
    titles = {e["url"]: e.get("title") for e in d.get("inventory", [])}
    gates = {g["fact_id"]: g for g in d["gates"]["proposal"]}
    record = d.get("canonical_record") or {}
    registry = d.get("registry") or {}
    sections: dict[str, list[dict]] = {k: [] for k, _, _ in SECTIONS}
    open_points: list[dict] = []

    for g in d["groups"]:
        field = g["field"]
        if field not in LABELS or field not in CARD_FIELDS:
            continue
        sec = next((k for k, _, pre in SECTIONS if field.startswith(pre)), None)
        if sec is None or not g["values"] or not isinstance(g["values"][0], dict):
            continue
        if field.startswith("tech.") and field.endswith(("_mwp", "_mw", "_mwh")) and g.get("subject") not in ("plant", None):
            continue  # component-level or other-project powers are not project characteristics
        merged: dict[str, dict] = {}
        for x in g["values"]:
            if not isinstance(x, dict):
                continue
            if field == "tech.technology" and x["value"] in ("floating_pv", "offshore") and x["occurrences"] < 10:
                continue  # passing mentions (alternatives, literature) are not the project's technology
            k = _fmt(field, x["value"])
            if field.startswith("schedule.duration.") and isinstance(x["value"], dict) and x["value"].get("max"):
                # '34 settimane' and '8 mesi' are the same duration: merge within 10 %
                mx = x["value"]["max"]
                near = next((kk for kk, mv in merged.items() if isinstance(mv["value"], dict)
                             and abs(mv["value"].get("max", 0) - mx) <= 0.1 * max(mx, mv["value"].get("max", 0))), None)
                k = near or k
            if k in merged:
                merged[k] = {**merged[k], "occurrences": merged[k]["occurrences"] + x["occurrences"]}
            else:
                merged[k] = dict(x)
        vals = sorted(merged.values(), key=lambda x: -x["occurrences"])
        if field.startswith("tech.bess_"):
            vals = [x for x in vals if x["occurrences"] >= 2]   # single mentions are often other plants (e.g. nearby BESS)
        if not vals:
            continue
        g = {**g, "status": "conflict" if len(vals) > 1 and g["status"] == "conflict" else
             ("single_value" if len(vals) == 1 else g["status"])}
        if field == "tech.nominal_power_mw" and any(x["field"] in ("tech.pv_dc_mwp", "tech.pv_ac_mw") for x in d["groups"]):
            continue  # nominal values are mostly per-block: DC/AC are shown instead
        dominant = (g["status"] == "conflict" and len(vals) > 1 and vals[0]["occurrences"] >= 3
                    and vals[0]["occurrences"] >= DOMINANCE * vals[1]["occurrences"] and field not in EXECUTION)
        if dominant:
            v = vals[0]
            minor = "; ".join(f"{_fmt(field, x['value'])} ({x['occurrences']})" for x in vals[1:4])
            sections[sec].append({"label": LABELS[field], "value": _fmt(field, v["value"]), "status": "da documenti",
                                  "occurrences": v["occurrences"], "source": _source(v["example"], titles),
                                  "note": f"Valore prevalente nei documenti. Valori minoritari (revisioni, parti o refusi): {minor}."})
            continue
        if g["status"] == "conflict":
            item = {"label": LABELS[field], "status": "in conflitto",
                    "values": [{"value": _fmt(field, v["value"]), "occurrences": v["occurrences"],
                                "source": _source(v["example"], titles)} for v in vals[:4]],
                    "note": "I documenti riportano valori diversi (revisioni o ambiti diversi): nessuno è stato scelto."}
            sections[sec].append(item)
            open_points.append({"label": LABELS[field], "reason": "valori diversi nei documenti"})
            continue
        v = vals[0]
        fid = v["fact_ids"][0]
        status = "da documenti"
        note = None
        if field in EXECUTION:
            status, note = "da confermare", "Indicato negli elaborati: non prova un contratto o un affidamento."
            open_points.append({"label": LABELS[field], "reason": "affidamento da confermare"})
        elif gates.get(fid, {}).get("blocking"):
            status = "da confermare"
        temporal = ", ".join(TEMPORAL_TEXT.get(t, t) for t in v.get("temporal", []))
        if field.startswith("schedule.date.") and "event_occurred" not in v.get("temporal", []):
            note = f"Natura del dato: {temporal}. Non prova l'avvio del cantiere."
            if set(v.get("temporal", [])) & {"template_reference", "unknown"}:
                status = "da confermare"
        sections[sec].append({"label": LABELS[field], "value": _fmt(field, v["value"]), "status": status,
                              "occurrences": v["occurrences"], "source": _source(v["example"], titles),
                              "note": note})

    # companies confirmed by registry (VIES): name and registered office
    group_vats = {f["value"] for f in d["facts"] if f["field"] == "company.vat_group"}
    vat_count: dict[str, int] = {}
    for f in d["facts"]:
        if f["field"] in ("company.vat", "company.vat_group"):
            vat_count[f["value"]] = vat_count.get(f["value"], 0) + 1
    public = re.compile(r"(?i)\bcomune\b|regione|provincia|arpa|\basl\b|azienda sanitaria|terna|snam|anas|ministero|agenzia|"
                        r"banca|bank|consorzio|universit|laborator|analisi e prove")
    companies = []
    for vat, r in sorted(registry.items(), key=lambda kv: -vat_count.get(kv[0], 0)):
        if not (r.get("valid") and r.get("name")) or public.search(r["name"]):
            continue
        is_group = vat in group_vats
        companies.append({"label": "Gruppo societario (gruppo IVA)" if is_group else "Società negli elaborati",
                          "value": r["name"].title(), "status": "verificato",
                          "detail": f"P. IVA {vat} — sede legale: {(r.get('address') or '').title()} — citata {vat_count.get(vat, 0)} volte",
                          "note": None if is_group else "Identità verificata; il ruolo nel progetto va letto nei documenti.",
                          "source": {"document": "registro europeo VIES", "url": "https://ec.europa.eu/taxation_customs/vies/"}})
    sections["societa"] = sections["societa"] + companies[:6]

    title = record.get("title") or p.get("title") or ""
    tm = re.search(r"(?i)(\d{1,3}(?:[.\s]\d{3})*(?:,\d+)?|\d+(?:[.,]\d+)?)\s*(MWp|MW|kWp|KwP|kW)\b", title)
    tech_head = []
    if tm:
        from .extract.context import parse_number
        num = parse_number(tm.group(1)) or 0
        unit = tm.group(2)
        mw = num / 1000 if unit.lower().startswith("k") else num
        note = None
        cands = [(g["field"], x) for g in d["groups"]
                 if g["field"] in ("tech.pv_dc_mwp", "tech.pv_ac_mw", "tech.nominal_power_mw")
                 for x in g["values"] if isinstance(x, dict) and isinstance(x.get("value"), (int, float)) and x["occurrences"] >= 10]
        for fld, x in sorted(cands, key=lambda c: -c[1]["occurrences"]):
            ratio = mw / x["value"] if x["value"] else 0
            if any(abs(ratio - k) < 0.002 * k for k in (10, 100, 1000, 0.1, 0.01, 0.001)):
                note = (f"Possibile refuso nel titolo: gli elaborati riportano {_fmt(fld, x['value'])} "
                        f"({x['occurrences']} volte).")
                break
        tech_head.append({"label": "Potenza (dal titolo del procedimento)", "status": "da confermare" if note else "verificato",
                          "note": note,
                          "value": _fmt("tech.pv_dc_mwp" if unit.lower().endswith("p") else "tech.nominal_power_mw", round(mw, 3)),
                          "source": {"document": "titolo del procedimento", "url": p.get("info_url"), "excerpt": tm.group(0)}})
    lots = _prevalent(d, "tech.lots_count")
    if lots:
        what = lots[0].get("qualifiers", {}).get("what", "lotti")
        tech_head.append({"label": "Suddivisione", "value": f"{lots[1]['value']} {what}", "status": "da documenti",
                          "source": _source(lots[1]["example"], titles)})
    piles = []
    for kind, label in (("totale", "Pali da infiggere (totale)"), ("tracker", "Pali per tracker"),
                        ("strutture fisse", "Pali per strutture fisse"), ("recinzione", "Pali di recinzione"),
                        ("non specificato", "Pali (tipo non indicato)")):
        g = next((g for g in d["groups"] if g["field"] == "tech.piles_count" and (g.get("subject") or "") == kind), None)
        if g and g["values"]:
            v = g["values"][0]
            others = [f"{x['value']:,}".replace(",", ".") for x in g["values"][1:3]]
            piles.append({"label": label, "value": f"{v['value']:,}".replace(",", "."),
                          "status": "da documenti" if len(g["values"]) == 1 else "in conflitto" if v["occurrences"] < DOMINANCE * g["values"][1]["occurrences"] else "da documenti",
                          "note": ("Altri valori negli elaborati: " + ", ".join(others)) if others else None,
                          "source": _source(v["example"], titles)})
    sections["tecnica"] = tech_head + sections["tecnica"] + piles
    contacts = _contacts(d, titles)
    gantt = _gantt(d, titles)

    # location: authority sheet + named/geometric facts
    loc = []
    if p.get("municipalities"):
        loc.append({"label": "Comuni", "value": ", ".join(p["municipalities"]) + (f" ({p['province']})" if p.get("province") else ""),
                    "status": "verificato", "source": {"document": "scheda ufficiale del procedimento", "url": p.get("info_url")}})
    elif record.get("municipalities"):
        mun = record["municipalities"]
        prov = record.get("province")
        loc.append({"label": "Comuni", "value": mun if (not prov or f"({prov})" in mun) else f"{mun} ({prov})",
                    "status": "da documenti", "note": "Dato dell'elenco progetti: la fonte non ha una scheda ufficiale strutturata.",
                    "source": {"document": "elenco progetti", "url": p.get("info_url")}})
    polys = [f for f in d["facts"] if f["field"] == "geo.area_polygon" and f["value"].get("geojson")]
    if polys:
        loc.append({"label": "Perimetri delle aree", "value": f"{len(polys)} poligoni ricostruiti dalle tabelle dei vertici",
                    "status": "da confermare", "note": "Da verificare sulle tavole prima dell'uso cartografico.",
                    "geojson": [f["value"]["geojson"] for f in polys[:12]],
                    "source": _source({"url": polys[0]["citation"]["url"], "page": polys[0]["citation"]["page"]}, titles)})
    corners, seen_pts = [], set()
    for f in d["facts"]:
        if f["field"] == "geo.latlon_point" and f["qualifiers"].get("object") in ("site_bbox_corner", "centroid"):
            key = (round(f["value"]["lat"], 4), round(f["value"]["lon"], 4))
            if key not in seen_pts:
                seen_pts.add(key)
                corners.append(f)
    if corners:
        loc.append({"label": "Coordinate del sito", "value": "; ".join(f"{c['value']['lat']:.5f} N, {c['value']['lon']:.5f} E" for c in corners[:4]),
                    "status": "da documenti", "note": "Spigoli o centri dichiarati negli elaborati, non il perimetro.",
                    "points": [[c["value"]["lat"], c["value"]["lon"]] for c in corners[:12]],
                    "source": _source({"url": corners[0]["citation"]["url"], "page": corners[0]["citation"]["page"]}, titles)})

    canonical_delta = [{"campo": r["field"], "valore_attuale": r.get("canonical"), "esito": r["class"]}
                       for r in d.get("delta", [])]
    a = d["acquisition"]
    return {
        "schema": CARD_SCHEMA, "dossier_schema": SCHEMA_VERSION,
        "project_key": (d.get("canonical", {}).get("project_keys") or [None])[0] or record.get("project_key"),
        "title": record.get("title") or p.get("title"),
        "source_url": p.get("info_url"), "source": p.get("source"),
        "updated": datetime.now(timezone.utc).date().isoformat(),
        "sections": [
            {"id": "societa", "title": "Società e ruoli", "items": sections["societa"]},
            {"id": "tecnica", "title": "Caratteristiche tecniche", "items": sections["tecnica"]},
            {"id": "ubicazione", "title": "Ubicazione", "items": loc},
            {"id": "contatti", "title": "Contatti trovati negli elaborati", "items": contacts,
             "note": "Recapiti stampati negli elaborati (intestazioni, firme, timbri), esclusi quelli di enti pubblici. "
                     "«Da confermare» = il soggetto accanto al recapito non è certo: verificare prima di contattare."},
            {"id": "tempi", "title": "Tempi e cantiere", "items": sections["tempi"]},
        ],
        "cronoprogramma": gantt,
        "documentazione": {"documenti": a["references"], "pagine": a["pages_total"],
                           "procedimenti": len(a.get("procedures", [])),
                           "completa": d["gates"]["acquisition"]["pass"]},
        "confronto": canonical_delta,
        "da_verificare": open_points[:12],
        "avvertenza": "Informazioni estratte automaticamente dalla documentazione pubblica del procedimento. "
                      "Ogni valore riporta documento e pagina; i dati 'da confermare' e 'in conflitto' richiedono verifica.",
    }


def safe_name(key: str) -> str:
    return re.sub(r"[^A-Za-z0-9_-]", "_", key or "senza_chiave")


def write_card(card: dict, out_dir: str | Path) -> Path:
    out = Path(out_dir)
    out.mkdir(parents=True, exist_ok=True)
    path = out / f"{safe_name(card['project_key'])}.json"
    path.write_text(json.dumps(card, ensure_ascii=False, indent=1), encoding="utf-8")
    idx_path = out / "index.json"
    idx = json.loads(idx_path.read_text(encoding="utf-8")) if idx_path.is_file() else {}
    idx[card["project_key"]] = {"file": path.name, "updated": card["updated"], "title": card["title"]}
    idx_path.write_text(json.dumps(idx, ensure_ascii=False, indent=1, sort_keys=True), encoding="utf-8")
    return path


def _prevalent(d: dict, field: str):
    g = next((g for g in d["groups"] if g["field"] == field and g["values"] and isinstance(g["values"][0], dict)), None)
    if not g:
        return None
    fact = next((f for f in d["facts"] if f["field"] == field), {})
    return fact, g["values"][0]


PUBLIC_MAIL = re.compile(r"(?i)\.gov\.it$|governo\.it|interno\.it|agenziademanio|autoritadistretto|astral|regione|comune|provincia|"
                         r"prefettura|\bente|arpa|ats-|arsial|cultura|beniculturali|mite|mase|"
                         r"minambiente|terna\.it|e-distribuzione|enel\.it$|asl|vigilfuoco|marina\.difesa|ispra|anas\.it|"
                         r"snam|consorzio|bonifica|cert\.|postacert\.regione")
PUBLIC_SUBJECT = re.compile(r"(?i)comune|regione|provincia|soprintendenza|arpa|ministero|terna|vigili|asl|consorzio|caserma|"
                            r"prefettura|questura|carabinieri|igiene\s+e\s+sanit|www\.regione|\.gov\.it")


def _norm(t: str) -> str:
    return re.sub(r"[^a-z0-9]", "", (t or "").lower())


def _clean_subject(t: str | None) -> str:
    t = (t or "").strip("'\"‘’“” ")
    t = re.split(r"(?i)\b(?:dell[a'’]\s*(?:impresa|ditta|societ[aà])|della\s+(?:ditta|societ[aà]))\s+", t)[-1]
    return re.sub(r"^[^\w]+|[\s'\"‘’“”ʼ`´]+$", "", t)


def _contacts(d: dict, titles: dict) -> list[dict]:
    """E-mail, PEC and phones found in the documents, grouped by the company printed next to them."""
    rows: dict[tuple, dict] = {}
    # owners for address-based matching: registry names first, then the subjects most often printed in the documents
    freq: dict[str, int] = {}
    for f in d["facts"]:
        if f["field"].startswith(("contact.", "company.")) and len(_norm(f.get("subject"))) >= 6:
            n = _clean_subject(f.get("subject"))
            freq[n] = freq.get(n, 0) + 1
    names = [r["name"].title() for r in (d.get("registry") or {}).values() if r.get("valid") and r.get("name")]
    names += sorted(freq, key=lambda n: -freq[n])
    for f in d["facts"]:
        if f["field"] not in ("contact.email", "contact.pec", "contact.phone") or f.get("status") == "rejected":
            continue
        val = str(f["value"])
        if f["field"] != "contact.phone" and PUBLIC_MAIL.search(val):
            continue
        subj = _clean_subject(f.get("subject"))
        excerpt = f["citation"].get("excerpt") or ""
        if PUBLIC_SUBJECT.search(subj):
            continue
        if f["field"] == "contact.phone" and (PUBLIC_SUBJECT.search(excerpt) or PUBLIC_MAIL.search(excerpt)):
            continue    # phone printed in a public body's letterhead
        if f["field"] != "contact.phone":
            # the address itself often names its owner: 'pianidellamarina@pec.it' belongs to Piani della Marina
            local, _, dom = val.lower().partition("@")
            tokens = [t for t in (_norm(local), _norm(dom.split(".")[0])) if len(t) >= 6]
            owner = next((name for name in names if any(t in _norm(name) or _norm(name) in t for t in tokens)), None)
            if owner:
                subj = owner
        key = (f["field"], val)
        r = rows.setdefault(key, {"n": 0, "subjects": {}, "fact": f})
        r["n"] += 1
        if subj:
            r["subjects"][subj] = r["subjects"].get(subj, 0) + 1
    kind = {"contact.email": "E-mail", "contact.pec": "PEC", "contact.phone": "Telefono"}
    out = []
    for (field, val), r in sorted(rows.items(), key=lambda kv: -kv[1]["n"]):
        subj = max(r["subjects"], key=r["subjects"].get) if r["subjects"] else None
        same_line = r["fact"]["qualifiers"].get("subject_association") == "same_line"
        out.append({"label": f"{kind[field]}" + (f" — {subj}" if subj else ""), "value": val,
                    "status": "da documenti" if (subj and same_line) else "da confermare",
                    "detail": f"compare {r['n']} volte",
                    "source": _source({"url": r["fact"]["citation"]["url"], "page": r["fact"]["citation"]["page"],
                                       "excerpt": r["fact"]["citation"]["excerpt"]}, titles)})
    return out[:14]


def _gantt(d: dict, titles: dict) -> dict:
    """Bars for phases with explicit month ranges; otherwise the prevalent construction duration as one bar."""
    phases: dict[str, dict] = {}
    schedule_doc = re.compile(r"(?i)cronoprogramma|gantt|programma\s+(?:dei\s+)?lavori|piano\s+di\s+sicurezza|cantierizzazione")
    for f in d["facts"]:
        if f["field"] != "schedule.phase" or not schedule_doc.search(titles.get(f["citation"]["url"]) or ""):
            continue    # month ranges in opinions or monitoring plans are not the programme of works
        v = f["value"]
        key = (v["name"], v["start_month"], v["end_month"])
        ph = phases.setdefault(v["name"], {"counts": {}, "source": None})
        ph["counts"][key] = ph["counts"].get(key, 0) + 1
        ph["source"] = ph["source"] or {"url": f["citation"]["url"], "page": f["citation"]["page"]}
    bars = []
    for name, ph in phases.items():
        (n, a, b), _ = max(ph["counts"].items(), key=lambda kv: kv[1])
        bars.append({"name": name, "start": a, "end": b, "positioned": True, "source": _source(ph["source"], titles),
                     "label": f"mesi {a + 1:g}–{b:g}"})
    bars.sort(key=lambda x: (x["start"], x["end"]))
    durations = {}
    for field, name in (("schedule.duration.construction", "Cantiere (durata complessiva)"),
                        ("schedule.duration.civil_works", "Opere civili"),
                        ("schedule.duration.electrical_works", "Opere elettriche"),
                        ("schedule.duration.installation", "Montaggi"),
                        ("schedule.duration.commissioning", "Collaudi e avviamento")):
        pv = _prevalent(d, field)
        if pv and isinstance(pv[1]["value"], dict) and pv[1]["value"].get("max", 0) >= 1:
            durations[name] = {"months": pv[1]["value"]["max"], "text": _fmt(field, pv[1]["value"]),
                               "source": _source(pv[1]["example"], titles)}
    if not bars:
        for name, dur in durations.items():
            bars.append({"name": name, "start": 0, "end": dur["months"], "positioned": False,
                         "label": dur["text"], "source": dur["source"]})
    milestones = []
    for f in d["groups"]:
        if f["field"].startswith("schedule.date.") and f["field"] in LABELS and f["values"] and isinstance(f["values"][0], dict):
            v = f["values"][0]
            milestones.append({"label": LABELS[f["field"]], "date": _fmt(f["field"], v["value"]), "iso": v["value"],
                               "nature": ", ".join(TEMPORAL_TEXT.get(t, t) for t in v.get("temporal", [])),
                               "source": _source(v["example"], titles)})
    milestones.sort(key=lambda m: m["iso"])
    total = max([b["end"] for b in bars] + [durations.get("Cantiere (durata complessiva)", {}).get("months", 0)] + [1])
    return {"bars": bars, "months": total, "milestones": milestones,
            "note": None if any(b["positioned"] for b in bars) else
            "Gli elaborati indicano solo le durate: le barre partono tutte dall'avvio del cantiere, la sequenza reale non è indicata."}
