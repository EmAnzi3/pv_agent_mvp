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

LABELS = {
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
        if field not in LABELS:
            continue
        sec = next((k for k, _, pre in SECTIONS if field.startswith(pre)), None)
        if sec is None or not g["values"] or not isinstance(g["values"][0], dict):
            continue
        if field.startswith("tech.") and field.endswith(("_mwp", "_mw", "_mwh")) and g.get("subject") not in ("plant", None):
            continue  # component-level or other-project powers are not project characteristics
        vals = g["values"]
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
        sections[sec].append({"label": LABELS[field], "value": _fmt(field, v["value"]), "status": status,
                              "occurrences": v["occurrences"], "source": _source(v["example"], titles),
                              "note": note})

    # companies confirmed by registry (VIES): name and registered office
    group_vats = {f["value"] for f in d["facts"] if f["field"] == "company.vat_group"}
    vat_count: dict[str, int] = {}
    for f in d["facts"]:
        if f["field"] in ("company.vat", "company.vat_group"):
            vat_count[f["value"]] = vat_count.get(f["value"], 0) + 1
    public = re.compile(r"(?i)comune|regione|provincia|arpa|asl|azienda sanitaria|terna|snam|anas|ministero|agenzia|"
                        r"banca|bank|consorzio|universit|laborator|analisi e prove")
    companies = []
    for vat, r in sorted(registry.items(), key=lambda kv: -vat_count.get(kv[0], 0)):
        if not (r.get("valid") and r.get("name")) or (public.search(r["name"]) and vat not in group_vats):
            continue
        is_group = vat in group_vats
        companies.append({"label": "Gruppo societario (gruppo IVA)" if is_group else "Società negli elaborati",
                          "value": r["name"].title(), "status": "verificato",
                          "detail": f"P. IVA {vat} — sede legale: {(r.get('address') or '').title()} — citata {vat_count.get(vat, 0)} volte",
                          "note": None if is_group else "Identità verificata; il ruolo nel progetto va letto nei documenti.",
                          "source": {"document": "registro europeo VIES", "url": "https://ec.europa.eu/taxation_customs/vies/"}})
    sections["societa"] = sections["societa"] + companies[:6]

    # location: authority sheet + named/geometric facts
    loc = []
    if p.get("municipalities"):
        loc.append({"label": "Comuni", "value": ", ".join(p["municipalities"]) + (f" ({p['province']})" if p.get("province") else ""),
                    "status": "verificato", "source": {"document": "scheda ufficiale del procedimento", "url": p.get("info_url")}})
    elif record.get("municipalities"):
        loc.append({"label": "Comuni", "value": f"{record['municipalities']} ({record.get('province') or ''})".replace(" ()", ""),
                    "status": "da documenti", "note": "Dato dell'elenco progetti: la fonte non ha una scheda ufficiale strutturata.",
                    "source": {"document": "elenco progetti", "url": p.get("info_url")}})
    polys = [f for f in d["facts"] if f["field"] == "geo.area_polygon" and f["value"].get("geojson")]
    if polys:
        loc.append({"label": "Perimetri delle aree", "value": f"{len(polys)} poligoni ricostruiti dalle tabelle dei vertici",
                    "status": "da confermare", "note": "Da verificare sulle tavole prima dell'uso cartografico.",
                    "geojson": [f["value"]["geojson"] for f in polys[:12]],
                    "source": _source({"url": polys[0]["citation"]["url"], "page": polys[0]["citation"]["page"]}, titles)})
    corners = [f for f in d["facts"] if f["field"] == "geo.latlon_point" and f["qualifiers"].get("object") in ("site_bbox_corner", "centroid")]
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
            {"id": "tempi", "title": "Tempi e cantiere", "items": sections["tempi"]},
        ],
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
