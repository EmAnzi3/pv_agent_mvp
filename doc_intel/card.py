"""Project card ('scheda') for the dashboard, built from a dossier.

The card is written for business users: plain labels, a status for every value,
the document and page it comes from. It never shows hashes, file paths or tool
names. Values that need human confirmation are shown as such, never as facts.

Layout (user review 20/10/2026): a summary of the essentials, the companies with their own
contacts (contacts that cannot be tied to a company or person are not shown), the location with a
map link, and a Gantt only when the documents give phases with a start and an end.

Status vocabulary shown to users:
  verificato      confirmed by an independent registry (VIES) or by the authority's own project sheet
  da documenti    stated in the project documents, consistent
  da verificare   documents disagree (the most frequent value is shown, the others listed) or need confirmation
"""
from __future__ import annotations

import json
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from . import SCHEMA_VERSION

CARD_SCHEMA = "pv-scheda/2.0"

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
        raw = re.sub(r"(?i)\s+(?:naturali(?:\s+e\s+consecutivi)?|lavorativi|solari)\b", "", raw)
        raw = re.sub(r"\s+", " ", raw).strip()
        md = re.fullmatch(r"(\d+)\s+(giorni|settimane)", raw)
        if md and int(md.group(1)) >= (60 if md.group(2) == "giorni" else 9):
            months = int(md.group(1)) / (30.4 if md.group(2) == "giorni" else 4.33)
            raw += f" (circa {months:.0f} mesi)"
        return raw
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


ROLE_TEXT = {"role.proponent": "Proponente", "role.designer": "Progettista", "role.geologist": "Geologo",
             "role.works_director": "Direttore lavori", "role.safety_coordinator": "Coordinatore sicurezza",
             "role.epc": "EPC (da confermare)", "role.contractor": "Impresa esecutrice (da confermare)",
             "role.bop": "BoP (da confermare)", "role.supplier": "Fornitore (da confermare)", "role.om": "O&M (da confermare)"}
ROLE_ORDER = list(ROLE_TEXT)
COMPANY_FORM = re.compile(r"(?i)\bs\.?\s?r\.?\s?l\b|\bs\.?\s?p\.?\s?a\b|\bs\.?\s?n\.?\s?c\b|\bs\.?\s?a\.?\s?s\b|\bs\.\s?s\.?|"
                          r"societ[aà]|\bgmbh\b|\bltd\b|\bs\.?\s?l\.?\s?u?\b|\bspa\b|\bsrl\b")
PERSON_TITLE = re.compile(r"(?i)^(?:ing|arch|geol|dott(?:\.ssa)?|dr|geom|per\.?\s?ind|avv|agr|prof|p\.?\s?i)\b\.?")
SCHED_TITLE = re.compile(r"(?i)cron?o?\s*programma|gantt|diagramma\s+(?:dei\s+)?lavori|programma\s+(?:dei\s+)?lavori")
ACCENTS = str.maketrans("àáèéìíòóùú", "aaeeiioouu")
LEGAL_PHRASES = re.compile(
    r"societ[aà]'?\s+(?:a\s+responsabilit[aà]'?\s+limitata(?:\s+semplificata)?|per\s+azioni|agr?icola|semplice|cooperativa|"
    r"di\s+ingegneria|consortile)|\b(?:s\.?\s?r\.?\s?l|s\.?\s?p\.?\s?a|s\.?\s?s|s\.?\s?n\.?\s?c|s\.?\s?a\.?\s?s|"
    r"unipersonale|semplificata|in\s+sigla)\b\.?")
ITALY = (35.4, 47.2, 6.5, 18.7)


def _ckey(name: str) -> str:
    """Identity key of a company or person: lower case, no accents, no legal form, letters and digits only.
    For titled persons the words are sorted, so 'Ing. Chiricotto Vincenzo' = 'Ing. Vincenzo Chiricotto'."""
    t = (name or "").lower().translate(ACCENTS)
    person = bool(PERSON_TITLE.match(t))
    t = PERSON_TITLE.sub("", t)
    t = re.sub(r"(?i)\b(?:dott|agronom[oa]|geolog[oa]|forestale|vet|geol|agr|ing|for|arch|geom)\b\.?", " ", t)
    t = LEGAL_PHRASES.sub(" ", t)
    if person:
        return "".join(sorted(re.findall(r"[a-z0-9]{2,}", t)))
    return re.sub(r"[^a-z0-9]", "", t)


def _same(a: str, b: str) -> bool:
    if not a or not b:
        return False
    if a == b:
        return True
    short, long_ = sorted((a, b), key=len)
    if len(short) >= 4 and long_.endswith(short) and len(long_) - len(short) <= 2:
        return True                      # 'Solar S.r.l.' read without the 'e-' of 'e-Solar S.r.l.'
    return len(short) >= 6 and short in long_


def _garbled(name: str) -> bool:
    """OCR noise such as 'EE EE A il EE ll crei sl'."""
    t = LEGAL_PHRASES.sub(" ", (name or "").lower())
    t = re.sub(r"\b[a-zà-ÿ]\.", " ", t)             # initials and abbreviations ('G.', 'Dott.')
    words = re.findall(r"[a-zà-ÿ]+", t)
    if not words or not re.search(r"[A-Za-zÀ-ÿ]{4}", name or ""):
        return True
    bad = sum(len(w) <= 2 or not re.search(r"[aeiouàèéìòù]", w) for w in words)
    return len(words) >= 4 and bad / len(words) > 0.3


def _lev(a: str, b: str) -> int:
    if abs(len(a) - len(b)) > 2:
        return 3
    prev = list(range(len(b) + 1))
    for i, ca in enumerate(a, 1):
        cur = [i]
        for j, cb in enumerate(b, 1):
            cur.append(min(prev[j] + 1, cur[j - 1] + 1, prev[j - 1] + (ca != cb)))
        prev = cur
    return prev[-1]


def _pretty(name: str) -> str:
    """'MARGISOLAR S.R.L.' -> 'Margisolar S.r.l.'; keeps mixed-case spellings as written."""
    n = re.sub(r"\s+", " ", (name or "").strip(" ,;-'\"‘’“”"))
    n = re.sub(r"(?i)societ[aà]'?\s+a\s+responsabilit[aà]'?\s+limitata(?:\s+semplificata)?", "S.r.l.", n)
    n = re.sub(r"(?i)societ[aà]'?\s+per\s+azioni", "S.p.A.", n)
    n = re.sub(r"\s+S\.r\.l\.\s*-?\s*societ[aà]'?\s+di\s+ingegneria$", " S.r.l.", n, flags=re.I)
    # all-capital words of 4+ letters are capitalised ('MARGISOLAR' -> 'Margisolar'); acronyms (PV, LAB) are kept
    n = " ".join(w.capitalize() if re.fullmatch(r"[A-ZÀ-Ý'’]{4,}", w) else w for w in n.split())
    n = re.sub(r"(?i)\bs\.?\s?r\.?\s?l\.?(?=\s|$)", "S.r.l.", n)
    n = re.sub(r"(?i)\bs\.?\s?p\.?\s?a\.?(?=\s|$)", "S.p.A.", n)
    n = re.sub(r"(?i)\bs\.?\s?s\.(?=\s|$)", "S.S.", n)
    n = re.sub(r"(S\.r\.l\.|S\.p\.A\.|S\.S\.)\s*[-–,].*$", r"\1", n)
    return n


def _example_source(f: dict, titles: dict) -> dict:
    c = f.get("citation") or {}
    return _source({"url": c.get("url"), "page": c.get("page"), "excerpt": c.get("excerpt")}, titles)


def _pick(d: dict, field: str, titles: dict, accept=None) -> dict | None:
    """Most frequent value of a field (equivalent spellings and durations merged); alternatives kept as hints."""
    g = next((g for g in d["groups"] if g["field"] == field and g["values"] and isinstance(g["values"][0], dict)
              and (not field.startswith("tech.") or g.get("subject") in ("plant", None, "totale"))), None)
    if not g:
        return None
    merged: dict[str, dict] = {}
    for x in g["values"]:
        if not isinstance(x, dict) or (accept and not accept(x["value"])):
            continue
        k = _fmt(field, x["value"])
        if field.startswith("schedule.duration.") and isinstance(x["value"], dict) and x["value"].get("max"):
            mx = x["value"]["max"]
            k = next((kk for kk, mv in merged.items() if isinstance(mv["value"], dict)
                      and abs(mv["value"].get("max", 0) - mx) <= 0.1 * max(mx, mv["value"].get("max", 0))), k)
        if field.startswith("tech.area_") and isinstance(x["value"], (int, float)):
            # 115 ha and 116 ha are the same site measured or rounded differently: merge within 3 %
            k = next((kk for kk, mv in merged.items() if isinstance(mv["value"], (int, float))
                      and abs(mv["value"] - x["value"]) <= 0.03 * max(mv["value"], x["value"])), k)
        if k in merged:
            merged[k]["occurrences"] += x["occurrences"]
        else:
            merged[k] = {**x, "occurrences": x["occurrences"], "text": k}
    vals = sorted(merged.values(), key=lambda x: -x["occurrences"])
    if not vals:
        return None
    top = vals[0]
    alts = [f"{v['text']} ({v['occurrences']})" for v in vals[1:4]]
    clear = len(vals) == 1 or top["occurrences"] >= DOMINANCE * vals[1]["occurrences"]
    return {"value": top["value"], "text": top["text"], "occurrences": top["occurrences"], "temporal": top.get("temporal", []),
            "status": "da documenti" if clear else "da verificare", "alternatives": alts,
            "source": _source(top.get("example"), titles)}


def _phase_name(n: str) -> str:
    """'SOTTOIMPIANTO A - Opere SOTTOIMPIANTO A' -> 'SOTTOIMPIANTO A - Opere' (label printed twice, in table and on the bar)."""
    n = re.sub(r"^\s*[A-Z]?\.?\d+(?:\.\d+)*\.?\s+", "", n)      # WBS codes ('B.6.1 Installazione ...')
    w = n.split()
    for k in range(1, len(w) // 2 + 1):
        tail = w[-k:]
        if w[:k] == tail and len(w) > k:
            return " ".join(w[:-k])
    return n


def _hint(p: dict) -> str | None:
    if not p["alternatives"]:
        return None
    lead = "Valore più ricorrente" if p["status"] == "da verificare" else "Valore prevalente"
    return f"{lead} negli elaborati; altri valori: " + ", ".join(p["alternatives"])


# --------------------------------------------------------------------------------------------- summary
def _summary(d: dict, p: dict, record: dict, titles: dict) -> tuple[list[dict], list[dict]]:
    tiles, open_points = [], []
    title = record.get("title") or p.get("title") or ""
    power_rx = r"(?i)(\d{1,3}(?:[.\s]\d{3})*(?:,\d+)?|\d+(?:[.,]\d+)?)\s*(MWp|MW|kWp|KwP|kW)\b"
    tm, title_src = re.search(power_rx, title), "titolo del procedimento"
    sheet = (p.get("raw") or {}).get("register_sheet") or {}
    if not tm and sheet.get("power_mw"):
        # a register sheet (ATOS) states the power in its own field, not in the plant name
        tm, title_src = re.search(power_rx, sheet["power_mw"].replace(".", ",") + " MW"), "registro impianti regionale"
    if tm:
        from .extract.context import parse_number
        num = parse_number(tm.group(1)) or 0
        unit = tm.group(2)
        mw = num / 1000 if unit.lower().startswith("k") else num
        note = None
        cands = [(g["field"], x) for g in d["groups"]
                 if g["field"] in ("tech.pv_dc_mwp", "tech.pv_ac_mw", "tech.nominal_power_mw") and g.get("subject") in ("plant", None)
                 for x in g["values"] if isinstance(x, dict) and isinstance(x.get("value"), (int, float)) and x["occurrences"] >= 10]
        cands.sort(key=lambda c: -c[1]["occurrences"])
        shown_field = "tech.pv_dc_mwp" if unit.lower().endswith("p") else "tech.nominal_power_mw"
        shown, status = mw, "verificato"
        source = {"document": title_src, "url": p.get("info_url"), "excerpt": tm.group(0)}
        title_seen = any(abs(x["value"] - mw) <= 0.01 * mw and x["occurrences"] >= 2 for _, x in
                         [(g["field"], x) for g in d["groups"] if g["field"] in ("tech.pv_dc_mwp", "tech.pv_ac_mw", "tech.nominal_power_mw")
                          for x in g["values"] if isinstance(x, dict) and isinstance(x.get("value"), (int, float))])
        for fld, x in cands:
            ratio = mw / x["value"] if x["value"] else 0
            shift = any(abs(ratio - k) < 0.002 * k for k in (10, 100, 1000, 0.1, 0.01, 0.001))
            # the documents win over the title when the title is a clear typo (decimal shift) or never confirmed
            if shift or (not title_seen and x["occurrences"] >= 30):
                shown, shown_field, status = x["value"], fld, "da documenti"
                note = f"Valore ricorrente negli elaborati ({x['occurrences']} volte); il titolo riporta {tm.group(0)}" + \
                       (" (probabile refuso)." if shift else ".")
                source = _source(x.get("example"), titles)
                open_points.append({"label": "Potenza", "reason": f"il titolo riporta {tm.group(0)}, gli elaborati {_fmt(fld, x['value'])}"})
                mw = x["value"]
                break
        lots = _pick(d, "tech.lots_count", titles)
        what = next(((f.get("qualifiers") or {}).get("what") for f in d["facts"] if f["field"] == "tech.lots_count"), "lotti")
        sub = " · ".join(x for x in (f"Suddivisa in {lots['value']} {what}" if lots else None, note) if x) or None
        tiles.append({"key": "potenza", "label": "Potenza", "status": status,
                      "value": _fmt(shown_field, round(shown, 3)), "sub": sub, "source": source})
    if not tm:
        # no power in the title: the plant power the documents state most often (an explicit declaration
        # such as 'capacità produttiva massima' counts like any other occurrence)
        cands = [(g["field"], x) for g in d["groups"]
                 if g["field"] in ("tech.pv_dc_mwp", "tech.nominal_power_mw", "tech.pv_ac_mw") and g.get("subject") in ("plant", None)
                 for x in g["values"] if isinstance(x, dict) and isinstance(x.get("value"), (int, float)) and x["value"] > 0]
        if cands:
            fld, x = max(cands, key=lambda c: (c[1]["occurrences"], c[0] == "tech.pv_dc_mwp"))
            others = sorted({round(o["value"], 3) for _, o in cands if abs(o["value"] - x["value"]) > 0.01 * x["value"]})
            sure = x["occurrences"] >= 3
            tiles.append({"key": "potenza", "label": "Potenza", "status": "da documenti" if sure else "da verificare",
                          "value": _fmt(fld, round(x["value"], 3)),
                          "sub": f"Dagli elaborati ({x['occurrences']} occorrenze); il titolo non indica la potenza"
                                 + (f"; altri valori: {', '.join(_fmt(fld, v) for v in others[:3])}" if others else ""),
                          "source": _source(x.get("example"), titles)})
            if not sure or others:
                open_points.append({"label": "Potenza", "reason": "non indicata nel titolo: valore ricavato dagli elaborati"})
            mw = x["value"] if fld != "tech.pv_ac_mw" else None
            tm = True if mw else None
    plant_mw = mw if tm else None
    tech = _pick(d, "tech.technology", titles)
    if tech and tech["value"] in ("floating_pv", "offshore") and tech["occurrences"] < 10:
        tech = None                      # passing mentions (alternatives, literature) are not the project's technology
    mount = _pick(d, "tech.mounting", titles)
    kind = " · ".join(x["text"] for x in (tech, mount) if x)
    if kind:
        tiles.append({"key": "impianto", "label": "Impianto", "value": kind[:1].upper() + kind[1:], "status": "da documenti",
                      "source": (tech or mount)["source"]})
    # plausibility against the plant power: a ground-mounted plant needs roughly 1-6 ha per MW,
    # panels cover roughly 0.3-3 ha per MW; values outside are other areas (cabins, parcels, roads)
    ok_tot = (lambda v: 0.8 * plant_mw <= v <= 8 * plant_mw) if plant_mw else None
    ok_pan = (lambda v: 0.25 * plant_mw <= v <= 3 * plant_mw) if plant_mw else None
    tot, pan = _pick(d, "tech.area_available_ha", titles, ok_tot), _pick(d, "tech.area_panels_ha", titles, ok_pan)
    if tot and pan and pan["value"] >= 0.85 * tot["value"]:
        # panels never cover (almost) the whole site: rows are spaced; such a value is the site or farmland area
        open_points.append({"label": "Superficie pannelli", "reason": f"valore letto ({pan['text']}) prossimo all'area totale: non mostrato"})
        pan = None
    if tot:
        tiles.append({"key": "area", "label": "Superficie complessiva", "value": tot["text"], "status": tot["status"],
                      "sub": _hint(tot), "source": tot["source"]})
    if pan:
        share = f"{pan['value'] / tot['value'] * 100:.0f}% dell'area" if tot else None
        tiles.append({"key": "pannelli", "label": "Superficie coperta dai pannelli", "value": pan["text"], "status": pan["status"],
                      "sub": " · ".join(x for x in (share, _hint(pan)) if x), "source": pan["source"]})
    piles = {}
    for g in d["groups"]:
        if g["field"] == "tech.piles_count" and g["values"] and isinstance(g["values"][0], dict):
            v = g["values"][0]
            piles[g.get("subject") or "non specificato"] = (v, len(g["values"]) > 1)
    structural = [(k, piles[k]) for k in ("totale", "tracker", "strutture fisse") if k in piles]
    if structural:
        k, (v, multi) = structural[0]
        parts = [f"{piles[x][0]['value']:,} {x}".replace(",", ".") for x in ("tracker", "strutture fisse") if x in piles and x != k]
        if "recinzione" in piles:
            parts.append(f"recinzione {piles['recinzione'][0]['value']:,}".replace(",", "."))
        tiles.append({"key": "pali", "label": "Pali da infiggere" + ("" if k == "totale" else f" ({k})"),
                      "value": f"{v['value']:,}".replace(",", "."), "status": "da verificare" if multi else "da documenti",
                      "sub": " · ".join(parts) or None, "source": _source(v["example"], titles)})
    elif "recinzione" in piles:
        v = piles["recinzione"][0]
        tiles.append({"key": "pali", "label": "Pali di recinzione", "value": f"{v['value']:,}".replace(",", "."),
                      "status": "da documenti", "sub": "Pali delle strutture non indicati negli elaborati",
                      "source": _source(v["example"], titles)})
    bess = _pick(d, "tech.bess_power_mw", titles)
    if bess and bess["occurrences"] >= 2:
        energy = _pick(d, "tech.bess_energy_mwh", titles)
        tiles.append({"key": "accumulo", "label": "Accumulo", "value": bess["text"] + (f" / {energy['text']}" if energy else ""),
                      "status": bess["status"], "sub": _hint(bess), "source": bess["source"]})
    # a plant of more than a few MW is not built in days: such values are single phases (site set-up, a crossing...)
    plausible = (lambda v: not isinstance(v, dict) or v.get("max", 0) >= 3) if (plant_mw or 0) >= 5 else None
    dur = _pick(d, "schedule.duration.construction", titles, plausible)
    start = _pick(d, "schedule.date.construction_start", titles)
    if dur or start:
        sub = None
        if start:
            nature = ", ".join(TEMPORAL_TEXT.get(t, t) for t in start["temporal"])
            sub = f"Inizio lavori {start['text']} ({nature})"
        tiles.append({"key": "cantiere", "label": "Durata cantiere", "value": dur["text"] if dur else "non indicata",
                      "status": dur["status"] if dur else "da verificare",
                      "sub": " · ".join(x for x in (sub, _hint(dur) if dur else None) if x) or None,
                      "source": (dur or start)["source"]})
    for t in tiles:
        if t["status"] == "da verificare" and t["key"] != "potenza":
            open_points.append({"label": t["label"], "reason": "valori diversi negli elaborati"})
    return tiles, open_points


# --------------------------------------------------------------------------------------------- companies
def _subjects(d: dict, titles: dict, sheet_proponent: str | None = None, sheet_url: str | None = None
              ) -> tuple[list[dict], list[dict], int]:
    ents: list[dict] = []

    def find(name: str, kind: str | None = None) -> dict | None:
        k = _ckey(name)
        if len(k) < 4:
            return None
        for e in ents:
            if (kind is None or e["kind"] == kind) and any(_same(k, x) for x in e["keys"]):
                return e
        return None

    def get(name: str, kind: str) -> dict | None:
        name = _clean_subject(name)
        if _garbled(name) or PUBLIC_SUBJECT.search(name or ""):
            return None
        e = find(name, kind)
        if e is None:
            if len(_ckey(name)) < 4:
                return None
            e = {"kind": kind, "keys": {_ckey(name)}, "names": {}, "roles": [], "vat": None, "registry": None,
                 "contacts": {}, "mentions": 0, "role_source": None}
            ents.append(e)
        e["keys"].add(_ckey(name))
        e["names"][name] = e["names"].get(name, 0) + 1
        return e

    def is_company(name: str) -> bool:
        return bool(COMPANY_FORM.search(name or ""))

    # 1. identities confirmed by the registry
    registry = d.get("registry") or {}
    vat_mentions: dict[str, int] = {}
    vat_names: dict[str, list[str]] = {}
    for f in d["facts"]:
        if f["field"] in ("company.vat", "company.vat_group"):
            vat_mentions[f["value"]] = vat_mentions.get(f["value"], 0) + 1
            if f.get("subject"):
                vat_names.setdefault(f["value"], []).append(_clean_subject(f["subject"]))
    group_vats = {f["value"] for f in d["facts"] if f["field"] == "company.vat_group"}
    public = re.compile(r"(?i)\bcomune\b|regione|provincia|arpa|\basl\b|azienda sanitaria|terna|snam|anas|ministero|agenzia|"
                        r"banca|bank|consorzio|universit")
    for vat, r in sorted(registry.items(), key=lambda kv: -vat_mentions.get(kv[0], 0)):
        if not (r.get("valid") and r.get("name")) or public.search(r["name"]):
            continue
        e = get(r["name"], "company")
        if e is None:
            continue
        e["vat"], e["registry"], e["mentions"] = vat, r, e["mentions"] + vat_mentions.get(vat, 0)
        e["vat_group"] = vat in group_vats
    # 2. proponent from the authority's project sheet, then roles documented in the elaborati
    if sheet_proponent:
        e = get(_clean_subject(sheet_proponent), "company" if is_company(sheet_proponent) else "person")
        if e is not None:
            e["roles"].append("Proponente")
            e["mentions"] += 1
            e["role_source"] = {"document": "scheda ufficiale del procedimento", "url": sheet_url}
    for field in ROLE_ORDER:
        g = next((g for g in d["groups"] if g["field"] == field and g["values"] and isinstance(g["values"][0], dict)), None)
        if not g:
            continue
        vals = [x for x in g["values"] if isinstance(x, dict)]
        top = vals[0]["occurrences"] if vals else 0
        for i, x in enumerate(vals):
            if i > 0 and x["occurrences"] < max(2, 0.25 * top):
                continue                 # rare alternatives are usually misreads or other documents' parties
            if _garbled(str(x["value"])):
                continue
            name = _clean_subject(str(x["value"]))
            e = get(name, "company" if is_company(name) else "person")
            if e is None:
                continue
            if ROLE_TEXT[field] not in e["roles"]:
                e["roles"].append(ROLE_TEXT[field])
            e["mentions"] += x["occurrences"]
            e["role_source"] = e["role_source"] or _source(x.get("example"), titles)
    # 3. contacts, attached to the person or company printed next to them
    dropped = 0
    for f in d["facts"]:      # professionals signing letterheads, so their e-mails can be matched by name
        q = f.get("qualifiers") or {}
        if f["field"].startswith("contact.") and q.get("person"):
            get(q["person"], "person")
    persons_known = [e for e in ents if e["kind"] == "person"]
    for f in d["facts"]:
        if f["field"] not in ("contact.email", "contact.pec", "contact.phone") or f.get("status") == "rejected":
            continue
        val = str(f["value"])
        q = f.get("qualifiers") or {}
        excerpt = (f.get("citation") or {}).get("excerpt") or ""
        if f["field"] != "contact.phone" and PUBLIC_MAIL.search(val):
            continue
        if f["field"] == "contact.phone" and (PUBLIC_SUBJECT.search(excerpt) or PUBLIC_MAIL.search(excerpt)):
            continue
        company = _clean_subject(f.get("subject"))
        if company and PUBLIC_SUBJECT.search(company):
            continue
        person = q.get("person")
        target = None
        if f["field"] != "contact.phone":            # the address often names its owner
            local, _, dom = val.lower().partition("@")
            tokens = [t for t in (_norm(local), _norm(dom.split(".")[0])) if len(t) >= 5]
            target = next((e for e in ents if e["kind"] == "company" and any(_same(t, k) or (len(k) >= 6 and k in t)
                                                                           for t in tokens for k in e["keys"])), None)
            if target is None:
                for e in persons_known:
                    words = [w for w in re.findall(r"[a-z]{3,}", min(e["names"], key=len).lower().translate(ACCENTS))
                             if not PERSON_TITLE.match(w)]
                    if len(words) >= 2 and words[-1] in _norm(local) and (words[0] in _norm(local) or _norm(local).startswith(words[0][0])):
                        target = e
                        break
        if target is None:
            if person and q.get("person_association") == "same_line" and q.get("subject_association") != "same_line":
                target = get(person, "person")
            elif company and q.get("subject_association", "none") != "none":
                target = get(company, "company")
            elif person:
                target = get(person, "person")
        if target is None:
            dropped += 1
            continue
        if f["field"] == "contact.phone":
            val = re.sub(r"^(?:\+39|0039)(?=[03])", "", val)     # '+393386316126' and '3386316126' are one number
        key = (f["field"], val)
        c = target["contacts"].setdefault(key, {"n": 0, "fact": f})
        c["n"] += 1
    # OCR variants of the same address across subjects ('planidellamarina' vs 'pianidellamarina'): keep the most frequent
    allc: dict[tuple, int] = {}
    for e in ents:
        for key, c in e["contacts"].items():
            allc[key] = allc.get(key, 0) + c["n"]
    canon: dict[tuple, tuple] = {}
    for key in sorted(allc, key=lambda k: -allc[k]):
        twin = next((k for k in canon.values() if k[0] == key[0] and k != key and allc[k] >= allc[key]
                     and _lev(k[1], key[1]) <= 2), None)
        canon[key] = twin or key
    for e in ents:
        for key in list(e["contacts"]):
            if canon[key] != key:
                del e["contacts"][key]
    owners: dict[tuple, list[tuple]] = {}
    for e in ents:
        for key, c in e["contacts"].items():
            owners.setdefault(key, []).append((c["n"] + (0.5 if e["kind"] == "person" else 0), id(e)))
    for e in ents:
        for key in list(e["contacts"]):
            best = max(owners[key])[1]
            if best != id(e):
                del e["contacts"][key]       # a contact belongs to one subject only: the one it is printed with most often
        items = sorted(e["contacts"].items(), key=lambda kv: -kv[1]["n"])
        for i, (k1, c1) in enumerate(items):
            for k2, c2 in items[:i]:
                if k1[0] == k2[0] and k2 in e["contacts"] and k1 in e["contacts"] and _lev(k1[1], k2[1]) <= 2:
                    e["contacts"][k2]["n"] += c1["n"]
                    del e["contacts"][k1]    # OCR variant ('planidellamarina' / 'pianidellamarina')
                    break
    kind_text = {"contact.pec": "PEC", "contact.email": "E-mail", "contact.phone": "Telefono"}
    main, others = [], []
    for e in ents:
        name = max(e["names"], key=lambda n: (e["names"][n], not n.isupper()))
        display = _pretty(e["registry"]["name"]) if e["registry"] else _pretty(name)
        contacts = sorted(e["contacts"].items(), key=lambda kv: (["contact.pec", "contact.email", "contact.phone"].index(kv[0][0]),
                                                                -kv[1]["n"]))
        item = {"name": display, "kind": "società" if e["kind"] == "company" else "persona", "roles": e["roles"],
                "vat": e["vat"], "vat_group": e.get("vat_group", False),
                "verified": bool(e["registry"]),
                "office": (e["registry"].get("address") or "").title() if e["registry"] else None,
                "contacts": [{"type": kind_text[f], "value": v, "occurrences": c["n"],
                              "source": _example_source(c["fact"], titles)} for (f, v), c in contacts[:6]],
                "source": e["role_source"] or ({"document": "registro europeo VIES", "url": "https://ec.europa.eu/taxation_customs/vies/"}
                                               if e["registry"] else None),
                "_rank": (0 if "Proponente" in e["roles"] else 1 if e["roles"] else 2 if e["registry"] else 3, -e["mentions"]
                          - sum(c["n"] for c in e["contacts"].values()))}
        if e["roles"]:
            main.append(item)
        elif item["contacts"] and e["kind"] == "company":
            others.append(item)
        elif item["contacts"] and e["kind"] == "person":
            others.append(item)
    main.sort(key=lambda x: x.pop("_rank"))
    others.sort(key=lambda x: (not x["verified"], x.pop("_rank")))
    return main[:10], others[:12], dropped


# --------------------------------------------------------------------------------------------- location
def _location(d: dict, p: dict, record: dict, titles: dict) -> dict:
    raw = p.get("municipalities") or [m.strip() for m in re.split(r",|;", record.get("municipalities") or "") if m.strip()]
    provs_in = [m.group(1) for x in raw for m in [re.search(r"\(([A-Z]{2})\)\s*$", x)] if m]
    raw = [re.sub(r"(?i)^.*\bcomun[ei]\s+d[ie]\s+", "", m).strip() for m in raw]   # 'ricadenti anche nel comune di Lucera'
    raw = [m for m in raw if m]
    munis = [re.sub(r"\s*\([A-Z]{2}\)\s*$", "", m) for m in raw]
    prov = ", ".join(dict.fromkeys(provs_in)) if len(set(provs_in)) > 1 else (p.get("province") or record.get("province"))
    if len(set(provs_in)) > 1:
        munis = raw                                     # 'Melara (RO), Ostiglia (MN)': keep each province
    region = p.get("region") or record.get("region")
    if munis and (not prov or not region):
        from .admin_areas import locate                 # ISTAT list: 'Loreo, Adria' -> RO, Veneto
        found = locate(raw, prov or "", region or "")
        if found:
            prov = prov or found["province"]
            region = region or found["region"].split(" / ")[0].split("/")[0]
    point, kind, src, conn, lots = _site_point(d, titles)
    from urllib.parse import quote_plus
    if point:
        maps = f"https://www.google.com/maps/search/?api=1&query={point[0]:.5f},{point[1]:.5f}"
    else:
        maps = "https://www.google.com/maps/search/?api=1&query=" + quote_plus(
            ", ".join([re.sub(r"\s*\(.*\)$", "", munis[0])] + [x for x in (prov if len(set(provs_in)) <= 1 else None, region) if x])) if munis else None
    return {"regione": region, "provincia": prov, "comuni": munis,
            "coordinate": {"lat": round(point[0], 5), "lon": round(point[1], 5), "tipo": kind, "source": src} if point else None,
            "maps_url": maps, "maps_precision": "coordinate" if point else "comune",
            "mappa_ufficiale": (p.get("raw") or {}).get("official_map_url"),
            "connessione": conn, "lotti": lots}


# weight of each kind of point as evidence of where the plant is
SITE_WEIGHT = {"official_outline": 12, "site_point": 10, "centroid": 9, "map_frame_small": 6, "site_bbox_corner": 5, "area_vertex": 5, "polygon": 5,
               "cabin": 3, "bess": 3, "survey_point": 3, "simulation_point": 3, "map_frame_large": 2, "unknown": 2}
SITE_TEXT = {"official_outline": "centro del perimetro pubblicato dal portale regionale",
             "site_point": "coordinate del sito dichiarate negli elaborati",
             "centroid": "baricentro dell'impianto dichiarato negli elaborati",
             "map_frame_small": "centro della tavola georeferenziata di progetto",
             "polygon": "centro dei perimetri ricostruiti dalle tabelle dei vertici",
             "area_vertex": "centro dei vertici delle aree d'impianto",
             "site_bbox_corner": "centro dell'area d'impianto (spigoli dichiarati)"}


def _site_point(d: dict, titles: dict):
    """Plant position: the best-supported cluster of coordinates, led by its most reliable point.
    Substations and connection points are kept apart: they are often kilometres away from the plant."""
    import math
    cands, conn = [], []
    for f in d["facts"]:
        q = f.get("qualifiers") or {}
        obj = q.get("object", "unknown")
        if f["field"] == "geo.latlon_point":
            lat, lon = f["value"]["lat"], f["value"]["lon"]
        elif f["field"] == "geo.utm_point" and q.get("lat") is not None:
            lat, lon = q["lat"], q["lon"]
        elif f["field"] == "geo.area_polygon" and (f["value"].get("geojson") or {}).get("coordinates"):
            ring = f["value"]["geojson"]["coordinates"][0]
            lat, lon = sum(c[1] for c in ring) / len(ring), sum(c[0] for c in ring) / len(ring)
            obj = "official_outline" if q.get("object") == "official_outline" else "polygon"
        elif f["field"] == "geo.map_extent":
            v = f["value"]
            lat, lon = v["lat"], v["lon"]
            obj = "map_frame_small" if max(v["width_km"], v["height_km"]) <= 6 else "map_frame_large"
        else:
            continue
        if not (ITALY[0] <= lat <= ITALY[1] and ITALY[2] <= lon <= ITALY[3]):
            continue
        if obj in ("substation", "connection_point"):
            conn.append((lat, lon, f))
        elif obj in SITE_WEIGHT:
            cands.append((lat, lon, f, obj, _lot_label(f)))
    point = kind = src = None
    if cands:
        near = lambda a, b: abs(a[0] - b[0]) < 0.027 and abs(a[1] - b[1]) < 0.027 / max(0.3, math.cos(math.radians(a[0])))
        best = max(cands, key=lambda c: (sum(SITE_WEIGHT[o[3]] for o in cands if near(c, o)), SITE_WEIGHT[c[3]]))
        group = [o for o in cands if near(best, o)]
        top = max(SITE_WEIGHT[o[3]] for o in group)
        lead = [o for o in group if SITE_WEIGHT[o[3]] == top]
        point = (sum(o[0] for o in lead) / len(lead), sum(o[1] for o in lead) / len(lead))
        kind = SITE_TEXT.get(lead[0][3], "posizione indicativa ricavata dalle coordinate negli elaborati")
        if len(group) > 1:
            kind += f" ({len(group)} riferimenti concordi)"
        src = _example_source(lead[0][2], titles)
    connection = None
    if conn:
        lat = sorted(c[0] for c in conn)[len(conn) // 2]
        lon = sorted(c[1] for c in conn)[len(conn) // 2]
        f = min(conn, key=lambda c: abs(c[0] - lat) + abs(c[1] - lon))[2]
        if not point or abs(lat - point[0]) + abs(lon - point[1]) > 0.004:
            connection = {"lat": round(lat, 5), "lon": round(lon, 5), "source": _example_source(f, titles),
                          "maps_url": f"https://www.google.com/maps/search/?api=1&query={lat:.5f},{lon:.5f}"}
    # one position per lot, when the documents tie coordinates to at least two distinct lots
    lots = []
    if point:
        by_lot: dict[str, list] = {}
        for c in cands:
            if c[4] and abs(c[0] - point[0]) < 0.15 and abs(c[1] - point[1]) < 0.2:      # within ~15 km of the plant
                by_lot.setdefault(c[4], []).append(c)
        if len(by_lot) >= 2:
            for lab in sorted(by_lot, key=lambda x: (re.sub(r"\s.*", "", x), x.split()[-1].zfill(3))):
                pts = by_lot[lab]
                top = max(SITE_WEIGHT[o[3]] for o in pts)
                lead = [o for o in pts if SITE_WEIGHT[o[3]] == top]
                la, lo = sum(o[0] for o in lead) / len(lead), sum(o[1] for o in lead) / len(lead)
                lots.append({"lotto": lab, "lat": round(la, 5), "lon": round(lo, 5),
                             "maps_url": f"https://www.google.com/maps/search/?api=1&query={la:.5f},{lo:.5f}",
                             "source": _example_source(lead[0][2], titles)})
    return point, kind, src, connection, lots[:12]


def _lot_label(f: dict) -> str | None:
    """Lot / area label of a coordinate: 'Lotto B' as written, or the area id of a vertex table ('Area 2')."""
    subj = (f.get("subject") or "").strip()
    if not subj:
        return None
    subj = subj.split("#")[0]
    if re.fullmatch(r"[A-Z]{1,2}\d?|\d{1,2}|[IVX]{1,4}", subj):
        return f"Area {subj}"
    if re.match(r"(?i)(lotto|area|campo|sottocampo|sottoimpianto|impianto|sezione|blocco)\s", subj):
        return subj[:1].upper() + subj[1:]
    return None


# --------------------------------------------------------------------------------------------- schedule
def _gantt(d: dict, titles: dict) -> dict:
    """A Gantt only from phases with a real start and end; otherwise the overall duration and the schedule document."""
    by_page: dict[tuple, list[dict]] = {}
    for f in d["facts"]:
        if f["field"] == "schedule.phase" and (f.get("qualifiers") or {}).get("method") in ("vector_gantt", "dated_table", "raster_gantt"):
            c = f["citation"]
            # pages of the same document and method form one programme (MS Project prints continue on the next page)
            by_page.setdefault((c["url"], f["qualifiers"].get("method")), []).append(f)
    bars, unit, src, method = [], "month", None, None
    if by_page:
        dur0 = _pick(d, "schedule.duration.construction", titles)
        dur_m = dur0["value"].get("max") if dur0 and isinstance(dur0["value"], dict) else None

        def rank(kv):
            (u, _), fs_ = kv
            methods = {f["qualifiers"].get("method") for f in fs_}
            reliability = 2 if "dated_table" in methods else 1 if "vector_gantt" in methods else 0
            span = max(f["value"]["end_month"] for f in fs_) - min(f["value"]["start_month"] for f in fs_)
            # a chart covering the stated construction time is the general programme, not a partial one (residual works)
            fits = dur_m is None or abs(span - dur_m) <= 0.4 * dur_m
            general = all((f.get("qualifiers") or {}).get("scope", "general") == "general" for f in fs_)
            # completeness first (a 6-row read of a coded chart loses to a 13-phase programme), then reliability
            # the plant's programme (modules, trackers) wins over the substation's or the grid connection's
            plant = any(re.search(r"(?i)modul|tracker|inseguitor|pannell|fotovolt|\bFV\b|\bpali\b", f["value"]["name"]) for f in fs_)
            return (general, plant, min(len(fs_), 10), reliability, bool(SCHED_TITLE.search(titles.get(u) or "")), fits, len(fs_))
        decom = re.compile(r"(?i)smont|dismiss|rimozion|demoliz|smaltim|ripristin")
        works = re.compile(r"(?i)cantier|scav|posa|montagg|install|recinz|cavidot|cabin|modul|tracker|struttur|collaud|"
                           r"viabil|opere|pali|infiss|conness|inverter|commission|progett|autorizz|realizz|lavor|fornitur")
        build = {k: v for k, v in by_page.items()
                 if sum(bool(decom.search(f["value"]["name"])) for f in v) < 0.4 * len(v)        # decommissioning charts
                 and (SCHED_TITLE.search(titles.get(k[0]) or "")
                      or sum(bool(works.search(f["value"]["name"])) for f in v) >= 0.3 * len(v))}   # other tables
        if not build:
            by_page = {}
    if by_page:
        (url, _m), fs = max(build.items(), key=rank)
        page = min(f["citation"]["page"] for f in fs)
        method = ("dated_table" if any(f["qualifiers"].get("method") == "dated_table" for f in fs)
                  else "vector_gantt" if any(f["qualifiers"].get("method") == "vector_gantt" for f in fs) else "raster_gantt")
        fs = [f for f in fs if f["qualifiers"].get("method") == method]
        # continuation pages of the same programme stay; decommissioning pages are a different programme
        pages: dict[int, list] = {}
        for f in fs:
            pages.setdefault(f["citation"]["page"], []).append(f)
        keep = [pg for pg, v in pages.items() if sum(bool(decom.search(f["value"]["name"])) for f in v) < 0.4 * len(v)]
        fs = [f for f in fs if f["citation"]["page"] in keep] or fs
        fs.sort(key=lambda f: (f["citation"]["page"], f["value"].get("order", 0)))
        axis_labels = next((f["value"]["axis_labels"] for f in fs if f["value"].get("axis_labels")), [])
        indents = sorted({round(f["value"]["indent"], 0) for f in fs if f["value"].get("indent") is not None})
        lv = []
        for x in indents:
            if not lv or x - lv[-1] > 3:
                lv.append(x)
        unit = "month" if all(f["value"].get("unit_known", True) for f in fs) else fs[0]["value"].get("unit") or "period"
        seen = set()
        for f in fs:
            v = f["value"]
            key = (v["name"].lower(), round(v["start_month"], 1), round(v["end_month"], 1))
            if key in seen:
                continue                     # same activity printed twice; a phase and its homonymous task stay
            seen.add(key)
            level = (next((i for i, x in enumerate(lv) if abs(x - v["indent"]) <= 3), 0)
                     if v.get("indent") is not None and lv else v.get("level", 0))
            bars.append({"name": _phase_name(v["name"]), "start": v["start_month"], "end": v["end_month"],
                         "level": min(level, 3), "page": f["citation"]["page"], **({"duration": v["duration"]} if v.get("duration") else {}),
                         **({"dates": [v["start_date"], v["end_date"]]} if v.get("start_date") else {})})
        for i, bb in enumerate(bars):
            bb["summary"] = i + 1 < len(bars) and bars[i + 1]["level"] > bb["level"]
        # a bare number after the name is the duration column, in the chart's unit ('trasformatori e quadri 46')
        tail = [re.search(r"\s(\d{1,4})$", bb["name"]) for bb in bars]
        if sum(bool(t) for t in tail) >= 0.5 * len(bars):
            # unit of the column from the bars themselves: number / length in months ~30 days, ~4.3 weeks, ~1 month
            ratios = sorted(int(t.group(1)) / (bb["end"] - bb["start"]) for bb, t in zip(bars, tail)
                            if t and bb["end"] - bb["start"] > 0.3 and int(t.group(1)) > 0)
            r = ratios[len(ratios) // 2] if ratios else 0
            word = "g" if r > 15 else "sett." if r > 2.5 else "mesi" if r > 0.5 else \
                {"week": "sett.", "month": "mesi", "day": "g"}.get(fs[0]["value"].get("unit"), "")
            for bb, t in zip(bars, tail):
                if t:
                    bb["name"] = bb["name"][: t.start()].strip()
                    bb.setdefault("duration", f"{t.group(1)} {word}".strip())
        # pages of one document printed as separate programmes (plant, substation): a heading between them
        out_bars, prev_end, prev_page, names = [], 0.0, None, set()
        for bb in bars:
            pg = bb.pop("page", None)
            restart = (not bb.get("dates")) and bb["start"] < 0.15 * prev_end and (pg != prev_page or bb["name"].lower() in names)
            if prev_page is not None and restart:     # time restarts: a second programme, not a continuation
                out_bars.append({"name": "Altra sezione del cronoprogramma" + (f" (pag. {pg})" if pg != prev_page else ""),
                                 "separator": True, "start": 0, "end": 0, "level": 0})
                names = set()
            names.add(bb["name"].lower())
            out_bars.append(bb)
            prev_end = bb["end"] if (restart and prev_page is not None) else max(prev_end, bb["end"])
            prev_page = pg
        bars = out_bars
        src = _source({"url": url, "page": page}, titles)
        span = max(b["end"] for b in bars) - min(b["start"] for b in bars) if bars else 0
        if method != "dated_table" and dur_m and span > 2 * dur_m and abs(span * 12 / 52 - dur_m) <= 0.4 * dur_m:
            for b in bars:                   # the axis counts weeks, not months (checked against the stated duration)
                b["start"], b["end"] = round(b["start"] * 12 / 52, 2), round(b["end"] * 12 / 52, 2)
    else:
        # month ranges written in the text of a programme of works, at least three distinct phases
        phases: dict[str, dict] = {}
        for f in d["facts"]:
            if f["field"] != "schedule.phase" or (f.get("qualifiers") or {}).get("method"):
                continue
            if not SCHED_TITLE.search(titles.get(f["citation"]["url"]) or ""):
                continue
            v = f["value"]
            ph = phases.setdefault(v["name"], {"counts": {}, "f": f})
            key = (v["start_month"], v["end_month"])
            ph["counts"][key] = ph["counts"].get(key, 0) + 1
        if len(phases) >= 3:
            for name, ph in phases.items():
                (a, b), _ = max(ph["counts"].items(), key=lambda kv: kv[1])
                bars.append({"name": name, "start": a, "end": b})
            bars.sort(key=lambda b: (b["start"], b["end"]))
            src = _example_source(next(iter(phases.values()))["f"], titles)
    phase_durations = []
    if True:
        by_doc: dict[str, list[dict]] = {}
        for f in d["facts"]:
            if f["field"] == "schedule.phase_duration":
                by_doc.setdefault(f["citation"]["url"], []).append(f)
        if by_doc:
            u, fs = max(by_doc.items(), key=lambda kv: (bool(SCHED_TITLE.search(titles.get(kv[0]) or "")), len(kv[1])))
            seen = set()
            for f in sorted(fs, key=lambda f: (f["citation"]["page"], f["value"]["order"])):
                v = f["value"]
                if v["name"].lower() in seen or re.match(r"(?i)durata\b", v["name"]):     # totals are shown apart
                    continue
                seen.add(v["name"].lower())
                raw = re.sub(r"\s(?:g|gg|d)$", " giorni", v["raw"])                 # '30 g' -> '30 giorni' (not 'giorniiorni')
                phase_durations.append({"name": v["name"], "duration": raw, "months": v["months"]})
            dur_src = _source({"url": u, "page": fs[0]["citation"]["page"]}, titles)
            if not bars:
                src = dur_src
    if bars and phase_durations and len(bars) < 0.5 * len(phase_durations):
        bars, src = [], dur_src          # a partial read of the drawing: the full list of phases is more useful
    if bars:
        phase_durations = []
    dur = _pick(d, "schedule.duration.construction", titles)
    milestones = []
    for field in ("schedule.date.construction_start", "schedule.date.construction_start_extension",
                  "schedule.date.construction_end", "schedule.date.commercial_operation"):
        pk = _pick(d, field, titles)
        if pk:
            temporal = pk["temporal"]
            milestones.append({"label": LABELS[field], "date": pk["text"], "iso": pk["value"],
                               "nature": ", ".join(TEMPORAL_TEXT.get(t, t) for t in temporal),
                               "certain": "event_occurred" in temporal, "source": pk["source"]})
    milestones.sort(key=lambda m: m["iso"])
    sched_doc = next(({"document": t, "url": u} for u, t in titles.items() if t and SCHED_TITLE.search(t)), None)
    chosen = (src or {}).get("url")
    others = [{"document": t, "url": u} for u, t in titles.items()
              if t and SCHED_TITLE.search(t) and u != chosen][:5]
    months = max([b["end"] for b in bars] + [1])
    note = None
    if not bars and phase_durations:
        note = "Il cronoprogramma riporta solo le durate delle fasi, senza date né sequenza: non è possibile disegnare il Gantt."
    elif not bars:
        note = ("Le fasi del cronoprogramma non sono leggibili automaticamente (allegato in formato immagine o non presente)."
                + (" Il documento è indicato sotto." if sched_doc else ""))
    axis_caption, axis_ticks = None, []
    axis_unit = {"week": "settimane", "month": "mesi", "day": "giorni"}.get(
        next((f["value"].get("unit") for f in d["facts"] if f["field"] == "schedule.phase" and f["value"].get("axis_labels")), None))
    if bars and all(b.get("dates") for b in bars):
        # dated programme: calendar months on the axis, as printed in MS Project timescales
        from datetime import date
        d0 = min(date.fromisoformat(b["dates"][0]) for b in bars)
        names = ["gen", "feb", "mar", "apr", "mag", "giu", "lug", "ago", "set", "ott", "nov", "dic"]
        y, m = d0.year, d0.month
        offset = (d0.day - 1) / 30.4
        for i in range(int(max(b["end"] for b in bars) + offset) + 1):
            axis_ticks.append({"label": f"{names[m - 1]} {str(y)[2:]}", "start": round(i - offset, 3), "end": round(i + 1 - offset, 3)})
            m += 1
            if m == 13:
                y, m = y + 1, 1
    if bars and by_page and not axis_ticks and axis_labels:
        words = [x["label"].split(" ", 1) for x in axis_labels]
        if all(len(w) == 2 for w in words) and len({w[0] for w in words}) == 1:
            axis_caption = words[0][0]                    # 'Mese' printed once, numbers on the ticks as in the document
            axis_ticks = [{"label": w[1], "start": x["start"], "end": x["end"]} for w, x in zip(words, axis_labels)]
        else:
            axis_ticks = [{"label": x["label"], "start": x["start"], "end": x["end"]} for x in axis_labels]
    return {"bars": bars, "unit": unit, "months": months, "source": src,
            "asse": {"titolo": axis_caption, "etichette": axis_ticks, "unita": axis_unit} if axis_ticks else None,
            "duration": {"text": dur["text"], "status": dur["status"], "hint": _hint(dur), "source": dur["source"]} if dur else None,
            "milestones": milestones, "document": sched_doc, "note": note, "altri": others,
            "durate_fasi": phase_durations[:30],
            "metodo": {"dated_table": "tabella con date", "vector_gantt": "diagramma vettoriale",
                       "raster_gantt": "diagramma in immagine (lettura automatica, da verificare)"}.get(method) if bars else None}



# --------------------------------------------------------------------------------------------- timing
ANCHORED = ("rilascio", "notifica", "pubblicazione", "efficacia", "emanazione", "adozione", "presente")


def _d(iso: str):
    from datetime import date
    y, m, dd = (iso.split("-") + ["1", "1"])[:3]
    return date(int(y), int(m), int(dd or 1))


def _add_months(day, months: float):
    from datetime import date
    import calendar
    whole = int(months)
    y, m = divmod(day.month - 1 + whole, 12)
    y, m = day.year + y, m + 1
    return date(y, m, min(day.day, calendar.monthrange(y, m)[1]))


def _it(day, month_only: bool = False) -> str:
    names = ["gennaio", "febbraio", "marzo", "aprile", "maggio", "giugno", "luglio", "agosto", "settembre", "ottobre", "novembre", "dicembre"]
    return f"{names[day.month - 1]} {day.year}" if month_only else day.strftime("%d/%m/%Y")


def _authorisation(d: dict, record: dict, p: dict, titles: dict) -> dict | None:
    """The act that authorises the works: the latest authorisation act read in the documents or in the
    procedure title. Nothing is assumed when no such act is written anywhere."""
    from .extract.schedule import ACT_RE, AUTH_CTX, _parse_date
    acts = []
    # an authorisation of THIS project is the act itself, or an act the procedure documents keep citing:
    # a one-off citation (another company's decree, a list of nearby projects) is not enough
    from collections import defaultdict
    cited_in = defaultdict(set)
    third_party = re.compile(r"(?i)osservazion|lista[\s_]+(?:di[\s_]+)?controllo|controdeduz")
    facts = [f for f in d["facts"] if not third_party.search(titles.get(f["citation"]["url"]) or "")]
    for f in facts:
        if f["field"] == "act.issue" and (f.get("qualifiers") or {}).get("kind") == "authorisation":
            cited_in[(f["value"]["number"], f["value"]["date"])].add(f["citation"]["url"])
    for f in facts:
        if f["field"] == "act.issue" and (f.get("qualifiers") or {}).get("kind") == "authorisation" and (
                f.get("subject") == "this_document" or len(cited_in[(f["value"]["number"], f["value"]["date"])]) >= 2):
            v = f["value"]
            acts.append({"type": v["type"], "number": v["number"], "date": v["date"], "own": f.get("subject") == "this_document",
                         "source": _source({"url": f["citation"]["url"], "page": f["citation"].get("page"),
                                            "excerpt": f["citation"].get("excerpt")}, titles)})
    for title in {record.get("title"), p.get("title")} - {None}:
        for m in ACT_RE.finditer(title):
            iso = _parse_date(m.group("date"))
            if iso and int(iso[:4]) >= 2015 and AUTH_CTX.search(title):
                acts.append({"type": re.sub(r"\s+", " ", m.group("type")).strip(), "number": m.group("num").strip(), "date": iso,
                             "own": True, "source": {"url": p.get("info_url"), "page": None, "document": "titolo del procedimento",
                                                     "excerpt": m.group(0)[:200]}})
    # an act cannot authorise a project before the project exists: drop acts older than the first filed document
    filed = sorted(e["index_date"] for e in d.get("inventory", []) if e.get("index_date"))
    if filed:
        floor = f"{int(filed[0][:4]) - 1}{filed[0][4:]}"
        acts = [a for a in acts if a["date"] >= floor]
    if not acts:
        return None
    # the procedure named after its act (Lazio 'Determinazione n° G00146 del ...', PAUR titles) is that act;
    # otherwise the decisive act is the most recent one, preferring the act's own document
    titled = [a for a in acts if a["source"].get("document") == "titolo del procedimento"]
    if titled:
        return max(titled, key=lambda a: a["date"])
    return max(acts, key=lambda a: (a["date"], a["own"]))


ACT_DOC = re.compile(r"(?i)decret|determin|provvediment|autorizzazion|P\.?A\.?U\.?R|parere|prescrizion|esito|delibera|atto")


def _deadline(d: dict, kind: str, titles: dict, prefer_url: str | None) -> dict | None:
    # deadlines bind only when written in the authorising act or in another act/opinion of the procedure,
    # not in a study quoting the rules of other projects
    cands = [f for f in d["facts"] if f["field"] == f"schedule.deadline.{kind}"
             and (kind in ("validity", "end_after_start") or (f["value"].get("anchor") or "") in ANCHORED)
             and (f["citation"]["url"] == prefer_url or ACT_DOC.search(titles.get(f["citation"]["url"]) or ""))]
    if not cands:
        return None
    from collections import Counter
    count = Counter(f["value"]["months"] for f in cands)
    best = max(cands, key=lambda f: (f["citation"]["url"] == prefer_url, count[f["value"]["months"]]))
    v = best["value"]
    return {"months": v["months"], "text": re.sub(r"\s*\(\s*\w+\s*\)", "", v["raw"]).strip(), "anchor": v.get("anchor"),
            "occurrences": count[v["months"]],
            "source": _source({"url": best["citation"]["url"], "page": best["citation"].get("page"),
                               "excerpt": best["citation"].get("excerpt")}, titles)}


def _timing(d: dict, record: dict, p: dict, titles: dict, gantt: dict, tiles: list[dict]) -> dict:
    """When the works happen, at three levels of evidence, never mixed:
    documentata (dates written in the documents), prescritta (act date + deadline imposed by the act),
    stimata (act date + construction duration of the project; labelled as an estimate, shown as a range)."""
    rows, notes = [], []
    auth = _authorisation(d, record, p, titles)
    # documented dates
    auth_iso = auth["date"] if auth else None
    for m in gantt.get("milestones") or []:
        nature = m.get("nature") or ""
        if re.search(r"(?i)piano di sicurezza|notifica|scenario|template", nature):
            continue                               # template fields and scenarios are not project dates
        if auth_iso and not m.get("certain") and m.get("iso", "9999") < auth_iso and m["label"] != "Proroga inizio lavori":
            continue                               # planned dates older than the authorisation: obsolete plans
        rows.append({"voce": m["label"], "livello": "documentata", "valore": m["date"],
                     "dettaglio": nature or None, "source": m.get("source")})
    dated = [b for b in gantt.get("bars") or [] if b.get("dates")]
    if dated:
        a = min(_d(b["dates"][0]) for b in dated)
        b = max(_d(b["dates"][1]) for b in dated)
        rows.append({"voce": "Cronoprogramma con date", "livello": "documentata", "valore": f"{_it(a)} → {_it(b)}",
                     "dettaglio": "date del programma lavori: un piano, non l'avanzamento effettivo", "source": gantt.get("source")})
    # construction duration: the programme's own span first, else the most frequent plausible duration
    dur_m, dur_src, dur_text = None, None, None
    if gantt.get("bars") and gantt.get("unit") == "month" and 3 <= (gantt.get("months") or 0) <= 48:
        dur_m, dur_src = float(gantt["months"]), gantt.get("source")
        dur_text = f"{dur_m:.0f} mesi (cronoprogramma)"
    else:
        t = next((t for t in tiles if t["key"] == "cantiere" and t["value"] != "non indicata"), None)
        mm = re.search(r"(\d+)\s*mesi|circa\s+(\d+)\s+mesi", (t or {}).get("value") or "")
        if t and mm:
            dur_m, dur_src, dur_text = float(mm.group(1) or mm.group(2)), t.get("source"), t["value"]
    if auth:
        A = _d(auth["date"])
        rows.append({"voce": "Atto autorizzativo", "livello": "documentata",
                     "valore": f"{auth['type']} n. {auth['number']} del {_it(A)}", "dettaglio": None, "source": auth["source"]})
        prefer = (auth.get("source") or {}).get("url")
        ds, de = _deadline(d, "start", titles, prefer), _deadline(d, "end", titles, prefer)
        dea, dv = _deadline(d, "end_after_start", titles, prefer), _deadline(d, "validity", titles, prefer)
        start_by = _add_months(A, ds["months"]) if ds else None
        end_by = _add_months(A, de["months"]) if de else (_add_months(start_by, dea["months"]) if start_by and dea else None)
        if ds:
            rows.append({"voce": "Inizio lavori", "livello": "prescritta", "valore": f"entro il {_it(start_by)}",
                         "dettaglio": f"«{ds['text']}» dall'atto del {_it(A)}", "source": ds["source"]})
        if end_by:
            src = de or dea
            rows.append({"voce": "Fine lavori", "livello": "prescritta", "valore": f"entro il {_it(end_by)}",
                         "dettaglio": f"«{src['text']}»" + (f" dall'atto del {_it(A)}" if de else " dall'inizio dei lavori"),
                         "source": src["source"]})
        if dv and not end_by:
            valid_to = _add_months(A, dv["months"])
            rows.append({"voce": "Validità dell'atto", "livello": "prescritta", "valore": f"fino al {_it(valid_to)}",
                         "dettaglio": f"«{dv['text']}»: i lavori vanno eseguiti entro questo termine", "source": dv["source"]})
        started = next((m for m in gantt.get("milestones") or [] if m["label"] == "Inizio lavori" and m.get("certain")), None)
        # estimate: a range, from the act date (works cannot start earlier) to the prescribed start deadline;
        # when the start of works is documented as an event, only the end is estimated, from that date
        if dur_m and started:
            rows.append({"voce": "Fine lavori", "livello": "stimata",
                         "valore": f"intorno a {_it(_add_months(_d(started['iso']), dur_m), True)}",
                         "dettaglio": f"inizio documentato + durata del cantiere: {dur_text}", "source": dur_src})
        elif dur_m:
            lo_end = _add_months(A, dur_m)
            hi_start = start_by
            hi_end = _add_months(start_by, dur_m) if start_by else None
            if end_by and hi_end and hi_end > end_by:
                hi_end = end_by
            rows.append({"voce": "Inizio lavori", "livello": "stimata",
                         "valore": f"tra {_it(A, True)} e {_it(hi_start, True)}" if hi_start else f"non prima di {_it(A, True)}",
                         "dettaglio": "non prima dell'atto autorizzativo" + (", non oltre il termine prescritto" if hi_start else
                                                                              "; nessun termine di inizio letto negli atti"),
                         "source": auth["source"]})
            rows.append({"voce": "Fine lavori", "livello": "stimata",
                         "valore": f"tra {_it(lo_end, True)} e {_it(hi_end, True)}" if hi_end else f"non prima di {_it(lo_end, True)}",
                         "dettaglio": f"inizio stimato + durata del cantiere: {dur_text}", "source": dur_src})
        else:
            notes.append("Durata del cantiere non leggibile negli elaborati: fine lavori non stimabile.")
    else:
        starts = [r for r in rows if r["voce"] == "Inizio lavori" and r["livello"] == "documentata"]
        if starts and dur_m:
            try:
                s0 = _d(next(m["iso"] for m in gantt.get("milestones") or [] if m["label"] == "Inizio lavori"))
                rows.append({"voce": "Fine lavori", "livello": "stimata", "valore": f"non prima di {_it(_add_months(s0, dur_m), True)}",
                             "dettaglio": f"inizio indicato nei documenti + durata del cantiere: {dur_text}", "source": dur_src})
            except StopIteration:
                pass
        notes.append("Nessun atto autorizzativo nei documenti analizzati: procedimento in corso o atto non pubblicato; "
                     "le date di cantiere non sono stimabili.")
    return {"righe": rows, "durata_cantiere": dur_text, "autorizzazione": auth and {
                "atto": f"{auth['type']} n. {auth['number']}", "data": _it(_d(auth["date"])), "source": auth["source"]},
            "note": notes,
            "legenda": {"documentata": "data scritta nei documenti", "prescritta": "termine imposto dall'atto, calcolato dalla sua data",
                        "stimata": "stima: atto + durata del cantiere; un intervallo, non una data certa"}}


# --------------------------------------------------------------------------------------------- status
def _status(d: dict, gantt: dict, today=None) -> dict | None:
    """Where the project stands, from planned dates versus documents filed afterwards.

    A planned date in the past does not prove that the plant was built: projects are often delayed,
    extended or sold. So the card says 'potenzialmente realizzato' only when the planned end of works
    has passed and nothing was filed after it; when later acts exist it asks to check the status."""
    from datetime import date, timedelta
    today = today or datetime.now(timezone.utc).date()

    def iso(v):
        try:
            return date.fromisoformat(str(v)[:10])
        except ValueError:
            return None

    def dates(fields, occurred=None):
        out = []
        for g in d["groups"]:
            if g["field"] in fields:
                for x in g["values"]:
                    if isinstance(x, dict) and iso(x.get("value")):
                        t = set(x.get("temporal", []))
                        if occurred is None or (("event_occurred" in t) == occurred):
                            out.append((iso(x["value"]), x))
        return out
    starts = [v for v, _ in dates({"schedule.date.construction_start"}, occurred=False)]
    ends = [v for v, _ in dates({"schedule.date.construction_end", "schedule.date.commercial_operation"}, occurred=False)]
    bars = [b for b in gantt.get("bars", []) if b.get("dates")]
    if bars:
        starts.append(min(iso(b["dates"][0]) for b in bars))
        ends.append(max(iso(b["dates"][1]) for b in bars))
    started = dates({"schedule.date.construction_start"}, occurred=True)
    acts = [v for v, _ in dates({f for f in LABELS if f.startswith("schedule.date.")}, occurred=True)]
    filed = [iso(e.get("index_date")) for e in d.get("inventory", []) if iso(e.get("index_date"))]
    latest = max(acts + filed) if acts + filed else None
    start, end = (max(starts) if starts else None), (max(ends) if ends else None)
    fmt = lambda x: x.strftime("%d/%m/%Y")
    later = f"; ultimo documento o atto del {fmt(latest)}" if latest else ""
    if started:
        v, _ = max(started)
        return {"level": "avviato", "label": "Lavori avviati (documentato)",
                "reason": f"Inizio lavori documentato il {fmt(v)}{later}."}
    if end and end < today and (latest is None or latest <= end + timedelta(days=90)):
        return {"level": "potenzialmente_realizzato", "label": "Potenzialmente realizzato",
                "reason": f"La fine lavori prevista ({fmt(end)}) è passata e non ci sono documenti successivi: "
                          f"verificare se l'impianto è in esercizio."}
    if start and start < today:
        if latest and latest > start:
            return {"level": "date_superate", "label": "Date di progetto superate: verificare lo stato",
                    "reason": f"Avvio previsto il {fmt(start)}, ma l'iter è proseguito{later}: "
                              f"probabile slittamento, non ancora realizzato o in corso."}
        return {"level": "potenzialmente_avviato", "label": "Potenzialmente avviato o realizzato",
                "reason": f"L'avvio dei lavori era previsto il {fmt(start)} e non risultano documenti successivi."}
    if start:
        return {"level": "previsto", "label": f"Avvio lavori previsto il {fmt(start)}",
                "reason": "Data di progetto: non prova l'effettivo avvio."}
    return None


def _register(p: dict) -> dict | None:
    """Last act recorded in a regional plant register (Toscana ATOS), reported as written.

    The register's 'Categoria Atto' says what the act is (an authorisation, a screening decision...):
    the card repeats it and never upgrades it to 'autorizzato'."""
    sh = (p.get("raw") or {}).get("register_sheet")
    if not sh:
        return None
    act = " ".join(x for x in (sh.get("act_type"), f"n. {sh['act_number']}" if sh.get("act_number") else None,
                               f"del {sh['act_date']}" if sh.get("act_date") else None) if x)
    label = (f"Registro impianti: {act}" if act else
             "Registro impianti: nessun atto registrato" if sh.get("no_act") else "Registro impianti regionale")
    bits = [f"{k}: {sh[f]}" for k, f in (("Categoria atto", "act_category"), ("Ente", "act_body"),
                                         ("Tipo di autorizzazione", "authorization_type"),
                                         ("Attivo", "active"), ("Ultima modifica del registro", "updated")) if sh.get(f)]
    linked = (p.get("raw") or {}).get("linked_procedure")
    if linked:
        bits.append(f"documenti dal procedimento collegato: {linked.get('title') or linked['url']}")
    return {"label": label, "reason": "; ".join(bits) + ".", "fonte": (p.get("raw") or {}).get("register_url") or p.get("info_url"),
            "impianto": sh.get("plant"), "ditta": sh.get("company"), "comune": sh.get("municipality"),
            "provincia": sh.get("province"), "fonte_energetica": sh.get("energy_source"),
            "potenza_mw": sh.get("power_mw"), "procedimento_collegato": linked}


# --------------------------------------------------------------------------------------------- card
def build_card(d: dict[str, Any]) -> dict[str, Any]:
    p = d["project"]
    titles = {e["url"]: e.get("title") for e in d.get("inventory", [])}
    record = d.get("canonical_record") or {}
    tiles, open_points = _summary(d, p, record, titles)
    main, others, dropped = _subjects(d, titles, p.get("proponent") or record.get("proponent"), p.get("info_url"))
    if dropped:
        open_points.append({"label": "Recapiti", "reason": f"{dropped} recapiti non attribuibili a un soggetto preciso: non mostrati"})
    for e in main:
        if any("da confermare" in r for r in e["roles"]):
            open_points.append({"label": e["name"], "reason": "ruolo esecutivo indicato negli elaborati, da confermare"})
    loc = _location(d, p, record, titles)
    gantt = _gantt(d, titles)
    status = _status(d, gantt)
    timing = _timing(d, record, p, titles, gantt, tiles)
    if status and status["level"] in ("potenzialmente_realizzato", "potenzialmente_avviato", "date_superate"):
        open_points.insert(0, {"label": "Stato del progetto", "reason": status["reason"]})
    register = _register(p)
    if register:
        if status is None:
            status = {"level": "registro", "label": register["label"], "reason": register["reason"]}
        else:
            status = dict(status, reason=status["reason"] + " " + register["label"] + ".")
    a = d["acquisition"]
    return {
        "schema": CARD_SCHEMA, "dossier_schema": SCHEMA_VERSION,
        "project_key": (d.get("canonical", {}).get("project_keys") or [None])[0] or record.get("project_key"),
        "title": record.get("title") or p.get("title"),
        "source_url": p.get("info_url"), "source": p.get("source"),
        "updated": datetime.now(timezone.utc).date().isoformat(),
        "sintesi": tiles,
        "ubicazione": loc,
        "soggetti": main,
        "altri_soggetti": others,
        "stato": status,
        "registro": register,
        "tempistica": timing,
        "cronoprogramma": gantt,
        "documentazione": {"documenti": a["references"], "pagine": a["pages_total"],
                           "completa": d["gates"]["acquisition"]["pass"]},
        "confronto": [{"campo": r["field"], "valore_attuale": r.get("canonical"), "esito": r["class"]} for r in d.get("delta", [])],
        "da_verificare": open_points[:10],
        "avvertenza": "Dati estratti automaticamente dalla documentazione pubblica del procedimento: ogni valore rimanda "
                      "a documento e pagina. I valori «da verificare» vanno controllati prima dell'uso.",
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



PUBLIC_MAIL = re.compile(r"(?i)\.gov\.it$|governo\.it|interno\.it|agenziademanio|autoritadistretto|astral|regione|comune|provincia|"
                         r"prefettura|\bente|arpa|ats-|arsial|cultura|beniculturali|mite|mase|"
                         r"minambiente|terna\.it|e-distribuzione|enel\.it$|asl|vigilfuoco|marina\.difesa|ispra|anas\.it|"
                         r"snam|consorzio|bonifica|cert\.|postacert\.regione|openfiber|fibercop|telecomitalia|italgas|retegas|acquedott")
PUBLIC_SUBJECT = re.compile(r"(?i)comune|regione|provincia|soprintendenza|arpa|ministero|terna|vigili|asl|consorzio|caserma|ufficio|"
                            r"prefettura|questura|carabinieri|igiene\s+e\s+sanit|www\.regione|\.gov\.it")


def _norm(t: str) -> str:
    return re.sub(r"[^a-z0-9]", "", (t or "").lower())


def _clean_subject(t: str | None) -> str:
    t = (t or "").strip("'\"‘’“” ")
    t = re.split(r"(?i)\b(?:dell[a'’]\s*(?:impresa|ditta|societ[aà])|della\s+(?:ditta|societ[aà]))\s+", t)[-1]
    t = re.sub(r"(?i)[\s,;-]+(?:tel(?:efono)?|cell(?:ulare)?|fax|e-?mail|pec|via|viale|corso|piazza)\b.*$", "", t)
    t = re.sub(r"(?i)^(?:il\s+|la\s+)?(?:responsabile|direttore|legale\s+rappresentante|amministratore)(?:\s+(?:tecnico|unico|del\s+laboratorio|delegato))?\s+", "", t)
    words = t.split()
    while len(words) > 2 and (re.fullmatch(r"[a-z]{1,3}|\d+|[^\w]+", words[0])):
        words.pop(0)                      # OCR debris before the name ('7 arm yy Sinergye Ring S.r.l.')
    t = " ".join(words)
    if COMPANY_FORM.search(t) and PERSON_TITLE.match(t):
        # 'Dott. Ing. Fabio Calcarella Whysol-E Sviluppo S.r.l.' -> the company after the signing professional
        rest = re.sub(r"(?i)^(?:(?:dott|ing|arch|geol|geom|avv|agr|prof|per\.?\s?ind)\.?\s*)+[A-ZÀ-Ýa-zà-ÿ'’]+\s+[A-ZÀ-Ýa-zà-ÿ'’]+\s+", "", t)
        if rest != t and COMPANY_FORM.search(rest) and len(_norm(rest)) >= 6:
            t = rest
    return re.sub(r"^[^\w]+|[\s'\"‘’“”ʼ`´,;]+$", "", t)
