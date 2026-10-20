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
    """Identity key of a company or person: lower case, no accents, no legal form, letters and digits only."""
    t = (name or "").lower().translate(ACCENTS)
    t = PERSON_TITLE.sub("", t)
    t = re.sub(r"(?i)\b(?:agronom[oa]|geolog[oa]|forestale|vet|geol|agr|ing|for)\b\.?", " ", t)
    return re.sub(r"[^a-z0-9]", "", LEGAL_PHRASES.sub(" ", t))


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


def _pick(d: dict, field: str, titles: dict) -> dict | None:
    """Most frequent value of a field (equivalent spellings and durations merged); alternatives kept as hints."""
    g = next((g for g in d["groups"] if g["field"] == field and g["values"] and isinstance(g["values"][0], dict)
              and (not field.startswith("tech.") or g.get("subject") in ("plant", None, "totale"))), None)
    if not g:
        return None
    merged: dict[str, dict] = {}
    for x in g["values"]:
        if not isinstance(x, dict):
            continue
        k = _fmt(field, x["value"])
        if field.startswith("schedule.duration.") and isinstance(x["value"], dict) and x["value"].get("max"):
            mx = x["value"]["max"]
            k = next((kk for kk, mv in merged.items() if isinstance(mv["value"], dict)
                      and abs(mv["value"].get("max", 0) - mx) <= 0.1 * max(mx, mv["value"].get("max", 0))), k)
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


def _hint(p: dict) -> str | None:
    if not p["alternatives"]:
        return None
    lead = "Valore più ricorrente" if p["status"] == "da verificare" else "Valore prevalente"
    return f"{lead} negli elaborati; altri valori: " + ", ".join(p["alternatives"])


# --------------------------------------------------------------------------------------------- summary
def _summary(d: dict, p: dict, record: dict, titles: dict) -> tuple[list[dict], list[dict]]:
    tiles, open_points = [], []
    title = record.get("title") or p.get("title") or ""
    tm = re.search(r"(?i)(\d{1,3}(?:[.\s]\d{3})*(?:,\d+)?|\d+(?:[.,]\d+)?)\s*(MWp|MW|kWp|KwP|kW)\b", title)
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
                note = f"Probabile refuso nel titolo: gli elaborati riportano {_fmt(fld, x['value'])} ({x['occurrences']} volte)."
                open_points.append({"label": "Potenza", "reason": note})
                break
        lots = _pick(d, "tech.lots_count", titles)
        what = next(((f.get("qualifiers") or {}).get("what") for f in d["facts"] if f["field"] == "tech.lots_count"), "lotti")
        sub = note or (f"Suddivisa in {lots['value']} {what}" if lots else None)
        tiles.append({"key": "potenza", "label": "Potenza", "status": "da verificare" if note else "verificato",
                      "value": _fmt("tech.pv_dc_mwp" if unit.lower().endswith("p") else "tech.nominal_power_mw", round(mw, 3)),
                      "sub": sub, "source": {"document": "titolo del procedimento", "url": p.get("info_url"), "excerpt": tm.group(0)}})
    tech = _pick(d, "tech.technology", titles)
    if tech and tech["value"] in ("floating_pv", "offshore") and tech["occurrences"] < 10:
        tech = None                      # passing mentions (alternatives, literature) are not the project's technology
    mount = _pick(d, "tech.mounting", titles)
    kind = " · ".join(x["text"] for x in (tech, mount) if x)
    if kind:
        tiles.append({"key": "impianto", "label": "Impianto", "value": kind[:1].upper() + kind[1:], "status": "da documenti",
                      "source": (tech or mount)["source"]})
    tot, pan = _pick(d, "tech.area_available_ha", titles), _pick(d, "tech.area_panels_ha", titles)
    if tot and pan and pan["value"] >= tot["value"]:
        pan = None                          # a panel area larger than the site is a misread
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
    dur = _pick(d, "schedule.duration.construction", titles)
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
        key = (f["field"], val)
        c = target["contacts"].setdefault(key, {"n": 0, "fact": f})
        c["n"] += 1
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
        if e["roles"] or (e["registry"] and vat_mentions.get(e["vat"], 0) >= 2):
            main.append(item)
        elif item["contacts"] and e["kind"] == "company":
            others.append(item)
        elif item["contacts"] and e["kind"] == "person":
            others.append(item)
    main.sort(key=lambda x: x.pop("_rank"))
    others.sort(key=lambda x: x.pop("_rank"))
    return main[:10], others[:12], dropped


# --------------------------------------------------------------------------------------------- location
def _location(d: dict, p: dict, record: dict, titles: dict) -> dict:
    raw = p.get("municipalities") or [m.strip() for m in re.split(r",|;", record.get("municipalities") or "") if m.strip()]
    provs_in = [m.group(1) for x in raw for m in [re.search(r"\(([A-Z]{2})\)\s*$", x)] if m]
    munis = [re.sub(r"\s*\([A-Z]{2}\)\s*$", "", m) for m in raw]
    prov = ", ".join(dict.fromkeys(provs_in)) if len(set(provs_in)) > 1 else (p.get("province") or record.get("province"))
    if len(set(provs_in)) > 1:
        munis = raw                                     # 'Melara (RO), Ostiglia (MN)': keep each province
    region = p.get("region") or record.get("region")
    # candidate site points: declared centre, area corners, centres of the reconstructed perimeters
    cands = []
    for f in d["facts"]:
        if f["field"] == "geo.latlon_point" and (f.get("qualifiers") or {}).get("object") in ("centroid", "site_bbox_corner"):
            cands.append((f["value"]["lat"], f["value"]["lon"], f, f["qualifiers"]["object"]))
        elif f["field"] == "geo.area_polygon" and (f["value"].get("geojson") or {}).get("coordinates"):
            ring = f["value"]["geojson"]["coordinates"][0]
            cands.append((sum(c[1] for c in ring) / len(ring), sum(c[0] for c in ring) / len(ring), f, "polygon"))
    cands = [c for c in cands if ITALY[0] <= c[0] <= ITALY[1] and ITALY[2] <= c[1] <= ITALY[3]]
    point = kind = src = None
    if cands:
        # the largest cluster (about 3 km) is the plant; isolated points are usually the grid connection or the substation
        best = max(cands, key=lambda c: (sum(abs(c[0] - o[0]) < 0.03 and abs(c[1] - o[1]) < 0.03 for o in cands), c[3] == "centroid"))
        near = [o for o in cands if abs(best[0] - o[0]) < 0.03 and abs(best[1] - o[1]) < 0.03]
        point = (sum(o[0] for o in near) / len(near), sum(o[1] for o in near) / len(near))
        kind = ("centro dell'impianto indicato negli elaborati" if any(o[3] == "centroid" for o in near)
                else "posizione indicativa dell'impianto, ricavata dalle coordinate negli elaborati")
        src = _example_source(near[0][2], titles)
    from urllib.parse import quote_plus
    if point:
        maps = f"https://www.google.com/maps/search/?api=1&query={point[0]:.5f},{point[1]:.5f}"
    else:
        maps = "https://www.google.com/maps/search/?api=1&query=" + quote_plus(
            ", ".join([re.sub(r"\s*\(.*\)$", "", munis[0])] + [x for x in (prov if len(set(provs_in)) <= 1 else None, region) if x])) if munis else None
    return {"regione": region, "provincia": prov, "comuni": munis,
            "coordinate": {"lat": round(point[0], 5), "lon": round(point[1], 5), "tipo": kind, "source": src} if point else None,
            "maps_url": maps, "maps_precision": "coordinate" if point else "comune"}


# --------------------------------------------------------------------------------------------- schedule
def _gantt(d: dict, titles: dict) -> dict:
    """A Gantt only from phases with a real start and end; otherwise the overall duration and the schedule document."""
    by_page: dict[tuple, list[dict]] = {}
    for f in d["facts"]:
        if f["field"] == "schedule.phase" and (f.get("qualifiers") or {}).get("method") in ("vector_gantt", "dated_table"):
            c = f["citation"]
            by_page.setdefault((c["url"], c["page"]), []).append(f)
    bars, unit, src = [], "month", None
    if by_page:
        def rank(kv):
            (u, _), fs_ = kv
            dated = any(f["qualifiers"].get("method") == "dated_table" for f in fs_)
            return (bool(SCHED_TITLE.search(titles.get(u) or "")), dated, len(fs_))
        (url, page), fs = max(by_page.items(), key=rank)
        method = "dated_table" if any(f["qualifiers"].get("method") == "dated_table" for f in fs) else "vector_gantt"
        fs = [f for f in fs if f["qualifiers"].get("method") == method]
        fs.sort(key=lambda f: f["value"].get("order", 0))
        unit = "month" if all(f["value"].get("unit_known", True) for f in fs) else fs[0]["value"].get("unit") or "period"
        seen = set()
        for f in fs:
            v = f["value"]
            if v["name"].lower() in seen:
                continue
            seen.add(v["name"].lower())
            bars.append({"name": v["name"], "start": v["start_month"], "end": v["end_month"],
                         **({"dates": [v["start_date"], v["end_date"]]} if v.get("start_date") else {})})
        src = _source({"url": url, "page": page}, titles)
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
    months = max([b["end"] for b in bars] + [1])
    note = None
    if not bars:
        note = ("Le fasi del cronoprogramma non sono leggibili automaticamente (allegato in formato immagine o non presente)."
                + (" Il documento è indicato sotto." if sched_doc else ""))
    return {"bars": bars, "unit": unit, "months": months, "source": src,
            "duration": {"text": dur["text"], "status": dur["status"], "hint": _hint(dur), "source": dur["source"]} if dur else None,
            "milestones": milestones, "document": sched_doc, "note": note}


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
        "cronoprogramma": _gantt(d, titles),
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
    return re.sub(r"^[^\w]+|[\s'\"‘’“”ʼ`´,;]+$", "", t)
