"""Technical configuration: PV, agrivoltaic, wind, BESS, grid connection.

PV, wind and BESS powers are separate fields and are never summed.
Every value keeps the sentence it came from; the reconciler decides which
values conflict, the extractor does not.
"""
from __future__ import annotations

import re

from ..schema import Fact
from .context import NUM, PageContext, parse_number

BESS_CTX = re.compile(r"(?i)\b(?:BESS|accumulo|storage|batteri[ae])\b")
WIND_CTX = re.compile(r"(?i)\b(?:eolic[oa]|aerogenerator[ei]|WTG|turbin[ae])\b")
PV_CTX = re.compile(r"(?i)\b(?:fotovoltaic[oa]|agrivoltaic[oa]|agro-?voltaic[oa]|moduli|pannelli|generatore\s+fotovoltaico)\b")
DC_CTX = re.compile(r"(?i)\b(?:DC|c\.c\.|corrente\s+continua|di\s+picco|picco)\b")
AC_CTX = re.compile(r"(?i)\b(?:AC|c\.a\.|corrente\s+alternata|in\s+immissione|immessa|di\s+connessione)\b")
GRID_CTX = re.compile(r"(?i)\b(?:RTN|connessione|allacci|stazione\s+elettrica|SE\b|SSE|sottostazione|stallo|cabina\s+primaria|Terna|e-?distribuzione|STMG|elettrodotto)\b")

POWER_RE = re.compile(r"(?P<num>" + NUM + r")\s*(?P<unit>MWp|MW\s?p\b|MWac|MW\s?ac\b|MWdc|MW\s?dc\b|MWh|MW|kWp|kWh|kW)(?![a-zA-Z])")
MODULE_WP_RE = re.compile(r"(?i)(?:modul[oi]|pannell[oi])[^.;\n]{0,60}?(?P<num>\d{3}(?:[.,]\d+)?)\s*W\s?p\b|"
                          r"(?P<num2>\d{3}(?:[.,]\d+)?)\s*W\s?p\b[^.;\n]{0,40}?(?:modul|pannell)")
MODULE_KWP_RE = re.compile(r"(?i)(?:moduli|generatori\s+fotovoltaici|pannelli)[^.;\n]{0,30}?da\s*(?P<num>0[.,]\d{3})\s*kWp")
MODULE_COUNT_RE = re.compile(r"(?i)(?:n\.?\s*|numero\s+(?:di\s+|totale\s+(?:di\s+)?)?|totale\s+)(?:moduli|pannelli)?\s*[:=]?\s*(?P<num>\d{1,3}(?:[.\s]\d{3})+|\d{4,6})\s*(?:moduli|pannelli)?")
INVERTER_RE = re.compile(r"(?i)inverter\s+(?:da\s+)?(?P<kw>" + NUM + r")\s*kW[a-z]*\s*[:=x×]?\s*(?:n\.?\s*)?(?P<count>\d{1,4})?|"
                         r"(?P<count2>\d{1,4})\s*inverter(?:\s+(?:di\s+stringa|centralizzat[io]|da)\s*(?P<kw2>" + NUM + r")?\s*(?:kW)?)?")
TRACKER_RE = re.compile(r"(?i)\b(?:tracker|inseguitor[ei])\s+(?:solar[ei]\s+)?mono-?assial[ei]|single[- ]axis|mono-?assial[ei]")
FIXED_RE = re.compile(r"(?i)\bstruttur[ae]\s+fiss[ae]\b|fixed[- ]tilt")
BIFACIAL_RE = re.compile(r"(?i)\bbifacciali?\b|\bbifacial\b")
VOLT_RE = re.compile(r"(?P<num>\d{2,3}(?:[.,]\d)?)\s*kV\b")
WTG_COUNT_RE = re.compile(r"(?i)(?:n\.?\s*)?(?P<num>\d{1,3})\s+(?:aerogeneratori|turbine\s+eoliche|WTG)\b")
HUB_RE = re.compile(r"(?i)altezza\s+(?:al\s+|del\s+)?mozzo[^0-9]{0,20}(?P<num>\d{2,3}(?:[.,]\d+)?)\s*m\b")
ROTOR_RE = re.compile(r"(?i)diametro\s+(?:del\s+)?rotore[^0-9]{0,20}(?P<num>\d{2,3}(?:[.,]\d+)?)\s*m\b")
AREA_RE = re.compile(r"(?i)(?P<kind>superficie|area)\s+(?P<qual>[a-zà-ÿ' ]{0,60}?)[:\s]{0,4}(?:di\s+|pari\s+a\s+|circa\s+|complessiv[ao]\s+)*(?P<num>" + NUM + r")\s*(?P<unit>ha|ettari|m2|m²|mq)\b")
AREA_PANELS = re.compile(r"(?i)captant|radiant|occupat[ao]\s+da(?:i|lle)?\s+(?:moduli|pannelli|strutture|tracker)|"
                         r"coperta\s+da|proiezione|impronta|pannell|moduli|tracker|strutture|pannellat|installazione")
AREA_AVAILABLE = re.compile(r"(?i)disponibil|catastal|totale|complessiv|lott[oi]|recintat|di\s+progetto|intervento|"
                            r"terren|fondo|aziend|propriet|contrattualizzat|nella\s+disponibilit|lorda|particell")
PILES_RE = re.compile(r"(?i)(?:(?:n\.|numero|totale)\s*(?:di\s+|complessivo\s+(?:di\s+)?)?)?(?P<num>\d{1,3}(?:[.\s]\d{3})+|\d{2,6})\s+"
                      r"(?P<what>pali|pile|montanti|paletti|micropali)\b|"
                      r"(?P<what2>pali|pile|montanti|micropali)\b[^.;\n]{0,50}?(?:n\.|numero|pari\s+a|totale\s+(?:di\s+)?)\s*(?P<num2>\d{1,3}(?:[.\s]\d{3})+|\d{2,6})")
LOTS_RE = re.compile(r"(?i)(?:suddivis[oa]|articolat[oa]|compost[oa]|distribuit[oa]|ripartit[oa])\s+(?:in|su)\s+(?:n\.\s*)?"
                     r"(?P<num>\d{1,2}|due|tre|quattro|cinque|sei|sette|otto|nove|dieci)\s+(?P<what>lotti|campi|aree|sottocampi|sezioni|impianti|blocchi)")
WORDNUM = {"due": 2, "tre": 3, "quattro": 4, "cinque": 5, "sei": 6, "sette": 7, "otto": 8, "nove": 9, "dieci": 10}
TECH_WORDS = [
    (re.compile(r"(?i)\bagrivoltaico\s+(?:evoluto|avanzato|innovativo)\b"), "agrivoltaic_advanced"),
    (re.compile(r"(?i)\bagri-?voltaic[oa]|agro-?voltaic[oa]|agro-?fotovoltaic[oa]\b"), "agrivoltaic"),
    (re.compile(r"(?i)\bfloating|flottante|galleggiante\b"), "floating_pv"),
    (re.compile(r"(?i)\boff-?shore\b"), "offshore"),
]


PLANT_CTX = re.compile(r"(?i)\b(?:complessiv[aoie]|totale|dell'impianto|impianto\s+(?:fotovoltaico|agrivoltaico|eolico)|"
                       r"potenza\s+(?:nominale|di\s+picco|installata|in\s+immissione)|denominato|parco)\b")
COMPONENT_CTX = re.compile(r"(?i)\b(?:sotto-?camp[oi]|campo\s+(?:n\.?\s*)?\d|sezion[ei]\s+\w{1,3}\b|cabin[ae]|blocc[oi]|"
                           r"inverter|trasformator[ei]|lotto|area\s+[A-Z0-9]{1,3}\b|string[ae]|quadr[oi]|ciascun[oa]|"
                           r"singol[oa]|per\s+ogni|unitari[oa]|altri\s+impianti|impianti\s+(?:esistenti|limitrofi|in\s+progetto)|\bcumul)")


def power_scope(local: str, sentence: str) -> str:
    """plant | component | other_project | unknown — only plant/unknown feed the canonical delta."""
    if re.search(r"(?i)altri\s+impianti|impianti\s+(?:esistenti|limitrofi|autorizzati)|\bcumul", sentence):
        return "other_project"
    plant_l, comp_l = PLANT_CTX.search(local), COMPONENT_CTX.search(local)
    if plant_l and not comp_l:
        return "plant"
    if comp_l:
        return "component"
    if PLANT_CTX.search(sentence) and not COMPONENT_CTX.search(sentence):
        return "plant"
    if COMPONENT_CTX.search(sentence):
        return "component"
    return "unknown"


def _sentence(ctx: PageContext, i: int) -> str:
    return ctx.window(i, 1, 1)


def _power_field(unit: str, local: str, sentence: str) -> tuple[str, str, float]:
    """Classify a power value: unit first, then the words right around the number, then the sentence."""
    u = unit.replace(" ", "").lower()
    if u in ("mwh", "kwh"):
        if BESS_CTX.search(local) or (BESS_CTX.search(sentence) and not re.search(r"(?i)produ|anno|annu|/a\b", sentence)):
            return "tech.bess_energy_mwh", "MWh", 1.0 if u == "mwh" else 0.001
        return "tech.energy_other_mwh", "MWh", 1.0 if u == "mwh" else 0.001
    scale = 0.001 if u.startswith("kw") else 1.0
    for ctx in (local, sentence):
        bess, wind, pv = BESS_CTX.search(ctx), WIND_CTX.search(ctx), PV_CTX.search(ctx)
        if bess and not pv and not wind:
            return "tech.bess_power_mw", "MW", scale
        if wind and not pv and not bess:
            return "tech.wind_power_mw", "MW", scale
        if bess or wind or pv:
            break
    if u in ("mwp", "kwp", "mwdc"):
        return "tech.pv_dc_mwp", "MWp", scale
    if u == "mwac":
        return "tech.pv_ac_mw", "MW", scale
    dc, ac = DC_CTX.search(local), AC_CTX.search(local)
    if dc and not ac:
        return "tech.pv_dc_mwp", "MWp", scale
    if ac and not dc:
        return "tech.pv_ac_mw", "MW", scale
    return "tech.nominal_power_mw", "MW", scale


def extract(ctx: PageContext) -> list[Fact]:
    facts: list[Fact] = []
    for i, line in enumerate(ctx.lines):
        if not line:
            continue
        sent = _sentence(ctx, i)
        for m in POWER_RE.finditer(line):
            num = parse_number(m.group("num"))
            if num is None or num <= 0:
                continue
            after = re.split(r"[,;(]|\s+con\s+|\s+e\s+(?:un|relativ)", line[m.end(): m.end() + 25])[0]
            before = line[max(0, m.start() - 25): m.start()]
            # the qualifier usually follows the number ("60 MW lato AC"); fall back to the words before it
            local = after if (DC_CTX.search(after) or AC_CTX.search(after) or BESS_CTX.search(after)) else before + " " + after
            field, unit, scale = _power_field(m.group("unit"), local, sent)
            value = round(num * scale, 6)
            # kW of single devices (inverter 350 kW, ausiliari) are not plant powers
            if scale < 1 and value < 1 and field != "tech.bess_energy_mwh":
                continue
            if value > 1500:  # implausible for a single project, likely a mis-tokenised number
                continue
            scope = power_scope(line[max(0, m.start() - 60): m.end() + 40], sent)
            facts.append(Fact(field=field, value=value, unit=unit, citation=ctx.cite(sent), subject=scope,
                              qualifiers={"raw": m.group(0), "scope": scope}, extractor="technical.power",
                              limits="Value as stated in this document/revision; may be historical"))
        for m in MODULE_WP_RE.finditer(line):
            v = parse_number(m.group("num") or m.group("num2"))
            if v and 250 <= v <= 900:
                facts.append(Fact(field="tech.module_power_wp", value=v, unit="Wp", citation=ctx.cite(sent),
                                  extractor="technical.module"))
        for m in MODULE_KWP_RE.finditer(line):
            v = parse_number(m.group("num"))
            if v and 0.25 <= v <= 0.9:
                facts.append(Fact(field="tech.module_power_wp", value=round(v * 1000, 3), unit="Wp",
                                  citation=ctx.cite(sent), extractor="technical.module"))
        if re.search(r"(?i)moduli|pannelli", line):
            for m in MODULE_COUNT_RE.finditer(line):
                if re.search(r"(?i)\b(?:CEI|EN|IEC|UNI|ISO|DIN|norm[ae]|serie)\s*[-:]?\s*$", line[max(0, m.start("num") - 15): m.start("num")]):
                    continue  # standard codes (CEI EN 61215, IEC 61730) are not quantities
                v = parse_number(m.group("num"))
                if v and 1000 <= v <= 2_000_000:
                    facts.append(Fact(field="tech.module_count", value=int(v), citation=ctx.cite(sent),
                                      extractor="technical.module"))
        for m in INVERTER_RE.finditer(line):
            kw = parse_number(m.group("kw") or m.group("kw2") or "")
            count = m.group("count") or m.group("count2")
            if kw and 1 <= kw <= 10000:
                facts.append(Fact(field="tech.inverter_power_kw", value=kw, unit="kW", citation=ctx.cite(line),
                                  extractor="technical.inverter"))
            if count and 1 <= int(count) <= 5000:
                facts.append(Fact(field="tech.inverter_count", value=int(count), citation=ctx.cite(line),
                                  extractor="technical.inverter"))
        if TRACKER_RE.search(line):
            facts.append(Fact(field="tech.mounting", value="single_axis_tracker", citation=ctx.cite(line),
                              extractor="technical.mounting"))
        if FIXED_RE.search(line):
            facts.append(Fact(field="tech.mounting", value="fixed", citation=ctx.cite(line),
                              extractor="technical.mounting"))
        if BIFACIAL_RE.search(line):
            facts.append(Fact(field="tech.module_technology", value="bifacial", citation=ctx.cite(line),
                              extractor="technical.module"))
        for rx, tech in TECH_WORDS:
            if rx.search(line):
                facts.append(Fact(field="tech.technology", value=tech, citation=ctx.cite(line),
                                  extractor="technical.technology"))
                break
        if GRID_CTX.search(sent):
            for m in VOLT_RE.finditer(line):
                v = parse_number(m.group("num"))
                if v and v in (15.0, 20.0, 30.0, 36.0, 66.0, 132.0, 150.0, 220.0, 380.0):
                    mv = v <= 36 and re.search(r"(?i)\bMT\b|media\s+tensione|interna|cabin[ae]\s+di\s+campo|cavidott[oi]\s+interr", sent) and v != 36.0
                    facts.append(Fact(field="grid.internal_mv_kv" if mv else "grid.voltage_kv", value=v, unit="kV", citation=ctx.cite(sent),
                                      qualifiers={"context": "grid"},
                                      limits="Voltage mentioned in a grid-connection sentence; may be internal MV",
                                      extractor="technical.grid"))
        for rx, field, lo, hi in ((WTG_COUNT_RE, "tech.wtg_count", 1, 300), (HUB_RE, "tech.wtg_hub_height_m", 40, 220),
                                  (ROTOR_RE, "tech.wtg_rotor_diameter_m", 40, 260)):
            for m in rx.finditer(line):
                v = parse_number(m.group("num"))
                if v and lo <= v <= hi:
                    facts.append(Fact(field=field, value=v, citation=ctx.cite(sent), extractor="technical.wind"))
        for m in AREA_RE.finditer(line):
            v = parse_number(m.group("num"))
            if not v:
                continue
            ha = v if m.group("unit").lower() in ("ha", "ettari") else v / 10000
            qual = (m.group("kind") + " " + m.group("qual")).strip()
            local = line[max(0, m.start() - 40): m.end()]
            if AREA_PANELS.search(qual) or AREA_PANELS.search(local):
                field = "tech.area_panels_ha"
            elif AREA_AVAILABLE.search(qual) or AREA_AVAILABLE.search(local):
                field = "tech.area_available_ha"
            else:
                field = "tech.area_ha"
            if 0.1 <= ha <= 5000:
                facts.append(Fact(field=field, value=round(ha, 4), unit="ha", citation=ctx.cite(sent),
                                  qualifiers={"kind": qual},
                                  limits="Area type from the wording next to the number",
                                  extractor="technical.area"))
        for m in PILES_RE.finditer(line):
            v = parse_number(m.group("num") or m.group("num2") or "")
            if not v or not (20 <= v <= 500_000):
                continue
            after = re.split(r"[,;]|\s+e\s+\d", line[m.end(): m.end() + 60])[0]
            before = line[max(0, m.start() - 50): m.start()]
            def kind_of(t):
                return ("recinzione" if re.search(r"(?i)recinzion", t) else
                        "tracker" if re.search(r"(?i)tracker|inseguitor", t) else
                        "strutture fisse" if re.search(r"(?i)fiss[ae]", t) else None)
            if re.search(r"(?i)complessiv|totale", before + m.group(0)):
                kind = "totale"
            else:
                kind = kind_of(m.group(0) + after) or kind_of(before) or kind_of(sent) or "non specificato"
            if re.search(r"(?i)illuminazion|videosorveglian|telecamer|lampion", line):
                continue
            facts.append(Fact(field="tech.piles_count", value=int(v), subject=kind, citation=ctx.cite(sent),
                              qualifiers={"kind": kind, "raw": m.group(0)}, extractor="technical.piles",
                              limits="Number of driven piles as stated; type from nearby words"))
        for m in LOTS_RE.finditer(line):
            n = m.group("num").lower()
            v = WORDNUM.get(n) or int(n)
            if 2 <= v <= 30:
                facts.append(Fact(field="tech.lots_count", value=v, citation=ctx.cite(sent),
                                  qualifiers={"what": m.group("what").lower()}, extractor="technical.lots"))
    return facts
