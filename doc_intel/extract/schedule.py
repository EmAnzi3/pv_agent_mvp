"""Timeline: durations, dates and their temporal nature.

A duration never becomes a date; a Gantt date never becomes an event; a date
inside a safety plan/notification template is a ``template_reference``.
Only explicit past-tense statements of works having started (or a "verbale di
consegna lavori") are ``event_occurred``, and even then they stay candidates.
"""
from __future__ import annotations

import re

from ..schema import Fact
from .context import PageContext

MONTHS = {m: i for i, m in enumerate(["gennaio", "febbraio", "marzo", "aprile", "maggio", "giugno", "luglio",
                                      "agosto", "settembre", "ottobre", "novembre", "dicembre"], 1)}
DUR_RE = re.compile(r"(?i)(?P<num>\d{1,3})(?:\s*[-–÷]\s*(?P<num2>\d{1,3}))?\s*(?:\(\s*[a-z]+\s*\)\s*)?(?P<unit>mesi|mese|settimane|giorni(?:\s+naturali(?:\s+e\s+consecutivi)?)?|anni)\b")
DATE_NUM_RE = re.compile(r"(?<!\d)(?P<d>[0-3]?\d)[/.\-](?P<m>[01]?\d)[/.\-](?P<y>20\d{2})(?!\d)")
DATE_TXT_RE = re.compile(r"(?i)\b(?:(?P<d>[0-3]?\d)\s+)?(?P<mon>" + "|".join(MONTHS) + r")\s+(?P<y>20\d{2})\b")
LABELS = [
    (re.compile(r"(?i)inizio\s+(?:dei\s+)?lavori|avvio\s+(?:dei\s+)?lavori|apertura\s+(?:del\s+)?cantiere|consegna\s+(?:dei\s+)?lavori"), "construction_start"),
    (re.compile(r"(?i)fine\s+(?:dei\s+)?lavori|ultimazione\s+(?:dei\s+)?lavori|chiusura\s+(?:del\s+)?cantiere"), "construction_end"),
    (re.compile(r"(?i)entrata\s+in\s+esercizio|messa\s+in\s+esercizio|\bCOD\b|esercizio\s+commerciale|primo\s+parallelo"), "commercial_operation"),
    (re.compile(r"(?i)collaud|commissioning"), "commissioning"),
]
SCOPE_RULES = [
    (re.compile(r"(?i)dismission|smantell|ripristin|decommission"), "decommissioning"),
    (re.compile(r"(?i)vita\s+utile|durata\s+(?:dell')?esercizio|esercizio\s+dell'impianto\s+per"), "useful_life"),
    (re.compile(r"(?i)monitoragg"), "monitoring"),
    (re.compile(r"(?i)progettazion"), "design"),
    (re.compile(r"(?i)autorizzat|iter\s+autorizzativ|rilascio\s+(?:dell')?autorizzazion"), "permitting"),
    (re.compile(r"(?i)approvvigionament|procurement|fornitur"), "procurement"),
    (re.compile(r"(?i)opere\s+civili|scavi|recinzion|viabilit"), "civil_works"),
    (re.compile(r"(?i)opere\s+elettriche|cavidott|cabin|connessione|SSE|stazione"), "electrical_works"),
    (re.compile(r"(?i)montaggio|installazion|posa\s+(?:dei\s+)?moduli|strutture"), "installation"),
    (re.compile(r"(?i)collaud|commissioning|avviamento"), "commissioning"),
    (re.compile(r"(?i)cantiere|lavori|costruzion|realizzazion|cronoprogramma"), "construction"),
]
TEMPLATE_CTX = re.compile(r"(?i)piano\s+di\s+sicurezza|\bPSC\b|\bP\.O\.S\.|\bPOS\b|notifica\s+preliminare|art\.?\s*99|"
                          r"fascicolo\s+(?:dell'opera|tecnico)|fac-?\s?simile|esempio|modello\s+di|anagrafica\s+di\s+cantiere|"
                          r"uomini[\s-]giorno|dati\s+(?:generali\s+)?(?:del|di)\s+cantiere|indirizzo\s+del\s+cantiere|durata\s+effettiva\s+dei\s+lavori|"
                          r"\(presunt[ao]\)|numero\s+(?:massimo\s+)?(?:presunto\s+)?(?:dei\s+)?lavoratori|ammontare\s+complessivo\s+presunto")
PLANNED_CTX = re.compile(r"(?i)cronoprogramma|gantt|previst[oaie]|ipotizzat|programmat|stimat|pianificat|si\s+prevede|presunt")
SCENARIO_CTX = re.compile(r"(?i)squadre|in\s+parallelo|scenario|ipotesi|alternativ|ottimizzat|nel\s+caso\s+in\s+cui")
CONDITIONAL_CTX = re.compile(r"(?i)dal(?:la)?\s+(?:data\s+(?:di|del)\s+)?(?:rilascio|ottenimento|conseguimento)|a\s+partire\s+da|decorrent|subordinat|condizionat|entro\s+\d+\s+(?:mesi|giorni)\s+dal")
EVENT_CTX = re.compile(r"(?i)verbale\s+di\s+consegna|comunicazione\s+(?:di\s+)?inizio\s+lavori|(?:sono|hanno\s+avuto|ha\s+avuto)\s+(?:iniziat|inizio)|"
                       r"(?:è|e')\s+stat[oa]\s+(?:avviat|iniziat|consegnat)|in\s+data\s+\S+\s+(?:sono|è)\s+(?:iniziat|stat)")


def temporal_nature(line: str, context: str, page_text: str) -> str:
    """Line-level cues win; the window and page header only add template/planned context."""
    if EVENT_CTX.search(line):
        return "event_occurred"
    if TEMPLATE_CTX.search(line) or TEMPLATE_CTX.search(context) or TEMPLATE_CTX.search(page_text[:2500]):
        return "template_reference"
    if SCENARIO_CTX.search(line):
        return "scenario"
    if CONDITIONAL_CTX.search(line):
        return "conditional"
    if PLANNED_CTX.search(line) or PLANNED_CTX.search(context) or PLANNED_CTX.search(page_text[:1500]):
        return "planned"
    return "unknown"


def scope_of(context: str) -> str:
    for rx, scope in SCOPE_RULES:
        if rx.search(context):
            return scope
    return "unspecified"


def _iso(d: str | None, m: int, y: str) -> str:
    return f"{int(y):04d}-{m:02d}" + (f"-{int(d):02d}" if d else "")


def extract(ctx: PageContext) -> list[Fact]:
    facts: list[Fact] = []
    for i, line in enumerate(ctx.lines):
        if not line:
            continue
        ctx_text = ctx.window(i, 1, 1)
        for m in DUR_RE.finditer(line):
            n1 = int(m.group("num"))
            n2 = int(m.group("num2")) if m.group("num2") else None
            unit = m.group("unit").lower().split()[0]
            unit = {"mese": "mesi"}.get(unit, unit)
            # durations only when the line speaks about time-bound activities
            local = line + " " + ctx_text
            scope = scope_of(line) if scope_of(line) != "unspecified" else scope_of(ctx_text)
            if scope in ("unspecified", "monitoring") and not re.search(r"(?i)durat|tempi|cronoprogramma|lavor", local):
                continue
            if unit == "anni" and scope not in ("useful_life", "decommissioning"):
                continue
            if unit == "anni" and n1 >= 15:
                scope = "useful_life"
            if unit == "giorni" and re.search(r"(?i)entro|termine|dalla\s+(?:data\s+di\s+)?ricezione|trasmett|comunica|preavviso|prima\s+dell", local):
                continue  # administrative deadlines, not works durations
            months = {"mesi": 1, "settimane": 12 / 52, "giorni": 12 / 365, "anni": 12}[unit]
            val = {"min": round(n1 * months, 2), "max": round((n2 or n1) * months, 2), "unit_source": unit,
                   "raw": m.group(0)}
            if val["max"] <= 0 or val["max"] > 600:
                continue
            nature = temporal_nature(line, ctx_text, ctx.text)
            facts.append(Fact(field=f"schedule.duration.{scope}", value=val, unit="months",
                              citation=ctx.cite(ctx_text),
                              temporal="scenario" if nature == "scenario" else ("conditional" if nature == "conditional" else "relative_duration"),
                              qualifiers={"context_nature": nature},
                              limits="A duration is not a start or completion date",
                              extractor="schedule.duration"))
        dates = []
        for m in DATE_NUM_RE.finditer(line):
            mo = int(m.group("m"))
            if 1 <= mo <= 12 and 1 <= int(m.group("d")) <= 31:
                dates.append((m, _iso(m.group("d"), mo, m.group("y"))))
        for m in DATE_TXT_RE.finditer(line):
            dates.append((m, _iso(m.group("d"), MONTHS[m.group("mon").lower()], m.group("y"))))
        if not dates:
            continue
        label, label_end = None, None
        for rx, name in LABELS:
            lm = rx.search(line)
            if lm:
                label, label_end = name, lm.end()
                break
            if rx.search(ctx.window(i, 1, 0)) and len(line) < 40:
                label, label_end = name, 0
                break
        if not label:
            continue  # unlabeled dates are document metadata, not milestones
        for m, iso in dates:
            # the date must follow its label closely, and be a plausible project date
            if m.start() < label_end - 1 or m.start() - label_end > 60 or int(iso[:4]) < 2015:
                continue
            nature = temporal_nature(line, ctx_text, ctx.text)
            facts.append(Fact(field=f"schedule.date.{label}", value=iso, citation=ctx.cite(ctx_text),
                              temporal=nature,
                              qualifiers={"raw": m.group(0)},
                              limits={"event_occurred": "Statement of an occurred event: confirm with an independent act",
                                      "template_reference": "Date inside a safety-plan/notification/template field: NOT evidence of works",
                                      "planned": "Planned date in a programme: NOT evidence that works started",
                                      }.get(nature, "Nature of this date not determinable from context"),
                              extractor="schedule.date"))
    return facts
