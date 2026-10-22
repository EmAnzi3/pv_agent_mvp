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
    (re.compile(r"(?i)proroga\s+(?:del(?:l')?\s*(?:termine\s+(?:di|per)\s+(?:l')?)?)?inizio\s+(?:dei\s+)?lavori"), "construction_start_extension"),
    (re.compile(r"(?i)inizio\s+(?:dei\s+)?lavori|avvio\s+(?:dei\s+)?lavori|apertura\s+(?:del\s+)?cantiere|consegna\s+(?:dei\s+)?lavori"), "construction_start"),
    (re.compile(r"(?i)fine\s+(?:dei\s+)?lavori|ultimazione\s+(?:dei\s+)?lavori|chiusura\s+(?:del\s+)?cantiere"), "construction_end"),
    (re.compile(r"(?i:entrata\s+in\s+esercizio|messa\s+in\s+esercizio|esercizio\s+commerciale|primo\s+parallelo)|\bCOD\b(?!\.)"), "commercial_operation"),
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


DOC_REF_BEFORE = re.compile(r"(?i)(?:preventivo|prot(?:ocollo)?\.?|nota|decreto|d\.\s?d\.|d\.\s?g\.\s?r\.|d\.\s?p\.\s?r\.|d\.?\s?lgs|determin[ae]|delibera|circolare|"
                            r"legge|parere|cod\.?\s+pratica|stmg|istanza|richiesta|comunicazione)[^.;]{0,40}"
                            r"(?:\bdel\b|\bin\s+data\b|\bn\.?\s*\S+\s+del\b|[-/]\s*)\s*$|\bal\s*$|cod\.\s*CER[^.;]{0,40}$")


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


RANGE_RE = re.compile(r"(?i)(?:dal(?:l')?\s*)?(?P<u1>mes[ei]|settiman[ae]|month)?\s*(?P<a>\d{1,2})\s*°?\s*(?P<u2>mes[ei]|settiman[ae])?\s*"
                      r"(?:al(?:l')?|-|–|÷|a)\s*(?P<u3>mes[ei]|settiman[ae])?\s*(?P<b>\d{1,2})\s*°?\s*(?P<u4>mese|mesi|settimana|settimane)?")
PHASE_NAMES = [
    (re.compile(r"(?i)allestimento|accantieramento|apertura\s+cantiere|preparazione\s+(?:del\s+)?(?:sito|area)"), "Allestimento cantiere"),
    (re.compile(r"(?i)recinzion|viabilit|movimento\s+terra|scotico|livellament|sbanc"), "Opere civili e viabilità"),
    (re.compile(r"(?i)infissione|pali|battitura"), "Infissione pali"),
    (re.compile(r"(?i)montaggio\s+(?:delle\s+)?strutture|tracker|strutture\s+di\s+supporto"), "Montaggio strutture"),
    (re.compile(r"(?i)posa\s+(?:dei\s+)?moduli|montaggio\s+(?:dei\s+)?moduli|installazione\s+(?:dei\s+)?moduli"), "Posa moduli"),
    (re.compile(r"(?i)cavidott|scav[oi]\s+(?:per\s+)?cav|posa\s+cavi"), "Cavidotti"),
    (re.compile(r"(?i)cabin|power\s+station|inverter"), "Cabine e inverter"),
    (re.compile(r"(?i)stazione\s+(?:elettrica|utente|di\s+(?:elevazione|trasformazione))|\bSSE\b|sottostazione|connessione"), "Sottostazione e connessione"),
    (re.compile(r"(?i)collaud|commissioning|avviamento|messa\s+in\s+servizio"), "Collaudi e avviamento"),
    (re.compile(r"(?i)ripristin|mitigazion|opere\s+a\s+verde|piantumazion|agricol"), "Mitigazioni e ripristini"),
]


UNIT_TO_MONTHS = {"month": 1.0, "week": 12 / 52, "day": 1 / 30.4}
TWO_DATES = re.compile(r"(?P<d1>\d{1,2}[/.-]\d{1,2}[/.-]\d{2,4})\D{1,20}?(?P<d2>\d{1,2}[/.-]\d{1,2}[/.-]\d{2,4})")


MONTH_NUM = {"gen": 1, "jan": 1, "feb": 2, "mar": 3, "apr": 4, "mag": 5, "may": 5, "giu": 6, "jun": 6, "lug": 7, "jul": 7,
             "ago": 8, "aug": 8, "set": 9, "sep": 9, "ott": 10, "oct": 10, "nov": 11, "dic": 12, "dec": 12}


def _date(t: str):
    from datetime import date
    m = re.match(r"(\d{1,2})[/.-](\d{1,2})[/.-](\d{2,4})", t)
    if m:
        d, mo, y = int(m.group(1)), int(m.group(2)), int(m.group(3))
    else:
        m = re.match(r"(?i)(\d{1,2})\s+([a-z]{3})[a-z]*\.?\s+(\d{2,4})", t)
        if not m or m.group(2).lower() not in MONTH_NUM:
            return None
        d, mo, y = int(m.group(1)), MONTH_NUM[m.group(2).lower()], int(m.group(3))
    y = y + 2000 if y < 100 else y
    try:
        return date(y, mo, d)
    except ValueError:
        return None


DUR_ONLY = re.compile(r"(?i)^(?P<n>\d{1,4}(?:[.,]\d+)?)\s*(?P<u>g|gg|giorni|days?|sett|settimane|w|mesi|ms)\.?$")


def _duration_lines(ctx: PageContext) -> list[dict]:
    """MS Project text order: 'ID' / 'Nome attività' / 'Durata' on separate lines ('2' / 'Fase di ingegneria' / '225 g')."""
    if not re.search(r"(?i)nome\s+attivit|attivit[aà]\b.{0,40}\bdurata|\bdurata\b", ctx.text[:1500]):
        return []
    lines = [l for l in ctx.lines if l]
    out = []
    for i in range(len(lines) - 1):
        m = DUR_ONLY.match(lines[i + 1].strip())
        name = lines[i].strip()
        if m and re.search(r"[A-Za-zÀ-ÿ]{3}", name) and not DUR_ONLY.match(name) and len(name) <= 90:
            out.append({"name": re.sub(r"^\d+\s+", "", name)[:80], "value": float(m.group("n").replace(",", ".")),
                        "unit": m.group("u").lower()})
    return out[:60] if len(out) >= 4 else []


def extract_gantt(ctx: PageContext) -> list[Fact]:
    """Phases from a drawn Gantt (rebuilt from the vector page) or from a dated activity table."""
    out = []
    g = getattr(ctx, "gantt", None) or {}
    # a programme limited to residual works or a single excerpt is not the general one
    scope = "partial" if re.search(r"(?i)residu|stralcio|opere\s+di\s+completamento", (ctx.text or "")[:1200]) else "general"
    if g.get("rows"):
        k = UNIT_TO_MONTHS.get(g.get("unit"), None)
        for i, r in enumerate(g["rows"]):
            out.append(Fact(field="schedule.phase", subject=r["name"],
                            value={"name": r["name"], "start_month": round(r["start"] * k, 2) if k else r["start"],
                                   "end_month": round(r["end"] * k, 2) if k else r["end"],
                                   "raw": f"{g.get('unit')} {r['start']:g}–{r['end']:g}", "order": i,
                                   "unit": g.get("unit"), "unit_known": k is not None,
                                   "level": r.get("level", 0), "indent": r.get("indent"), "summary": r.get("summary", False),
                                   "duration": r.get("duration"),
                                   **({"axis_labels": [{"label": x["label"], "start": round(x["start"] * k, 3) if k else x["start"],
                                                        "end": round(x["end"] * k, 3) if k else x["end"]} for x in g.get("axis_labels", [])]}
                                      if i == 0 else {})},
                            citation=ctx.cite(r["name"]), temporal="planned",
                            qualifiers={"method": g.get("method", "vector_gantt"), "rows": len(g["rows"]), "scope": scope},
                            limits="Bar positions read from the drawing; periods follow the chart's own axis",
                            extractor="schedule.gantt"))
    drows = g.get("duration_rows") or _duration_lines(ctx)
    for i, r in enumerate(drows):
        u = r["unit"]
        months = r["value"] / (30.4 if u.startswith(("g", "d")) else 4.33 if u.startswith(("s", "w")) else 1)
        out.append(Fact(field="schedule.phase_duration", subject=r["name"],
                        value={"name": r["name"], "months": round(months, 2), "raw": f"{r['value']:g} {u}", "order": i},
                        citation=ctx.cite(r["name"]), temporal="planned",
                        qualifiers={"method": "duration_table", "rows": len(drows), "scope": scope},
                        limits="Duration of a phase; start and sequence not stated", extractor="schedule.gantt"))
    if not (g.get("table_rows") or re.search(r"(?i)cron?o?\s*programma|gantt|programma\s+(?:dei\s+)?lavori", ctx.text[:4000])):
        return out
    rows = []
    for line in g.get("table_rows") or ctx.lines:
        from ..gantt_vector import DATE_ANY, _clean_label
        ds = list(DATE_ANY.finditer(line))
        if len(ds) < 2:
            continue
        d1, d2 = _date(ds[0].group(0)), _date(ds[1].group(0))
        name = re.sub(r"(?i)\b\d+(?:[.,]\d+)?\s*(?:g|gg|giorni|days?|sett|settimane|mesi|ms|w|d|g\.l\.)\b\.?|"
                      r"\b(?:lun|mar|mer|gio|ven|sab|dom|mon|tue|wed|thu|fri|sat|sun)\b\.?|\b\d+[.,]\d+\b", "",
                      line[: ds[0].start()])
        name = _clean_label(name)
        if d1 and d2 and d2 >= d1 and re.search(r"[A-Za-zÀ-ÿ]{4}", name):
            rows.append((name[:80], d1, d2, line))
    if len(rows) >= 3:
        t0 = min(r[1] for r in rows)
        for i, (name, d1, d2, line) in enumerate(rows[:80]):
            out.append(Fact(field="schedule.phase", subject=name,
                            value={"name": name, "start_month": round((d1 - t0).days / 30.4, 2),
                                   "end_month": round(((d2 - t0).days + 1) / 30.4, 2),
                                   "start_date": d1.isoformat(), "end_date": d2.isoformat(), "order": i,
                                   "raw": f"{d1:%d/%m/%Y}–{d2:%d/%m/%Y}", "unit": "month", "unit_known": True},
                            citation=ctx.cite(line[:200]), temporal="planned",
                            qualifiers={"method": "dated_table", "rows": len(rows), "scope": scope},
                            limits="Dates of the programme of works: a plan, not progress", extractor="schedule.gantt"))
    return out


def extract_phases(ctx: PageContext) -> list[Fact]:
    """Gantt-like statements: an activity name with an explicit month (or week) range on the same line."""
    out = []
    page_is_schedule = bool(re.search(r"(?i)cronoprogramma|diagramma\s+di\s+gantt|programma\s+(?:dei\s+)?lavori|fasi\s+di\s+cantiere", ctx.text[:4000]))
    for i, line in enumerate(ctx.lines):
        if not line or len(line) > 220:
            continue
        name = next((n for rx, n in PHASE_NAMES if rx.search(line)), None)
        if not name:
            continue
        m = RANGE_RE.search(line)
        if not m:
            continue
        units = " ".join(filter(None, [m.group("u1"), m.group("u2"), m.group("u3"), m.group("u4")])).lower()
        if not page_is_schedule:
            continue    # month ranges elsewhere (opinions, monitoring plans) are not the programme of works
        a, b = int(m.group("a")), int(m.group("b"))
        if not (1 <= a <= b <= 72):
            continue
        k = 12 / 52 if "settiman" in units else 1
        out.append(Fact(field="schedule.phase", value={"name": name, "start_month": round((a - 1) * k, 2),
                                                        "end_month": round(b * k, 2), "raw": m.group(0).strip()},
                        subject=name, citation=ctx.cite(line), temporal="planned",
                        limits="Phase position stated in the programme of works: a plan, not progress",
                        extractor="schedule.phase"))
    return out


# ----------------------------------------------------------------------------- authorisation acts and deadlines
NUM_WORDS = {"un": 1, "uno": 1, "una": 1, "due": 2, "tre": 3, "quattro": 4, "cinque": 5, "sei": 6, "sette": 7, "otto": 8,
             "nove": 9, "dieci": 10, "dodici": 12, "diciotto": 18, "ventiquattro": 24, "trenta": 30, "trentasei": 36,
             "sessanta": 60, "novanta": 90, "centoventi": 120, "centottanta": 180, "trecentosessantacinque": 365}
ACT_RE = re.compile(
    r"(?i)(?P<type>determinazion[ei](?:\s+dirigenziale)?|determina(?:\s+dirigenziale)?|decreto(?:\s+dirigenziale|\s+ministeriale|\s+interministeriale)?|"
    r"D\.\s?D\.|D\.\s?M\.|D\.\s?G\.\s?R\.|deliberazione|provvedimento\s+autorizzatorio\s+unico(?:\s+regionale)?|"
    r"provvedimento\s+unico(?:\s+in\s+materia\s+ambientale)?|provvedimento|autorizzazione\s+unica|P\.?A\.?U\.?R\.?)"
    r"[^.;\n]{0,70}?\b(?:n\.?|n°|nr\.?|numero)\s*(?P<num>[A-Z]{0,4}[\s\-]?\d[\w./\-]{0,14})\s+del(?:l')?\s+"
    r"(?P<date>[0-3]?\d[/.\-][01]?\d[/.\-]20\d{2}|[0-3]?\d\s+(?:" + "|".join(MONTHS) + r")\s+20\d{2})")
AUTH_CTX = re.compile(r"(?i)autorizzazione\s+unica|autorizzatorio|provvedimento\s+unico|P\.?A\.?U\.?R|rilasci|autorizz|"
                      r"compatibilit[aà]\s+ambientale|giudizio\s+(?:positivo|favorevole)|esclusion[ei]\s+dalla\s+(?:procedura\s+di\s+)?V\.?I\.?A|"
                      r"non\s+assoggettab|esito\s+(?:positivo|favorevole)|favorevole")
NOT_PROJECT_ACT = re.compile(r"(?i)D\.?\s?Lgs|legge|L\.\s?R\.|regolamento|circolare|linee\s+guida|D\.?P\.?R|D\.?P\.?C\.?M|piano\s+(?:regionale|paesaggistico)|"
                             r"nomina|delega|organizzazion|bilancio|PNIEC|PNRR\b(?!\))")
PREAMBLE = re.compile(r"(?i)(?:^|[;:•\-–]\s*)(?:vist[oaie]|richiamat[oaie]|ai\s+sensi|in\s+attuazione)\b")
NORM_CTX = re.compile(r"(?i)regolament|linee\s+guida|oneri\s+istruttori|competenz|canone|norme\s+di\s+attuazione|PGRA|piano\s+(?:di\s+gestione|regionale|paesaggistico)|"
                      r"recante|disciplina|organizzazion|nomina|delega|D\.?\s?P\.?\s?R|presidente\s+della\s+repubblica|giunta\s+regionale\s+n|"
                      r"stazione\s+elettrica|ampliamento\s+della\s+stazione|elettrodotto")
AUTH_OUTCOME = re.compile(r"(?i)giudizio\s+(?:positivo|favorevole)|compatibilit[aà]\s+ambientale|autorizzazione\s+unica|P\.?A\.?U\.?R|"
                          r"provvedimento\s+(?:autorizzatorio|unico)|esclusion[ei]\s+dalla|non\s+assoggettab|rilasciat[oa]|autorizzat[oa]")
THIS_PROJECT = re.compile(r"(?i)(?:del|al|per\s+il|relativ[oa]\s+al)\s+progetto|dell'impianto|per\s+l'impianto|in\s+oggetto|di\s+cui\s+all'oggetto|"
                          r"il\s+presente\s+progetto|della\s+societ[aà]\s+proponente")
DEADLINE_RE = re.compile(
    r"(?i)entro\s+(?:e\s+non\s+oltre\s+)?(?:il\s+termine\s+(?:massimo\s+|perentorio\s+)?di\s+)?"
    r"(?P<n>\d{1,3}|" + "|".join(sorted(NUM_WORDS, key=len, reverse=True)) + r")\s*(?:\(\s*\w+\s*\)\s*)?"
    r"(?P<unit>ann[io]|mes[ei]|giorni)\b(?P<tail>(?:(?!entro\b)[^.;]){0,120})")
START_WORDS = re.compile(r"(?i)inizi|avvi[oa]|avviat|intrapres|apertura\s+del\s+cantiere|consegna\s+dei\s+lavori")
END_WORDS = re.compile(r"(?i)ultimat|ultimazion|conclu|termin(?:at|e\s+dei\s+lavori)|complet|fine\s+(?:dei\s+)?lavori|entra\w*\s+in\s+esercizio|mess[ao]\s+in\s+esercizio")
WORKS_WORDS = re.compile(r"(?i)lavori|impianto|opere|cantiere|realizzazion|costruzion|esercizio")
VALIDITY_RE = re.compile(r"(?i)(?:efficacia|validit[aà]|valid[oa]|efficace)[^.;]{0,80}?(?:per\s+(?:un\s+periodo\s+di\s+)?|pari\s+a\s+)?"
                         r"(?P<n>\d{1,2}|" + "|".join(sorted(NUM_WORDS, key=len, reverse=True)) + r")\s*(?:\(\s*\w+\s*\)\s*)?(?P<unit>ann[io])\b|"
                         r"(?i:efficacia\s+(?P<q>quinquennale|triennale|decennale))")


def _parse_date(text: str) -> str | None:
    m = DATE_NUM_RE.search(text)
    if m:
        mo = int(m.group("m"))
        return _iso(m.group("d"), mo, m.group("y")) if 1 <= mo <= 12 else None
    m = DATE_TXT_RE.search(text)
    return _iso(m.group("d"), MONTHS[m.group("mon").lower()], m.group("y")) if m else None


def _acts(ctx: PageContext, i: int, line: str) -> list[Fact]:
    out = []
    window = ctx.window(i, 1, 1)
    for m in ACT_RE.finditer(line):
        iso = _parse_date(m.group("date"))
        if not iso or int(iso[:4]) < 2015 or NOT_PROJECT_ACT.search(m.group(0)):
            continue
        own = bool(ctx.page <= 2 and m.start() < 40 and not PREAMBLE.search(line[:m.start() + 1]))   # heading of the act itself
        sentence = line + " " + window
        if PREAMBLE.search(line[:m.start() + 1]) or NORM_CTX.search(sentence):
            kind = "other"                         # 'VISTO il D.P.R. n. 120...', regulations, fees, competences, plans
        elif own and AUTH_CTX.search(sentence + " " + ctx.text[:2500]):
            kind = "authorisation"
        elif AUTH_OUTCOME.search(sentence) and THIS_PROJECT.search(sentence):
            kind = "authorisation"                 # 'con Decreto n. 324 del ... giudizio positivo ... del progetto in oggetto'
        else:
            kind = "other"
        out.append(Fact(field="act.issue", value={"type": re.sub(r"\s+", " ", m.group("type")).strip(), "number": m.group("num").strip(),
                                                 "date": iso, "kind": kind},
                        subject="this_document" if own else "referenced",
                        citation=ctx.cite(window), qualifiers={"kind": kind, "own": bool(own)},
                        limits="Act number and date as written; its content (favourable or not) is read from the context",
                        extractor="schedule.act"))
    return out


def _deadlines(ctx: PageContext, i: int, line: str) -> list[Fact]:
    out = []
    window = ctx.window(i, 1, 1)
    for m in DEADLINE_RE.finditer(line):
        before = line[max(0, m.start() - 160): m.start()]
        tail = m.group("tail")
        if not WORKS_WORDS.search(before + tail):
            continue
        n_raw = m.group("n").lower()
        n = int(n_raw) if n_raw.isdigit() else NUM_WORDS.get(n_raw)
        unit = m.group("unit").lower()[:4]
        if not n:
            continue
        months = n * 12 if unit.startswith("ann") else n if unit.startswith("mes") else round(n / 30.4, 2)
        if unit.startswith("gior") and n < 60:
            continue                                  # days: notices and transmissions, not works deadlines
        anchor_m = re.search(r"(?i)\bdal(?:la|l')?\s*(?:data\s+(?:di|del(?:la)?)\s+)?(?P<a>rilascio|notifica|pubblicazione|efficacia|"
                             r"emanazione|adozione|presente|comunicazione|ricevimento|inizio\s+(?:dei\s+)?lavori|data\s+di\s+inizio)", tail)
        anchor = (anchor_m.group("a").lower() if anchor_m else None)
        kind = None
        last_start = max((x.end() for x in START_WORDS.finditer(before)), default=-1)
        last_end = max((x.end() for x in END_WORDS.finditer(before)), default=-1)
        if last_end > last_start:
            kind = "end"
        elif last_start >= 0:
            kind = "start"
        if not kind:
            continue
        if anchor and anchor.startswith(("inizio", "data di inizio")) and kind == "end":
            kind = "end_after_start"                  # 'ultimati entro 36 mesi dall'inizio dei lavori'
        out.append(Fact(field=f"schedule.deadline.{kind}", value={"n": n, "unit": unit, "months": months, "anchor": anchor,
                                                                  "raw": m.group(0)[:160]},
                        citation=ctx.cite(window), qualifiers={"anchor": anchor},
                        limits="Deadline imposed or proposed in the documents, counted from the anchor event: a latest date, not a plan",
                        extractor="schedule.deadline"))
    for m in VALIDITY_RE.finditer(line):
        if not re.search(r"(?i)provvediment|decreto|determin|autorizzazion|giudizio|parere|V\.?I\.?A", line + " " + window):
            continue
        if m.group("q"):
            n = {"quinquennale": 5, "triennale": 3, "decennale": 10}[m.group("q").lower()]
        else:
            n_raw = m.group("n").lower()
            n = int(n_raw) if n_raw.isdigit() else NUM_WORDS.get(n_raw)
        if not n or n > 15:
            continue
        out.append(Fact(field="schedule.deadline.validity", value={"n": n, "unit": "anni", "months": n * 12, "anchor": "rilascio",
                                                                   "raw": m.group(0)[:160]},
                        citation=ctx.cite(window), limits="Period of validity of the act: works must be done within it",
                        extractor="schedule.deadline"))
    return out


def extract(ctx: PageContext) -> list[Fact]:
    facts: list[Fact] = extract_gantt(ctx) + extract_phases(ctx)
    for i, line in enumerate(ctx.lines):
        if not line:
            continue
        facts.extend(_acts(ctx, i, line))
        facts.extend(_deadlines(ctx, i, line))
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
            before = line[max(0, m.start() - 60): m.start()]
            if DOC_REF_BEFORE.search(before) and label != "construction_start_extension":
                continue  # date of a referenced act (preventivo, protocollo, decreto...), not a milestone
            # the date must follow its label closely, and be a plausible project date
            if m.start() < label_end - 1 or m.start() - label_end > 60 or int(iso[:4]) < 2015:
                continue
            nature = temporal_nature(line, ctx_text, ctx.text)
            if label == "construction_start_extension":
                nature = "event_occurred" if DOC_REF_BEFORE.search(before) else nature
            facts.append(Fact(field=f"schedule.date.{label}", value=iso, citation=ctx.cite(ctx_text),
                              temporal=nature,
                              qualifiers={"raw": m.group(0)},
                              limits="Date of the act granting an extension of the works-start deadline: NOT the start of works"
                              if label == "construction_start_extension" else
                              {"event_occurred": "Statement of an occurred event: confirm with an independent act",
                                      "template_reference": "Date inside a safety-plan/notification/template field: NOT evidence of works",
                                      "planned": "Planned date in a programme: NOT evidence that works started",
                                      }.get(nature, "Nature of this date not determinable from context"),
                              extractor="schedule.date"))
    return facts
