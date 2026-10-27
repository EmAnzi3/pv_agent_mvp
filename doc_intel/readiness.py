"""'Vicini al cantiere': how close each radar project is to construction, from the signals already
collected (project cards, GSE FER X rankings, execution evidence, press). Information only.

Every project gets a phase, a score and the list of reasons behind it - never a bare number:
  phase   realizzato      works ended / plant in operation (documented) or planned end passed with no later act
          in_corso        start of works documented (or planned start passed with nothing filed after it)
          entro_12_mesi   documented or planned start date within the next 12 months
          possibile       authorised, start window (estimated from the act and its validity) open in the next
                          12 months, or planned dates passed while the procedure goes on
          oltre_12_mesi   planned start more than 12 months away
          autorizzato     authorising act known, no date to place the works
          in_iter         card without an authorising act (procedure in progress)
  score   phase base + FER X incentive (4) + builder known from a site act (3) / from the drawings (1)
          + compatible press news (1) + authorising act (1) + compliance check in progress (2)
          - 3 when the procedure title says 'archiviato'
  level   Alta (score >= 7), Media (4-6), Bassa (1-3); 'realizzato' projects get no level.
Dates are those of the card (documented, prescribed or estimated, each labelled as such).
"""
from __future__ import annotations

import json
import re
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

MONTHS = {m: i for i, m in enumerate(["gennaio", "febbraio", "marzo", "aprile", "maggio", "giugno", "luglio", "agosto",
                                      "settembre", "ottobre", "novembre", "dicembre"], 1)}
PHASE_LABEL = {"realizzato": "Realizzato o in esercizio", "in_corso": "Cantiere in corso",
               "entro_12_mesi": "Avvio entro 12 mesi", "possibile": "Avvio possibile entro 12 mesi",
               "oltre_12_mesi": "Avvio oltre 12 mesi", "autorizzato": "Autorizzato, date non note",
               "in_iter": "In iter autorizzativo"}
BASE = {"in_corso": 6, "entro_12_mesi": 5, "possibile": 3, "oltre_12_mesi": 2, "autorizzato": 2, "in_iter": 0}


def _dates(text: str) -> list[date]:
    out = []
    for d, m, y in re.findall(r"\b(\d{1,2})/(\d{1,2})/(20\d{2})\b", text or ""):
        try:
            out.append(date(int(y), int(m), int(d)))
        except ValueError:
            pass
    if not out:
        for m, y in re.findall(r"\b(\d{1,2})/(20\d{2})\b", text or ""):
            if 1 <= int(m) <= 12:
                out.append(date(int(y), int(m), 1))
    if not out:
        for mn, y in re.findall(r"(?i)\b(" + "|".join(MONTHS) + r")\s+(20\d{2})\b", text or ""):
            out.append(date(int(y), MONTHS[mn.lower()], 1))
    return out


def _fmt(d: date) -> str:
    return d.strftime("%d/%m/%Y")


def classify(card: dict | None, ferx: list | None = None, news: list | None = None, today: date | None = None,
             record: dict | None = None) -> dict:
    today = today or datetime.now(timezone.utc).date()
    horizon = today + timedelta(days=365)
    reasons: list[str] = []
    phase, window = None, None
    if card:
        t = card.get("tempistica") or {}
        rows = t.get("righe") or []
        get = lambda voce, livello=None: [r for r in rows if r.get("voce") == voce and (livello is None or r.get("livello") == livello)]
        planned = lambda r: re.search(r"(?i)previsione|condizionat", r.get("dettaglio") or "")
        start_doc = [d for r in get("Inizio lavori", "documentata") + get("Proroga inizio lavori", "documentata")
                     for d in _dates(r["valore"])]
        plan = [ds[0] for r in get("Cronoprogramma con date") for ds in [_dates(r["valore"])] if ds]
        lb = [d for r in get("Inizio lavori", "stimata") for d in _dates(r["valore"])]
        ub = [d for r in get("Inizio lavori", "prescritta") + get("Validità dell'atto") for d in _dates(r["valore"])]
        ended = [d for r in get("Fine lavori", "documentata") + get("Entrata in esercizio", "documentata")
                 if not planned(r) for d in _dates(r["valore"]) if d < today]
        stato = (card.get("stato") or {}).get("level")
        auth = t.get("autorizzazione")
        if auth:
            reasons.append(f"Atto autorizzativo: {auth.get('atto')} del {auth.get('data')}")
        if ended or stato == "potenzialmente_realizzato":
            phase = "realizzato"
            window = f"fine lavori / esercizio {_fmt(max(ended))}" if ended else (card.get("stato") or {}).get("reason")
        elif stato == "avviato":
            phase, window = "in_corso", (card.get("stato") or {}).get("reason")
        else:
            start = max(start_doc) if start_doc else (min(plan) if plan else None)
            if start and today <= start <= horizon:
                phase, window = "entro_12_mesi", f"avvio {'documentato' if start_doc else 'da cronoprogramma'} {_fmt(start)}"
            elif start and start > horizon:
                phase, window = "oltre_12_mesi", f"avvio {'documentato' if start_doc else 'da cronoprogramma'} {_fmt(start)}"
            elif stato in ("potenzialmente_avviato", "date_superate") and auth:
                reason = (card.get("stato") or {}).get("reason") or ""
                ps = [d for d in _dates((re.search(r"(?i)previst\w*\s+il\s+(\S+)", reason) or [None, ""])[1]) if d.year >= 2015]
                ls = _dates((re.search(r"(?i)atto\s+del\s+(\S+)", reason) or [None, ""])[1])
                phase = "possibile"
                window = ("date di progetto superate" + (f" (avvio previsto {_fmt(ps[0])})" if ps else "") +
                          (f", ultimo atto del {_fmt(ls[0])}" if ls else "") + ": verificare lo stato")
            elif auth and lb:
                upper = min(ub) if ub else None
                if upper and upper < today:
                    phase, window = "autorizzato", f"termine dell'atto {_fmt(upper)} superato: verificare proroghe"
                elif min(lb) <= horizon:
                    phase = "possibile"
                    window = f"avvio stimato non prima di {min(lb).strftime('%m/%Y')}" + (
                        f", entro il {_fmt(upper)} (validità dell'atto)" if upper else "; termine dell'atto non indicato nei documenti")
                else:
                    phase, window = "oltre_12_mesi", f"avvio stimato non prima di {min(lb).strftime('%m/%Y')}"
            elif auth:
                phase = "autorizzato"
            else:
                phase = "in_iter"
        ex = card.get("esecuzione") or {}
        notice = next((i for i in ex.get("imprese") or [] if re.search(
            r"(?i)comunicazion\w*\s+(?:di\s+)?(?:inizio|avvio)|inizio\s+lavori|avvio\s+lavori",
            ((i.get("source") or {}).get("document") or ""))), None)
        if notice and phase not in ("realizzato", "in_corso"):
            phase = "in_corso"
            window = f"comunicazione di inizio lavori agli atti ({(notice.get('source') or {}).get('document', '')[:60]})"
        if ex.get("livello"):
            names = ", ".join(dict.fromkeys(i["impresa"] for i in ex.get("imprese") or []))
            reasons.append(("Impresa esecutrice in atto di cantiere: " if ex["livello"] == "A" else
                            "Impresa indicata negli elaborati: ") + names)
    rec_text = f"{(record or {}).get('project_type') or ''} {(record or {}).get('title') or ''}"
    ottemperanza = bool(re.search(r"(?i)ottemperanz", rec_text))
    archived = bool(re.search(r"(?i)\barchiviat[oa]\b", (record or {}).get("title") or ""))
    if ottemperanza and phase not in ("realizzato", "in_corso"):
        reasons.append("Verifica di ottemperanza alle prescrizioni in corso: passaggio che precede il cantiere")
        if phase in (None, "in_iter", "autorizzato", "oltre_12_mesi"):
            phase, window = "possibile", window or "verifica di ottemperanza in corso"
    if archived:
        reasons.append("Attenzione: il titolo del procedimento riporta «archiviato»: verificare se il progetto è ancora attivo")
    if ferx:
        x = ferx[0]
        reasons.append(f"Incentivo FER X assegnato ({x.get('procedura')}, posizione {x.get('posizione')})")
        phase = phase or "autorizzato"
    if news:
        reasons.append(f"Notizie di stampa compatibili: {len(news)}")
    if phase is None:
        return {}
    if phase == "realizzato":
        return {"phase": phase, "phase_label": PHASE_LABEL[phase], "window": window, "score": 0, "level": None,
                "reasons": reasons}
    score = BASE[phase] + (4 if ferx else 0) + (1 if news else 0) + (2 if ottemperanza else 0) - (3 if archived else 0)
    if card:
        ex = card.get("esecuzione") or {}
        score += 3 if ex.get("livello") == "A" else 1 if ex.get("livello") == "B" else 0
        score += 1 if (card.get("tempistica") or {}).get("autorizzazione") else 0
    level = "Alta" if score >= 7 else "Media" if score >= 4 else "Bassa" if score >= 1 else None
    return {"phase": phase, "phase_label": PHASE_LABEL[phase], "window": window, "score": score, "level": level,
            "reasons": reasons}


def run(data_json: str, memory: str) -> dict:
    mem = Path(memory)
    records = json.loads(Path(data_json).read_text(encoding="utf-8-sig")).get("records", [])
    live = json.loads((mem / "schede" / "live_index.json").read_text(encoding="utf-8"))
    load = lambda p: (json.loads(p.read_text(encoding="utf-8")).get("projects") or {}) if p.exists() else {}
    ferx, news = load(mem / "ferx" / "matches.json"), load(mem / "news" / "matches.json")
    out, counts = {}, {}
    for r in records:
        key = r.get("project_key")
        if not key or r.get("is_terna"):
            continue
        card = None
        if key in live:
            try:
                card = json.loads((mem / "schede" / live[key]["file"]).read_text(encoding="utf-8"))
            except (OSError, ValueError):
                card = None
        res = classify(card, ferx.get(key), news.get(key), record=r)
        if res:
            out[key] = res
            counts[res["level"] or res["phase"]] = counts.get(res["level"] or res["phase"], 0) + 1
    (mem / "readiness").mkdir(exist_ok=True)
    (mem / "readiness" / "index.json").write_text(json.dumps({
        "updated": datetime.now(timezone.utc).date().isoformat(), "projects": out}, ensure_ascii=False, indent=0,
        sort_keys=True), encoding="utf-8")
    return {"progetti_valutati": len(out), "per_livello": counts}
