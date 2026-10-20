"""Rebuild a Gantt chart from a vector PDF page (Excel / MS Project / CAD exports).

A programme of works is usually drawn, not written: activity names on the left, a time axis
(1, 2, 3 ... or month names) across the top and coloured bars. The text layer alone loses the
bars, so this module reads word positions and filled rectangles and maps each bar to the label
on its row and to the axis.

``reconstruct`` is pure (lists in, dict out) so it can be tested without a PDF library;
``from_fitz_page`` adapts a PyMuPDF page. Scanned (image-only) schedules cannot be rebuilt.
"""
from __future__ import annotations

import re
import statistics
from typing import Any

MONTHS = {"gen": 1, "feb": 2, "mar": 3, "apr": 4, "mag": 5, "giu": 6, "lug": 7, "ago": 8, "set": 9, "ott": 10,
          "nov": 11, "dic": 12, "jan": 1, "may": 5, "jun": 6, "jul": 7, "aug": 8, "sep": 9, "oct": 10, "dec": 12}
TICK_RE = re.compile(r"^(?:[MSWG]|M\.|S\.)?(\d{1,3})[°ª]?$", re.I)
SCHEDULE_HINT = re.compile(r"(?i)cronoprogramma|gantt|diagramma|programma\s+(?:dei\s+)?lavori|fasi\s+(?:di\s+)?(?:cantiere|lavoro)|"
                           r"attivit[aà]|lavorazion")
DATE_OR_DUR = re.compile(r"(?i)\b\d{1,2}[/.-]\d{1,2}[/.-]\d{2,4}\b|\b\d+(?:[.,]\d+)?\s*(?:g|gg|giorni|sett|settimane|mesi|ms|w|d|days)\b\.?")


def _tick_value(text: str) -> tuple[int | None, str]:
    t = text.strip().strip(".:").lower()
    m = TICK_RE.match(t)
    if m:
        prefix = t[: m.start(1)]
        return int(m.group(1)), ("week" if prefix.startswith(("s", "w")) else "month" if prefix.startswith("m") else "")
    if t[:3] in MONTHS and len(t) <= 9 and t.isalpha():
        return MONTHS[t[:3]], "month_name"
    my = re.match(r"^([a-z]{3})[a-z]*[-\s'’/.]?(\d{2}|\d{4})$", t)          # 'set-26', 'gen 2027'
    if my and my.group(1) in MONTHS:
        y = int(my.group(2))
        y = y + 2000 if y < 100 else y
        return y * 12 + MONTHS[my.group(1)] - 1, "month_year"
    return None, ""


def _axis(words: list[tuple]) -> dict | None:
    """Longest row of evenly spaced, consecutive tick labels."""
    rows: list[list[tuple]] = []
    for w in sorted((w for w in words if _tick_value(w[4])[0] is not None), key=lambda w: (w[1] + w[3]) / 2):
        yc = (w[1] + w[3]) / 2
        if rows and abs(((rows[-1][0][1] + rows[-1][0][3]) / 2) - yc) <= 2.5:
            rows[-1].append(w)
        else:
            rows.append([w])
    best = None
    for row in rows:
        row.sort(key=lambda w: w[0])
        vals = [_tick_value(w[4]) for w in row]
        kinds = {k for _, k in vals}
        seq, cur = [], []
        prev = None
        for w, (v, k) in zip(row, vals):
            if k == "month_year":
                v_eff = v if prev is None or v == prev + 1 else None
            elif k == "month_name" and prev is not None:
                v_eff = prev + 1 if (v - 1) % 12 == prev % 12 else None   # month names wrap across years
            else:
                v_eff = v if prev is None or k == "month_name" else (v if v == prev + 1 else None)
            if prev is None or v_eff is not None:
                cur.append((v_eff if v_eff is not None else v, w))
                prev = cur[-1][0]
            else:
                if len(cur) > len(seq):
                    seq = cur
                cur, prev = [(v, w)], v
        if len(cur) > len(seq):
            seq = cur
        if len(seq) < 4:
            continue
        xs = [(w[0] + w[2]) / 2 for _, w in seq]
        gaps = [b - a for a, b in zip(xs, xs[1:])]
        if min(gaps) <= 0 or statistics.pstdev(gaps) > 0.25 * statistics.mean(gaps):
            continue
        if best is None or len(seq) > len(best["seq"]):
            unit = ("month" if kinds & {"month_name", "month_year", "month"} else "week" if "week" in kinds else "")
            best = {"seq": seq, "y": (seq[0][1][1] + seq[0][1][3]) / 2, "unit": unit}
    if not best:
        return None
    vs = [v for v, _ in best["seq"]]
    xs = [(w[0] + w[2]) / 2 for _, w in best["seq"]]
    mv, mx = statistics.mean(vs), statistics.mean(xs)
    b = sum((v - mv) * (x - mx) for v, x in zip(vs, xs)) / sum((v - mv) ** 2 for v in vs)
    a = mx - b * mv
    best.update({"a": a, "b": b, "first": vs[0], "last": vs[-1], "x_min": xs[0] - b / 2, "x_max": xs[-1] + b / 2})
    return best


def _lines(words: list[tuple]) -> list[dict]:
    """Group words into text lines (by block/line ids when present, else by y)."""
    groups: dict[tuple, list[tuple]] = {}
    for w in words:
        key = (w[5], w[6]) if len(w) >= 7 else (round((w[1] + w[3]) / 4),)
        groups.setdefault(key, []).append(w)
    out = []
    for ws in groups.values():
        ws.sort(key=lambda w: w[0])
        out.append({"text": " ".join(w[4] for w in ws), "x0": min(w[0] for w in ws), "x1": max(w[2] for w in ws),
                    "y0": min(w[1] for w in ws), "y1": max(w[3] for w in ws), "words": ws})
    return out


def _clean_label(text: str) -> str:
    t = DATE_OR_DUR.sub(" ", text)
    t = re.sub(r"^\s*(?:\d+(?:\.\d+)*[.)]?\s+)+", "", t)          # leading ids / WBS numbers
    t = re.sub(r"\s{2,}", " ", t).strip(" -–:;|")
    return t


def reconstruct(words: list[tuple], rects: list[tuple], width: float, height: float, page_text: str = "") -> dict | None:
    """words: (x0, y0, x1, y1, text[, block, line]); rects: (x0, y0, x1, y1, (r, g, b) fill)."""
    axis = _axis(words)
    if not axis:
        return None
    a, b = axis["a"], axis["b"]
    span = axis["x_max"] - axis["x_min"]
    unit = axis["unit"]
    if not unit:
        low = page_text.lower()
        weeks, months, days = low.count("settiman") + low.count("week"), low.count("mes") + low.count("month"), low.count("giorn")
        unit = "week" if weeks > months and weeks >= days else "day" if days > months and days > weeks else "month" if months else "period"
    heights = [w[3] - w[1] for w in words if w[3] > w[1]]
    text_h = statistics.median(heights) if heights else 8
    lines = [ln for ln in _lines(words) if ln["y0"] > axis["y"] + 1 and ln["x1"] < axis["x_min"] + 0.3 * b
             and re.search(r"[A-Za-zÀ-ÿ]{3}", ln["text"])]
    bars: list[tuple] = []
    for r in rects:
        x0, y0, x1, y1, fill = r
        w, h = x1 - x0, y1 - y0
        if fill is None or h < 1.5 or h > 3.5 * text_h or w < 0.25 * b or w > 0.95 * span:
            continue
        if all(c >= 0.9 for c in fill) or all(c <= 0.12 for c in fill) and h < 3:
            continue                                  # white backgrounds, black rules
        if y0 < axis["y"] or x0 < axis["x_min"] - 0.3 * b or x1 > axis["x_max"] + 0.3 * b:
            continue
        bars.append(r)
    rows: dict[int, dict] = {}
    centres = sorted({round((r[1] + r[3]) / 2, 1) for r in bars})
    gaps = [b - a for a, b in zip(centres, centres[1:]) if b - a > 1]
    pitch = statistics.median(gaps) if gaps else 3 * text_h
    for x0, y0, x1, y1, _ in bars:
        yc = (y0 + y1) / 2
        # a wrapped activity name spans several text lines inside the same row
        cand = [ln for ln in lines if abs((ln["y0"] + ln["y1"]) / 2 - yc) <= 0.45 * pitch]
        if not cand:
            continue
        cand.sort(key=lambda ln: ln["y0"])
        ln = {"text": " ".join(c["text"] for c in cand), "y0": cand[0]["y0"], "id": tuple(id(c) for c in cand)}
        # tick v is the centre of period v, i.e. of the interval [v-1, v]; time is counted from the first tick
        t0 = (x0 - a) / b - 0.5 - (axis["first"] - 1)
        t1 = (x1 - a) / b - 0.5 - (axis["first"] - 1)
        key = ln["id"]
        row = rows.setdefault(key, {"line": ln, "start": t0, "end": t1})
        row["start"], row["end"] = min(row["start"], t0), max(row["end"], t1)
    out = []
    for row in sorted(rows.values(), key=lambda r: r["line"]["y0"]):
        name = _clean_label(row["line"]["text"])
        if len(name) < 3:
            continue
        start, end = max(0.0, round(row["start"], 1)), round(row["end"], 1)
        if end - start < 0.2:
            continue
        out.append({"name": name[:80], "start": start, "end": end})
    if len(out) < 3:
        return None
    return {"unit": unit, "axis": [axis["first"], axis["last"]], "rows": out[:60], "method": "vector_gantt"}


def from_fitz_page(page: Any) -> dict | None:
    """Adapter for a PyMuPDF page; returns None on pages without a drawable time axis."""
    text = page.get_text("text") or ""
    if not SCHEDULE_HINT.search(text):
        return None
    words = [(w[0], w[1], w[2], w[3], w[4], w[5], w[6]) for w in page.get_text("words")]
    if len(words) > 20000:
        return None
    rects = []
    try:
        drawings = page.get_drawings()
    except Exception:
        return None
    if len(drawings) > 60000:
        return None
    for d in drawings:
        fill = d.get("fill")
        if fill is None:
            continue
        r = d.get("rect")
        if r is not None:
            rects.append((r.x0, r.y0, r.x1, r.y1, tuple(fill[:3])))
    g = reconstruct(words, rects, page.rect.width, page.rect.height, text)
    rows = table_rows(words)
    if rows:
        g = dict(g or {}, table_rows=rows)
    return g or None


def table_rows(words: list[tuple]) -> list[str]:
    """Text of each table row (words sharing a baseline), for MS Project-like tables with start/end dates."""
    out: list[list[tuple]] = []
    for w in sorted(words, key=lambda w: ((w[1] + w[3]) / 2, w[0])):
        yc = (w[1] + w[3]) / 2
        if out and abs((out[-1][0][1] + out[-1][0][3]) / 2 - yc) <= 2.0:
            out[-1].append(w)
        else:
            out.append([w])
    rows = [" ".join(w[4] for w in sorted(r, key=lambda w: w[0])) for r in out]
    dated = [r for r in rows if len(re.findall(r"\d{1,2}[/.-]\d{1,2}[/.-]\d{2,4}", r)) >= 2]
    return dated[:120] if len(dated) >= 3 else []
