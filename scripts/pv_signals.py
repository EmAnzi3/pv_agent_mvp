"""«Avanzamenti verso il cantiere» for the PV change report (same idea as the wind report).

The PV report compares the published docs/data.json with the new one. The site signals (priority from
readiness, FER X, EPC found in the documents, press news) live on the memory branch doc-intel-state and
change every night, so they are compared at the same two moments: the state of the memory branch when
docs/data.json was last published (git log) and its state now. Nothing is stored locally; if GitHub is
not reachable the section is left out with a note.
"""
from __future__ import annotations

import json
import subprocess
import time
import urllib.request
from typing import Any

REPO = "EmAnzi3/pv_agent_mvp"
BRANCH = "doc-intel-state"
FILES = {"readiness": "readiness/index.json", "cards": "schede/live_index.json", "news": "news/matches.json"}
LEVELS = {None: 0, "": 0, "Bassa": 1, "Media": 2, "Alta": 3}
NEAR_SITE = ("in_corso", "entro_12_mesi", "realizzato")
EPC_LABEL = {"A": "in atto di cantiere", "B": "solo negli elaborati"}


def _get_json(url: str) -> Any:
    req = urllib.request.Request(url, headers={"Cache-Control": "no-cache", "User-Agent": "pv-agent-report"})
    with urllib.request.urlopen(req, timeout=60) as r:
        return json.loads(r.read().decode("utf-8"))


def _sha_at(path: str, until_iso: str) -> str | None:
    rows = _get_json(f"https://api.github.com/repos/{REPO}/commits?sha={BRANCH}&path={path}&until={until_iso}&per_page=1")
    return rows[0]["sha"] if rows else None


def memory_state(at_iso: str | None = None) -> dict:
    out = {}
    for name, path in FILES.items():
        ref = BRANCH
        if at_iso:
            ref = _sha_at(path, at_iso)
            if not ref:
                out[name] = {}
                continue
        out[name] = _get_json(f"https://raw.githubusercontent.com/{REPO}/{ref}/{path}?t={int(time.time())}")
    return out


def last_publish(path: str = "docs/data.json") -> str | None:
    """When the published data.json was last committed (the 'previous' side of the PV report)."""
    try:
        r = subprocess.run(["git", "log", "-1", "--format=%cI", "--", path], capture_output=True, text=True, timeout=30)
        return r.stdout.strip() or None
    except Exception:
        return None


def snapshot(state: dict) -> dict:
    rd = (state.get("readiness") or {}).get("projects", {})
    cards = state.get("cards") or {}
    news = (state.get("news") or {}).get("projects", {})
    keys = set(rd) | set(cards) | set(news)
    snap = {}
    for k in keys:
        x, c = rd.get(k) or {}, cards.get(k) or {}
        snap[k] = {
            "level": x.get("level") or "",
            "phase": x.get("phase") or "",
            "phase_label": x.get("phase_label") or "",
            "ferx": any("FER X" in r for r in x.get("reasons") or []),
            "epc": c.get("epc") or "",
            "news": len(news.get(k) or []),
        }
    return snap


def compare(prev: dict, cur: dict) -> dict[str, list[dict]]:
    """project_key -> list of {label, old, new} for the changes that bring a project closer to the site."""
    out: dict[str, list[dict]] = {}
    for k, n in cur.items():
        o = prev.get(k) or {"level": "", "phase": "", "phase_label": "", "ferx": False, "epc": "", "news": 0}
        ch = []
        # a project just read that lands on «Bassa · in iter» is not a step forward: only a priority that
        # reaches Media/Alta, or an authorisation, counts
        if LEVELS.get(n["level"], 0) > LEVELS.get(o["level"], 0) and (
                LEVELS.get(n["level"], 0) >= 2 or n["phase"] in ("autorizzato",) + NEAR_SITE):
            ch.append({"label": "Priorità cantiere",
                       "old": " · ".join(x for x in [o["level"], o["phase_label"]] if x) or "—",
                       "new": " · ".join(x for x in [n["level"], n["phase_label"]] if x)})
        elif n["phase"] in NEAR_SITE and o["phase"] != n["phase"]:
            ch.append({"label": "Fase", "old": o["phase_label"] or "—", "new": n["phase_label"]})
        if n["ferx"] and not o["ferx"]:
            ch.append({"label": "FER X", "old": "no", "new": "incentivo assegnato"})
        if n["epc"] and (not o["epc"] or (n["epc"] == "A" and o["epc"] != "A")):
            ch.append({"label": "EPC nei documenti", "old": EPC_LABEL.get(o["epc"], "—"), "new": EPC_LABEL.get(n["epc"], n["epc"])})
        if n["news"] > o["news"]:
            ch.append({"label": "Notizie di stampa", "old": str(o["news"] or "nessuna"), "new": str(n["news"])})
        if ch:
            out[k] = ch
    return out


def progress(new_records: list[dict]) -> tuple[list[dict], str]:
    """Rows for the report (record + changes), sorted by MW, and a note on the comparison window."""
    since = last_publish()
    try:
        cur = snapshot(memory_state())
        prev = snapshot(memory_state(since)) if since else {}
    except Exception as exc:
        return [], f"Segnali di cantiere non disponibili ({exc})."[:200]
    if not prev:
        return [], "Segnali di cantiere: nessuno stato precedente da confrontare."
    by_key = {str(r.get("project_key") or ""): r for r in new_records}
    rows = [{"record": by_key[k], "diffs": ch} for k, ch in compare(prev, cur).items() if k in by_key]
    rows.sort(key=lambda r: -float(r["record"].get("power_mw") or 0))
    return rows, f"Rispetto alla pubblicazione del {since[8:10]}/{since[5:7]}/{since[:4]}: priorità cantiere salita, FER X, EPC trovato nei documenti, nuove notizie di stampa."
