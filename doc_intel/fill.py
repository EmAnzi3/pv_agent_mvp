"""Fill EMPTY radar fields from the project cards (approved policy, 21/10/2026).

Only empty fields are filled, never overwritten: power (MW), province, region, municipalities.
A value is taken only when it is solid:
* power: the card's power tile is 'verificato' (title confirmed by the documents) or 'da documenti'
  (most frequent plant power in the documents, at least 3 occurrences, no conflicting value flagged);
* province/region/municipalities: the card's location (authority sheet, documents or ISTAT list).
Differences between radar and cards (a radar value that disagrees) are listed in the audit, never applied.
Every change is written to the audit CSV and marked on the record (_filled_from_card).
"""
from __future__ import annotations

import csv
import json
import re
from pathlib import Path
from typing import Any


def _mw(tile: dict) -> float | None:
    m = re.match(r"\s*([\d.]+(?:,\d+)?)\s*(MWp|MW|kWp|kW)\b", tile.get("value") or "")
    if not m:
        return None
    num = float(m.group(1).replace(".", "").replace(",", ".")) if "," in m.group(1) else float(m.group(1))
    return round(num / 1000, 6) if m.group(2).lower().startswith("k") else num


def _empty(v: Any) -> bool:
    return v in (None, "", 0, 0.0, "n/d")


def card_values(card: dict) -> dict:
    out = {}
    tile = next((t for t in card.get("sintesi", []) if t.get("key") == "potenza"), None)
    if tile and tile.get("status") in ("verificato", "da documenti"):
        mw = _mw(tile)
        if mw and 0.05 <= mw <= 5000:
            out["power_mw"] = mw
    loc = card.get("ubicazione") or {}
    if loc.get("provincia") and re.fullmatch(r"[A-Z]{2}(, [A-Z]{2})*", loc["provincia"]):
        out["province"] = loc["provincia"]
    if loc.get("regione"):
        out["region"] = loc["regione"]
    if loc.get("comuni"):
        out["municipalities"] = ", ".join(loc["comuni"])
    return out


def fill_radar(data_path: str | Path, cards_dir: str | Path, audit: str | Path | None = None) -> dict:
    data_path, cards_dir = Path(data_path), Path(cards_dir)
    data = json.loads(data_path.read_text(encoding="utf-8-sig"))
    cards = {}
    for f in cards_dir.glob("*.json"):
        if f.name in ("index.json", "live_index.json"):
            continue
        try:
            c = json.loads(f.read_text(encoding="utf-8"))
        except (OSError, ValueError):
            continue
        if c.get("project_key"):
            cards[c["project_key"]] = c
    rows, filled, differences = [], 0, 0
    lists = [data.get("records", [])] + ([data["top_projects"]] if isinstance(data.get("top_projects"), list) else [])
    for lst in lists:
        for r in lst:
            c = cards.get(r.get("project_key"))
            if not c:
                continue
            for field, value in card_values(c).items():
                current = r.get(field)
                if _empty(current):
                    r[field] = value
                    r.setdefault("_filled_from_card", [])
                    if field not in r["_filled_from_card"]:
                        r["_filled_from_card"].append(field)
                    if lst is data.get("records"):
                        filled += 1
                        rows.append({"project_key": r["project_key"], "field": field, "old": current, "new": value,
                                     "action": "filled", "card_updated": c.get("updated"), "url": r.get("url")})
                elif field == "power_mw" and isinstance(current, (int, float)) and abs(current - value) > 0.03 * max(current, value):
                    if lst is data.get("records"):
                        differences += 1
                        rows.append({"project_key": r["project_key"], "field": field, "old": current, "new": value,
                                     "action": "difference_not_applied", "card_updated": c.get("updated"), "url": r.get("url")})
    data_path.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")
    if audit:
        Path(audit).parent.mkdir(parents=True, exist_ok=True)
        with open(audit, "w", newline="", encoding="utf-8-sig") as fh:
            w = csv.DictWriter(fh, fieldnames=["project_key", "field", "old", "new", "action", "card_updated", "url"], delimiter=";")
            w.writeheader()
            w.writerows(rows)
    return {"filled": filled, "differences": differences, "cards": len(cards)}
