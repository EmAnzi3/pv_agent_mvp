"""Trade press and listed-EPC news matched to radar projects (evidence level C: compatible news, to verify).

Daily: read the RSS feeds of the sector press (pv-magazine.it, QualEnergia, Rinnovabili.it, SolareB2B),
keep the articles about photovoltaic plants being built or contracted (EPC, contract, works start...),
extract MW, municipalities (ISTAT list), and the companies named as builders, then match them to the
radar records:
  * 'comune + MW'        - a municipality of the record is named and one MW value is within 15 %;
  * 'comune + proponente' - a municipality of the record and the record's proponent are named;
  * 'provincia + MW + proponente' - no municipality, but province/region, MW within 5 % and proponent.
A municipality alone or MW alone never makes a match. Matches are information to verify, never a
confirmed EPC (levels A/B come only from the project documents).

Memory branch: news/items.json (relevant articles, 400 days) and news/matches.json
(project_key -> articles). Articles are read politely (one request every 2 s, a cap per run).
"""
from __future__ import annotations

import html as html_lib
import json
import re
import time
import xml.etree.ElementTree as ET
from datetime import datetime, timedelta, timezone
from email.utils import parsedate_to_datetime
from pathlib import Path
from typing import Any, Callable

from .admin_areas import _key as place_key, _table as comuni_table

SOURCES = [
    {"name": "pv magazine Italia", "feed": "https://www.pv-magazine.it/feed/"},
    {"name": "QualEnergia", "feed": "https://www.qualenergia.it/feed/"},
    {"name": "Rinnovabili.it", "feed": "https://www.rinnovabili.it/feed/"},
    {"name": "SolareB2B", "feed": "https://www.solareb2b.it/feed/"},
]
UA = {"User-Agent": "Mozilla/5.0 (PV Agent news reader; contact via GitHub EmAnzi3/pv_agent_mvp)"}
PV = re.compile(r"(?i)fotovoltaic|agrivoltaic|agrovoltaic|agri-?fotovoltaic|solar[ei]?\b|\bFV\b|\bPV\b|MWp")
BUILD = re.compile(r"(?i)\bEPC\b|commess|contratt|aggiudic|appalt|costruzion|cantier|avvio\s+(?:dei\s+)?lavori|"
                   r"inizio\s+(?:dei\s+)?lavori|realizzer|realizzazione|balance\s+of\s+plant|\bBoP\b|affidat|"
                   r"entrat\w*\s+in\s+esercizio|allacciat|inaugur|connessione\s+alla\s+rete|financial\s+close|"
                   r"finanziament|notice\s+to\s+proceed")
MW_RE = re.compile(r"(?i)(\d{1,3}(?:[.\s]\d{3})*(?:,\d+)?|\d+(?:[.,]\d+)?)\s*(MWp|MWac|MWdc|MW|kWp)\b")
LEGAL = re.compile(r"(?:[A-Z][\w&'’.\-]*\s+){0,4}[A-Z][\w&'’.\-]*\s+(?:S\.?p\.?A\.?|S\.?r\.?l\.?|SpA|Srl|S\.?A\.?|GmbH|S\.?L\.?)\b")
BUILDER_CTX = re.compile(r"(?i)\bEPC\b|contractor|appaltat|commess|aggiudic|realizzat\w*\s+da|costruit\w*\s+da|"
                         r"affidat\w*\s+a|general\s+contractor|balance\s+of\s+plant|\bBoP\b|si\s+occuper")
# EPC / builders active in Italy (listed companies publish their contracts); extended from the cards
KNOWN_BUILDERS = ["ESI", "ESPE", "Comal", "Innovatec", "Solarig", "Circet", "Altenia", "Protesa", "Tecnolines",
                  "Aiem Group", "Panorama", "Impresa Gema", "Geosecure", "BayWa r.e.", "Elmya", "Gransolar", "GRS",
                  "Prodiel", "Gonvarri", "Eiffage", "Sirti", "Ansaldo Energia", "Sun Ballast", "Convert Italia",
                  "Enertronica", "Sicilsaldo", "Renco", "Siram", "Building Energy", "Ecotech", "Emeren",
                  "Sonnedix", "European Energy", "Ergycapital"]
KEEP_DAYS = 400


def _text(node: ET.Element | None) -> str:
    return (node.text or "").strip() if node is not None else ""


def parse_feed(xml_text: str) -> list[dict]:
    """RSS 2.0 items: title, link, date (ISO), text (content:encoded or description, tags removed)."""
    items = []
    try:
        root = ET.fromstring(xml_text.encode("utf-8") if isinstance(xml_text, str) else xml_text)
    except ET.ParseError:
        return items
    ns = {"content": "http://purl.org/rss/1.0/modules/content/"}
    for it in root.iter("item"):
        body = _text(it.find("content:encoded", ns)) or _text(it.find("description"))
        try:
            date = parsedate_to_datetime(_text(it.find("pubDate"))).astimezone(timezone.utc).date().isoformat()
        except (TypeError, ValueError):
            date = None
        items.append({"title": html_lib.unescape(_text(it.find("title"))), "link": _text(it.find("link")),
                      "date": date, "text": clean_html(body)})
    return items


def clean_html(s: str) -> str:
    s = re.sub(r"(?is)<(script|style)[^>]*>.*?</\1>", " ", s or "")
    s = re.sub(r"<[^>]+>", " ", s)
    return re.sub(r"\s+", " ", html_lib.unescape(s)).strip()


def relevant(item: dict) -> bool:
    t = f"{item.get('title', '')} {item.get('text', '')}"
    return bool(PV.search(t) and BUILD.search(t))


def mw_values(text: str) -> list[float]:
    out = []
    for num, unit in MW_RE.findall(text):
        n = num.replace(" ", "")
        if re.fullmatch(r"\d{1,3}(?:\.\d{3})+(?:,\d+)?", n):
            n = n.replace(".", "")
        n = n.replace(",", ".")
        try:
            v = float(n)
        except ValueError:
            continue
        if unit.lower() == "kwp":
            v /= 1000
        if 0.5 <= v <= 3000:
            out.append(round(v, 3))
    return sorted(set(out))


def municipalities(text: str) -> list[str]:
    """ISTAT municipality names found in the text (2+ letters, capitalised in the text)."""
    table = comuni_table()
    found = []
    words = re.findall(r"[A-ZÀ-Ý][\wÀ-ÿ'’\-]+(?:\s+(?:d[ie]l?l?[aoei]?|di|del|della|sul|sulla|in|al|'|[A-ZÀ-Ý][\wÀ-ÿ'’\-]+))*", text)
    for w in words:
        parts = w.split()
        for i in range(len(parts)):
            for j in range(min(len(parts), i + 5), i, -1):
                k = place_key(" ".join(parts[i:j]))
                if k in table and len(k) > 3:
                    found.append(table[k][0][0])
                    break
    return sorted(set(found))


def builders(text: str) -> list[str]:
    """Companies named next to EPC/contract wording, plus known builders named anywhere."""
    out = []
    for sent in re.split(r"(?<=[.;:!?])\s+", text):
        if BUILDER_CTX.search(sent):
            out += [m.group(0).strip() for m in LEGAL.finditer(sent)]
            out += [b for b in KNOWN_BUILDERS if re.search(rf"(?<![\w-]){re.escape(b)}(?![\w-])", sent)]
    return sorted(set(out))


def _comp_key(name: str) -> str:
    s = place_key(name)
    return re.sub(r"\b(s ?r ?l|s ?p ?a|srl|spa|s ?a|societa|agricola|soc|unipersonale)\b", " ", s).strip()


def match(item: dict, records: list[dict]) -> list[tuple[dict, str]]:
    text = f"{item['title']} {item['text']}"
    mws, comuni = item.get("mw") or mw_values(text), {place_key(c) for c in (item.get("comuni") or municipalities(text))}
    low = place_key(text)
    out = []
    for r in records:
        if r.get("is_terna") or not r.get("project_key"):
            continue
        rmw = float(r.get("power_mw") or 0)
        rc = {place_key(x) for x in re.split(r"[,;/]", str(r.get("municipalities") or "")) if x.strip()}
        prop = _comp_key(str(r.get("proponent") or "").split(";")[0])
        prop_hit = len(prop) >= 5 and prop in low
        mw_hit = lambda tol: rmw > 0 and any(abs(v - rmw) <= tol * rmw for v in mws)
        if rc & comuni:
            if mw_hit(0.15):
                out.append((r, "comune + MW"))
            elif prop_hit:
                out.append((r, "comune + proponente"))
        elif prop_hit and mw_hit(0.05) and (str(r.get("province") or "") and re.search(rf"\b{re.escape(str(r['province']))}\b", text)
                                            or (r.get("region") and str(r["region"]).lower() in text.lower())):
            out.append((r, "provincia + MW + proponente"))
    return out


def run(data_json: str, memory: str, get: Callable[[str], Any] | None = None, backfill_pages: int = 0,
        max_articles: int = 40, sleep: Callable[[float], None] = time.sleep, log: Callable[[str], None] = print) -> dict:
    import requests
    get = get or (lambda u: requests.get(u, headers=UA, timeout=30))
    mem = Path(memory) / "news"
    mem.mkdir(parents=True, exist_ok=True)
    items_path, matches_path = mem / "items.json", mem / "matches.json"
    known = {i["link"]: i for i in (json.loads(items_path.read_text(encoding="utf-8")) if items_path.exists() else [])}
    stats = {"feeds": {}, "new_relevant": 0, "articles_fetched": 0}
    fetched = 0
    for src in SOURCES:
        pages = [src["feed"]] + [f"{src['feed']}?paged={p}" for p in range(2, backfill_pages + 2)]
        n_ok = 0
        for url in pages:
            try:
                r = get(url)
                if getattr(r, "status_code", 200) != 200:
                    break
                feed_items = parse_feed(r.text)
            except Exception as exc:                       # one feed down never stops the others
                log(f"[news] {src['name']}: {str(exc)[:120]}")
                break
            finally:
                sleep(2)
            if not feed_items:
                break
            n_ok += len(feed_items)
            for it in feed_items:
                if not it["link"] or it["link"] in known or not relevant(it):
                    continue
                if len(it["text"]) < 600 and fetched < max_articles:   # summary only: read the article
                    try:
                        page = get(it["link"])
                        body = re.search(r"(?is)<article[^>]*>(.*?)</article>", page.text)
                        it["text"] = clean_html(body.group(1) if body else page.text)[:20000]
                        fetched += 1
                    except Exception:
                        pass
                    finally:
                        sleep(2)
                    if not relevant(it):
                        continue
                text = f"{it['title']} {it['text']}"
                it.update({"source": src["name"], "mw": mw_values(text), "comuni": municipalities(text),
                           "builders": builders(text), "text": it["text"][:4000]})
                known[it["link"]] = it
                stats["new_relevant"] += 1
        stats["feeds"][src["name"]] = n_ok
    stats["articles_fetched"] = fetched
    cutoff = (datetime.now(timezone.utc) - timedelta(days=KEEP_DAYS)).date().isoformat()
    items = [i for i in known.values() if not i.get("date") or i["date"] >= cutoff]
    items.sort(key=lambda i: i.get("date") or "", reverse=True)
    items_path.write_text(json.dumps(items, ensure_ascii=False, indent=0), encoding="utf-8")
    records = json.loads(Path(data_json).read_text(encoding="utf-8-sig")).get("records", [])
    matches: dict[str, list] = {}
    for it in items:
        for r, basis in match(it, records):
            matches.setdefault(r["project_key"], []).append({
                "title": it["title"], "link": it["link"], "date": it.get("date"), "source": it.get("source"),
                "basis": basis, "builders": it.get("builders") or [], "mw": it.get("mw") or []})
    matches_path.write_text(json.dumps({"updated": datetime.now(timezone.utc).date().isoformat(),
                                        "level": "C", "note": "notizia compatibile con il progetto: da verificare",
                                        "projects": matches}, ensure_ascii=False, indent=0, sort_keys=True), encoding="utf-8")
    stats.update({"items": len(items), "projects_matched": len(matches)})
    log(f"[news] {json.dumps(stats, ensure_ascii=False)}")
    return stats
