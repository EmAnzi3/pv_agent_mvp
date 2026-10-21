"""Human-readable Markdown rendering of a dossier JSON (same content, no new facts)."""
from __future__ import annotations

import json
from typing import Any

CLASS_LABEL = {"confermato": "Confermato", "nuovo_verificato": "Nuovo verificato",
               "conflittuale": "Conflittuale", "non_determinabile": "Non determinabile",
               "non_applicabile": "Non applicabile"}

SECTIONS = [
    ("Società, ruoli e contatti", ("role.", "company.", "contact.")),
    ("Configurazione tecnica", ("tech.", "grid.")),
    ("Ubicazione e geometrie", ("geo.",)),
    ("Cronoprogramma", ("schedule.",)),
]


def _fmt(v: Any) -> str:
    if isinstance(v, dict):
        if "vertices_source" in v:
            return f"poligono {v['vertex_count']} vertici, {v['area_ha']} ha, {v['crs']}"
        if "min" in v and "max" in v:
            return f"{v['min']:g}" + (f"–{v['max']:g}" if v["max"] != v["min"] else "") + " mesi"
        return json.dumps(v, ensure_ascii=False)[:120]
    return str(v)


def _link(ex: dict) -> str:
    doc = ex["url"].rsplit("/", 1)[-1]
    return f"[{doc}, p.{ex.get('page')}]({ex['url']})"


def render(d: dict[str, Any]) -> str:
    p, a = d["project"], d["acquisition"]
    out = [f"# Dossier documentale — {p.get('title') or p['project_id']}", "",
           f"Schema `{d['schema']}` · toolbox {d['toolbox_version']} · generato {d['generated_utc']}",
           f"Stato review: **{d['review']['status']}** — approvazione automatica: **no**", "",
           "## 1. Inventario documentale", "",
           "| Procedura | Dichiarati | Censiti | Pagine indice | Errori |", "|---|---|---|---|---|"]
    for pr in a["procedures"]:
        out.append(f"| [{pr['id']}]({pr['url']}) | {pr['declared']} | {pr['enumerated']} | {pr['index_pages']} | {len(pr['errors'])} |")
    out += ["", f"Riferimenti {a['references']} · elaborati {a['processed_references']} · contenuti distinti (SHA-256) "
            f"{a['distinct_sha256']} · duplicati {a['duplicate_references']} · download falliti {a['download_failures']}",
            f"Pagine {a['pages_total']}: digitali {a['pages_digital']}, OCR {a['pages_ocr']}, OCR in attesa "
            f"{a['pages_ocr_pending']}, illeggibili {a['pages_unreadable']} · errori parser {a['parse_errors']}", ""]
    ag = d["gates"]["acquisition"]
    out.append(f"Gate acquisizione: **{'PASS' if ag['pass'] else 'APERTO'}**" + ("" if ag["pass"] else " — " + "; ".join(ag["issues"])))
    out += ["", "## 2. Confronto con il record canonico", "",
            f"Corrispondenza: `{d['canonical'].get('status')}` ({d['canonical'].get('policy', '')}) "
            f"chiavi {d['canonical'].get('project_keys')}", "",
            "| Campo | Canonico | Fonte | Classificazione |", "|---|---|---|---|"]
    for r in d["delta"]:
        out.append(f"| {r['field']} | {r['canonical']} | {_fmt(r['source'])} | {CLASS_LABEL.get(r['class'], r['class'])} |")
    for title, prefixes in SECTIONS:
        out += ["", f"## {title}", "", "| Campo | Soggetto | Valore | Stato | Occ. | Esempio |", "|---|---|---|---|---|---|"]
        rows = [g for g in d["groups"] if g["field"].startswith(prefixes)]
        if not rows:
            out.append("| — | — | nessuna evidenza estratta (≠ assente nei documenti) | non_determinabile | | |")
        for g in rows:
            vals = g["values"]
            if vals and isinstance(vals[0], dict):
                for v in vals[:6]:
                    out.append(f"| {g['field']} | {g['subject'] or ''} | {_fmt(v['value'])} | {g['status']} | "
                               f"{v['occurrences']} | {_link(v['example'])} |")
            else:
                out.append(f"| {g['field']} | {g['subject'] or ''} | {', '.join(map(str, vals))} | {g['status']} | | {g.get('note','')} |")
    pts = [f for f in d["facts"] if f["field"] == "geo.utm_point"]
    if pts:
        objs: dict[str, int] = {}
        for f in pts:
            objs[f["qualifiers"]["object"]] = objs.get(f["qualifiers"]["object"], 0) + 1
        out += ["", "Punti UTM per oggetto: " + ", ".join(f"{k} {v}" for k, v in sorted(objs.items()))]
    out += ["", "## Proposta di arricchimento (non applicata)", "",
            "| Campo | Soggetto | Valore | Bloccante | Motivi |", "|---|---|---|---|---|"]
    gates = {g["fact_id"]: g for g in d["gates"]["proposal"]}
    for item in d["proposal"]:
        g = gates.get(item["fact_id"], {})
        out.append(f"| {item['field']} | {item['subject'] or ''} | {_fmt(item['value'])} | "
                   f"{'sì' if g.get('blocking') else 'no'} | {'; '.join(g.get('reasons', []))} |")
    e = d["efficiency"]
    out += ["", "## Efficienza", "",
            f"Durata {e['seconds_total']} s · {e['seconds_per_document']} s/documento · pagine OCR {e['ocr_pages_used']} · cache {e['cache']}", ""]
    return "\n".join(out)
