"""GSE FER X rankings (photovoltaic): which radar projects were awarded the incentive. Information only.

Table A of each competitive procedure lists the plants admitted in a useful position (company, VAT,
region, province, municipality, power). An awarded plant has a guaranteed tariff and a deadline to enter
operation: the strongest public signal that it will be built.

The GSE site refuses automated downloads from cloud networks, so the tables are downloaded by hand from
https://www.gse.it/servizi-per-te/fonti-rinnovabili/ferx/graduatorie and read with
`python -m doc_intel ferx-import <pdf>...`, which writes doc_intel/data/ferx_awarded.json (public data).
`python -m doc_intel ferx` then matches the awarded plants to the radar and saves ferx/matches.json on
the memory branch:
  * 'P. IVA del proponente' - the plant's VAT number is the VAT of the project's proponent (card);
  * 'comune + proponente'   - same municipality, same company name and a compatible power (within 35 %);
A different company with a similar power in the same municipality is never matched.
"""
from __future__ import annotations

import json
import re
from datetime import datetime, timezone
from pathlib import Path

from .admin_areas import _key as place_key, _table as comuni_table

DATA = Path(__file__).parent / "data" / "ferx_awarded.json"
PROCEDURES = {"PC_FTVNZIA_2025": ("FER X transitorio – fotovoltaico NZIA (PC_FTVNZIA_2025_01)", "2025-12-10"),
              "PC_FTV_2025": ("FER X transitorio – fotovoltaico (PC_FTV_2025_01)", "2025-12-01")}
REGIONS = ["VALLE D'AOSTA", "PIEMONTE", "LOMBARDIA", "TRENTINO ALTO ADIGE", "VENETO", "FRIULI VENEZIA GIULIA", "LIGURIA",
           "EMILIA ROMAGNA", "TOSCANA", "UMBRIA", "MARCHE", "LAZIO", "ABRUZZO", "MOLISE", "CAMPANIA", "PUGLIA",
           "BASILICATA", "CALABRIA", "SICILIA", "SARDEGNA"]
ROW = re.compile(r"^(?P<pos>\d+)\s+(?P<code>FER\d+)\s+(?P<name>.+?)\s+(?P<vat>\d{11})\s+(?P<place>.+?)\s+"
                 r"(?P<cat>Nuova Costruzione|Integrale Ricostruzione|Potenziamento|Rifacimento|Riattivazione|[A-Z][a-z]+(?: [A-Za-z]+)?)\s+"
                 r"(?P<pct>[\d,]+%)\s+(?P<flags>(?:(?:SI|NO)\s+)+)(?P<date>\d{1,2}/\d{1,2}/\d{2})\s+\d{1,2}\.\d{2}\s+"
                 r"(?P<kw>[\d.]+,\d{3})(?:\s+(?P<cum>[\d.]+,\d{3}))?(?:\s+(?P<quota>[\d. ]+,\d{3}))?")


def _num(s: str | None) -> float | None:
    if not s:
        return None
    return float(re.sub(r"[ .]", "", s).replace(",", "."))


def split_place(place: str) -> tuple[str, str, str]:
    """'EMILIA ROMAGNA REGGIO EMILIA GUASTALLA' -> region, province, municipality (ISTAT list for the last)."""
    reg = next((r for r in sorted(REGIONS, key=len, reverse=True) if place.upper().startswith(r)), "")
    rest = place[len(reg):].strip()
    words, table = rest.split(), comuni_table()
    for i in range(len(words)):                       # longest municipality name at the end
        cand = " ".join(words[i:])
        if place_key(cand) in table and i > 0:
            return reg.title(), " ".join(words[:i]).title(), table[place_key(cand)][0][0]
    return reg.title(), (words[0].title() if words else ""), " ".join(words[1:]).title()


def parse_text(text: str, procedure: str) -> list[dict]:
    rows = []
    lines = [ln.strip() for ln in text.splitlines()]
    for i, ln in enumerate(lines):
        if not re.match(r"^\d+\s+FER\d+", ln):
            continue
        joined = ln + (" " + lines[i + 1] if i + 1 < len(lines) and not re.match(r"^\d+\s+FER\d+", lines[i + 1]) else "")
        m = ROW.match(joined)
        if not m:
            continue
        region, province, comune = split_place(m.group("place"))
        kw = _num(m.group("kw"))
        quota = _num(m.group("quota"))
        rows.append({"procedura": procedure, "posizione": int(m.group("pos")), "codice": m.group("code"),
                     "ragione_sociale": m.group("name").strip(), "piva": m.group("vat"),
                     "regione": region, "provincia": province, "comune": comune, "categoria": m.group("cat"),
                     "ribasso": m.group("pct"), "data_domanda": m.group("date"),
                     "kw": kw, "kw_ammessi": quota if quota and kw and quota <= kw * 1.001 else kw})
    return rows


def import_pdfs(paths: list[str]) -> dict:
    from pypdf import PdfReader
    rows = []
    for p in paths:
        key = next((k for k in PROCEDURES if k in Path(p).name), None)
        if not key:
            raise SystemExit(f"{p}: procedura non riconosciuta dal nome del file")
        text = "\n".join(pg.extract_text() or "" for pg in PdfReader(p).pages)
        rows += parse_text(text, PROCEDURES[key][0])
    out = {"fonte": "GSE – Graduatorie FER X, Tabella A (ammessi in posizione utile)",
           "url": "https://www.gse.it/servizi-per-te/fonti-rinnovabili/ferx/graduatorie",
           "procedure": {v[0]: {"pubblicata": v[1]} for v in PROCEDURES.values()}, "impianti": rows}
    DATA.write_text(json.dumps(out, ensure_ascii=False, indent=0), encoding="utf-8")
    return {"impianti": len(rows), "MW": round(sum(r["kw"] or 0 for r in rows) / 1000, 1)}


def _comp(name: str) -> str:
    return re.sub(r"\b(s ?r ?l|s ?p ?a|srl|spa|s ?a ?s|societa|agricola|unipersonale)\b", " ", place_key(name)).strip()


def match(awarded: list[dict], records: list[dict], card_vats: dict[str, set[str]]) -> dict[str, list]:
    """For every radar record, the awarded plant in the same municipality: same VAT as the proponent
    first, then the same company with a compatible power (within 35 %)."""
    by_comune: dict[str, list[dict]] = {}
    for a in awarded:
        by_comune.setdefault(place_key(a["comune"]), []).append(a)
    strong: dict[str, tuple] = {}
    for r in records:
        if r.get("is_terna") or not r.get("project_key"):
            continue
        key, rmw = r["project_key"], float(r.get("power_mw") or 0)
        pool = [a for c in {place_key(x) for x in re.split(r"[,;/]", str(r.get("municipalities") or "")) if x.strip()}
                for a in by_comune.get(c, [])]
        if not pool:
            continue
        prop = _comp(str(r.get("proponent") or "").split(";")[0])
        ratio = lambda a: abs(rmw - a["kw"] / 1000) / max(rmw, a["kw"] / 1000) if rmw and a["kw"] else 9
        hit = next(((a, "P. IVA del proponente") for a in pool if a["piva"] in card_vats.get(key, ())), None)
        if not hit:
            same = [a for a in pool if prop and _comp(a["ragione_sociale"]) == prop and ratio(a) <= 0.35]
            if same:
                hit = (min(same, key=ratio), "comune + proponente")
        if hit:
            strong[key] = hit
    # a different company with a similar power in the same municipality is NOT a match: tested on the
    # 2025 tables, those pairs were mostly different plants (port authority roofs, other developers)
    fields = ("procedura", "posizione", "codice", "ragione_sociale", "piva", "comune", "provincia", "kw", "kw_ammessi", "data_domanda")
    return {key: [{**{k: a[k] for k in fields}, "basis": basis}] for key, (a, basis) in strong.items()}


def run(data_json: str, memory: str) -> dict:
    awarded = json.loads(DATA.read_text(encoding="utf-8"))["impianti"]
    records = json.loads(Path(data_json).read_text(encoding="utf-8-sig")).get("records", [])
    card_vats: dict[str, set[str]] = {}
    for f in (Path(memory) / "schede").glob("*.json"):
        if f.name in ("index.json", "live_index.json"):
            continue
        try:
            c = json.loads(f.read_text(encoding="utf-8"))
        except (OSError, ValueError):
            continue
        vats = {e.get("vat") for e in (c.get("soggetti") or []) if "Proponente" in (e.get("roles") or []) and e.get("vat")}
        if c.get("project_key") and vats:
            card_vats[c["project_key"]] = vats
    m = match(awarded, records, card_vats)
    out_dir = Path(memory) / "ferx"
    out_dir.mkdir(parents=True, exist_ok=True)
    (out_dir / "matches.json").write_text(json.dumps({
        "updated": datetime.now(timezone.utc).date().isoformat(), "fonte": "GSE – Graduatorie FER X, Tabella A",
        "url": "https://www.gse.it/servizi-per-te/fonti-rinnovabili/ferx/graduatorie", "projects": m},
        ensure_ascii=False, indent=0, sort_keys=True), encoding="utf-8")
    basis = {}
    for v in m.values():
        for x in v:
            basis[x["basis"]] = basis.get(x["basis"], 0) + 1
    return {"impianti_ammessi": len(awarded), "progetti_radar_abbinati": len(m), "per_criterio": basis,
            "MW_abbinati": round(sum(float(next((r.get("power_mw") or 0) for r in records if r.get("project_key") == k)) for k in m), 1)}
