"""Wind change report as a local HTML file, like the PV changes_latest.html.

The report itself is built in the cloud at every census rebuild (doc_intel/wind_changes.py, memory branch
wind/changes/). This command downloads the latest one (or waits for a newer one after a push) and writes
reports/change_reports/wind_changes_latest.html with the same look as the PV report.
"""
from __future__ import annotations

import html
import json
import time
import urllib.request
from datetime import datetime, timezone
from pathlib import Path

RAW = "https://raw.githubusercontent.com/EmAnzi3/pv_agent_mvp/doc-intel-state/wind/changes/"
CSS = """body {
  font-family: Arial, sans-serif;
  color: #111827;
  background: #f3f6f8;
  line-height: 1.45;
  margin: 0;
  padding: 24px;
}
main {
  max-width: 1500px;
  margin: 0 auto;
}
h1 {
  margin-bottom: 4px;
}
.small {
  color: #6b7280;
  font-size: 13px;
}
.cards {
  display: grid;
  grid-template-columns: repeat(4, minmax(160px, 1fr));
  gap: 12px;
  margin: 18px 0 24px;
}
.card {
  border: 1px solid #d1d5db;
  border-radius: 14px;
  padding: 14px;
  background: white;
}
.card strong {
  display: block;
  font-size: 28px;
  color: #0f172a;
}
section {
  background: white;
  border: 1px solid #d1d5db;
  border-radius: 14px;
  padding: 18px;
  margin-bottom: 20px;
  overflow-x: auto;
}
table {
  border-collapse: collapse;
  width: 100%;
  margin: 12px 0 8px;
  font-size: 13px;
}
th, td {
  border-bottom: 1px solid #e5e7eb;
  padding: 8px;
  vertical-align: top;
}
th {
  background: #f9fafb;
  text-align: left;
  position: sticky;
  top: 0;
}
a {
  color: #0f766e;
  font-weight: 700;
  text-decoration: none;
}
ul {
  margin: 0;
  padding-left: 18px;
}
"""
LABELS = {"card": "Scheda documenti"}


def _get(name: str) -> dict:
    req = urllib.request.Request(RAW + name + f"?t={int(time.time())}", headers={"Cache-Control": "no-cache"})
    with urllib.request.urlopen(req, timeout=60) as r:
        return json.loads(r.read().decode("utf-8"))


def fetch(newer_than: str | None = None, wait_min: float = 0) -> dict:
    """Latest report; with newer_than (ISO UTC) waits up to wait_min minutes for one generated after it."""
    deadline = time.time() + wait_min * 60
    while True:
        rep = {}
        try:
            rep = _get("latest.json")
        except Exception as exc:
            print(f"[wind-report] lettura non riuscita: {exc}")
        if rep and (not newer_than or (rep.get("generated") or "") >= newer_than):
            return rep
        if time.time() >= deadline:
            if rep:
                print("[wind-report] nessun report piu' recente: mostro l'ultimo disponibile "
                      "(il censimento potrebbe non aver trovato cambiamenti, o e' ancora in corso)")
            return rep
        print("[wind-report] attendo il censimento nel cloud...")
        time.sleep(30)


def esc(v) -> str:
    return html.escape("" if v is None else str(v))


def mw(v) -> str:
    try:
        return f"{float(v):,.1f}".replace(",", "X").replace(".", ",").replace("X", ".")
    except (TypeError, ValueError):
        return "n/d"


def _val(field: str, v) -> str:
    if isinstance(v, list):
        return ", ".join(map(str, v)) or "—"
    if field == "power_mw":
        return mw(v) + " MW" if v is not None else "—"
    if field == "card":
        return "presente" if v else "assente"
    if field == "news":
        return f"{v} notizie" if v else "nessuna"
    return str(v) if v not in (None, "") else "—"


def _title(r: dict) -> str:
    t = esc((r.get("title") or "")[:160]) + ("…" if len(r.get("title") or "") > 160 else "")
    u = r.get("url") or ""
    t = f'<a href="{esc(u)}" target="_blank" rel="noopener noreferrer">{t}</a>' if u.startswith("http") else t
    return t + (" <em>(variante)</em>" if r.get("secondaria") else "")


def _loc(r: dict) -> str:
    return esc(" - ".join(x for x in [r.get("province") or "", r.get("municipalities") or ""] if x) or "n/d")


def _rec_table(title: str, rows: list[dict]) -> str:
    if not rows:
        return f"<section><h2>{esc(title)}</h2><p>Nessun record.</p></section>"
    body = "".join(
        f"<tr><td>{esc(r.get('source_label'))}</td><td>{esc(r.get('region'))}</td><td>{_loc(r)}</td>"
        f"<td>{mw(r.get('power_mw'))}</td><td>{esc(r.get('proponent'))}</td><td>{_title(r)}</td>"
        f"<td>{esc(r.get('procedure_status'))}{'<br><span class=small>' + esc(r.get('readiness')) + '</span>' if r.get('readiness') else ''}</td></tr>"
        for r in rows)
    return (f"<section><h2>{esc(title)} ({len(rows)})</h2><table><thead><tr><th>Fonte</th><th>Regione</th>"
            f"<th>Localizzazione</th><th>MW</th><th>Proponente</th><th>Titolo</th><th>Stato</th></tr></thead>"
            f"<tbody>{body}</tbody></table></section>")


def _chg_table(title: str, rows: list[dict], note: str = "") -> str:
    if not rows:
        return f"<section><h2>{esc(title)}</h2><p>Nessun record.</p></section>"
    body = ""
    for c in rows:
        items = "".join(f"<li><strong>{esc(d['label'])}</strong>: {esc(_val(d['field'], d['old']))} → {esc(_val(d['field'], d['new']))}</li>"
                        for d in c["changes"])
        body += (f"<tr><td>{esc(c.get('source_label'))}</td><td>{esc(c.get('region'))}</td><td>{_loc(c)}</td>"
                 f"<td>{mw(c.get('power_mw'))}</td><td>{esc(c.get('proponent'))}</td><td>{_title(c)}</td><td><ul>{items}</ul></td></tr>")
    return (f"<section><h2>{esc(title)} ({len(rows)})</h2>{f'<p class=small>{esc(note)}</p>' if note else ''}<table><thead><tr>"
            f"<th>Fonte</th><th>Regione</th><th>Localizzazione</th><th>MW</th><th>Proponente</th><th>Titolo</th>"
            f"<th>Modifiche</th></tr></thead><tbody>{body}</tbody></table></section>")


def build_html(rep: dict) -> str:
    mods = rep.get("modificati", [])
    prog = [c for c in mods if c.get("progress")]
    only_card = [c for c in mods if not c.get("progress") and all(d["field"] == "card" for d in c["changes"])]
    other = [c for c in mods if not c.get("progress") and c not in only_card]
    c, m = rep.get("counts", {}), rep.get("mw", {})
    d = lambda iso: "/".join(reversed((iso or "")[:10].split("-")))
    cards = "".join(f'<div class="card"><strong>{v}</strong>{esc(k)}</div>' for k, v in [
        ("Avanzamenti verso il cantiere", len(prog)), ("Nuovi progetti", c.get("nuovi", 0)),
        ("Altre modifiche", len(other)), ("Progetti usciti", c.get("rimossi", 0))])
    return f"""<!doctype html>
<html lang="it">
<head>
<meta charset="utf-8">
<title>Wind Agent - report cambiamenti</title>
<style>
{CSS}.cards {{ grid-template-columns: repeat(4, minmax(160px, 1fr)); }}
</style>
</head>
<body>
<main>
  <h1>Wind Agent - report cambiamenti</h1>
  <p class="small">Censimento del {d(rep.get('census') or rep.get('date'))} confrontato con quello del {d(rep.get('previous_census'))} · generato il {esc((rep.get('generated') or '')[:16].replace('T', ' '))} UTC</p>
  <div class="cards">{cards}</div>
  <section>
    <h2>Riepilogo</h2>
    <p>
      Record precedenti: <strong>{c.get('precedenti', 0)}</strong> → record attuali: <strong>{c.get('attuali', 0)}</strong><br>
      MW nuovi: <strong>{mw(m.get('nuovi'))}</strong> (onshore {mw(m.get('nuovi_onshore'))})<br>
      MW usciti: <strong>{mw(m.get('rimossi'))}</strong><br>
      Nuove schede documentali senza cambi di fase: <strong>{len(only_card)}</strong>
    </p>
  </section>
  {_chg_table("Avanzamenti verso il cantiere", prog, "Priorità cantiere, FER X, contractor verificati, EPC nei documenti, stampa, procedura chiusa.")}
  {_rec_table("Nuovi progetti", rep.get("nuovi", []))}
  {_chg_table("Altre modifiche", other)}
  {_rec_table("Progetti usciti dal censimento", rep.get("rimossi", []))}
  {_chg_table("Nuove schede documentali", only_card, "Documenti letti per la prima volta: nessun cambio di fase del progetto.")}
</main>
</body>
</html>
"""


def run(out: str, newer_than: str | None = None, wait_min: float = 0) -> int:
    rep = fetch(newer_than, wait_min)
    if not rep.get("counts"):
        print("[wind-report] nessun report disponibile")
        return 1
    p = Path(out)
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(build_html(rep), encoding="utf-8")
    print(f"[wind-report] {p} (censimento {rep.get('census')}, {rep['counts'].get('avanzamenti', 0)} avanzamenti, "
          f"{rep['counts'].get('nuovi', 0)} nuovi, {rep['counts'].get('rimossi', 0)} usciti)")
    return 0
