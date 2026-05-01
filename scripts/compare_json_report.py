from __future__ import annotations

import argparse
import csv
import html
import json
import re
from datetime import datetime
from pathlib import Path
from typing import Any


FIELDS_TO_COMPARE = [
    ("power_mw", "MW"),
    ("proponent", "Proponente"),
    ("region", "Regione"),
    ("province", "Provincia"),
    ("municipalities", "Comune/i"),
    ("status", "Stato"),
    ("status_raw", "Stato grezzo"),
    ("source_label", "Fonte"),
    ("source_group", "Gruppo fonte"),
    ("title", "Titolo"),
    ("url", "URL"),
]


def clean(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, list):
        return ", ".join(clean(x) for x in value if clean(x))
    if isinstance(value, dict):
        return json.dumps(value, ensure_ascii=False, sort_keys=True)
    return re.sub(r"\s+", " ", str(value)).strip()


def norm(value: Any) -> str:
    text = clean(value).lower()
    for src, dst in {
        "à": "a", "è": "e", "é": "e", "ì": "i", "ò": "o", "ù": "u",
        "’": "'", "‘": "'", "“": '"', "”": '"',
    }.items():
        text = text.replace(src, dst)
    text = re.sub(r"[^a-z0-9]+", " ", text)
    return re.sub(r"\s+", " ", text).strip()


def parse_float(value: Any) -> float | None:
    if value is None or value == "":
        return None
    if isinstance(value, (int, float)):
        return float(value)

    text = clean(value)
    text = re.sub(r"[^0-9,.\-]", "", text)

    if not text:
        return None

    if "," in text and "." in text:
        if text.rfind(",") > text.rfind("."):
            text = text.replace(".", "").replace(",", ".")
        else:
            text = text.replace(",", "")
    elif "," in text:
        text = text.replace(".", "").replace(",", ".")

    try:
        return float(text)
    except ValueError:
        return None


def fmt_mw(value: Any) -> str:
    number = parse_float(value)
    if number is None:
        return clean(value) or "n/d"
    return f"{number:,.3f}".replace(",", "X").replace(".", ",").replace("X", ".")


def load_records(path: Path) -> list[dict[str, Any]]:
    if not path.exists():
        return []

    data = json.loads(path.read_text(encoding="utf-8"))

    if isinstance(data, dict):
        records = data.get("records") or []
    elif isinstance(data, list):
        records = data
    else:
        records = []

    return [r for r in records if isinstance(r, dict)]


def record_key(record: dict[str, Any]) -> str:
    for field in ("project_key", "canonical_key", "id", "external_id"):
        value = clean(record.get(field))
        if value:
            return f"{field}:{value}"

    url = clean(record.get("url") or record.get("source_url"))
    if url:
        return f"url:{url}"

    source = clean(record.get("source_group") or record.get("source_label") or record.get("source"))
    title = norm(record.get("title"))
    province = norm(record.get("province"))
    municipalities = norm(record.get("municipalities"))

    return f"fallback:{source}|{title}|{province}|{municipalities}"


def index_records(records: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    result: dict[str, dict[str, Any]] = {}
    duplicates: dict[str, int] = {}

    for record in records:
        key = record_key(record)

        if key in result:
            duplicates[key] = duplicates.get(key, 1) + 1
            key = f"{key}#dup{duplicates[key]}"

        result[key] = record

    return result


def comparable(field: str, value: Any) -> str:
    if field == "power_mw":
        number = parse_float(value)
        return "" if number is None else f"{number:.6f}"
    return norm(value)


def display_value(field: str, value: Any) -> str:
    if field == "power_mw":
        return fmt_mw(value)
    return clean(value) or "n/d"


def source_of(record: dict[str, Any]) -> str:
    return clean(record.get("source_label") or record.get("source_group") or record.get("source")) or "n/d"


def location_of(record: dict[str, Any]) -> str:
    province = clean(record.get("province"))
    municipalities = clean(record.get("municipalities"))

    if province and municipalities:
        return f"{province} - {municipalities}"

    return province or municipalities or "n/d"


def title_of(record: dict[str, Any], max_len: int = 160) -> str:
    title = clean(record.get("title")) or "n/d"
    if len(title) > max_len:
        return title[: max_len - 1] + "…"
    return title


def url_of(record: dict[str, Any]) -> str:
    return clean(record.get("url") or record.get("source_url"))


def compare(old_records: list[dict[str, Any]], new_records: list[dict[str, Any]]) -> dict[str, Any]:
    old_idx = index_records(old_records)
    new_idx = index_records(new_records)

    old_keys = set(old_idx)
    new_keys = set(new_idx)

    added = [new_idx[k] for k in sorted(new_keys - old_keys)]
    removed = [old_idx[k] for k in sorted(old_keys - new_keys)]

    changed = []

    for key in sorted(old_keys & new_keys):
        old = old_idx[key]
        new = new_idx[key]
        diffs = []

        for field, label in FIELDS_TO_COMPARE:
            if comparable(field, old.get(field)) != comparable(field, new.get(field)):
                diffs.append({
                    "field": field,
                    "label": label,
                    "old": display_value(field, old.get(field)),
                    "new": display_value(field, new.get(field)),
                })

        if diffs:
            changed.append({
                "key": key,
                "record": new,
                "diffs": diffs,
            })

    return {
        "old_count": len(old_records),
        "new_count": len(new_records),
        "added": added,
        "removed": removed,
        "changed": changed,
    }


def esc(value: Any) -> str:
    return html.escape(clean(value))


def total_mw(records: list[dict[str, Any]]) -> float:
    total = 0.0
    for record in records:
        mw = parse_float(record.get("power_mw"))
        if mw is not None:
            total += mw
    return total


def html_record_table(title: str, records: list[dict[str, Any]], max_rows: int = 500) -> str:
    if not records:
        return f"<h2>{esc(title)}</h2><p>Nessun record.</p>"

    rows = []

    for record in records[:max_rows]:
        url = url_of(record)
        title_html = esc(title_of(record))

        if url:
            title_html = f'<a href="{esc(url)}" target="_blank" rel="noopener noreferrer">{title_html}</a>'

        rows.append(
            "<tr>"
            f"<td>{esc(source_of(record))}</td>"
            f"<td>{esc(record.get('region') or 'n/d')}</td>"
            f"<td>{esc(location_of(record))}</td>"
            f"<td>{esc(fmt_mw(record.get('power_mw')))}</td>"
            f"<td>{esc(record.get('proponent') or 'n/d')}</td>"
            f"<td>{title_html}</td>"
            "</tr>"
        )

    note = ""
    if len(records) > max_rows:
        note = f"<p><em>Mostrati {max_rows} record su {len(records)}.</em></p>"

    return f"""
    <h2>{esc(title)} ({len(records)})</h2>
    {note}
    <table>
      <thead>
        <tr>
          <th>Fonte</th>
          <th>Regione</th>
          <th>Localizzazione</th>
          <th>MW</th>
          <th>Proponente</th>
          <th>Titolo</th>
        </tr>
      </thead>
      <tbody>{''.join(rows)}</tbody>
    </table>
    """


def html_changed_table(changed: list[dict[str, Any]], max_rows: int = 500) -> str:
    if not changed:
        return "<h2>Record modificati</h2><p>Nessun record.</p>"

    rows = []

    for item in changed[:max_rows]:
        record = item["record"]
        url = url_of(record)
        title_html = esc(title_of(record))

        if url:
            title_html = f'<a href="{esc(url)}" target="_blank" rel="noopener noreferrer">{title_html}</a>'

        diffs_html = "<ul>" + "".join(
            f"<li><strong>{esc(d['label'])}</strong>: {esc(d['old'])} → {esc(d['new'])}</li>"
            for d in item["diffs"]
        ) + "</ul>"

        rows.append(
            "<tr>"
            f"<td>{esc(source_of(record))}</td>"
            f"<td>{esc(record.get('region') or 'n/d')}</td>"
            f"<td>{esc(location_of(record))}</td>"
            f"<td>{esc(fmt_mw(record.get('power_mw')))}</td>"
            f"<td>{esc(record.get('proponent') or 'n/d')}</td>"
            f"<td>{title_html}</td>"
            f"<td>{diffs_html}</td>"
            "</tr>"
        )

    note = ""
    if len(changed) > max_rows:
        note = f"<p><em>Mostrati {max_rows} record modificati su {len(changed)}.</em></p>"

    return f"""
    <h2>Record modificati ({len(changed)})</h2>
    {note}
    <table>
      <thead>
        <tr>
          <th>Fonte</th>
          <th>Regione</th>
          <th>Localizzazione</th>
          <th>MW</th>
          <th>Proponente</th>
          <th>Titolo</th>
          <th>Modifiche</th>
        </tr>
      </thead>
      <tbody>{''.join(rows)}</tbody>
    </table>
    """


def build_html(result: dict[str, Any]) -> str:
    added = result["added"]
    removed = result["removed"]
    changed = result["changed"]

    return f"""<!doctype html>
<html lang="it">
<head>
<meta charset="utf-8">
<title>PV Agent - report cambiamenti</title>
<style>
body {{
  font-family: Arial, sans-serif;
  color: #111827;
  background: #f3f6f8;
  line-height: 1.45;
  margin: 0;
  padding: 24px;
}}
main {{
  max-width: 1500px;
  margin: 0 auto;
}}
h1 {{
  margin-bottom: 4px;
}}
.small {{
  color: #6b7280;
  font-size: 13px;
}}
.cards {{
  display: grid;
  grid-template-columns: repeat(4, minmax(160px, 1fr));
  gap: 12px;
  margin: 18px 0 24px;
}}
.card {{
  border: 1px solid #d1d5db;
  border-radius: 14px;
  padding: 14px;
  background: white;
}}
.card strong {{
  display: block;
  font-size: 28px;
  color: #0f172a;
}}
section {{
  background: white;
  border: 1px solid #d1d5db;
  border-radius: 14px;
  padding: 18px;
  margin-bottom: 20px;
  overflow-x: auto;
}}
table {{
  border-collapse: collapse;
  width: 100%;
  margin: 12px 0 8px;
  font-size: 13px;
}}
th, td {{
  border-bottom: 1px solid #e5e7eb;
  padding: 8px;
  vertical-align: top;
}}
th {{
  background: #f9fafb;
  text-align: left;
  position: sticky;
  top: 0;
}}
a {{
  color: #0f766e;
  font-weight: 700;
  text-decoration: none;
}}
ul {{
  margin: 0;
  padding-left: 18px;
}}
</style>
</head>
<body>
<main>
  <h1>PV Agent - report cambiamenti</h1>
  <p class="small">Generato il {esc(datetime.now().strftime("%d/%m/%Y %H:%M"))}</p>

  <div class="cards">
    <div class="card"><strong>{len(added)}</strong>Nuovi progetti</div>
    <div class="card"><strong>{len(changed)}</strong>Record modificati</div>
    <div class="card"><strong>{len(removed)}</strong>Progetti rimossi</div>
    <div class="card"><strong>{result["new_count"]}</strong>Record attuali</div>
  </div>

  <section>
    <h2>Riepilogo</h2>
    <p>
      Record precedenti: <strong>{result["old_count"]}</strong> → record attuali: <strong>{result["new_count"]}</strong><br>
      MW nuovi: <strong>{esc(fmt_mw(total_mw(added)))}</strong><br>
      MW rimossi: <strong>{esc(fmt_mw(total_mw(removed)))}</strong>
    </p>
  </section>

  <section>{html_record_table("Nuovi progetti", added)}</section>
  <section>{html_changed_table(changed)}</section>
  <section>{html_record_table("Progetti rimossi", removed)}</section>
</main>
</body>
</html>
"""


def write_csv(path: Path, result: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)

    with path.open("w", newline="", encoding="utf-8-sig") as f:
        writer = csv.writer(f, delimiter=";")
        writer.writerow([
            "change_type", "source", "region", "province", "municipalities",
            "mw", "proponent", "title", "field", "old_value", "new_value", "url"
        ])

        for record in result["added"]:
            writer.writerow([
                "added", source_of(record), clean(record.get("region")),
                clean(record.get("province")), clean(record.get("municipalities")),
                fmt_mw(record.get("power_mw")), clean(record.get("proponent")),
                title_of(record, 300), "", "", "", url_of(record)
            ])

        for item in result["changed"]:
            record = item["record"]
            for diff in item["diffs"]:
                writer.writerow([
                    "changed", source_of(record), clean(record.get("region")),
                    clean(record.get("province")), clean(record.get("municipalities")),
                    fmt_mw(record.get("power_mw")), clean(record.get("proponent")),
                    title_of(record, 300), diff["label"], diff["old"], diff["new"], url_of(record)
                ])

        for record in result["removed"]:
            writer.writerow([
                "removed", source_of(record), clean(record.get("region")),
                clean(record.get("province")), clean(record.get("municipalities")),
                fmt_mw(record.get("power_mw")), clean(record.get("proponent")),
                title_of(record, 300), "", "", "", url_of(record)
            ])


def main() -> int:
    parser = argparse.ArgumentParser(description="Confronta due data.json e genera report HTML/CSV.")
    parser.add_argument("--old", required=True)
    parser.add_argument("--new", required=True)
    parser.add_argument("--out-html", required=True)
    parser.add_argument("--out-csv", required=True)
    args = parser.parse_args()

    old_records = load_records(Path(args.old))
    new_records = load_records(Path(args.new))

    result = compare(old_records, new_records)

    html_path = Path(args.out_html)
    csv_path = Path(args.out_csv)

    html_path.parent.mkdir(parents=True, exist_ok=True)
    html_path.write_text(build_html(result), encoding="utf-8")

    write_csv(csv_path, result)

    print("[compare-json-report]")
    print(f"old_records: {result['old_count']}")
    print(f"new_records: {result['new_count']}")
    print(f"added: {len(result['added'])}")
    print(f"changed: {len(result['changed'])}")
    print(f"removed: {len(result['removed'])}")
    print(f"html: {html_path}")
    print(f"csv: {csv_path}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
