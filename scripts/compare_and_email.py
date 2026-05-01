from __future__ import annotations

import argparse
import html
import json
import os
import re
import smtplib
import ssl
from datetime import datetime
from email.message import EmailMessage
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
    for a, b in {
        "à": "a", "è": "e", "é": "e", "ì": "i", "ò": "o", "ù": "u",
        "’": "'", "‘": "'", "“": '"', "”": '"',
    }.items():
        text = text.replace(a, b)
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
    result = {}
    duplicates = {}

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


def table_records(title: str, records: list[dict[str, Any]], max_rows: int = 80) -> str:
    if not records:
        return f"<h2>{esc(title)}</h2><p>Nessun record.</p>"

    rows = []

    for record in records[:max_rows]:
        url = clean(record.get("url"))
        title_html = esc(title_of(record))

        if url:
            title_html = f'<a href="{esc(url)}">{title_html}</a>'

        rows.append(
            "<tr>"
            f"<td>{esc(source_of(record))}</td>"
            f"<td>{esc(location_of(record))}</td>"
            f"<td>{esc(fmt_mw(record.get('power_mw')))}</td>"
            f"<td>{esc(record.get('proponent') or 'n/d')}</td>"
            f"<td>{title_html}</td>"
            "</tr>"
        )

    note = ""
    if len(records) > max_rows:
        note = f"<p><em>Mostrati {max_rows} su {len(records)} record.</em></p>"

    return f"""
    <h2>{esc(title)} ({len(records)})</h2>
    {note}
    <table>
      <thead>
        <tr>
          <th>Fonte</th>
          <th>Localizzazione</th>
          <th>MW</th>
          <th>Proponente</th>
          <th>Titolo</th>
        </tr>
      </thead>
      <tbody>{''.join(rows)}</tbody>
    </table>
    """


def table_changed(changed: list[dict[str, Any]], max_rows: int = 80) -> str:
    if not changed:
        return "<h2>Record modificati</h2><p>Nessun record.</p>"

    rows = []

    for item in changed[:max_rows]:
        record = item["record"]
        url = clean(record.get("url"))
        title_html = esc(title_of(record))

        if url:
            title_html = f'<a href="{esc(url)}">{title_html}</a>'

        diffs_html = "<ul>" + "".join(
            f"<li><strong>{esc(d['label'])}</strong>: {esc(d['old'])} → {esc(d['new'])}</li>"
            for d in item["diffs"]
        ) + "</ul>"

        rows.append(
            "<tr>"
            f"<td>{esc(source_of(record))}</td>"
            f"<td>{esc(location_of(record))}</td>"
            f"<td>{esc(fmt_mw(record.get('power_mw')))}</td>"
            f"<td>{esc(record.get('proponent') or 'n/d')}</td>"
            f"<td>{title_html}</td>"
            f"<td>{diffs_html}</td>"
            "</tr>"
        )

    note = ""
    if len(changed) > max_rows:
        note = f"<p><em>Mostrati {max_rows} su {len(changed)} record modificati.</em></p>"

    return f"""
    <h2>Record modificati ({len(changed)})</h2>
    {note}
    <table>
      <thead>
        <tr>
          <th>Fonte</th>
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


def build_html(result: dict[str, Any], dashboard_url: str) -> str:
    added = result["added"]
    removed = result["removed"]
    changed = result["changed"]

    dashboard_link = ""
    if dashboard_url:
        dashboard_link = f'<p><a href="{esc(dashboard_url)}">Apri dashboard aggiornata</a></p>'

    return f"""<!doctype html>
<html>
<head>
<meta charset="utf-8">
<style>
body {{
  font-family: Arial, sans-serif;
  color: #111827;
  line-height: 1.45;
}}
table {{
  border-collapse: collapse;
  width: 100%;
  margin: 12px 0 28px;
  font-size: 13px;
}}
th, td {{
  border: 1px solid #d1d5db;
  padding: 8px;
  vertical-align: top;
}}
th {{
  background: #f3f4f6;
  text-align: left;
}}
.card {{
  display: inline-block;
  border: 1px solid #d1d5db;
  border-radius: 8px;
  padding: 12px;
  margin: 6px 8px 6px 0;
  background: #f9fafb;
  min-width: 130px;
}}
.card strong {{
  display: block;
  font-size: 22px;
}}
.small {{
  color: #6b7280;
  font-size: 12px;
}}
</style>
</head>
<body>
<h1>PV Agent - aggiornamenti rilevati</h1>
<p class="small">Generato il {esc(datetime.now().strftime("%d/%m/%Y %H:%M"))}</p>
{dashboard_link}

<div>
  <div class="card"><strong>{len(added)}</strong>Nuovi progetti</div>
  <div class="card"><strong>{len(changed)}</strong>Record modificati</div>
  <div class="card"><strong>{len(removed)}</strong>Progetti rimossi</div>
  <div class="card"><strong>{result["new_count"]}</strong>Record attuali</div>
</div>

<p>
  Record precedenti: <strong>{result["old_count"]}</strong> → record attuali: <strong>{result["new_count"]}</strong><br>
  MW nuovi: <strong>{esc(fmt_mw(total_mw(added)))}</strong><br>
  MW rimossi: <strong>{esc(fmt_mw(total_mw(removed)))}</strong>
</p>

{table_records("Nuovi progetti", added)}
{table_changed(changed)}
{table_records("Progetti rimossi", removed)}

<p class="small">Email generata automaticamente da GitHub Actions. Nessun file report è stato pubblicato nel repository.</p>
</body>
</html>
"""


def build_text(result: dict[str, Any], dashboard_url: str) -> str:
    lines = [
        "PV Agent - aggiornamenti rilevati",
        "",
        f"Nuovi progetti: {len(result['added'])}",
        f"Record modificati: {len(result['changed'])}",
        f"Progetti rimossi: {len(result['removed'])}",
        f"Record precedenti: {result['old_count']} -> record attuali: {result['new_count']}",
    ]

    if dashboard_url:
        lines += ["", f"Dashboard: {dashboard_url}"]

    lines += ["", "Nuovi progetti:"]
    for record in result["added"][:20]:
        lines.append(
            f"- [{source_of(record)}] {location_of(record)} | "
            f"{fmt_mw(record.get('power_mw'))} MW | "
            f"{record.get('proponent') or 'n/d'} | {title_of(record, 110)}"
        )

    lines += ["", "Record modificati:"]
    for item in result["changed"][:20]:
        record = item["record"]
        diffs = "; ".join(f"{d['label']}: {d['old']} -> {d['new']}" for d in item["diffs"])
        lines.append(f"- [{source_of(record)}] {location_of(record)} | {title_of(record, 100)} | {diffs}")

    lines += ["", "Progetti rimossi:"]
    for record in result["removed"][:20]:
        lines.append(
            f"- [{source_of(record)}] {location_of(record)} | "
            f"{fmt_mw(record.get('power_mw'))} MW | "
            f"{record.get('proponent') or 'n/d'} | {title_of(record, 110)}"
        )

    return "\n".join(lines)


def split_recipients(value: str) -> list[str]:
    return [x.strip() for x in re.split(r"[;,]", value or "") if x.strip()]


def send_email(subject: str, html_body: str, text_body: str) -> None:
    smtp_host = os.environ.get("SMTP_HOST", "").strip()
    smtp_port = int(os.environ.get("SMTP_PORT", "587").strip() or "587")
    smtp_user = os.environ.get("SMTP_USER", "").strip()
    smtp_password = os.environ.get("SMTP_PASSWORD", "")
    mail_from = os.environ.get("MAIL_FROM", smtp_user).strip()
    recipients = split_recipients(os.environ.get("MAIL_TO", ""))

    missing = []
    for name, value in {
        "SMTP_HOST": smtp_host,
        "SMTP_USER": smtp_user,
        "SMTP_PASSWORD": smtp_password,
        "MAIL_FROM": mail_from,
        "MAIL_TO": ",".join(recipients),
    }.items():
        if not value:
            missing.append(name)

    if missing:
        raise RuntimeError("Secrets email mancanti: " + ", ".join(missing))

    msg = EmailMessage()
    msg["Subject"] = subject
    msg["From"] = mail_from
    msg["To"] = ", ".join(recipients)
    msg.set_content(text_body)
    msg.add_alternative(html_body, subtype="html")

    if smtp_port == 465:
        context = ssl.create_default_context()
        with smtplib.SMTP_SSL(smtp_host, smtp_port, context=context) as server:
            server.login(smtp_user, smtp_password)
            server.send_message(msg)
    else:
        with smtplib.SMTP(smtp_host, smtp_port) as server:
            server.ehlo()
            server.starttls(context=ssl.create_default_context())
            server.ehlo()
            server.login(smtp_user, smtp_password)
            server.send_message(msg)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--old", required=True)
    parser.add_argument("--new", required=True)
    parser.add_argument("--dashboard-url", default="")
    parser.add_argument("--subject-prefix", default="PV Agent")
    parser.add_argument("--send-on-no-changes", action="store_true")
    args = parser.parse_args()

    old_records = load_records(Path(args.old))
    new_records = load_records(Path(args.new))

    result = compare(old_records, new_records)

    added = len(result["added"])
    changed = len(result["changed"])
    removed = len(result["removed"])

    print("[compare-and-email]")
    print(f"old_records: {result['old_count']}")
    print(f"new_records: {result['new_count']}")
    print(f"added: {added}")
    print(f"changed: {changed}")
    print(f"removed: {removed}")

    has_changes = added or changed or removed

    if not has_changes and not args.send_on_no_changes:
        print("[compare-and-email] Nessun cambiamento. Email non inviata.")
        return 0

    today = datetime.now().strftime("%d/%m/%Y")

    if has_changes:
        subject = f"{args.subject_prefix} - {added} nuovi, {changed} modificati, {removed} rimossi - {today}"
    else:
        subject = f"{args.subject_prefix} - nessun cambiamento - {today}"

    send_email(
        subject=subject,
        html_body=build_html(result, args.dashboard_url),
        text_body=build_text(result, args.dashboard_url),
    )

    print("[compare-and-email] Email inviata.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
