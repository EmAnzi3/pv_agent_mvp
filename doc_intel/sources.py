"""Verify every source end-to-end on one real sample: reachability, method, documents, analysis.

Meant to run from an Italian network (some regional portals refuse foreign IPs), e.g.
with ``verifica_fonti_documentali.bat`` on a PC. Never writes to the canonical dataset.
"""
from __future__ import annotations

import csv
import json
import time
import traceback
from datetime import datetime, timezone
from pathlib import Path
from typing import Callable

from .adapters import adapter_for_url
from .canonical import load_records
from .probe import pick_samples, probe_url


def verify_sources(data_json: str, store: str, out_dir: str, only: list[str] | None = None,
                   render: Callable | None = None, max_docs: int = 25, budget_min: float = 10,
                   ocr: bool = True, log: Callable[[str], None] = print) -> list[dict]:
    from .pipeline import run
    samples = pick_samples(load_records(data_json))
    rows = []
    for src, sample in samples.items():
        if only and src not in only:
            continue
        t0 = time.time()
        log(f"\n=== {src}: {sample['records']} record, campione {sample['url']}")
        row = {"source": src, "records": sample["records"], "sample_url": sample["url"],
               "url_identifies_procedure": sample["url_identifies_procedure"]}
        try:
            p = probe_url(sample["url"], render=render)
            http = p.get("http") or {}
            row.update({"reachable": not http.get("error"), "http_status": http.get("status"),
                        "network_error": http.get("error_class"), "probe_verdict": p["verdict"]["status"],
                        "probe_method": p["verdict"]["method"], "doc_links_on_page": http.get("doc_link_count")})
        except Exception as exc:
            row.update({"reachable": False, "probe_error": str(exc)[:200]})
        try:
            adapter = adapter_for_url(sample["url"])
            row["adapter"] = type(adapter).__name__
        except LookupError:
            row["adapter"] = None
        if row["adapter"] and row.get("reachable", True):
            try:
                d = run(sample["url"], store, data_json, ocr=ocr, max_docs=max_docs, workers=3,
                        time_budget_s=budget_min * 60, registry=None, log=log)
                a = d["acquisition"]
                row.update({"documents_listed": a["references"], "documents_analysed": a["processed_references"],
                            "download_failures": a["download_failures"], "pages": a["pages_total"],
                            "pages_ocr": a["pages_ocr"], "facts": len(d["facts"]),
                            "proponent_found": any(f["field"] == "role.proponent" for f in d["facts"]),
                            "power_found": any(f["field"].startswith("tech.pv") for f in d["facts"]),
                            "canonical_match": d["canonical"].get("status"),
                            "gate_issues": d["gates"]["acquisition"]["issues"]})
                ok = a["references"] > 0 and a["download_failures"] == 0 and a["pages_total"] > 0
                row["status"] = "supportata" if ok else "parziale"
            except Exception as exc:
                row.update({"status": "errore", "error": f"{type(exc).__name__}: {exc}"[:300],
                            "trace": traceback.format_exc()[-800:]})
        elif not row.get("reachable", True):
            row["status"] = "non raggiungibile"
        else:
            row["status"] = "da sviluppare (nessun adapter)"
        row["seconds"] = round(time.time() - t0, 1)
        log(f"=== {src}: {row['status']} ({row['seconds']} s)")
        rows.append(row)
    write_reports(rows, out_dir)
    return rows


def write_reports(rows: list[dict], out_dir: str) -> None:
    out = Path(out_dir)
    out.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now(timezone.utc).isoformat(timespec="seconds")
    (out / "fonti_latest.json").write_text(json.dumps({"generated_utc": stamp, "rows": rows}, ensure_ascii=False,
                                                      indent=1, default=str), encoding="utf-8")
    cols = ["source", "records", "status", "reachable", "adapter", "documents_listed", "documents_analysed",
            "download_failures", "pages", "facts", "proponent_found", "power_found", "probe_method",
            "network_error", "seconds", "sample_url"]
    with (out / "fonti_latest.csv").open("w", newline="", encoding="utf-8-sig") as fh:
        w = csv.DictWriter(fh, fieldnames=cols, extrasaction="ignore", delimiter=";")
        w.writeheader()
        w.writerows(rows)
    md = [f"# Verifica fonti documentali — {stamp}", "",
          "| Fonte | Record | Esito | Documenti trovati / analizzati | Pagine | Fatti | Note |", "|---|---|---|---|---|---|---|"]
    for r in rows:
        note = r.get("error") or ", ".join(r.get("gate_issues") or []) or r.get("network_error") or r.get("probe_method") or ""
        md.append(f"| {r['source']} | {r['records']} | {r['status']} | {r.get('documents_listed', '-')} / "
                  f"{r.get('documents_analysed', '-')} | {r.get('pages', '-')} | {r.get('facts', '-')} | {str(note)[:120]} |")
    (out / "fonti_latest.md").write_text("\n".join(md) + "\n", encoding="utf-8")
