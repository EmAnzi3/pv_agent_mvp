from __future__ import annotations

import json
import socket
import sys
from dataclasses import asdict
from datetime import datetime
from pathlib import Path
from urllib.parse import urlparse

import requests

from app.collectors.piemonte import PiemonteCollector, START_URL


SCRIVA_VIA_URL = (
    "https://scriva-servizi.regione.piemonte.it/"
    "scrivaconsweb/procedimenti/AMB/VIA/1/competenza-territorio"
)

SERVICE_PAGE_URL = "https://www.servizi.piemonte.it/srv/valutazioni-ambientali/"

EXPECTED_CODES = {
    "2024-20/VI",
    "2025-140/VI",
    "2025-144/VI",
    "2026-118/VI",
    "2025-87/VI",
}


def resolve_host(url: str) -> dict:
    host = urlparse(url).hostname
    result = {"url": url, "host": host, "resolved": False, "addresses": []}

    if not host:
        result["error"] = "host assente"
        return result

    try:
        infos = socket.getaddrinfo(host, None)
        addresses = sorted({info[4][0] for info in infos if info[4]})
        result["resolved"] = True
        result["addresses"] = addresses
    except Exception as exc:
        result["error"] = f"{type(exc).__name__}: {exc}"

    return result


def http_probe(session: requests.Session, url: str) -> dict:
    result = {"url": url, "ok": False}

    try:
        response = session.get(url, timeout=30, allow_redirects=True)
        result.update(
            {
                "ok": response.ok,
                "status_code": response.status_code,
                "final_url": response.url,
                "content_type": response.headers.get("content-type"),
                "content_length": len(response.content),
            }
        )
    except Exception as exc:
        result["error"] = f"{type(exc).__name__}: {exc}"

    return result


def main() -> int:
    out_dir = Path("reports/piemonte_probe")
    out_dir.mkdir(parents=True, exist_ok=True)

    session = requests.Session()
    session.headers.update(
        {
            "User-Agent": "Mozilla/5.0 PV-Agent-MVP Piemonte-Probe/1.0",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        }
    )

    urls = [START_URL, SCRIVA_VIA_URL, SERVICE_PAGE_URL]
    dns = [resolve_host(url) for url in urls]
    http = [http_probe(session, url) for url in urls]

    print("")
    print("=" * 78)
    print("PIEMONTE SOURCE PROBE")
    print("=" * 78)

    for item in dns:
        status = "OK" if item["resolved"] else "FAIL"
        detail = ", ".join(item.get("addresses") or []) or item.get("error", "")
        print(f"[DNS {status}] {item['host']} -> {detail}")

    for item in http:
        status = "OK" if item["ok"] else "FAIL"
        detail = (
            f"HTTP {item.get('status_code')} -> {item.get('final_url')}"
            if item.get("status_code") is not None
            else item.get("error", "")
        )
        print(f"[HTTP {status}] {item['url']} -> {detail}")

    collector_results = []
    collector_error = None

    legacy_http = next(item for item in http if item["url"] == START_URL)

    if legacy_http["ok"]:
        try:
            collector = PiemonteCollector()
            collector_results = collector.fetch()
        except Exception as exc:
            collector_error = f"{type(exc).__name__}: {exc}"
    else:
        collector_error = "legacy SKVIA non raggiungibile: collector non eseguito"

    rows = []
    detected_codes = set()

    for result in collector_results:
        row = asdict(result)
        rows.append(row)

        external_id = str(result.external_id or "")
        for code in EXPECTED_CODES:
            if code.lower() in external_id.lower():
                detected_codes.add(code)

        title = str(result.payload.get("title") or "")
        status = str(result.payload.get("status_raw") or "")
        for code in EXPECTED_CODES:
            if code.lower() in f"{title} {status}".lower():
                detected_codes.add(code)

    missing_codes = sorted(EXPECTED_CODES - detected_codes)

    summary = {
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "dns": dns,
        "http": http,
        "collector_results": len(rows),
        "collector_error": collector_error,
        "expected_codes": sorted(EXPECTED_CODES),
        "detected_codes": sorted(detected_codes),
        "missing_codes": missing_codes,
        "results": rows,
    }

    output = out_dir / "probe_latest.json"
    output.write_text(
        json.dumps(summary, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )

    print("")
    print(f"Collector Piemonte: {len(rows)} record")
    if collector_error:
        print(f"Collector error: {collector_error}")

    print("Baseline trovata:", ", ".join(sorted(detected_codes)) or "nessuna")
    print("Baseline mancante:", ", ".join(missing_codes) or "nessuna")
    print(f"Report: {output}")

    scriva_http = next(item for item in http if item["url"] == SCRIVA_VIA_URL)

    if not legacy_http["ok"]:
        if scriva_http["ok"]:
            print("")
            print(
                "ESITO: legacy SKVIA non raggiungibile ma SCRIVA risponde. "
                "Non integrare ancora nel BAT globale: serve migrare/affiancare la sorgente Piemonte."
            )
            return 2

        print("")
        print(
            "ESITO: né legacy SKVIA né SCRIVA risultano raggiungibili da questa macchina. "
            "Problema di rete/DNS da risolvere prima del parsing."
        )
        return 3

    if collector_error:
        print("")
        print("ESITO: rete legacy OK, ma collector fallito. Serve correggere il flusso applicativo.")
        return 4

    if missing_codes:
        print("")
        print(
            "ESITO: collector eseguito, ma la baseline manuale non è ancora completa. "
            "Continuare il debug Piemonte isolato."
        )
        return 5

    print("")
    print("ESITO: probe Piemonte OK e baseline manuale completa.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
