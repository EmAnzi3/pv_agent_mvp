"""Registry identity check for Italian VAT numbers via the EU VIES REST API.

Free public service, no key. Results are cached in the store (``registry/``) and
are evidence in their own right: name and registered address *as of request date*.
A VAT whose registered name differs from the company it was positionally linked
to in a document is flagged as an association error, never silently re-assigned.
"""
from __future__ import annotations

import json
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable

from .extract.entities import company_key, valid_vat_it
from .schema import Fact

VIES_URL = "https://ec.europa.eu/taxation_customs/vies/rest-api/ms/IT/vat/{vat}"
CACHE_DAYS = 30


def _norm_name(name: str) -> str:
    n = company_key(name or "")
    for suffix in ("societaaresponsabilitalimitata", "srl", "spa", "srls", "sas", "snc"):
        if n.endswith(suffix):
            n = n[: -len(suffix)]
    return n


class ViesClient:
    def __init__(self, cache_dir: str | Path | None = None, get_json: Callable[[str], dict] | None = None,
                 pause: float = 1.0):
        self.cache = Path(cache_dir) if cache_dir else None
        self.get_json = get_json or self._http
        self.pause = pause

    def _http(self, url: str) -> dict:
        import requests
        r = requests.get(url, timeout=30, headers={"Accept": "application/json"})
        r.raise_for_status()
        return r.json()

    def check(self, vat: str) -> dict[str, Any]:
        if self.cache:
            p = self.cache / f"IT{vat}.json"
            if p.is_file():
                data = json.loads(p.read_text(encoding="utf-8"))
                age = (datetime.now(timezone.utc) - datetime.fromisoformat(data["checked_at"])).days
                if age <= CACHE_DAYS:
                    return data
        try:
            raw = self.get_json(VIES_URL.format(vat=vat))
            data = {"vat": vat, "valid": bool(raw.get("isValid")), "name": (raw.get("name") or "").strip() or None,
                    "address": " ".join((raw.get("address") or "").split()) or None,
                    "request_date": raw.get("requestDate"), "source": "VIES", "error": None}
        except Exception as exc:
            data = {"vat": vat, "valid": None, "name": None, "address": None, "source": "VIES",
                    "error": str(exc)[:200]}
        data["checked_at"] = datetime.now(timezone.utc).isoformat(timespec="seconds")
        if self.cache and data["error"] is None:
            self.cache.mkdir(parents=True, exist_ok=True)
            (self.cache / f"IT{vat}.json").write_text(json.dumps(data, ensure_ascii=False), encoding="utf-8")
        time.sleep(self.pause)
        return data


def check_vats(facts: list[Fact], client: ViesClient, limit: int = 40) -> dict[str, dict]:
    """Check each distinct well-formed VAT once; annotate facts in place."""
    vats = []
    for f in facts:
        if f.field == "company.vat" and f.qualifiers.get("checksum_ok") and f.value not in vats:
            vats.append(f.value)
    results = {v: client.check(v) for v in vats[:limit]}
    for f in facts:
        if f.field != "company.vat" or f.value not in results:
            continue
        r = results[f.value]
        f.qualifiers["registry"] = {k: r.get(k) for k in ("valid", "name", "address", "request_date", "source", "error")}
        if r.get("name") and f.subject:
            match = _norm_name(r["name"]).startswith(_norm_name(f.subject)) or _norm_name(f.subject).startswith(_norm_name(r["name"]))
            f.qualifiers["registry_match"] = bool(match)
            if not match:
                f.status = "rejected"
                f.limits = (f"Registry name '{r['name']}' differs from the company '{f.subject}' it was linked to "
                            "in this document: positional association error")
        elif r.get("name"):
            f.qualifiers["registry_match"] = None
        if r.get("valid") is False:
            f.status = "rejected"
            f.limits = "VAT not active in VIES on the request date"
    return results
