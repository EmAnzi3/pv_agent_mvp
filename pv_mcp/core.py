"""Read-only access to explicitly selected published PV radar artifacts."""
from __future__ import annotations

import json
import math
import re
import time
import unicodedata
from collections import Counter
from pathlib import Path
from urllib.request import Request, build_opener, HTTPRedirectHandler

ROOT = "https://raw.githubusercontent.com/EmAnzi3/pv_agent_mvp/"
FILES = {
    "radar": ("main", "docs/data.json"),
    "changes": ("main", "docs/changes_latest.json"),
    "readiness": ("doc-intel-state", "readiness/index.json"),
    "cards": ("doc-intel-state", "schede/live_index.json"),
    "ferx": ("doc-intel-state", "ferx/matches.json"),
}
ROW_FIELDS = ("project_key", "title", "proponent", "region", "province",
              "municipalities", "project_type", "power_mw", "status", "url",
              "source", "updated_at", "province_deduced", "municipalities_deduced")
CHANGE_FIELDS = ("project_key", "region", "province", "municipalities",
                 "title", "proponent", "power_mw", "what")
MAX_BYTES = 8 * 1024 * 1024
CARD_NAME = re.compile(r"(?:prj|src|url)_[a-zA-Z0-9_-]{1,100}\.json")


class NoRedirect(HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        return None


def norm(value):
    text = unicodedata.normalize("NFKD", str(value or "").casefold())
    return " ".join("".join(c for c in text if not unicodedata.combining(c)).split())


def scalar_fields(row, fields):
    return {k: row[k] for k in fields if k in row and
            (row[k] is None or isinstance(row[k], (str, int, float, bool)))}


def valid_power(value):
    return (isinstance(value, (int, float)) and not isinstance(value, bool)
            and math.isfinite(value) and value >= 0)


def source_url(branch, path):
    return ROOT + branch + "/" + path


def card_path(entry):
    name = entry.get("file") if isinstance(entry, dict) else None
    if not isinstance(name, str):
        raise ValueError("Invalid card index entry")
    if name.startswith("schede/"):
        name = name[len("schede/"):]
    if not CARD_NAME.fullmatch(name):
        raise ValueError("Unapproved card path")
    return "schede/" + name


class PublishedData:
    """No arbitrary URL, redirects, secrets, raw manifests or write operations."""
    def __init__(self, local_root=None, ttl=120):
        self.local_root = Path(local_root) if local_root else None
        self.ttl = ttl
        self.cache = {}
        self.opener = build_opener(NoRedirect())

    def read(self, branch, path):
        if (branch, path) not in FILES.values() and not (
            branch == "doc-intel-state" and path.startswith("schede/")
            and CARD_NAME.fullmatch(path[len("schede/"):])
        ):
            raise ValueError("Artifact not allowlisted")
        key = (branch, path)
        now = time.monotonic()
        cached = self.cache.get(key)
        if cached and now - cached[0] < self.ttl:
            return cached[1]
        try:
            if self.local_root:
                with (self.local_root / branch / path).open("rb") as f:
                    raw = f.read(MAX_BYTES + 1)
            else:
                request = Request(source_url(branch, path), headers={"User-Agent": "PV-Radar-MCP-Pilot/0.1"})
                with self.opener.open(request, timeout=15) as response:
                    raw = response.read(MAX_BYTES + 1)
            if len(raw) > MAX_BYTES:
                raise ValueError("Artifact exceeds size limit")
            data = json.loads(raw.decode("utf-8-sig"))
            if not isinstance(data, dict):
                raise ValueError("Expected object root")
        except (OSError, ValueError) as exc:
            raise RuntimeError("Published artifact unavailable or invalid: " + path) from exc
        if len(self.cache) >= 32:
            self.cache.clear()
        self.cache[key] = (now, data)
        return data

    def get(self, name):
        return self.read(*FILES[name])


class Radar:
    def __init__(self, data):
        self.data = data

    def records(self):
        payload = self.data.get("radar")
        rows = payload.get("records")
        if not isinstance(rows, list):
            raise ValueError("Radar records missing")
        out = {}
        for row in rows:
            if not isinstance(row, dict):
                raise ValueError("Invalid radar row")
            if (row.get("is_terna") or row.get("is_punctual") is False
                    or norm(row.get("source")) == "terna_econnextion"
                    or "terna econnextion" in norm(row.get("title"))):
                continue
            key = row.get("project_key")
            if not isinstance(key, str) or not key or key in out:
                raise ValueError("Missing or duplicate canonical project key")
            out[key] = row
        return payload, out

    def search(self, region="", province="", municipality="", project_type="",
               query="", min_mw=None, status="", limit=20, offset=0):
        if not 1 <= limit <= 50 or offset < 0:
            raise ValueError("limit must be 1..50 and offset nonnegative")
        if min_mw is not None and not valid_power(min_mw):
            raise ValueError("min_mw must be finite and nonnegative")
        payload, rows = self.records()
        results = []
        for row in rows.values():
            if region and norm(row.get("region")) != norm(region):
                continue
            if province and norm(province) not in [norm(p) for p in re.split(r"[,;/]", str(row.get("province") or ""))]:
                continue
            if municipality and norm(municipality) not in norm(row.get("municipalities")):
                continue
            if project_type and norm(project_type) != norm(row.get("project_type")):
                continue
            if status and norm(status) not in norm(row.get("status")):
                continue
            if query and norm(query) not in norm(" ".join(str(row.get(k) or "") for k in ("title", "proponent", "municipalities"))):
                continue
            power = row.get("power_mw")
            if min_mw is not None and (not valid_power(power) or power < min_mw):
                continue
            results.append(scalar_fields(row, ROW_FIELDS))
        results.sort(key=lambda r: (-(r["power_mw"] if valid_power(r.get("power_mw")) else -1), r["project_key"]))
        return {"source": source_url(*FILES["radar"]), "generated_at": payload.get("summary", {}).get("generated_at"),
                "total_matches": len(results), "offset": offset, "limit": limit,
                "projects": results[offset:offset+limit], "project_types_available": sorted({str(r.get("project_type") or "") for r in rows.values()}),
                "warning": "Reported radar values; not a verified start date or execution award. Terna aggregates excluded. Power types may differ."}

    def detail(self, project_key):
        payload, rows = self.records()
        if project_key not in rows:
            return {"found": False, "project_key": project_key, "reason": "Not in current canonical radar"}
        result = {"found": True, "project": scalar_fields(rows[project_key], ROW_FIELDS),
                  "source": source_url(*FILES["radar"]), "generated_at": payload.get("summary", {}).get("generated_at")}
        index = self.data.get("cards")
        entry = index.get(project_key)
        if not entry:
            result["card_status"] = "not_indexed"
            return result
        path = card_path(entry)
        card = self.data.read("doc-intel-state", path)
        if card.get("project_key") != project_key:
            # A card index may alias keys; do not silently attach a different project.
            result["card_status"] = "identity_mismatch"
            return result
        location = card.get("ubicazione") or {}
        coord = location.get("coordinate") or {}
        evidence = coord.get("source") or {}
        company_fields = ("name", "kind", "verified")
        companies = []
        for group in ("soggetti", "altri_soggetti"):
            for item in card.get(group) or []:
                if not isinstance(item, dict):
                    continue
                companies.append({**scalar_fields(item, company_fields),
                                  "roles": [r for r in item.get("roles", []) if isinstance(r, str)][:15],
                                  "source": scalar_fields(item.get("source") or {}, ("document", "url", "page")),
                                  "group": group})
        result.update({
            "card_status": "available", "card_updated": card.get("updated"),
            "card_source": source_url("doc-intel-state", path),
            "official_source": card.get("source_url"),
            "location": {**scalar_fields(location, ("regione", "provincia", "maps_url", "maps_precision")),
                         "comuni": [x for x in location.get("comuni", []) if isinstance(x, str)],
                         "coordinate": {**scalar_fields(coord, ("lat", "lon", "tipo")),
                                        "source": scalar_fields(evidence, ("url", "page", "document"))}},
            "companies": companies[:40],
            "company_warning": "Listed roles are not proof of an EPC award; company verification is not execution confirmation. Dedicated execution evidence not exposed in this pilot.",
            "document_coverage": scalar_fields(card.get("documentazione") or {}, ("documenti", "pagine", "completa")),
            "warning": card.get("avvertenza"),
        })
        return result

    def readiness(self, project_key):
        _, rows = self.records()
        payload = self.data.get("readiness")
        item = payload.get("projects", {}).get(project_key) if project_key in rows else None
        return {"project_key": project_key, "found": item is not None,
                "source": source_url(*FILES["readiness"]), "updated": payload.get("updated"),
                "readiness": item, "warning": "Radar classification includes estimates, not a guaranteed or independently verified construction date. Preserve the reasons and phase."}

    def changes(self, region="", kind="all", limit=30, offset=0):
        if kind not in ("all", "nuovi", "avanzamenti", "realizzati") or not 1 <= limit <= 50 or offset < 0:
            raise ValueError("Invalid kind, limit or offset")
        payload = self.data.get("changes")
        _, rows = self.records()
        groups = ("nuovi", "avanzamenti", "realizzati") if kind == "all" else (kind,)
        records = []
        for group in groups:
            for row in payload.get(group, []):
                if region and norm(row.get("region")) != norm(region):
                    continue
                key = row.get("project_key")
                records.append({**scalar_fields(row, CHANGE_FIELDS), "change_type": group,
                                "official_source": rows.get(key, {}).get("url")})
        return {"source": source_url(*FILES["changes"]), "since": payload.get("since"), "date": payload.get("date"),
                "total_matches": len(records), "offset": offset, "limit": limit, "changes": records[offset:offset+limit],
                "warning": "Only the most recent published comparison interval, not a historical 30-day search."}

    def ferx(self, project_key):
        _, rows = self.records()
        payload = self.data.get("ferx")
        entries = payload.get("projects", {}).get(project_key, []) if project_key in rows else []
        return {"project_key": project_key, "found": bool(entries), "updated": payload.get("updated"),
                "source": source_url(*FILES["ferx"]), "official_source": payload.get("url"),
                "matches": [scalar_fields(r, ("basis", "codice", "comune", "provincia", "data_domanda", "kw",
                                            "kw_ammessi", "posizione", "procedura", "ragione_sociale")) for r in entries][:30],
                "warning": "An incentive match is not a construction award or proof of works starting."}

    def health(self):
        payload, rows = self.records()
        out = {"radar": {"source": source_url(*FILES["radar"]), "generated_at": payload.get("summary", {}).get("generated_at"),
                         "canonical_projects": len(rows), "project_types": dict(Counter(str(r.get("project_type") or "") for r in rows.values())),
                         "reported_suspicious_rows": payload.get("data_quality", {}).get("suspicious_rows")}}
        for name in ("changes", "readiness", "cards", "ferx"):
            try:
                data = self.data.get(name)
                index = data if name == "cards" else data.get("projects", {})
                out[name] = {"available": True, "source": source_url(*FILES[name]),
                             "updated": data.get("updated") if name not in ("cards", "changes") else (data.get("date") if name == "changes" else None),
                             "indexed": len(index) if name != "changes" else None,
                             "canonical_matches": len(set(rows) & set(index)) if name != "changes" else None}
            except (RuntimeError, ValueError):
                out[name] = {"available": False, "source": source_url(*FILES[name])}
        out["warning"] = "Artifact access and coverage only; collector success, original document completeness and current real-world status are not independently verified."
        return out
