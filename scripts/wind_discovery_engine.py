from __future__ import annotations

import argparse
import hashlib
import json
import re
import unicodedata
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
DATA = ROOT / "docs" / "wind" / "data"
REGISTRIES = [DATA / "discovery-v04.json", DATA / "discovery-census-v04.json", DATA / "discovery-census-v04b.json"]
RULES = DATA / "identity-rules-v04.json"
INDEX = DATA / "discovery-index-v04.json"
REFRESH_LOG = DATA / "refresh-log-v04.json"


def load(path: Path, default=None):
    if not path.exists():
        return default
    with path.open("r", encoding="utf-8-sig") as handle:
        return json.load(handle)


def dump(path: Path, payload) -> None:
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def norm(value) -> str:
    text = unicodedata.normalize("NFKD", str(value or ""))
    text = "".join(ch for ch in text if not unicodedata.combining(ch)).lower()
    return re.sub(r"[^a-z0-9]+", "-", text).strip("-")


def myterna_ids(candidate: dict) -> list[str]:
    raw = candidate.get("myterna") or candidate.get("myterna_ids") or ""
    if isinstance(raw, list):
        values = raw
    else:
        values = re.findall(r"\d{6,}", str(raw))
    return sorted(dict.fromkeys(str(x) for x in values if str(x).strip()))


def activity_class(candidate: dict) -> str:
    if candidate.get("status") == "rejected":
        return "rejected"
    value = candidate.get("activity_class")
    if value in {"current", "stale_scoping"}:
        return value
    return "current"


def identity_key(candidate: dict) -> str:
    if candidate.get("identity_group"):
        anchor = "group:" + norm(candidate["identity_group"])
    else:
        ids = myterna_ids(candidate)
        if ids:
            anchor = "myterna:" + ids[0]
        elif candidate.get("mase_operation_anchor"):
            anchor = "mase-op:" + norm(candidate["mase_operation_anchor"])
        else:
            anchor = "fallback:" + "|".join(
                [
                    norm(candidate.get("site_type")),
                    norm(candidate.get("name")),
                    norm(candidate.get("area")),
                ]
            )
    digest = hashlib.sha256(anchor.encode("utf-8")).hexdigest()[:16]
    return f"wind:{digest}"


def fingerprint(candidate: dict, fields: list[str]) -> str:
    body = {field: candidate.get(field) for field in fields}
    body["myterna_ids"] = myterna_ids(candidate)
    body["activity_class"] = activity_class(candidate)
    raw = json.dumps(body, sort_keys=True, ensure_ascii=False, separators=(",", ":"))
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def combined_registry() -> dict:
    payloads = [load(path) for path in REGISTRIES]
    if any(not payload for payload in payloads):
        missing = [str(path) for path, payload in zip(REGISTRIES, payloads) if not payload]
        raise SystemExit(f"[FAIL] registry mancanti: {missing}")
    candidates = []
    for path, payload in zip(REGISTRIES, payloads):
        for candidate in payload.get("candidates", []):
            row = dict(candidate)
            row["registry_source"] = path.name
            row["activity_class"] = activity_class(row)
            candidates.append(row)
    return {
        "version": "0.4.2-combined-registry",
        "as_of": max(str(x.get("as_of") or "") for x in payloads),
        "candidates": candidates,
    }


def build_index(registry: dict, rules: dict) -> dict:
    fields = rules["change_fingerprint_fields"]
    rows = []
    for candidate in registry.get("candidates", []):
        row = dict(candidate)
        row["identity_key"] = identity_key(candidate)
        row["change_fingerprint"] = fingerprint(candidate, fields)
        rows.append(row)

    current = [x for x in rows if x["activity_class"] == "current"]
    stale = [x for x in rows if x["activity_class"] == "stale_scoping"]
    rejected = [x for x in rows if x["activity_class"] == "rejected"]
    non_rejected = current + stale

    return {
        "version": "0.4.2-index",
        "as_of": registry.get("as_of"),
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "candidate_count": len(rows),
        "current_candidate_count": len(current),
        "stale_candidate_count": len(stale),
        "rejected_candidate_count": len(rejected),
        "non_rejected_candidate_count": len(non_rejected),
        "current_onshore_count": sum(x.get("site_type") == "onshore" for x in current),
        "current_offshore_count": sum(x.get("site_type") == "offshore" for x in current),
        "current_wind_mw": round(sum(float(x.get("wind_mw") or 0) for x in current), 3),
        "current_bess_mw": round(sum(float(x.get("bess_mw") or 0) for x in current), 3),
        "stale_wind_mw": round(sum(float(x.get("wind_mw") or 0) for x in stale), 3),
        "rejected_wind_mw": round(sum(float(x.get("wind_mw") or 0) for x in rejected), 3),
        "candidates": rows,
    }


def validate(index: dict) -> None:
    candidate_ids = [x["candidate_id"] for x in index["candidates"]]
    if len(candidate_ids) != len(set(candidate_ids)):
        raise SystemExit("[FAIL] candidate_id duplicati")

    by_identity: dict[str, list[dict]] = {}
    for row in index["candidates"]:
        by_identity.setdefault(row["identity_key"], []).append(row)
        if row.get("site_type") not in {"onshore", "offshore"}:
            raise SystemExit(f"[FAIL] {row['candidate_id']}: site_type non valido")
        if row.get("activity_class") not in {"current", "stale_scoping", "rejected"}:
            raise SystemExit(f"[FAIL] {row['candidate_id']}: activity_class non valida")
        if not row.get("sources"):
            raise SystemExit(f"[FAIL] {row['candidate_id']}: nessuna fonte")

    collisions = {key: rows for key, rows in by_identity.items() if len(rows) > 1}
    unresolved = []
    for key, rows in collisions.items():
        groups = {x.get("identity_group") for x in rows}
        if len(groups) != 1 or None in groups:
            unresolved.append((key, [x["candidate_id"] for x in rows]))
    if unresolved:
        raise SystemExit(f"[FAIL] collisioni identity non riconciliate: {unresolved}")


def diff(old: dict | None, new: dict) -> list[dict]:
    if not old:
        return [
            {
                "event": "baseline",
                "candidate_id": row["candidate_id"],
                "identity_key": row["identity_key"],
                "new_fingerprint": row["change_fingerprint"],
            }
            for row in new["candidates"]
        ]

    old_by_id = {x["candidate_id"]: x for x in old.get("candidates", [])}
    new_by_id = {x["candidate_id"]: x for x in new.get("candidates", [])}
    events = []
    for cid, row in new_by_id.items():
        previous = old_by_id.get(cid)
        if previous is None:
            events.append({"event": "discovered", "candidate_id": cid, "identity_key": row["identity_key"], "new_fingerprint": row["change_fingerprint"]})
        elif previous.get("change_fingerprint") != row.get("change_fingerprint"):
            events.append({
                "event": "changed",
                "candidate_id": cid,
                "identity_key": row["identity_key"],
                "old_fingerprint": previous.get("change_fingerprint"),
                "new_fingerprint": row["change_fingerprint"],
            })
    for cid, row in old_by_id.items():
        if cid not in new_by_id:
            events.append({"event": "missing_from_refresh", "candidate_id": cid, "identity_key": row.get("identity_key")})
    return events


def main() -> None:
    parser = argparse.ArgumentParser(description="Wind Radar v0.4 discovery identity/change engine")
    parser.add_argument("--write", action="store_true", help="write derived index and refresh log")
    args = parser.parse_args()

    registry = combined_registry()
    rules = load(RULES)
    if not rules:
        raise SystemExit("[FAIL] identity rules mancanti")

    previous = load(INDEX)
    current = build_index(registry, rules)
    validate(current)
    events = diff(previous, current)

    print(f"[OK] discovery: {current['candidate_count']} candidati")
    print(f"[OK] current/stale/rejected: {current['current_candidate_count']}/{current['stale_candidate_count']}/{current['rejected_candidate_count']}")
    print(f"[OK] current onshore/offshore: {current['current_onshore_count']}/{current['current_offshore_count']}")
    print(f"[OK] current wind/BESS: {current['current_wind_mw']:.1f} MW / {current['current_bess_mw']:.1f} MW")
    print(f"[OK] stale/rejected wind: {current['stale_wind_mw']:.1f} / {current['rejected_wind_mw']:.1f} MW")
    print(f"[OK] identity keys uniche: {len({x['identity_key'] for x in current['candidates']})}")
    print(f"[OK] eventi refresh: {len(events)}")

    if args.write:
        dump(INDEX, current)
        log = load(REFRESH_LOG, {"version": "0.4.2-refresh-log", "runs": []})
        log["runs"].append({
            "as_of": registry.get("as_of"),
            "generated_at": current["generated_at"],
            "candidate_count": current["candidate_count"],
            "current_candidate_count": current["current_candidate_count"],
            "stale_candidate_count": current["stale_candidate_count"],
            "rejected_candidate_count": current["rejected_candidate_count"],
            "current_wind_mw": current["current_wind_mw"],
            "current_bess_mw": current["current_bess_mw"],
            "events": events,
        })
        dump(INDEX, current)
        dump(REFRESH_LOG, log)
        print(f"[OK] scritto {INDEX.relative_to(ROOT)}")
        print(f"[OK] scritto {REFRESH_LOG.relative_to(ROOT)}")


if __name__ == "__main__":
    main()
