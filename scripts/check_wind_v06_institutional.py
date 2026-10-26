#!/usr/bin/env python3
import json
from pathlib import Path
from urllib.parse import urlparse

ROOT = Path(__file__).resolve().parents[1]
DATA = ROOT / "docs" / "wind" / "data"
registry = json.loads((DATA / "institutional-source-network-v06.json").read_text(encoding="utf-8"))
audit = json.loads((DATA / "institutional-source-network-v06b.json").read_text(encoding="utf-8"))

assert registry["version"] == "0.6.0"
assert audit["version"] == "0.6.0-b"
assert registry["origin_repo"] == "EmAnzi3/pv_agent_mvp"
assert registry["monitoring"]["national_core_cadence_days"] == 1
assert registry["monitoring"]["priority_regional_cadence_days"] == 3
assert registry["monitoring"]["standard_regional_cadence_days"] == 7
assert registry["monitoring"]["coverage_audit_days"] == 30

reuse = registry["pv_agent_reuse"]
assert len(reuse["active_regional_collectors"]) >= 13
assert set(reuse["active_national_collectors"]) == {
    "app/collectors/mase.py",
    "app/collectors/mase_provvedimenti.py",
    "app/collectors/terna_econnextion.py",
}
assert "app/collectors/basilicata.py" in reuse["available_not_active"]
assert "app/collectors/puglia.py" in reuse["disabled"]

sources = registry["sources"] + audit["sources"]
ids = [s["id"] for s in sources]
assert len(ids) == len(set(ids)), "duplicate institutional source ids"
assert len(sources) >= 30, f"institutional source network too small after audit: {len(sources)}"

for s in sources:
    assert s["priority"] in {"A", "B", "C"}, s["id"]
    assert s["level"] in {"national", "regional"}, s["id"]
    assert s.get("wind_adaptation"), s["id"]
    assert s.get("evidence_ceiling"), s["id"]
    assert isinstance(s.get("cadence_days"), int) and s["cadence_days"] >= 1, s["id"]
    assert s.get("origin_collector") or s.get("status", "").startswith("audited_official"), f"{s['id']}: missing collector without direct-source audit"
    for key in ["official_url", "discovery_url", "secondary_url"]:
        url = s.get(key)
        if url:
            parsed = urlparse(url)
            assert parsed.scheme in {"http", "https"} and parsed.netloc, f"{s['id']}: invalid {key} {url}"

by_id = {s["id"]: s for s in sources}
for required in [
    "mase-via", "mase-provvedimenti", "terna-econnextion",
    "puglia-sistema-energia", "sardegna-sira", "sicilia-sivvi",
    "basilicata-via", "calabria-via", "campania-viavas",
    "toscana-gea", "toscana-atos",
    "abruzzo-via", "abruzzo-fer-au", "liguria-via-procedimenti", "molise-au-eolico",
    "marche-via-regional", "marche-via-state-mirror", "marche-energy-au",
    "lazio-via-project-list", "emilia-romagna-viavasweb", "lombardia-ferau",
    "umbria-via-list", "veneto-fer-procedures", "piemonte-sivia"
]:
    assert required in by_id, f"missing required institutional node {required}"

assert by_id["toscana-atos"]["priority"] == "A"
assert "expand ALLOWED_TYPES" in by_id["toscana-atos"]["wind_adaptation"]
assert by_id["puglia-via-fer"]["status"] == "inherited_disabled_reusable"
assert "aggregate" in by_id["terna-econnextion"]["status"] or "aggregate" in by_id["terna-econnextion"]["channel"].lower() or "aggregate" in by_id["terna-econnextion"]["wind_adaptation"].lower()

original_gaps = {x["region"]: x for x in registry["coverage_gaps"]}
resolved = {x["region"] for x in audit["resolved_coverage_gaps"]}
assert {"Abruzzo", "Liguria", "Marche", "Molise"} <= resolved
for region in resolved:
    assert region in original_gaps, f"resolved gap was not declared originally: {region}"
    assert any(s.get("region") == region and s["priority"] == "A" for s in audit["sources"]), f"{region}: resolution lacks priority-A source"
remaining_gaps = set(original_gaps) - resolved
for region in ["Abruzzo", "Liguria", "Marche", "Molise"]:
    assert region not in remaining_gaps, f"{region} should be resolved at source-discovery level"

# Explicit guard: inherited PV collectors are source/parser assets, not wind-ready collectors by default.
assert "PV-only keyword filters must not be copied" in " ".join(registry["principles"])

print(
    f"v0.6 institutional network OK: {len(sources)} source nodes, "
    f"{sum(s['priority']=='A' for s in sources)} priority A, "
    f"{len(resolved)} source gaps resolved, {len(remaining_gaps)} remaining declared gaps"
)
