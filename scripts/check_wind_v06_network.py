#!/usr/bin/env python3
import json
from pathlib import Path
from urllib.parse import urlparse

ROOT = Path(__file__).resolve().parents[1]
DATA = ROOT / "docs" / "wind" / "data"
base = json.loads((DATA / "company-network-v06.json").read_text(encoding="utf-8"))
tranche_b = json.loads((DATA / "company-network-v06b.json").read_text(encoding="utf-8"))
tranche_c = json.loads((DATA / "company-network-v06c.json").read_text(encoding="utf-8"))
tranche_d = json.loads((DATA / "company-network-v06d.json").read_text(encoding="utf-8"))
tranche_e = json.loads((DATA / "company-network-v06e.json").read_text(encoding="utf-8"))
manifest = json.loads((DATA / "projects.json").read_text(encoding="utf-8"))
projects = []
for chunk in manifest["chunks"]:
    projects.extend(json.loads((DATA / chunk).read_text(encoding="utf-8")))
project_ids = {p["id"] for p in projects}
project_aliases = {"on-lama-cupa": "lama-cupa"}

assert base["version"] == "0.6.0"
assert tranche_b["version"] == "0.6.0"
assert tranche_c["version"] == "0.6.0-c"
assert tranche_d["version"] == "0.6.0-d"
assert tranche_e["version"] == "0.6.0-e"
assert base["monitoring"]["high_priority_cadence_days"] == 7
assert base["monitoring"]["standard_cadence_days"] == 14
assert base["monitoring"]["universe_refresh_days"] == 30
assert any("anev.org/soci" in s.get("url", "") for s in base["discovery_sources"])

# v06/b/c/d are new-node tranches and therefore keep unique IDs. v06e is an
# explicit update overlay and may only reference nodes already introduced.
new_node_parts = (base, tranche_b, tranche_c, tranche_d)
new_node_ids = [c["id"] for payload in new_node_parts for c in payload.get("companies", [])]
assert len(new_node_ids) == len(set(new_node_ids)), "duplicate company ids across new-node tranches"
update_ids = [row["id"] for row in tranche_e.get("updates", [])]
assert len(update_ids) == len(set(update_ids)), "duplicate company ids inside current-update overlay"
assert set(update_ids).issubset(set(new_node_ids)), f"overlay references unknown nodes: {set(update_ids)-set(new_node_ids)}"

by_id = {}
for payload in new_node_parts:
    for company in payload.get("companies", []):
        by_id[company["id"]] = dict(company)
for update in tranche_e.get("updates", []):
    cid = update["id"]
    previous = by_id[cid]
    merged = {**previous, **update}
    for key in ["cluster", "known_capabilities", "project_links", "watch_urls"]:
        merged[key] = list(dict.fromkeys([*(previous.get(key) or []), *(update.get(key) or [])]))
    by_id[cid] = merged

companies = list(by_id.values())
assert len(companies) >= 61, f"expanded network too small: {len(companies)}"
assert sum(bool(c.get("watch_urls")) for c in companies) >= 53, "company watch URL coverage too low"

for c in companies:
    assert c["commercial_priority"] in {"A", "B", "C"}, c["id"]
    assert c.get("cluster"), c["id"]
    assert c.get("wind_relevance"), c["id"]
    assert c.get("relationship_status"), c["id"]
    for pid in c.get("project_links", []):
        canonical_pid = project_aliases.get(pid, pid)
        assert canonical_pid in project_ids, f"{c['id']}: unknown project link {pid}"
    for url in c.get("watch_urls", []):
        parsed = urlparse(url)
        assert parsed.scheme in {"http", "https"} and parsed.netloc, f"{c['id']}: invalid URL {url}"

for required in [
    "espe", "plc", "tozzi-green", "fagioli", "mammoet", "vestas", "rwe", "erg",
    "edison-rinnovabili", "plenitude", "baywa-re", "sape-costruzioni", "ivpc", "renexia",
    "renext-solutions", "ox2-italia", "wpd-italia", "hitachi-energy", "tratos-cavi",
    "yce-blades", "engie-italia", "alerion", "goldwind-energy-italy",
    "blu-costruzioni", "egm-project", "barone-costruzione", "gruppo-novello",
    "la-molisana-trasporti", "pizzulo-costruzioni", "simic", "fc-wind-service",
    "progeco-group", "socep", "vector-renewables"
]:
    assert required in by_id, f"missing required network node {required}"

espe = by_id["espe"]
assert espe["commercial_priority"] == "A"
assert espe["wind_relevance"] == "adjacent_high_value"
assert espe["project_links"] == [], "ESPE must not be project-linked without evidence"
assert "wind_full_bop" not in espe["cluster"], "ESPE must not be labelled proven utility-scale wind Full BoP"
assert any("fornitori" in u for u in espe["watch_urls"]), "ESPE supplier route must be monitored"

plc = by_id["plc"]
assert plc["commercial_priority"] == "A"
assert "wind_full_bop" in plc["cluster"]
assert set(plc["project_links"]) >= {"tarsia-ovest", "castelfranco-cer"}

tozzi = by_id["tozzi-green"]
assert tozzi["commercial_priority"] == "A"
assert "wind_epc" in tozzi["cluster"]

sape = by_id["sape-costruzioni"]
assert sape["commercial_priority"] == "A"
assert "wind_full_bop" in sape["cluster"]
assert "wtg_installation" not in sape["cluster"], "SAPE site says BoP excludes WTG supply/installation"
assert sape["project_links"] == [], "SAPE must not be linked to a canonical project without evidence"

ivpc = by_id["ivpc"]
assert ivpc["commercial_priority"] == "A"
assert "wtg_installation" in ivpc["cluster"]
assert "o_and_m" in ivpc["cluster"]

wpd = by_id["wpd-italia"]
assert "on-nulvi-sedini-wpd" in wpd["project_links"]
assert wpd["relationship_status"] == "proven_in_radar_non_execution"

# New execution-oriented nodes must remain conservative about project attribution.
blu = by_id["blu-costruzioni"]
assert blu["project_links"] == [], "historical Carlentini references must not be mapped to current canonical repowering"
assert "historical" in blu["relationship_status"]

pizzulo = by_id["pizzulo-costruzioni"]
assert pizzulo["project_links"] == [], "geographic proximity to Andretta-Bisaccia is not project evidence"
assert "geographically" in pizzulo["relationship_status"]

egm = by_id["egm-project"]
assert egm["project_links"] == ["serra-giannina"]
assert egm["relationship_status"] == "proven_in_radar_non_execution"

simic = by_id["simic"]
assert "wind_epc" in simic["cluster"]
assert simic["project_links"] == [], "turnkey track record must not imply a canonical award"

progeco = by_id["progeco-group"]
assert progeco["commercial_priority"] == "A"
assert progeco["project_links"] == ["andretta-bisaccia"]
assert progeco["relationship_status"] == "project_specific_support_signal"
assert "construction_supervision" in progeco["cluster"]
assert "civil_bop" not in progeco["cluster"], "site-management support signal must not become civil execution attribution"
assert "electrical_bop" not in progeco["cluster"], "site-management support signal must not become electrical execution attribution"

socep = by_id["socep"]
assert socep["commercial_priority"] == "B"
assert socep["project_links"] == ["alia-sclafani"]
assert socep["relationship_status"] == "historical_same_site_supplier"
assert "civil_works" in socep["cluster"]
assert "historical" in socep["relationship_status"], "old-site reference must remain explicitly historical"

vector = by_id["vector-renewables"]
assert vector["commercial_priority"] == "A"
assert vector["project_links"] == ["tricarico"]
assert vector["relationship_status"] == "confirmed_project_support"
assert "construction_monitoring" in vector["cluster"]
assert "wind_full_bop" not in vector["cluster"], "lender-side advisor must not become execution contractor"

mammana = by_id["mammana-michelangelo"]
assert set(mammana["project_links"]) >= {"tarsia-ovest", "carlentini"}
assert mammana["relationship_status"] == "proven_in_radar_multi_project"
assert "current WTG foundation concrete execution" in mammana["known_capabilities"]

hydro = by_id["hydro-engineering"]
assert hydro["relationship_status"] == "confirmed_project_support_multi_project"
assert set(hydro["project_links"]) >= {"carlentini", "nulvi-ploaghe"}
assert "Direzione Lavori" in hydro["known_capabilities"]
assert "wind repowering project development" in hydro["known_capabilities"]
assert any("hydroeng.it/settori/eolico" in u for u in hydro["watch_urls"])
assert any("7445475651026972672" in u for u in hydro["watch_urls"]), "Nulvi-Ploaghe direct project reference must be watched"

# New nodes and current-update overlays must be operationally actionable.
for payload in (tranche_b, tranche_c, tranche_d):
    for c in payload["companies"]:
        assert c.get("last_checked") == "2026-09-05", c["id"]
        assert c.get("next_action"), f"{c['id']}: missing next_action"
        assert c.get("watch_urls"), f"{c['id']}: missing watch_urls"
for update in tranche_e["updates"]:
    assert update.get("last_checked") in {"2026-09-05", "2026-09-06"}, update["id"]
    assert update.get("next_action"), f"{update['id']}: missing next_action"
    assert update.get("watch_urls"), f"{update['id']}: missing watch_urls"

# Network intelligence remains separate from canonical project data; execution
# attribution is carried by the project-specific evidence/enrichment layer.
assert "companies" not in manifest

print(
    f"v0.6 company network OK: {len(companies)} players, "
    f"{sum(c['commercial_priority']=='A' for c in companies)} priority A, "
    f"{len(tranche_b['companies']) + len(tranche_c['companies']) + len(tranche_d['companies'])} new expansion nodes, "
    f"{len(tranche_e['updates'])} current node updates"
)
