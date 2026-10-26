#!/usr/bin/env python3
from __future__ import annotations

from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
WIND = ROOT / "docs" / "wind"
html = (WIND / "index.html").read_text(encoding="utf-8")
app = (WIND / "assets" / "app.js").read_text(encoding="utf-8")
js = (WIND / "assets" / "province-map-v06.js").read_text(encoding="utf-8")
css = (WIND / "assets" / "province-map-v06.css").read_text(encoding="utf-8")

assert 'id="provinceMap"' in html
assert 'Mappa per provincia' in html
assert 'data-metric="mw"' in html
assert 'data-metric="projects"' in html
assert 'data-metric="e4mw"' in html
assert 'assets/province-map-v06.css' in html
assert 'assets/province-map-v06.js' in html
assert 'echarts@5' in html
assert 'italy-base.svg' not in html, "old static marker-map base should no longer be visible"
assert 'class="map-marker' not in html

assert "openpolis/geojson-italy" in js
assert "wind_italy_provinces" in js
assert "type:'map'" in js
assert "visualMap" in js
assert "BESS non sommato" in js
assert "row.mw+=+p.mw" in js
assert "row.e4mw+=+p.mw" in js
assert "row.projects++" in js
assert "normProvince(p.province)" in js
assert "$('province')" in js
assert "q.value=province" not in js, "province click must not overwrite free-text search"
assert "select.value=province" in js and "dispatchEvent(new Event('change'" in js
assert "ensureProvinceFilter" in app
assert "fill(e.province,projects.map(p=>p.province))" in app
assert "pv&&p.province!==pv" in app
assert "confine:true" in js, "province tooltip must remain inside the map on desktop/mobile"
assert "max-width:300px" in js, "province tooltip needs a bounded readable width"
assert "state.metric!=='mw'" in js and "state.metric!=='projects'" in js and "state.metric!=='e4mw'" in js, "selected metric must not be duplicated in tooltip"
assert "p.lat" not in js and "p.lon" not in js, "province map must not regress to project marker coordinates"
assert ".province-map-viz" in css
assert ".legacy-map-compat" in css

print("v0.6 province map OK: dedicated province filter, preserved search and confined non-duplicated tooltip")
