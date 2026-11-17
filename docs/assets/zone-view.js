/* «La mia zona» view shared by the PV and wind dashboards.
   The page passes an adapter; the zone is the value of the existing regionFilter / provinceFilter selects
   (moved into the sticky bar), so the pipeline table below follows the same zone.
   The chosen zone is remembered in this browser (localStorage, best effort) and shared by the two pages. */
(function () {
  'use strict';
  const STORE = 'alayanRadarZone';
  const TILES = [
    ['VDA', "Valle d'Aosta", 2, 1], ['TAA', 'Trentino-Alto Adige', 4, 1], ['FVG', 'Friuli-Venezia Giulia', 5, 1],
    ['PIE', 'Piemonte', 2, 2], ['LOM', 'Lombardia', 3, 2], ['VEN', 'Veneto', 4, 2],
    ['LIG', 'Liguria', 2, 3], ['EMR', 'Emilia-Romagna', 3, 3],
    ['TOS', 'Toscana', 3, 4], ['UMB', 'Umbria', 4, 4], ['MAR', 'Marche', 5, 4],
    ['LAZ', 'Lazio', 4, 5], ['ABR', 'Abruzzo', 5, 5], ['MOL', 'Molise', 6, 5],
    ['SAR', 'Sardegna', 2, 6], ['CAM', 'Campania', 5, 6], ['PUG', 'Puglia', 6, 6],
    ['BAS', 'Basilicata', 6, 7], ['CAL', 'Calabria', 6, 8], ['SIC', 'Sicilia', 5, 9]
  ];
  const SHADES = [[80, '#1f5f86', '#ffffff'], [30, '#5d9fc0', '#ffffff'], [5, '#b9d9e8', '#0f2a3a'], [1, '#dbe9f1', '#0f2a3a'], [0, '#eef2f6', '#64748b']];

  const num = v => { const n = Number(v); return Number.isFinite(n) ? n : 0; };
  function it(value, dec) {
    const n = num(value);
    let [i, d = ''] = Math.abs(n).toFixed(dec || 0).split('.');
    i = i.replace(/\B(?=(\d{3})+(?!\d))/g, '.'); d = d.replace(/0+$/, '');
    return (n < 0 ? '-' : '') + i + (d ? ',' + d : '');
  }
  const mw = v => it(v, num(v) >= 100 ? 0 : 1) + ' MW';
  const gw = v => it(num(v) / 1000, 1) + ' GW';
  const esc = v => String(v == null ? '' : v).replace(/&/g, '&amp;').replace(/[<]/g, '&lt;').replace(/>/g, '&gt;').replace(/"/g, '&quot;').replace(/'/g, '&#039;');
  const $ = id => document.getElementById(id);
  const itDate = iso => (iso || '').slice(0, 10).split('-').reverse().join('/');
  const titleCase = s => /[a-z]/.test(s || '') ? (s || '') : (s || '').toLowerCase().replace(/(^|[\s'(-])([a-zà-ù])/g, (m, a, b) => a + b.toUpperCase());

  let A = null;   // the page adapter

  const zone = () => ({ region: $('regionFilter').value, province: $('provinceFilter').value });
  const inZone = (x, z) => (!z.region || x.region === z.region) && (!z.province || (x.provinces || []).includes(z.province));
  const zoneName = z => z.province ? (A.provinceLabel ? A.provinceLabel(z.province) : z.province) : (z.region || 'Italia');
  const placeOf = x => [titleCase(x.place || ''), (x.provinces || []).join('/')].filter(Boolean).join(' · ') || x.region || 'n/d';
  const isNear = x => x.rd && (x.rd.level === 'Alta' || x.rd.level === 'Media');
  const nearSort = (a, b) => num(b.rd && b.rd.score) - num(a.rd && a.rd.score) || num(b.mw) - num(a.mw);

  function save() {
    try { localStorage.setItem(STORE, JSON.stringify(zone())); } catch (e) { /* storage blocked: the zone just is not remembered */ }
  }
  function restore() {
    let z = null;
    try { z = JSON.parse(localStorage.getItem(STORE) || 'null'); } catch (e) { z = null; }
    if (!z || !z.region) return;
    const reg = $('regionFilter');
    if (![...reg.options].some(o => o.value === z.region)) return;
    reg.value = z.region; reg.dispatchEvent(new Event('change'));
    const prov = $('provinceFilter');
    if (z.province && [...prov.options].some(o => o.value === z.province)) { prov.value = z.province; prov.dispatchEvent(new Event('change')); }
  }
  function setRegion(region) {
    const reg = $('regionFilter'), prov = $('provinceFilter');
    if (prov.value) { prov.value = ''; }
    reg.value = region; reg.dispatchEvent(new Event('change'));
    prov.dispatchEvent(new Event('change'));
  }

  function kpi(label, value, note, primary) {
    return `<div class="zone-kpi${primary ? ' primary' : ''}"><div class="label">${esc(label)}</div><div class="value">${value}</div><div class="note">${note}</div></div>`;
  }

  function render() {
    if (!A) return;
    const z = zone(), all = A.items(), items = all.filter(x => inZone(x, z));
    const near = items.filter(isNear).sort(nearSort);
    const ch = A.changes() || {};
    const nuovi = (ch.nuovi || []).filter(x => inZone(x, z)), avanz = (ch.avanzamenti || []).filter(x => inZone(x, z));
    const sum = list => list.reduce((s, x) => s + num(x.mw), 0);
    const name = zoneName(z);

    // national line in the header
    const allNear = all.filter(isNear);
    if ($('natLine')) $('natLine').innerHTML = `<strong>Italia</strong> · ${it(all.length)} progetti · ${gw(sum(all))} · `
      + `<strong>${it(allNear.length)}</strong> vicini al cantiere${A.updated ? ' · aggiornato ' + esc(itDate(A.updated())) : ''}`;
    $('zoneSummary').textContent = z.region ? `${name}: ${it(items.length)} progetti, ${it(near.length)} vicini al cantiere` : 'Scegli la tua zona: la pagina e la tabella si filtrano';
    $('zoneReset').hidden = !z.region;

    // KPIs
    const since = ch.since ? 'dal ' + itDate(ch.since) : (ch.date ? 'aggiornamento del ' + itDate(ch.date) : '');
    const withCard = items.filter(x => x.card).length;
    $('zoneKpis').innerHTML = [
      kpi('Vicini al cantiere', it(near.length), esc(mw(sum(near))) + ' · priorità alta e media', true),
      kpi("Novità dall'ultimo aggiornamento", ch.loaded ? it(nuovi.length + avanz.length) : '–',
        ch.loaded ? `${it(nuovi.length)} nuovi · ${it(avanz.length)} avanzamenti${since ? ' · ' + esc(since) : ''}` : 'Disponibile dal prossimo aggiornamento'),
      kpi('Progetti in zona', it(items.length), esc(mw(sum(items))) + (A.scopeNote ? ' · ' + esc(A.scopeNote) : '')),
      kpi('Con scheda documenti', it(withCard), items.length ? Math.round(withCard * 100 / items.length) + '% dei progetti in zona' : '')
    ].join('');

    // near-site list
    $('nearTitle').textContent = `Vicini al cantiere in ${name}`;
    const byProv = {};
    near.forEach(x => (x.provinces || []).slice(0, 1).forEach(p => { byProv[p] = (byProv[p] || 0) + 1; }));
    const provLine = Object.entries(byProv).sort((a, b) => b[1] - a[1]).slice(0, 8).map(([p, n]) => `${esc(p)} ${it(n)}`).join(' · ');
    $('nearByProv').textContent = near.length ? (z.province ? `${it(near.length)} progetti` : `${it(near.length)} progetti${provLine ? ' · ' + provLine : ''}`) : '';
    const top = near.slice(0, 8);
    $('nearList').innerHTML = top.length
      ? '<div class="near-row head"><span>Dove · proponente</span><span class="mw">MW</span><span>Fase</span><span>Perché</span><span></span></div>'
        + top.map(x => `<div class="near-row">
            <span title="${esc(x.title)}"><strong>${esc(placeOf(x))}</strong><span class="who">${esc(x.proponent || 'Proponente n/d')}</span></span>
            <span class="mw">${x.mw ? esc(mw(x.mw)) : '–'}</span>
            <span class="ph"><span class="phase-pill ${x.rd.level === 'Alta' ? 'alta' : 'media'}" title="${esc(x.rd.window || '')}">${esc(x.rd.phase_label || x.rd.level)}</span></span>
            <span class="why">${esc((x.rd.reasons || []).slice(0, 2).join(' · '))}</span>
            <span class="acts">${A.actions(x)}</span></div>`).join('')
      : `<div class="zone-empty">Nessun progetto con segnali di cantiere in ${esc(name)} per ora.</div>`;
    $('nearMore').hidden = near.length <= top.length;
    $('nearMore').textContent = `Vedi tutti i ${it(near.length)} nella tabella →`;

    // news in the zone
    const newsList = (list, tag, empty) => list.length
      ? list.slice(0, 6).map(x => `<div class="news-item"><span title="${esc(x.title)}"><strong>${esc(placeOf(x))}</strong>${x.proponent ? ' · ' + esc(x.proponent) : ''}${x.mw ? ' · ' + esc(mw(x.mw)) : ''}</span><span class="tag">${esc(tag(x))}</span></div>`).join('')
        + (list.length > 6 ? `<div class="small muted">e altri ${it(list.length - 6)}</div>` : '')
      : `<div class="zone-empty">${empty}</div>`;
    const wait = 'Disponibile dal prossimo aggiornamento.';
    $('progressList').innerHTML = ch.loaded ? newsList(avanz, x => x.what || 'avanzamento', `Nessun avanzamento in ${esc(name)} nell'ultimo aggiornamento.`) : `<div class="zone-empty">${wait}</div>`;
    $('newList').innerHTML = ch.loaded ? newsList(nuovi, x => x.what || 'nuovo', `Nessun nuovo progetto in ${esc(name)} nell'ultimo aggiornamento.`) : `<div class="zone-empty">${wait}</div>`;
    if ($('changesSince')) $('changesSince').textContent = since;

    // national picture: tile map (near-site projects per region, whole country) and the page charts
    const perRegion = {};
    allNear.forEach(x => { perRegion[x.region] = (perRegion[x.region] || 0) + 1; });
    const options = new Set([...$('regionFilter').options].map(o => o.value));
    $('tileMap').innerHTML = TILES.map(([code, region, col, row]) => {
      const n = perRegion[region] || 0, s = SHADES.find(x => n >= x[0]);
      const on = z.region === region ? ' on' : '';
      const ok = options.has(region);
      return `<button type="button" class="${on.trim()}" style="grid-column:${col};grid-row:${row};background:${s[1]};color:${s[2]}"
        data-region="${esc(region)}" title="${esc(region)}: ${it(n)} vicini al cantiere"${ok ? '' : ' disabled'} aria-pressed="${on ? 'true' : 'false'}">
        <span class="c">${code}</span><span class="n">${it(n)}</span></button>`;
    }).join('');
    $('natCharts').innerHTML = A.national().map(c => {
      const max = Math.max(1, ...c.rows.map(r => num(r.value)));
      return `<div class="zone-card"><div class="zone-head"><h2>${esc(c.title)}</h2><span class="small">${esc(c.note || '')}</span></div>
        <div class="hbars">${c.rows.map(r => `<div class="hbar"><span title="${esc(r.label)}">${esc(r.label)}</span>
          <span class="track"><span class="fill" style="display:block;width:${(num(r.value) * 100 / max).toFixed(1)}%"></span></span>
          <span class="v">${esc(c.unit === 'n' ? it(r.value) : mw(r.value))}</span></div>`).join('') || '<div class="zone-empty">Nessun dato.</div>'}</div></div>`;
    }).join('');
  }

  function mount(adapter) {
    A = adapter;
    ['regionFilter', 'provinceFilter'].forEach(id => $(id).addEventListener('change', () => { save(); render(); }));
    $('zoneReset').addEventListener('click', () => setRegion(''));
    $('tileMap').addEventListener('click', e => {
      const b = e.target.closest('button[data-region]');
      if (!b || b.disabled) return;
      setRegion(zone().region === b.dataset.region ? '' : b.dataset.region);
      $('zoneBar').scrollIntoView({ behavior: 'smooth', block: 'start' });
    });
    $('nearMore').addEventListener('click', () => { A.showNear(); $('pipelineSection').scrollIntoView({ behavior: 'smooth', block: 'start' }); });
    restore();
    render();
  }

  window.ZoneView = { mount, render, titleCase };
})();
