/* Wind Agent Dashboard: the wind census read with the same «La mia zona» view, filters and table as the PV dashboard.
   Data (memory branch doc-intel-state, public): wind/census.json, schede/live_index.json, readiness/wind_index.json.
   The verified projects (curated radar) are part of the census: badge «Verificato» with stage, contractors and gaps. */
(function () {
  'use strict';
  const RAW = 'https://raw.githubusercontent.com/EmAnzi3/pv_agent_mvp/doc-intel-state/';
  const SOURCE_LABEL = {
    mase: 'MASE', puglia_opendata: 'Puglia', sistema_puglia_energia: 'Puglia', basilicata: 'Basilicata',
    campania: 'Campania', calabria: 'Calabria', sardegna: 'Sardegna', sicilia: 'Sicilia', gse_ferx: 'GSE FER X', curato: 'Verificato'
  };
  let records = [], summary = {}, schedeIndex = {}, ready = {}, updated = '';

  // ---------------------------------------------------------------- helpers (same as the PV dashboard)
  const toNumber = v => { const n = Number(v); return Number.isFinite(n) ? n : 0; };
  function formatItalianNumber(value, maxDecimals = 0) {
    const n = toNumber(value), sign = n < 0 ? '-' : '';
    let [i, d = ''] = Math.abs(n).toFixed(maxDecimals).split('.');
    i = i.replace(/\B(?=(\d{3})+(?!\d))/g, '.'); d = d.replace(/0+$/, '');
    return sign + i + (d ? ',' + d : '');
  }
  const fmtNum = v => formatItalianNumber(v, 0);
  const fmtMw = v => (v === null || v === undefined || v === '') ? '' : formatItalianNumber(v, 3) + ' MW';
  const text = v => v === null || v === undefined ? '' : String(v);
  const escapeHtml = v => text(v).replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;').replace(/"/g, '&quot;').replace(/'/g, '&#039;');
  const setText = (id, v) => { const n = document.getElementById(id); if (n) n.textContent = v; };
  const itDate = iso => (iso || '').split('-').reverse().join('/');
  function safeUrl(v) {
    const raw = text(v).trim();
    if (!/^https?:\/\//i.test(raw)) return '';
    try { const u = new URL(raw); return /^https?:$/.test(u.protocol) ? u.href : ''; } catch (e) { return ''; }
  }
  const link = v => { const u = safeUrl(v); return u ? `<a href="${escapeHtml(u)}" target="_blank" rel="noopener noreferrer">Apri</a>` : ''; };
  const titleCase = s => /[a-z]/.test(s) ? s : s.toLowerCase().replace(/(^|[\s'(-])([a-zà-ù])/g, (m, a, b) => a + b.toUpperCase());
  const mwBand = v => toNumber(v) >= 100 ? 'mw-xl' : toNumber(v) >= 50 ? 'mw-lg' : '';

  // ---------------------------------------------------------------- reading of a census record
  const isOffshore = r => /offshore/i.test(r.project_type || '');
  const isLive = r => !r.secondaria;
  const operational = () => records.filter(r => isLive(r) && !isOffshore(r));
  const offshore = () => records.filter(r => isLive(r) && isOffshore(r));
  const lv = r => (ready[r.project_key] || {}).level;
  const isReady = r => ['Alta', 'Media'].includes(lv(r));

  function phase(r) {
    if (r.curato && (r.source === 'gse_ferx' || r.source === 'curato'))
      return r.curato.fase ? 'Verificato: ' + r.curato.fase : 'Verificato';
    if (r.source === 'gse_ferx') return 'Procedura da individuare';
    if (r.source === 'curato') return r.curato && r.curato.fase ? 'Verificato: ' + r.curato.fase : 'Verificato';
    const p = r.procedure || '', st = (r.procedure_status || '').split(', ')[0] || '';
    if (r.source_group === 'regione') {
      const t = `${p} ${r.procedure_status || ''} ${r.title || ''}`;
      if (/ottemperanza/i.test(t)) return 'Post-VIA: ottemperanza';
      if (/assoggettabilit|screening/i.test(t)) return 'Screening';
      if (/conclus|chius|rilasciat|favorevol|positiv|emanat|adottat/i.test(r.procedure_status || '')) return 'Procedura regionale conclusa';
      return 'Procedura regionale in corso';
    }
    if (/ottemperanza/i.test(p)) return 'Post-VIA: ottemperanza';
    if (/proroga/i.test(p)) return 'Post-VIA: proroga';
    if (/valutazione preliminare|variant|modific/i.test(p)) return 'Modifica di progetto';
    if (/definizione (?:dei )?contenuti|scoping|livello elaborati/i.test(p)) return 'Scoping (pre-VIA)';
    if (/assoggettabilit/i.test(p)) return 'Screening';
    if (/conclus/i.test(st)) return 'VIA conclusa';
    if (/impatto ambientale|provvedimento unico|autorizzazione|via/i.test(p)) return 'VIA in istruttoria';
    return 'Altra procedura';
  }
  const PHASE_ORDER = ['Verificato', 'Post-VIA: ottemperanza', 'Post-VIA: proroga', 'VIA conclusa', 'Procedura regionale conclusa',
    'VIA in istruttoria', 'Procedura regionale in corso', 'Screening', 'Scoping (pre-VIA)', 'Procedura da individuare', 'Modifica di progetto', 'Altra procedura'];
  const phaseGroup = r => phase(r).startsWith('Verificato') ? 'Verificato' : phase(r);

  function displayTitle(r) {
    if (r.source === 'gse_ferx') return `Impianto eolico ${r.proponent || ''} – ${r.municipalities} (graduatoria FER X)`;
    if (r.source === 'puglia_opendata') return `Impianto eolico – ${r.municipalities}`;
    return r.title || '';
  }

  function badges(r) {
    const out = [];
    const s = schedeIndex[r.project_key];
    if (s) out.push(`<button class="sc-btn" type="button" data-scheda="${escapeHtml(s.file)}">Scheda</button>`);
    const rd = ready[r.project_key];
    if (rd && isReady(r)) out.push(`<span class="sc-epc sc-ready sc-ready-${rd.level === 'Alta' ? 'a' : 'm'}" title="${escapeHtml(rd.phase_label + (rd.window ? ': ' + rd.window : ''))}">Cantiere ${rd.level === 'Alta' ? '●●' : '●'}</span>`);
    if (s && s.epc) out.push(`<span class="sc-epc" title="${s.epc === 'A' ? 'Impresa esecutrice in un atto di cantiere' : 'Impresa esecutrice indicata negli elaborati'}">EPC ${s.epc === 'A' ? '✓' : '?'}</span>`);
    const nw = (window.PVNews || {})[r.project_key];
    if (nw) out.push(`<span class="sc-epc sc-press" title="Notizie di stampa compatibili con il progetto: da verificare">Stampa ${nw.length}</span>`);
    if ((r.ferx || []).length) out.push(`<span class="sc-epc sc-ferx" title="Incentivo FER X assegnato (GSE, ${escapeHtml(r.ferx[0].procedura)}, posizione ${escapeHtml(r.ferx[0].posizione)})">FER X</span>`);
    if (r.curato) {
      const c = r.curato, tip = [`Progetto verificato a mano: ${c.fase || ''}`,
        (c.contractor || []).length ? 'Contractor confermati: ' + c.contractor.join(', ') : 'Contractor esecutivi non ancora confermati',
        c.prossima_tappa ? 'Prossima tappa: ' + c.prossima_tappa : '',
        (c.lacune || []).length ? 'Lacune supply chain: ' + c.lacune.join(', ') : ''].filter(Boolean).join(' · ');
      out.push(`<span class="sc-epc sc-ver" title="${escapeHtml(tip)}">Verificato</span>`);
    }
    if (r.source === 'gse_ferx' && !r.curato) out.push('<span class="sc-epc sc-lead" title="In graduatoria FER X ma senza procedura VIA trovata: probabile repowering o autorizzazione regionale semplificata">Da individuare</span>');
    if (r.secondaria) out.push('<span class="sc-epc sc-side" title="Modifica, variante o proroga di un progetto: non conteggiata nei totali">Modifica</span>');
    return out.join(' ');
  }
  const badgeClass = r => r.source === 'mase' ? 'badge mase' : r.source === 'gse_ferx' ? 'badge gse' : r.source === 'curato' ? 'badge curato' : 'badge';
  const sourceLabel = r => SOURCE_LABEL[r.source] || r.source_label || r.source;

  function row(r, top) {
    const prop = text(r.proponent) ? escapeHtml(r.proponent) : '<span class="muted">n/d</span>';
    // like the PV 'Tipo' column: the procedure, not the whole status history (that is in the card and the CSV)
    const status = r.source === 'gse_ferx' ? 'graduatoria FER X, posizione ' + (r.ferx[0] || {}).posizione
      : r.source === 'curato' ? 'radar verificato' : text(r.procedure).replace(/\s*\((?:PNIEC-PNRR|Scoping)\)/, '');
    const ph = `<span class="phase">${escapeHtml(phase(r))}</span><span class="small muted">${escapeHtml(isOffshore(r) ? 'Offshore' : 'Onshore')}${status ? ' · ' + escapeHtml(status) : ''}</span>`;
    const cells = top
      ? [`<td class="project-title"><strong>${escapeHtml(displayTitle(r))}</strong></td>`, `<td class="proponent-col">${prop}</td>`,
         `<td>${escapeHtml(r.region)}</td>`, `<td>${escapeHtml(r.province)}</td>`, `<td>${escapeHtml(titleCase(r.municipalities || ''))}</td>`,
         `<td><span class="${badgeClass(r)}">${escapeHtml(sourceLabel(r))}</span></td>`]
      : [`<td><span class="${badgeClass(r)}">${escapeHtml(sourceLabel(r))}</span></td>`, `<td>${escapeHtml(r.region)}</td>`,
         `<td>${escapeHtml(r.province)}</td>`, `<td>${escapeHtml(titleCase(r.municipalities || ''))}</td>`,
         `<td class="project-title"><strong>${escapeHtml(displayTitle(r))}</strong></td>`, `<td class="proponent-col">${prop}</td>`];
    return `<tr class="${mwBand(r.power_mw)}">${cells.join('')}<td>${ph}</td><td class="num">${fmtMw(r.power_mw)}</td>
      <td><span class="sc-cell">${link(r.url)}${badges(r)}</span></td></tr>`;
  }

  // ---------------------------------------------------------------- pipeline
  function fillSelect(id, first, values, label = v => v) {
    const el = document.getElementById(id);
    el.innerHTML = `<option value="">${escapeHtml(first)}</option>` + values.map(v => `<option value="${escapeHtml(v)}">${escapeHtml(label(v))}</option>`).join('');
  }
  // geography: the province list follows the region (sigla + name); picking a
  // province with no region selected sets its region
  const PROV_NAMES = {"AG":"Agrigento","AL":"Alessandria","AN":"Ancona","AO":"Aosta","AP":"Ascoli Piceno","AQ":"L'Aquila","AR":"Arezzo","AT":"Asti","AV":"Avellino","BA":"Bari","BG":"Bergamo","BI":"Biella","BL":"Belluno","BN":"Benevento","BO":"Bologna","BR":"Brindisi","BS":"Brescia","BT":"Barletta-Andria-Trani","BZ":"Bolzano","CA":"Cagliari","CB":"Campobasso","CE":"Caserta","CH":"Chieti","CL":"Caltanissetta","CN":"Cuneo","CO":"Como","CR":"Cremona","CS":"Cosenza","CT":"Catania","CZ":"Catanzaro","EN":"Enna","FC":"Forlì-Cesena","FE":"Ferrara","FG":"Foggia","FI":"Firenze","FM":"Fermo","FR":"Frosinone","GE":"Genova","GO":"Gorizia","GR":"Grosseto","IM":"Imperia","IS":"Isernia","KR":"Crotone","LC":"Lecco","LE":"Lecce","LI":"Livorno","LO":"Lodi","LT":"Latina","LU":"Lucca","MB":"Monza e Brianza","MC":"Macerata","ME":"Messina","MI":"Milano","MN":"Mantova","MO":"Modena","MS":"Massa-Carrara","MT":"Matera","NA":"Napoli","NO":"Novara","NU":"Nuoro","OR":"Oristano","PA":"Palermo","PC":"Piacenza","PD":"Padova","PE":"Pescara","PG":"Perugia","PI":"Pisa","PN":"Pordenone","PO":"Prato","PR":"Parma","PT":"Pistoia","PU":"Pesaro e Urbino","PV":"Pavia","PZ":"Potenza","RA":"Ravenna","RC":"Reggio Calabria","RE":"Reggio Emilia","RG":"Ragusa","RI":"Rieti","RM":"Roma","RN":"Rimini","RO":"Rovigo","SA":"Salerno","SI":"Siena","SO":"Sondrio","SP":"La Spezia","SR":"Siracusa","SS":"Sassari","SU":"Sud Sardegna","SV":"Savona","TA":"Taranto","TE":"Teramo","TN":"Trento","TO":"Torino","TP":"Trapani","TR":"Terni","TS":"Trieste","TV":"Treviso","UD":"Udine","VA":"Varese","VB":"Verbano-Cusio-Ossola","VC":"Vercelli","VE":"Venezia","VI":"Vicenza","VR":"Verona","VT":"Viterbo","VV":"Vibo Valentia"};
  const provLabel = code => PROV_NAMES[code] ? `${code} – ${PROV_NAMES[code]}` : code;
  const regionOf = r => (r.region || '').split(' / ')[0];
  const provincesOf = r => (r.province || '').split(/[,/ ]+/).filter(p => /^[A-Z]{2}$/.test(p));
  function refreshProvinces() {
    const region = val('regionFilter'), el = document.getElementById('provinceFilter'), current = el.value;
    const counts = {};
    records.forEach(r => {
      if (region && regionOf(r) !== region) return;
      provincesOf(r).forEach(p => { counts[p] = (counts[p] || 0) + 1; });
    });
    const codes = Object.keys(counts).sort((a, b) => provLabel(a).localeCompare(provLabel(b), 'it'));
    fillSelect('provinceFilter', region ? `Tutte le province (${region})` : 'Tutte le province', codes, provLabel);   // no counts: the table hides offshore and variants by default
    el.value = counts[current] ? current : '';
  }
  function regionForProvince(code) {
    const counts = {};
    records.forEach(r => { if (provincesOf(r).includes(code) && regionOf(r)) counts[regionOf(r)] = (counts[regionOf(r)] || 0) + 1; });
    return Object.keys(counts).sort((a, b) => counts[b] - counts[a])[0] || '';
  }
  function initFilters() {
    const uniq = arr => [...new Set(arr.filter(Boolean))].sort((a, b) => a.localeCompare(b, 'it'));
    fillSelect('sourceFilter', 'Tutte le fonti', uniq(records.map(sourceLabel)));
    fillSelect('regionFilter', 'Tutte le regioni', uniq(records.map(r => (r.region || '').split(' / ')[0])));
    refreshProvinces();
    fillSelect('phaseFilter', 'Tutte le fasi', PHASE_ORDER.filter(k => records.some(r => phaseGroup(r) === k)));
  }
  const val = id => document.getElementById(id).value;
  function matches(r) {
    const q = val('searchInput').trim().toLowerCase(), kind = val('dataKindFilter'), epc = val('epcFilter');
    if (!document.getElementById('sideFilter').checked && r.secondaria) return false;
    if (q && !`${r.title} ${r.proponent} ${r.municipalities} ${r.province} ${r.region}`.toLowerCase().includes(q)) return false;
    if (kind === 'onshore' && isOffshore(r)) return false;
    if (kind === 'offshore' && !isOffshore(r)) return false;
    if (kind === 'card' && !schedeIndex[r.project_key]) return false;
    if (kind === 'ferx' && !(r.ferx || []).length) return false;
    if (kind === 'cantiere' && !isReady(r)) return false;
    if (kind === 'verified' && !r.curato) return false;
    if (kind === 'lead' && r.source !== 'gse_ferx') return false;
    if (epc === 'news') {
      if (!(window.PVNews || {})[r.project_key]) return false;
    } else if (epc) {
      const e = (schedeIndex[r.project_key] || {}).epc;
      if (epc === 'any' ? !e : e !== epc) return false;
    }
    if (val('sourceFilter') && sourceLabel(r) !== val('sourceFilter')) return false;
    if (val('regionFilter') && !(r.region || '').startsWith(val('regionFilter'))) return false;
    if (val('provinceFilter') && !(r.province || '').split(/[,/ ]+/).includes(val('provinceFilter'))) return false;
    if (val('phaseFilter') && phaseGroup(r) !== val('phaseFilter')) return false;
    return true;
  }
  function sorted(items) {
    // default order: closest to a building site first, then MW (the PV table keeps the dataset order)
    const f = val('sortFieldFilter') || 'readiness', m = val('sortDirectionFilter') === 'asc' ? 1 : -1;
    return [...items].sort((a, b) => {
      if (f === 'readiness') {
        const sc = r => (ready[r.project_key] || {}).level ? ready[r.project_key].score : -1;
        return ((sc(a) - sc(b)) || (toNumber(a.power_mw) - toNumber(b.power_mw))) * m;
      }
      if (f === 'power_mw') return ((a.power_mw == null ? -Infinity : toNumber(a.power_mw)) - (b.power_mw == null ? -Infinity : toNumber(b.power_mw))) * m;
      const g = r => f === 'source_label' ? sourceLabel(r) : f === 'title' ? displayTitle(r) : text(r[f]);
      return g(a).localeCompare(g(b), 'it', { sensitivity: 'base' }) * m;
    });
  }
  function renderPipeline() {
    const list = sorted(records.filter(matches));
    setText('visibleCount', fmtNum(list.length));
    const btn = document.getElementById('exportFilteredCsv');
    btn.disabled = !list.length; btn.textContent = list.length ? `Esporta CSV (${fmtNum(list.length)})` : 'Nessun risultato';
    document.getElementById('pipelineBody').innerHTML = list.slice(0, 700).map(r => row(r, false)).join('') ||
      '<tr><td colspan="9" class="empty">Nessun progetto con questi filtri.</td></tr>';
  }
  function exportCsv() {
    const list = sorted(records.filter(matches));
    if (!list.length) return;
    const cell = v => '"' + text(v).replace(/[\r\n]+/g, ' ').trim().split('"').join('""') + '"';
    const head = ['Fonte', 'Regione', 'Provincia', 'Comune/i', 'Progetto', 'Proponente', 'Tipo', 'Fase', 'Procedura', 'Stato', 'MW', 'Link',
      'Verificato', 'Contractor confermati', 'Prossima tappa', 'Lacune supply chain', 'FER X', 'Vicini al cantiere', 'Fase cantiere', 'Finestra', 'Motivi', 'EPC', 'Scheda'];
    const rows = list.map(r => {
      const rd = ready[r.project_key] || {};
      return [sourceLabel(r), r.region, r.province, r.municipalities, displayTitle(r), r.proponent, isOffshore(r) ? 'Offshore' : 'Onshore',
        phase(r), r.procedure, r.procedure_status, r.power_mw == null ? '' : formatItalianNumber(r.power_mw, 3), r.url,
        r.curato ? r.curato.nome + ' (' + (r.curato.fase || '') + ')' : '', r.curato ? (r.curato.contractor || []).join(', ') : '',
        r.curato ? r.curato.prossima_tappa || '' : '', r.curato ? (r.curato.lacune || []).join(', ') : '', (r.ferx || []).map(x => '#' + x.posizione).join(' '),
        rd.level || '', rd.phase_label || '', rd.window || '', (rd.reasons || []).join(' | '),
        (schedeIndex[r.project_key] || {}).epc || '', schedeIndex[r.project_key] ? 'sì' : ''];
    });
    const csv = [head, ...rows].map(x => x.map(cell).join(';')).join('\r\n');
    const blob = new Blob([String.fromCharCode(0xFEFF), csv], { type: 'text/csv;charset=utf-8' });
    const d = new Date(), p = n => String(n).padStart(2, '0');
    const a = document.createElement('a');
    a.href = URL.createObjectURL(blob);
    a.download = `eolico_pipeline_filtrata_${d.getFullYear()}${p(d.getMonth() + 1)}${p(d.getDate())}_${p(d.getHours())}${p(d.getMinutes())}.csv`;
    document.body.appendChild(a); a.click(); a.remove();
    setTimeout(() => URL.revokeObjectURL(a.href), 2000);
  }

  // ---------------------------------------------------------------- «la mia zona»
  let zoneChanges = { loaded: false };
  const zoneItem = r => ({
    key: r.project_key, region: regionOf(r), provinces: provincesOf(r), place: r.municipalities, proponent: r.proponent,
    title: displayTitle(r), mw: r.power_mw, url: r.url, rd: ready[r.project_key] || null,
    card: !!schedeIndex[r.project_key], file: schedeIndex[r.project_key]
  });
  const changeItem = (m, what) => ({
    key: m.project_key, region: regionOf(m), provinces: provincesOf(m), place: m.municipalities, proponent: m.proponent,
    title: m.title, mw: m.power_mw, what
  });
  function zoneActions(x) {
    const u = safeUrl(x.url);
    return (x.file ? `<button class="sc-btn" type="button" data-scheda="${escapeHtml(x.file.file)}">Scheda</button>` : '')
      + (u ? `<a href="${escapeHtml(u)}" target="_blank" rel="noopener noreferrer">Portale</a>` : '');
  }
  function zoneNational() {
    const ph = {}, off = {};
    operational().forEach(r => { const k = phaseGroup(r); ph[k] = (ph[k] || 0) + toNumber(r.power_mw); });
    offshore().forEach(r => { const k = regionOf(r) || 'n/d'; off[k] = (off[k] || 0) + toNumber(r.power_mw); });
    return [
      { title: 'MW onshore per fase autorizzativa', note: 'Italia', unit: 'mw',
        rows: PHASE_ORDER.filter(k => ph[k]).map(k => ({ label: k.replace('Post-VIA: ', 'Post-VIA ').replace('Verificato', 'Verificati'), value: ph[k] })) },
      { title: 'MW offshore per regione', note: 'prime 5, pipeline separata', unit: 'mw',
        rows: Object.entries(off).sort((a, b) => b[1] - a[1]).slice(0, 5).map(([label, value]) => ({ label, value })) }
    ];
  }

  // ---------------------------------------------------------------- start
  const getJson = u => fetch(u, { cache: 'no-cache' }).then(r => r.ok ? r.json() : {}).catch(() => ({}));
  document.addEventListener('click', e => {
    const b = e.target.closest('[data-scheda]');
    if (!b || !window.PVScheda) return;
    getJson(RAW + 'schede/' + encodeURIComponent(b.dataset.scheda)).then(c => { if (c && c.project_key) window.PVScheda.open(c); });
  });
  Promise.all([getJson(RAW + 'wind/census.json'), getJson(RAW + 'schede/live_index.json'), getJson(RAW + 'readiness/wind_index.json'),
               getJson(RAW + 'news/wind_matches.json')])
    .then(([census, idx, rd, nw]) => {
      window.PVNews = (nw && nw.projects) || {};
      records = census.records || [];
      summary = census.summary || {};
      updated = census.updated || '';
      schedeIndex = idx || {};
      ready = (rd && rd.projects) || {};
      window.PVReady = ready;
      window.PVFerx = Object.fromEntries(records.filter(r => (r.ferx || []).length).map(r => [r.project_key, r.ferx]));
      if (!records.length) {
        document.getElementById('pipelineBody').innerHTML = '<tr><td colspan="9" class="empty">Censimento non disponibile al momento.</td></tr>';
        return;
      }
      initFilters(); renderPipeline();
      // registered first, so the province list is updated before the table is redrawn
      document.getElementById('regionFilter').addEventListener('change', refreshProvinces);
      document.getElementById('provinceFilter').addEventListener('change', () => {
        const code = val('provinceFilter'), regionSelect = document.getElementById('regionFilter');
        if (code && !regionSelect.value) {
          regionSelect.value = regionForProvince(code);
          refreshProvinces();
          document.getElementById('provinceFilter').value = code;
        }
      });
      ['searchInput', 'dataKindFilter', 'epcFilter', 'sourceFilter', 'regionFilter', 'provinceFilter', 'phaseFilter',
       'sortFieldFilter', 'sortDirectionFilter', 'sideFilter'].forEach(id => {
        document.getElementById(id).addEventListener('input', renderPipeline);
        document.getElementById(id).addEventListener('change', renderPipeline);
      });
      document.getElementById('exportFilteredCsv').addEventListener('click', exportCsv);
      window.ZoneView.mount({
        items: () => operational().map(zoneItem),
        changes: () => zoneChanges,
        actions: zoneActions,
        national: zoneNational,
        provinceLabel: provLabel,
        updated: () => updated,
        scopeNote: 'onshore',
        showNear: () => { const k = document.getElementById('dataKindFilter'); k.value = 'cantiere'; k.dispatchEvent(new Event('change')); }
      });
      getJson(RAW + 'wind/changes/latest.json').then(c => {
        if (!c || !c.date) return;
        const what = m => {
          const ch = (m.changes || []).find(x => x.progress) || (m.changes || [])[0] || {};
          return ch.field === 'procedure_status' ? text(ch.new) : [ch.label, ch.new].filter(Boolean).join(': ');
        };
        zoneChanges = { loaded: true, date: c.date, since: c.previous_census,
          nuovi: (c.nuovi || []).filter(m => !m.secondaria).map(m => changeItem(m, 'nuovo')),
          avanzamenti: (c.modificati || []).filter(m => m.progress).map(m => changeItem(m, what(m))) };
        window.ZoneView.render();
      });
    });
})();
