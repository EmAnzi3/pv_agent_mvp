/* Wind Agent Dashboard: the wind census read with the same sections, filters and table as the PV dashboard.
   Data (memory branch doc-intel-state, public): wind/census.json, schede/live_index.json, readiness/wind_index.json.
   The verified projects (curated radar) are part of the census: badge «Verificato» with stage, contractors and gaps. */
(function () {
  'use strict';
  const RAW = 'https://raw.githubusercontent.com/EmAnzi3/pv_agent_mvp/doc-intel-state/';
  const CHART_COLORS = ['#0f766e', '#1d4ed8', '#334155', '#d97706', '#16a34a', '#7c3aed', '#0e7490', '#b45309', '#475569', '#059669', '#2563eb', '#64748b'];
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
  const chartColors = n => Array.from({ length: n }, (_, i) => CHART_COLORS[i % CHART_COLORS.length]);

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

  // ---------------------------------------------------------------- sections
  function regions() {
    const by = {};
    records.filter(isLive).forEach(r => {
      const k = (r.region || 'n/d').split(' / ')[0];
      const x = by[k] = by[k] || { region: k, count: 0, mw: 0, off: 0, ready: 0 };
      if (isOffshore(r)) x.off += toNumber(r.power_mw);
      else { x.count += 1; x.mw += toNumber(r.power_mw); if (isReady(r)) x.ready += 1; }
    });
    // same reading as the PV score: operational MW in GW + 0.05 per project + the macro pipeline (here offshore) / 5 GW
    return Object.values(by).map(x => Object.assign(x, { score: Math.round((x.mw / 1000 + x.count * 0.05 + x.off / 5000) * 10) / 10 }))
      .sort((a, b) => b.score - a.score);
  }

  function initKpis() {
    const op = operational(), off = offshore();
    const mw = op.reduce((s, r) => s + toNumber(r.power_mw), 0), offMw = off.reduce((s, r) => s + toNumber(r.power_mw), 0);
    const sources = new Set(records.map(sourceLabel));
    setText('heroRecords', fmtNum(op.length)); setText('heroMw', formatItalianNumber(mw, 3)); setText('heroDate', updated);
    const regional = new Set(records.filter(r => r.source_group === 'regione').map(r => (r.region || '').split(' / ')[0]));
    setText('censusDate', itDate(updated));
    setText('heroSources', `MASE, ${regional.size} portali regionali, GSE FER X, verifica manuale`);
    setText('kpiRecords', fmtNum(op.length)); setText('kpiMw', fmtMw(mw)); setText('kpiOffMw', fmtMw(offMw));
    setText('kpiOffNote', `${fmtNum(off.length)} progetti: dato separato, in gran parte in scoping o VIA`);
    setText('kpiSources', fmtNum(sources.size)); setText('kpiDate', updated);
    setText('kpiReady', fmtNum(records.filter(r => isLive(r) && isReady(r)).length));
    const fx = records.filter(r => (r.ferx || []).length);
    setText('kpiFerx', fmtNum(fx.length));
    setText('kpiFerxNote', `di cui ${fmtNum(fx.filter(r => r.source === 'gse_ferx').length)} senza procedura trovata`);
    setText('kpiVerified', fmtNum(new Set(records.filter(r => r.curato).map(r => r.curato.id)).size));
    setText('kpiCards', fmtNum(records.filter(r => schedeIndex[r.project_key]).length));
    setText('kpiArchived', fmtNum(summary.archiviati_esclusi || 0));
    setText('kpiArchivedNote', `${fmtMw(summary.mw_archiviati || 0)} con tutte le procedure archiviate`);
  }

  function initRegions() {
    document.getElementById('regionBody').innerHTML = regions().slice(0, 20).map(x => `<tr>
      <td><strong>${escapeHtml(x.region)}</strong></td><td class="num">${formatItalianNumber(x.score, 1)}</td>
      <td class="num">${fmtNum(x.count)}</td><td class="num">${fmtMw(x.mw)}</td><td class="num">${fmtMw(x.off)}</td>
      <td class="num">${fmtNum(x.ready)}</td></tr>`).join('');
  }

  function chartOptions(title, isMw = false) {
    return {
      responsive: true, maintainAspectRatio: true,
      plugins: {
        legend: { display: false },
        title: { display: true, text: title, color: '#334155', font: { size: 15, weight: 'bold' } },
        tooltip: { backgroundColor: '#0f172a', padding: 10, callbacks: { label: c => isMw ? fmtMw(c.parsed.y) : fmtNum(c.parsed.y) } }
      },
      scales: {
        x: { grid: { display: false }, ticks: { color: '#64748b' } },
        y: { beginAtZero: true, grid: { color: '#e8edf2' }, ticks: { color: '#64748b', callback: v => isMw ? fmtMw(v).replace(' MW', '') : fmtNum(v) } }
      }
    };
  }
  function bar(id, rows, label, key, title, isMw) {
    if (!window.Chart) return;
    new Chart(document.getElementById(id), {
      type: 'bar',
      data: { labels: rows.map(x => x.label), datasets: [{ label, data: rows.map(x => x[key]), backgroundColor: chartColors(rows.length), borderRadius: 8 }] },
      options: chartOptions(title, isMw)
    });
  }
  function initCharts() {
    if (!window.Chart) return;
    const reg = regions();
    bar('regionPriorityChart', reg.slice(0, 12).map(x => ({ label: x.region, v: x.score })), 'Score priorità', 'v', 'Indice priorità commerciale per regione');
    bar('operationalCountChart', [...reg].sort((a, b) => b.count - a.count).slice(0, 12).map(x => ({ label: x.region, v: x.count })), 'Progetti operativi', 'v', 'Pipeline operativa: numero progetti per regione');
    const ph = {};
    operational().forEach(r => { const k = phaseGroup(r); ph[k] = (ph[k] || 0) + toNumber(r.power_mw); });
    bar('phaseMwChart', PHASE_ORDER.filter(k => ph[k]).map(k => ({ label: k.replace('Post-VIA: ', ''), v: ph[k] })), 'MW', 'v', 'Pipeline operativa: MW per fase autorizzativa', true);
    bar('offshoreMwChart', [...reg].filter(x => x.off).sort((a, b) => b.off - a.off).slice(0, 12).map(x => ({ label: x.region, v: x.off })), 'MW', 'v', 'Pipeline offshore: MW per regione', true);
    const src = {};
    records.filter(isLive).forEach(r => { const k = sourceLabel(r); src[k] = (src[k] || 0) + 1; });
    const s = Object.entries(src).sort((a, b) => b[1] - a[1]);
    new Chart(document.getElementById('sourceChart'), {
      type: 'doughnut',
      data: { labels: s.map(x => x[0]), datasets: [{ label: 'Record', data: s.map(x => x[1]), backgroundColor: chartColors(s.length), borderWidth: 0 }] },
      options: { responsive: true, cutout: '62%', plugins: {
        legend: { position: 'bottom', labels: { boxWidth: 12, color: '#64748b' } },
        title: { display: true, text: 'Distribuzione progetti per fonte', color: '#334155', font: { size: 15, weight: 'bold' } },
        tooltip: { backgroundColor: '#0f172a', padding: 10 } } }
    });
  }

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

  function initTop() {
    const rank = r => (ready[r.project_key] || {}).score || 0;
    const top = operational().filter(r => isReady(r) || r.curato)
      .sort((a, b) => rank(b) - rank(a) || toNumber(b.power_mw) - toNumber(a.power_mw)).slice(0, 25);
    document.getElementById('topBody').innerHTML = top.map(r => row(r, true)).join('') ||
      '<tr><td colspan="9" class="empty">Nessun progetto con segnali di cantiere.</td></tr>';
  }

  // ---------------------------------------------------------------- pipeline
  function fillSelect(id, first, values) {
    const el = document.getElementById(id);
    el.innerHTML = `<option value="">${first}</option>` + values.map(v => `<option value="${escapeHtml(v)}">${escapeHtml(v)}</option>`).join('');
  }
  function initFilters() {
    const uniq = arr => [...new Set(arr.filter(Boolean))].sort((a, b) => a.localeCompare(b, 'it'));
    fillSelect('sourceFilter', 'Tutte le fonti', uniq(records.map(sourceLabel)));
    fillSelect('regionFilter', 'Tutte le regioni', uniq(records.map(r => (r.region || '').split(' / ')[0])));
    fillSelect('provinceFilter', 'Tutte le province', uniq(records.flatMap(r => (r.province || '').split(/[,/ ]+/))).filter(p => /^[A-Z]{2}$/.test(p)));
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
      initKpis(); initRegions(); initTop(); initFilters(); renderPipeline(); initCharts();
      ['searchInput', 'dataKindFilter', 'epcFilter', 'sourceFilter', 'regionFilter', 'provinceFilter', 'phaseFilter',
       'sortFieldFilter', 'sortDirectionFilter', 'sideFilter'].forEach(id => {
        document.getElementById(id).addEventListener('input', renderPipeline);
        document.getElementById(id).addEventListener('change', renderPipeline);
      });
      document.getElementById('exportFilteredCsv').addEventListener('click', exportCsv);
    });
})();
