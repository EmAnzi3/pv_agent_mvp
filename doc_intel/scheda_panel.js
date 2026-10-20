/* Pannello "Scheda progetto" per la dashboard PV Agent.
   Uso: PVScheda.open(card) dove card è l'oggetto prodotto da doc_intel.card.build_card.
   Nessuna dipendenza; stili in scheda_panel.css. */
(function () {
  const STATUS = {
    'verificato': { cls: 'st-ok', text: 'Verificato' },
    'da documenti': { cls: 'st-doc', text: 'Da documenti' },
    'da confermare': { cls: 'st-warn', text: 'Da confermare' },
    'in conflitto': { cls: 'st-conf', text: 'Valori diversi' }
  };

  function esc(v) {
    return String(v == null ? '' : v).replace(/[&<>"']/g, c => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' }[c]));
  }

  function safeHref(u) {
    try { const p = new URL(u); return (p.protocol === 'https:' || p.protocol === 'http:') ? p.href : ''; } catch (e) { return ''; }
  }

  function sourceLink(src) {
    if (!src) return '';
    const href = safeHref(src.url);
    const label = esc(src.document || 'documento') + (src.page ? ', pag. ' + esc(src.page) : '');
    const link = href ? `<a href="${esc(href)}" target="_blank" rel="noopener noreferrer">${label}</a>` : label;
    const quote = src.excerpt ? `<span class="sc-quote">«${esc(src.excerpt)}»</span>` : '';
    return `<div class="sc-src">Fonte: ${link}${quote}</div>`;
  }

  function badge(status) {
    const s = STATUS[status] || { cls: 'st-doc', text: status };
    return `<span class="sc-badge ${s.cls}">${esc(s.text)}</span>`;
  }

  function item(it) {
    let body;
    if (it.values) {
      body = `<ul class="sc-alts">${it.values.map(v =>
        `<li><span class="sc-val">${esc(v.value)}</span> <span class="sc-occ">${esc(v.occurrences)} volte</span>${sourceLink(v.source)}</li>`).join('')}</ul>`;
    } else {
      body = `<div class="sc-val">${esc(it.value)}</div>` + (it.detail ? `<div class="sc-detail">${esc(it.detail)}</div>` : '')
        + sourceLink(it.source);
    }
    const note = it.note ? `<div class="sc-note">${esc(it.note)}</div>` : '';
    return `<div class="sc-item"><div class="sc-head"><span class="sc-label">${esc(it.label)}</span>${badge(it.status)}</div>${body}${note}</div>`;
  }

  function gantt(g) {
    if (!g || (!(g.bars || []).length && !(g.milestones || []).length)) return '';
    const bars = g.bars || [];
    const months = Math.max(1, Math.ceil(g.months || 1));
    const rowH = 26, labelW = 170, chartW = 330, top = 22;
    const x = m => labelW + (m / months) * chartW;
    const step = months <= 12 ? 1 : months <= 24 ? 3 : 6;
    let ticks = '';
    for (let m = 0; m <= months; m += step) {
      ticks += `<line x1="${x(m)}" y1="${top - 4}" x2="${x(m)}" y2="${top + bars.length * rowH}" class="g-grid"/>`
        + `<text x="${x(m)}" y="${top - 8}" class="g-tick" text-anchor="middle">${m}</text>`;
    }
    const rows = bars.map((b, i) => {
      const y = top + i * rowH;
      const w = Math.max(3, x(b.end) - x(b.start));
      return `<text x="${labelW - 8}" y="${y + 16}" class="g-label" text-anchor="end">${esc(b.name)}</text>`
        + `<rect x="${x(b.start)}" y="${y + 5}" width="${w}" height="15" rx="4" class="${b.positioned ? 'g-bar' : 'g-bar g-free'}"/>`
        + `<text x="${x(b.start) + w + 5}" y="${y + 16}" class="g-val">${esc(b.label || ('mesi ' + (b.start + 1) + '–' + b.end))}</text>`;
    }).join('');
    const h = top + bars.length * rowH + 8;
    const svg = bars.length ? `<div class="g-wrap"><svg viewBox="0 0 ${labelW + chartW + 80} ${h}" role="img" aria-label="Cronoprogramma in mesi">
      <text x="${labelW}" y="10" class="g-tick">mesi dall'avvio del cantiere</text>${ticks}${rows}</svg></div>` : '';
    const ms = (g.milestones || []).map(m =>
      `<li><span class="g-date">${esc(m.date)}</span> <strong>${esc(m.label)}</strong> <span class="sc-occ">${esc(m.nature)}</span>${sourceLink(m.source)}</li>`).join('');
    return `<section class="sc-sec"><h3>Cronoprogramma</h3>${svg}
      ${g.note ? `<div class="sc-note">${esc(g.note)}</div>` : ''}
      ${ms ? `<ul class="g-ms">${ms}</ul>` : ''}</section>`;
  }

  function render(card) {
    const doc = card.documentazione || {};
    const href = safeHref(card.source_url);
    const sections = (card.sections || []).filter(s => s.items && s.items.length && s.id !== 'tempi').map(s =>
      `<section class="sc-sec"><h3>${esc(s.title)}</h3>${s.note ? `<div class="sc-note">${esc(s.note)}</div>` : ''}${s.items.map(item).join('')}</section>`).join('')
      + gantt(card.cronoprogramma);
    const open = (card.da_verificare || []).length
      ? `<section class="sc-sec sc-open"><h3>Da verificare</h3><ul>${card.da_verificare.map(o =>
        `<li><strong>${esc(o.label)}</strong>: ${esc(o.reason)}</li>`).join('')}</ul></section>` : '';
    return `
      <header class="sc-top">
        <div class="sc-eyebrow">Scheda progetto · aggiornata il ${esc((card.updated || '').split('-').reverse().join('/'))}</div>
        <h2 id="scTitle">${esc(card.title)}</h2>
        <div class="sc-facts">
          <span><strong>${esc(doc.documenti)}</strong> documenti analizzati</span>
          <span><strong>${esc(doc.pagine)}</strong> pagine</span>
          <span>${doc.completa ? 'Documentazione letta per intero' : 'Lettura documentazione incompleta'}</span>
        </div>
        ${href ? `<a class="sc-mother" href="${esc(href)}" target="_blank" rel="noopener noreferrer">Apri il procedimento ufficiale</a>` : ''}
      </header>
      <div class="sc-body">${sections}${open}
        <p class="sc-warn">${esc(card.avvertenza)}</p>
      </div>`;
  }

  let panel, backdrop, lastFocus;
  function ensure() {
    if (panel) return;
    backdrop = document.createElement('div');
    backdrop.className = 'sc-backdrop';
    backdrop.hidden = true;
    panel = document.createElement('aside');
    panel.className = 'sc-panel';
    panel.setAttribute('role', 'dialog');
    panel.setAttribute('aria-modal', 'true');
    panel.setAttribute('aria-labelledby', 'scTitle');
    panel.hidden = true;
    panel.innerHTML = '<button class="sc-close" type="button" aria-label="Chiudi scheda">Chiudi</button><div class="sc-content"></div>';
    document.body.append(backdrop, panel);
    backdrop.addEventListener('click', close);
    panel.querySelector('.sc-close').addEventListener('click', close);
    document.addEventListener('keydown', e => { if (e.key === 'Escape' && !panel.hidden) close(); });
  }
  function open(card) {
    ensure();
    lastFocus = document.activeElement;
    panel.querySelector('.sc-content').innerHTML = render(card);
    panel.scrollTop = 0;
    backdrop.hidden = false;
    panel.hidden = false;
    panel.querySelector('.sc-close').focus();
  }
  function close() {
    if (!panel) return;
    panel.hidden = true;
    backdrop.hidden = true;
    if (lastFocus && lastFocus.focus) lastFocus.focus();
  }
  window.PVScheda = { open, close, render };
})();
