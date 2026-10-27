/* Pannello "Scheda progetto" per la dashboard PV Agent (schema pv-scheda/2.0).
   Uso: PVScheda.open(card) dove card è l'oggetto prodotto da doc_intel.card.build_card.
   Ordine di lettura: sintesi -> società e contatti -> cronoprogramma -> ubicazione -> da verificare.
   Nessuna dipendenza; stili in scheda_panel.css. */
(function () {
  function esc(v) {
    return String(v == null ? '' : v).replace(/[&<>"']/g, c => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' }[c]));
  }
  function safeHref(u) {
    try { const p = new URL(u); return (p.protocol === 'https:' || p.protocol === 'http:') ? p.href : ''; } catch (e) { return ''; }
  }
  function src(s, text) {
    if (!s) return '';
    let href = safeHref(s.url);
    if (href && s.page && !href.includes('#')) href += '#page=' + encodeURIComponent(s.page);
    const tip = (s.document || 'documento') + (s.page ? ', pag. ' + s.page : '') + (s.excerpt ? ' — «' + s.excerpt + '»' : '');
    const label = esc(text || 'fonte');
    return href ? `<a class="sc-src" href="${esc(href)}" target="_blank" rel="noopener noreferrer" title="${esc(tip)}">${label}</a>`
                : `<span class="sc-src" title="${esc(tip)}">${label}</span>`;
  }
  function flag(status) {
    return status === 'da verificare' ? '<span class="sc-flag">da verificare</span>'
         : status === 'verificato' ? '<span class="sc-ok">verificato</span>' : '';
  }

  function tiles(list) {
    if (!list || !list.length) return '';
    return `<div class="sc-tiles">${list.map(t => `
      <div class="sc-tile${t.status === 'da verificare' ? ' is-warn' : ''}">
        <div class="sc-tlabel">${esc(t.label)} ${flag(t.status)}</div>
        <div class="sc-tval">${esc(t.value)}</div>
        ${t.sub ? `<div class="sc-tsub">${esc(t.sub)}</div>` : ''}
        <div class="sc-tfoot">${src(t.source)}</div>
      </div>`).join('')}</div>`;
  }

  function contactRow(c) {
    const isPhone = c.type === 'Telefono';
    return `<li class="sc-crow"><span class="sc-ctype">${esc(isPhone ? 'Tel.' : c.type)}</span>
      <span class="sc-cval">${esc(c.value)}</span>
      <button type="button" class="sc-copy" data-copy="${esc(c.value)}" aria-label="Copia ${esc(c.value)}">Copia</button>
      ${src(c.source)}</li>`;
  }
  function company(e) {
    const roles = (e.roles || []).map(r => `<span class="sc-role${/confermare/.test(r) ? ' is-warn' : ''}">${esc(r)}</span>`).join('');
    const meta = [];
    if (e.vat) meta.push(`P. IVA ${esc(e.vat)}${e.verified ? ' <span class="sc-ok">verificata</span>' : ''}${e.vat_group ? ' (gruppo IVA)' : ''}`);
    if (e.office) meta.push('Sede ' + esc(e.office));
    const contacts = (e.contacts || []).length
      ? `<ul class="sc-clist">${e.contacts.map(contactRow).join('')}</ul>`
      : '<p class="sc-none">Nessun recapito associato negli elaborati.</p>';
    return `<article class="sc-co">
      <div class="sc-cohead"><h4>${esc(e.name)}</h4>${e.kind === 'persona' ? '<span class="sc-kind">professionista</span>' : ''}</div>
      ${roles ? `<div class="sc-roles">${roles}</div>` : ''}
      ${meta.length ? `<div class="sc-meta">${meta.join(' · ')}</div>` : ''}
      ${contacts}</article>`;
  }
  function subjects(card) {
    const main = card.soggetti || [], others = card.altri_soggetti || [];
    if (!main.length && !others.length) return '';
    return `<section class="sc-sec"><h3>Società e contatti</h3>
      <div class="sc-cos">${main.length ? main.map(company).join('') : '<p class="sc-none">Nessun ruolo indicato esplicitamente negli elaborati.</p>'}</div>
      ${others.length ? `<details class="sc-more"><summary>Altre società e professionisti citati negli elaborati (${others.length})</summary>
        <div class="sc-cos">${others.map(company).join('')}</div></details>` : ''}</section>`;
  }

  function gantt(g) {
    if (!g) return '';
    const bars = g.bars || [];
    let chart = '';
    if (bars.length) {
      const unit = g.unit === 'month' ? 'mesi' : g.unit === 'week' ? 'settimane' : 'periodi';
      const asse = g.asse || null;
      const ticksDoc = asse ? asse.etichette : [];
      const real = bars.filter(b => !b.separator);
      const lo = Math.min(0, ...real.map(b => b.start), ...ticksDoc.map(t => t.start));
      const hi = Math.max(1, ...real.map(b => b.end), ...ticksDoc.map(t => t.end));
      const hasDur = bars.some(b => b.duration);
      const bare = ticksDoc.length && ticksDoc.every(t => /^\d+$/.test(t.label));
      // axis caption as printed ('Mese'); for bare numbers the unit of the chart; none for 'W1'/'gen 21' labels
      const caption = asse && asse.titolo ? asse.titolo : (asse ? (bare ? (asse.unita || unit) : '') : `${unit} dall'avvio`);
      const rowH = 22, labelW = 230, durW = hasDur ? 46 : 0, chartW = 300, head = caption ? 34 : 26;
      const x0 = labelW + durW;
      const x = m => x0 + ((m - lo) / (hi - lo)) * chartW;
      const bottom = head + bars.length * rowH;
      let ticks = '';
      if (ticksDoc.length) {
        // the document's own timescale ('Mese 1 ... Mese 24', or calendar months): labels thinned to stay legible
        const longest = Math.max(...ticksDoc.map(t => String(t.label).length));
        const every = Math.max(1, Math.ceil(ticksDoc.length * (longest * 5.6 + 6) / chartW));   // ~5.6 px per character at 10px
        ticksDoc.forEach((t, i) => {
          if (i % every !== 0) return;               // grid and label only where a heading is printed
          ticks += `<line x1="${x(t.start)}" y1="${head - 4}" x2="${x(t.start)}" y2="${bottom}" class="g-grid"/>`;
          ticks += `<text x="${(x(t.start) + x(t.end)) / 2}" y="${head - 8}" class="g-tick" text-anchor="middle">${esc(t.label)}</text>`;
        });
      } else {
        const span = Math.ceil(hi - lo);
        const step = span <= 12 ? 1 : span <= 24 ? 2 : span <= 48 ? 6 : 12;
        for (let m = Math.ceil(lo); m <= hi; m += step) {
          ticks += `<line x1="${x(m)}" y1="${head - 4}" x2="${x(m)}" y2="${bottom}" class="g-grid"/>`
            + `<text x="${x(m)}" y="${head - 8}" class="g-tick" text-anchor="middle">${m}</text>`;
        }
      }
      const rows = bars.map((b, i) => {
        const y = head + i * rowH, w = Math.max(3, x(b.end) - x(b.start));
        if (b.separator) {
          return `<line x1="8" y1="${y + 4}" x2="${x0 + chartW}" y2="${y + 4}" class="g-sep"/>`
            + `<text x="8" y="${y + 16}" class="g-label g-strong">${esc(b.name)}</text>`;
        }
        const lvl = Math.min(3, b.level || 0), ind = lvl * 10;
        const max = 34 - Math.round(ind / 6);
        const name = b.name.length > max ? b.name.slice(0, max - 1) + '…' : b.name;
        const when = b.dates ? b.dates.map(v => v.split('-').reverse().join('/')).join(' – ') : `${unit} ${+b.start.toFixed(1)}–${+b.end.toFixed(1)}`;
        const bar = b.summary
          ? `<rect x="${x(b.start)}" y="${y + 8}" width="${w}" height="7" class="g-sum"/>`
            + `<path d="M${x(b.start)} ${y + 15}l0 4l4 -4zM${x(b.start) + w} ${y + 15}l0 4l-4 -4z" class="g-sum"/>`
          : `<rect x="${x(b.start)}" y="${y + 5}" width="${w}" height="13" rx="3" class="g-bar"/>`;
        return `<g><title>${esc(b.name)}${b.duration ? ' · ' + esc(b.duration) : ''}: ${esc(when)}</title>`
          + `<text x="${8 + ind}" y="${y + 15}" class="g-label${b.summary ? ' g-strong' : ''}">${esc(name)}</text>`
          + (hasDur ? `<text x="${x0 - 6}" y="${y + 15}" class="g-val" text-anchor="end">${esc(b.duration || '')}</text>` : '')
          + bar + '</g>';
      }).join('');
      chart = `<div class="g-wrap"><svg viewBox="0 0 ${x0 + chartW + 12} ${bottom + 6}" role="img" aria-label="Diagramma di Gantt">
        <text x="8" y="${head - 8}" class="g-tick">Attività</text>
        ${hasDur ? `<text x="${x0 - 6}" y="${head - 8}" class="g-tick" text-anchor="end">Durata</text>` : ''}
        ${caption ? `<text x="${x0 + chartW / 2}" y="10" class="g-tick g-cap" text-anchor="middle">${esc(caption)}</text>` : ''}
        ${ticks}${rows}</svg></div>
        <div class="sc-fine">Fasi lette dal cronoprogramma${g.metodo ? ` (${esc(g.metodo)})` : ''}${real[0] && real[0].dates ? ` (avvio previsto ${esc(real.filter(b => b.dates).map(b => b.dates[0]).sort()[0].split('-').reverse().join('/'))})` : ''} ${src(g.source, 'apri')}</div>`;
    }
    const pd = !bars.length && (g.durate_fasi || []).length
      ? `<table class="g-dt"><thead><tr><th>Fase</th><th>Durata</th></tr></thead><tbody>${g.durate_fasi.map(p =>
          `<tr><td>${esc(p.name)}</td><td>${esc(p.duration)}</td></tr>`).join('')}</tbody></table>
         <div class="sc-fine">Fasi e durate dal cronoprogramma ${src(g.source, 'apri')}</div>` : '';
    const d = g.duration;
    const dur = d ? `<div class="g-dur"><span>Durata cantiere</span> <strong>${esc(d.text)}</strong> ${flag(d.status)} ${src(d.source)}
      ${d.hint ? `<div class="sc-fine">${esc(d.hint)}</div>` : ''}</div>` : '';
    const ms = (g.milestones || []).map(m =>
      `<li><span class="g-date">${esc(m.date)}</span> ${esc(m.label)} <span class="sc-fine">${esc(m.nature)}</span> ${src(m.source)}</li>`).join('');
    const doc = !bars.length && g.document ? `<div class="sc-fine">Cronoprogramma: ${src(g.document, g.document.document)}</div>` : '';
    const alt = (g.altri || []).length ? `<div class="sc-fine">Altri cronoprogrammi negli elaborati: ${g.altri.map(o => src(o, o.document)).join(' · ')}</div>` : '';
    if (!chart && !pd && !dur && !ms && !doc) return '';
    return `<section class="sc-sec"><h3>Cronoprogramma</h3>${chart}${pd}${dur}
      ${!bars.length && g.note ? `<p class="sc-note">${esc(g.note)}</p>` : ''}${doc}
      ${ms ? `<ul class="g-ms">${ms}</ul>` : ''}${alt}</section>`;
  }

  function location(l, href) {
    if (!l) return '';
    const rows = [];
    if (l.regione) rows.push(['Regione', esc(l.regione)]);
    if (l.provincia) rows.push(['Provincia', esc(l.provincia)]);
    if ((l.comuni || []).length) rows.push([l.comuni.length > 1 ? 'Comuni' : 'Comune', esc(l.comuni.join(', '))]);
    if (l.coordinate) rows.push(['Coordinate', `${l.coordinate.lat.toFixed(5)} N, ${l.coordinate.lon.toFixed(5)} E
      <div class="sc-fine">${esc(l.coordinate.tipo)} ${src(l.coordinate.source)}</div>`]);
    if ((l.lotti || []).length) {
      rows.push([`Lotti (${l.lotti.length})`, `<ul class="sc-lots">${l.lotti.map(x => {
        const m = safeHref(x.maps_url);
        return `<li><strong>${esc(x.lotto)}</strong> <span class="sc-coord">${x.lat.toFixed(5)} N, ${x.lon.toFixed(5)} E</span>
          ${m ? `<a class="sc-src" href="${esc(m)}" target="_blank" rel="noopener noreferrer">mappa</a>` : ''} ${src(x.source)}</li>`;
      }).join('')}</ul>`]);
    }
    if (l.connessione) {
      const cm = safeHref(l.connessione.maps_url);
      rows.push(['Connessione', `${l.connessione.lat.toFixed(5)} N, ${l.connessione.lon.toFixed(5)} E
        <div class="sc-fine">stazione elettrica / punto di connessione ${src(l.connessione.source)}
        ${cm ? ` · <a class="sc-src" href="${esc(cm)}" target="_blank" rel="noopener noreferrer">mappa</a>` : ''}</div>`]);
    }
    const maps = safeHref(l.maps_url);
    return `<section class="sc-sec"><h3>Ubicazione</h3><dl class="sc-dl">${rows.map(r => `<dt>${r[0]}</dt><dd>${r[1]}</dd>`).join('')}</dl>
      ${maps ? `<a class="sc-link" href="${esc(maps)}" target="_blank" rel="noopener noreferrer">Apri in Google Maps${l.maps_precision === 'comune' ? ' (comune)' : ''}</a>` : ''}
      ${safeHref(l.mappa_ufficiale) ? `<a class="sc-link" href="${esc(safeHref(l.mappa_ufficiale))}" target="_blank" rel="noopener noreferrer">Mappa ufficiale del procedimento</a>` : ''}</section>`;
  }

  function timing(t) {
    if (!t) return '';
    const rows = t.righe || [];
    const lv = { documentata: 'Documentata', prescritta: 'Prescritta', stimata: 'Stima' };
    const order = { documentata: 0, prescritta: 1, stimata: 2 };
    const sorted = rows.slice().sort((a, b) => order[a.livello] - order[b.livello]);
    const body = sorted.length ? `<table class="sc-tt"><tbody>${sorted.map(r => `<tr class="lv-${esc(r.livello)}">
        <td><span class="sc-lv lv-${esc(r.livello)}">${esc(lv[r.livello] || r.livello)}</span></td>
        <td><strong>${esc(r.voce)}</strong>${r.dettaglio ? `<div class="sc-fine">${esc(r.dettaglio)}</div>` : ''}</td>
        <td class="sc-tv">${esc(r.valore)} ${src(r.source)}</td></tr>`).join('')}</tbody></table>` : '';
    const notes = (t.note || []).map(n => `<p class="sc-fine">${esc(n)}</p>`).join('');
    const legend = `<p class="sc-fine sc-leg"><span class="sc-lv lv-documentata">Documentata</span> scritta nei documenti ·
      <span class="sc-lv lv-prescritta">Prescritta</span> termine dell'atto dalla sua data ·
      <span class="sc-lv lv-stimata">Stima</span> intervallo calcolato, non una data certa</p>`;
    return `<section class="sc-sec"><h3>Tempistica dei lavori</h3>${body}${notes}${rows.length ? legend : ''}</section>`;
  }
  function execution(x) {
    if (!x || !(x.imprese || []).length) return '';
    const lv = { A: ['documentata', 'Atto di cantiere'], B: ['prescritta', 'Da elaborati'] };
    const rows = x.imprese.map(r => `<tr>
        <td><span class="sc-lv lv-${lv[r.evidenza][0]}">${esc(lv[r.evidenza][1])}</span></td>
        <td><strong>${esc(r.impresa)}</strong><div class="sc-fine">${esc(r.ruolo)} · ${esc(r.ambito)}${r.partita_iva ? ' · P. IVA ' + esc(r.partita_iva) : ''}</div></td>
        <td class="sc-tv">${src(r.source)}</td></tr>`).join('');
    const leg = x.legenda || {};
    return `<section class="sc-sec"><h3>Chi costruisce</h3><table class="sc-tt"><tbody>${rows}</tbody></table>
      <p class="sc-fine sc-leg"><span class="sc-lv lv-documentata">Atto di cantiere</span> ${esc(leg.A || '')} ·
      <span class="sc-lv lv-prescritta">Da elaborati</span> ${esc(leg.B || '')}</p></section>`;
  }
  function readiness(key) {
    const x = (window.PVReady || {})[key];
    if (!x) return '';
    const lv = x.level ? `<span class="sc-lv ${x.level === 'Alta' ? 'lv-documentata' : x.level === 'Media' ? 'lv-prescritta' : 'lv-stimata'}">Priorità ${esc(x.level)}</span> ` : '';
    const reasons = (x.reasons || []).map(r => `<li>${esc(r)}</li>`).join('');
    return `<section class="sc-sec"><h3>Vicini al cantiere</h3>
      <p>${lv}<strong>${esc(x.phase_label)}</strong>${x.window ? ' · ' + esc(x.window) : ''}</p>
      ${reasons ? `<ul class="sc-open">${reasons}</ul>` : ''}
      <p class="sc-fine">Sintesi dei segnali raccolti (atti, date, incentivo FER X, imprese, stampa): un'indicazione, non una certezza.
      Le date stimate restano stime.</p></section>`;
  }
  function incentive(key) {
    const x = ((window.PVFerx || {})[key] || [])[0];
    if (!x) return '';
    const mw = v => (v / 1000).toLocaleString('it-IT', { maximumFractionDigits: 3 }) + ' MW';
    const quota = x.kw_ammessi && x.kw && x.kw_ammessi < x.kw ? ` · quota ammessa ${mw(x.kw_ammessi)}` : '';
    return `<section class="sc-sec"><h3>Incentivo FER X assegnato</h3><dl class="sc-dl">
      <dt>Graduatoria</dt><dd>${esc(x.procedura)} · posizione ${esc(x.posizione)} · codice ${esc(x.codice)}</dd>
      <dt>Soggetto</dt><dd>${esc(x.ragione_sociale)} · P. IVA ${esc(x.piva)}</dd>
      <dt>Impianto</dt><dd>${esc(x.comune)} (${esc(x.provincia)}) · ${mw(x.kw)}${quota}</dd>
      <dt>Abbinamento</dt><dd>${esc(x.basis)} · <a class="sc-src" href="https://www.gse.it/servizi-per-te/fonti-rinnovabili/ferx/graduatorie" target="_blank" rel="noopener noreferrer">graduatoria GSE</a></dd></dl>
      <p class="sc-fine">Impianto ammesso in posizione utile (Tabella A): tariffa garantita e obbligo di entrata in esercizio
      entro i termini del bando. È il segnale pubblico più forte che l'impianto verrà costruito.</p></section>`;
  }
  function press(key) {
    const list = ((window.PVNews || {})[key] || []).slice(0, 6);
    if (!list.length) return '';
    const rows = list.map(n => `<li><a class="sc-src" href="${esc(safeHref(n.link))}" target="_blank" rel="noopener noreferrer">${esc(n.title)}</a>
      <div class="sc-fine">${esc(n.source || '')}${n.date ? ' · ' + esc(n.date.split('-').reverse().join('/')) : ''} · abbinata per ${esc(n.basis)}${(n.builders || []).length ? ' · imprese citate: ' + esc(n.builders.join(', ')) : ''}</div></li>`).join('');
    return `<section class="sc-sec"><h3>Notizie di stampa compatibili</h3><ul class="sc-open">${rows}</ul>
      <p class="sc-fine sc-leg"><span class="sc-lv lv-stimata">Da verificare</span> articolo che cita comune e potenza (o proponente) del progetto:
      possibile riferimento allo stesso impianto, non una conferma dell'EPC.</p></section>`;
  }
  function render(card) {
    const l = card.ubicazione || {};
    const withProv = (l.comuni || []).some(c => /\(/.test(c));
    const place = [(l.comuni || []).join(', ') + (l.provincia && !withProv ? ` (${l.provincia})` : ''), l.regione].filter(Boolean).join(' · ');
    const href = safeHref(card.source_url), maps = safeHref(l.maps_url);
    const doc = card.documentazione || {};
    const open = (card.da_verificare || []).length
      ? `<section class="sc-sec"><h3>Da verificare</h3><ul class="sc-open">${card.da_verificare.map(o =>
        `<li><strong>${esc(o.label)}</strong>: ${esc(o.reason)}</li>`).join('')}</ul></section>` : '';
    return `
      <div class="sc-top">
        <div class="sc-eyebrow">Scheda progetto</div>
        <h2 id="scTitle">${esc(card.title)}</h2>
        ${place ? `<div class="sc-place">${esc(place)}</div>` : ''}
        ${card.stato ? `<div class="sc-status st-${esc(card.stato.level)}"><strong>${esc(card.stato.label)}</strong><span>${esc(card.stato.reason)}</span></div>` : ''}
        ${card.registro && card.registro.fonte ? `<div class="sc-note">Fonte registro: <a class="sc-src" href="${esc(card.registro.fonte)}" target="_blank" rel="noopener">scheda impianto</a></div>` : ''}
        <div class="sc-actions">
          ${href ? `<a class="sc-link" href="${esc(href)}" target="_blank" rel="noopener noreferrer">Procedimento ufficiale</a>` : ''}
          ${maps ? `<a class="sc-link" href="${esc(maps)}" target="_blank" rel="noopener noreferrer">Google Maps</a>` : ''}
        </div>
      </div>
      ${tiles(card.sintesi)}
      <div class="sc-body">${readiness(card.project_key)}${incentive(card.project_key)}${execution(card.esecuzione)}${press(card.project_key)}${timing(card.tempistica)}${subjects(card)}${gantt(card.cronoprogramma)}${location(l)}${open}
        <p class="sc-warn">${esc(doc.documenti)} documenti, ${esc(doc.pagine)} pagine analizzati${doc.da_leggere ? ` (lettura in corso: altri ${esc(doc.da_leggere)} documenti nelle prossime notti)` : (doc.completa ? '' : ' (lettura incompleta)')};
          aggiornata il ${esc((card.updated || '').split('-').reverse().join('/'))}. ${esc(card.avvertenza)}</p>
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
    panel.addEventListener('click', e => {
      const b = e.target.closest('.sc-copy');
      if (!b) return;
      const done = () => { b.textContent = 'Copiato'; setTimeout(() => { b.textContent = 'Copia'; }, 1400); };
      const val = b.dataset.copy;
      try {
        navigator.clipboard.writeText(val).then(done, () => select(b));
      } catch (err) { select(b); }
    });
    document.addEventListener('keydown', e => { if (e.key === 'Escape' && !panel.hidden) close(); });
  }
  function select(b) {
    const v = b.previousElementSibling;
    const r = document.createRange();
    r.selectNodeContents(v);
    const s = window.getSelection();
    s.removeAllRanges();
    s.addRange(r);
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
