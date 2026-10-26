(() => {
  'use strict';

  const escapeHtml = value => String(value ?? '').replace(/[&<>"']/g, char => ({
    '&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#039;'
  }[char]));

  const fmtNumber = value => new Intl.NumberFormat('it-IT', {maximumFractionDigits: 2}).format(Number(value || 0));
  const fmtDateTime = value => {
    if (!value) return 'n.d.';
    const date = new Date(value);
    if (Number.isNaN(date.getTime())) return String(value);
    return new Intl.DateTimeFormat('it-IT', {
      day:'2-digit', month:'2-digit', year:'numeric', hour:'2-digit', minute:'2-digit'
    }).format(date);
  };

  function metric(label, value, note='') {
    return '<div class="local-run-metric"><span>' + escapeHtml(label) + '</span><b>' +
      escapeHtml(value) + '</b>' + (note ? '<small>' + escapeHtml(note) + '</small>' : '') + '</div>';
  }

  async function renderLocalRun() {
    try {
      const response = await fetch('data/local-run-status.json?ts=' + Date.now(), {cache:'no-store'});
      if (!response.ok) return;
      const data = await response.json();
      const canonical = data.canonical || {};
      const institutional = data.institutional || {};
      const company = data.company || {};
      const digest = data.digest || {};
      const queue = data.execution_queue || {};
      const errors = Number(institutional.errors || 0) + Number(company.errors || 0);

      const section = document.createElement('section');
      section.className = 'panel local-run-panel ' + (errors ? 'has-warning' : 'is-ok');
      section.setAttribute('aria-label', 'Ultimo aggiornamento locale Wind Radar');
      section.innerHTML =
        '<div class="local-run-head">' +
          '<div><div class="eyebrow dark">Aggiornamento locale</div><h2>Ultimo run del BAT</h2>' +
          '<p>' + escapeHtml(fmtDateTime(data.generated_at)) + ' · modalità ' + escapeHtml(data.mode || 'due') +
          ' · ' + escapeHtml(data.duration_seconds ?? 0) + ' s</p></div>' +
          '<span class="local-run-state">' + (errors ? 'Completato con avvisi' : 'Aggiornamento OK') + '</span>' +
        '</div>' +
        '<div class="local-run-grid">' +
          metric('Canonico', (canonical.projects ?? 0) + ' progetti', fmtNumber(canonical.wind_mw) + ' MW wind') +
          metric('Fonti istituzionali', (institutional.executed ?? 0) + ' eseguite', (institutional.new_or_changed ?? 0) + ' nuove/modificate') +
          metric('Company watch', (company.executed ?? 0) + ' player', (company.new_or_changed ?? 0) + ' nuove/modificate') +
          metric('Da revisionare', digest.actionable_events ?? 0, (digest.events ?? 0) + ' eventi valutati') +
          metric('Execution queue', queue.projects ?? 0, (queue.open_scope_count ?? 0) + ' scope aperti') +
          metric('Errori fonti', errors, errors ? 'il canonico resta invariato' : 'nessun errore registrato') +
        '</div>' +
        '<div class="local-run-guard">' + escapeHtml(data.guard || '') + '</div>';

      const anchor = document.querySelector('.portfolio-summary') || document.querySelector('.kpi-grid');
      if (anchor) anchor.insertAdjacentElement('afterend', section);
    } catch (error) {
      console.debug('Wind local-run status unavailable:', error);
    }
  }

  if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', renderLocalRun, {once:true});
  } else {
    renderLocalRun();
  }
})();
