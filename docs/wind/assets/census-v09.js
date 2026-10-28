/* v0.9 – Censimento eolico: every live wind project on the MASE VIA portal + the FER X wind plants with no
   procedure found yet, read like the PV radar (filters, signals, project cards). Data on the memory branch:
     wind/census.json             census records (wind-census.yml, weekly)
     schede/live_index.json       project cards (wind-cards.yml, nightly)
     readiness/wind_index.json    'vicini al cantiere' (doc-intel-ferx.yml, daily)
   The curated radar above stays the verified layer: nothing here is promoted into it. */
(function () {
  const RAW = "https://raw.githubusercontent.com/EmAnzi3/pv_agent_mvp/doc-intel-state/";
  const PAGE = 60;
  const esc = s => String(s == null ? "" : s).replace(/[&<>"']/g, c => ({"&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;"}[c]));
  const num = (v, d = 1) => v == null || v === "" ? "–" : Number(v).toLocaleString("it-IT", {maximumFractionDigits: d});
  const getJson = u => fetch(u, {cache: "no-cache"}).then(r => r.ok ? r.json() : {}).catch(() => ({}));
  let rows = [], cards = {}, ready = {}, shown = PAGE, sortKey = "ready";

  function shortName(r) {
    if (r.source === "gse_ferx") return `${r.municipalities} – ${r.proponent}`;
    const t = r.title || "";
    const q = t.match(/[«"“]([^»"”]{3,60})[»"”]/) || t.match(/denominat[oa]\s+([A-Z][^,;(]{2,50})/i);
    if (q) return q[1].trim();
    return t.replace(/^(Valutazione|Verifica|Provvedimento|Definizione)[^:]*:\s*/i, "").slice(0, 90) + (t.length > 90 ? "…" : "");
  }
  // a plain reading of the MASE procedure: where the project stands in the environmental assessment
  function phase(r) {
    if (r.source === "gse_ferx") return "Procedura regionale (da individuare)";
    const p = r.procedure || "", st = status(r)[0] || "";
    if (r.source_group === "regione") {
      const t = `${p} ${r.procedure_status || ""} ${r.title || ""}`;
      if (/ottemperanza/i.test(t)) return "Post-VIA: ottemperanza";
      if (/assoggettabilit|screening/i.test(t)) return "Screening";
      if (/conclus|rilasciat|favorevol|positiv|archiviat|emanat|adottat/i.test(r.procedure_status || "")) return "Procedura regionale conclusa";
      return "Procedura regionale in corso";
    }
    if (/ottemperanza/i.test(p)) return "Post-VIA: ottemperanza";
    if (/proroga/i.test(p)) return "Post-VIA: proroga";
    if (/valutazione preliminare|variant|modific/i.test(p)) return "Modifica di progetto";
    if (/definizione (?:dei )?contenuti|scoping/i.test(p)) return "Scoping (pre-VIA)";
    if (/assoggettabilit/i.test(p)) return "Screening";
    if (/conclus/i.test(st)) return "VIA conclusa (esito da verificare)";
    if (/impatto ambientale|provvedimento unico|autorizzazione|via/i.test(p)) return "VIA in istruttoria";
    return "Altra procedura";
  }
  const kind = r => /offshore/i.test(r.project_type || "") ? "Offshore" : "Onshore";
  const status = r => (r.procedure_status || "").split(", ").filter(Boolean);
  const live = r => status(r).some(s => /in corso|istruttoria|avvi/i.test(s));
  const lv = r => (ready[r.project_key] || {}).level;
  const epc = r => (cards[r.project_key] || {}).epc;

  function signals(r) {
    const out = [];
    if (r.curato) out.push(`<span class="wc-b wc-cur" title="Progetto già nel radar curato">Radar · ${esc(r.curato.fase || "")}</span>`);
    if ((r.ferx || []).length) out.push(`<span class="wc-b wc-ferx" title="${esc(r.ferx[0].procedura)}">FER X #${esc(r.ferx[0].posizione)}</span>`);
    const x = ready[r.project_key];
    if (x && x.level) out.push(`<span class="wc-b wc-ready-${x.level === "Alta" ? "a" : x.level === "Media" ? "m" : "b"}" title="${esc(x.phase_label)}${x.window ? " · " + esc(x.window) : ""}">Cantiere ${esc(x.level)}</span>`);
    else if (x && x.phase === "realizzato") out.push(`<span class="wc-b wc-done">Realizzato</span>`);
    if (epc(r)) out.push(`<span class="wc-b wc-epc">EPC ${esc(epc(r))}</span>`);
    if ((r.altre_fonti || []).length) out.push(`<span class="wc-b wc-side" title="${esc(r.altre_fonti.map(a => a.fonte + (a.procedura ? ": " + a.procedura : "")).join(" · "))}">Anche in Regione</span>`);
    if (r.secondaria) out.push(`<span class="wc-b wc-side" title="Modifica, variante, ottemperanza o proroga di un progetto: non conteggiata nei totali">Procedura secondaria</span>`);
    if (r.source === "gse_ferx") out.push(`<span class="wc-b wc-lead" title="Impianto in graduatoria FER X senza procedura VIA nazionale trovata: probabilmente autorizzato in Regione">Procedura da individuare</span>`);
    return out.join(" ");
  }

  function filtered() {
    const q = (document.getElementById("wcQ").value || "").toLowerCase().trim();
    const reg = document.getElementById("wcRegion").value, tipo = document.getElementById("wcKind").value;
    const show = document.getElementById("wcShow").value, side = document.getElementById("wcSide").checked;
    let out = rows.filter(r => {
      if (r.secondaria && !side) return false;
      if (q && !`${r.title} ${r.proponent} ${r.municipalities} ${r.province}`.toLowerCase().includes(q)) return false;
      if (reg && r.region !== reg) return false;
      if (tipo && kind(r) !== tipo) return false;
      const ph = document.getElementById("wcPhase").value, src = document.getElementById("wcSrc").value;
      if (src && (r.source_group || "") !== src) return false;
      if (ph && phase(r) !== ph) return false;
      if (show === "card" && !cards[r.project_key]) return false;
      if (show === "ferx" && !(r.ferx || []).length) return false;
      if (show === "ready" && !["Alta", "Media"].includes(lv(r))) return false;
      if (show === "epc" && !epc(r)) return false;
      if (show === "new" && r.curato) return false;
      if (show === "lead" && r.source !== "gse_ferx") return false;
      if (show === "live" && !live(r)) return false;
      return true;
    });
    const rank = r => ({Alta: 3, Media: 2, Bassa: 1}[lv(r)] || 0);
    if (sortKey === "mw") out.sort((a, b) => (b.power_mw || 0) - (a.power_mw || 0));
    if (sortKey === "ready") out.sort((a, b) => rank(b) - rank(a) || (b.power_mw || 0) - (a.power_mw || 0));
    if (sortKey === "date") out.sort((a, b) => dkey(b) - dkey(a));
    return out;
  }
  function dkey(r) {
    const d = ((r.procedures || [])[0] || {}).avvio || r.date_presented || "";
    const m = d.match(/(\d{2})\/(\d{2})\/(\d{4})/);
    return m ? Number(m[3] + m[2] + m[1]) : 0;
  }

  function render() {
    const list = filtered();
    const mw = list.reduce((s, r) => s + (r.power_mw || 0), 0);
    document.getElementById("wcCount").textContent = `${list.length} progetti · ${num(mw, 0)} MW`;
    document.getElementById("wcBody").innerHTML = list.slice(0, shown).map(r => {
      const card = cards[r.project_key];
      const proc = (r.procedures || [])[0];
      return `<tr>
        <td><b>${esc(shortName(r))}</b><small>${esc(r.proponent || "proponente non indicato")}</small></td>
        <td>${esc(r.municipalities || "–")}${r.province ? ` (${esc(r.province)})` : ""}<small>${esc(r.region || "")} · ${kind(r)}</small></td>
        <td class="num">${num(r.power_mw)}</td>
        <td><b class="wc-ph">${esc(phase(r))}</b><small>${esc(r.procedure || "")}${status(r).length ? " · " + esc(status(r).join(" · ")) : ""}${proc && proc.avvio ? " · avvio " + esc(proc.avvio) : ""}</small></td>
        <td class="wc-sig">${signals(r)}</td>
        <td class="wc-act">${card ? `<button type="button" class="wc-btn" data-wcard="${esc(card.file)}">Scheda</button>` : ""}
          ${r.url ? `<a class="wc-link" href="${esc(r.url)}" target="_blank" rel="noopener">Fonte</a>` : ""}</td></tr>`;
    }).join("") || `<tr><td colspan="6" class="wc-empty">Nessun progetto con questi filtri.</td></tr>`;
    const more = document.getElementById("wcMore");
    more.hidden = list.length <= shown;
    more.textContent = `Mostra altri ${Math.min(PAGE, list.length - shown)} di ${list.length - shown}`;
  }

  function kpis(sum) {
    const main = rows.filter(r => !r.secondaria);
    const withCard = main.filter(r => cards[r.project_key]).length;
    const near = main.filter(r => ["Alta", "Media"].includes(lv(r))).length;
    const items = [
      ["Progetti vivi", num(sum.progetti != null ? sum.progetti : main.length, 0),
       `${num((sum.mw_onshore || 0) / 1000, 1)} GW onshore · ${num((sum.mw_offshore || 0) / 1000, 1)} GW offshore`],
      ["Con scheda", num(withCard, 0), "lettura notturna dei documenti, MW decrescenti"],
      ["Incentivo FER X", num(main.filter(r => (r.ferx || []).length).length, 0), `di cui ${num(sum.lead_ferx, 0)} senza procedura nazionale`],
      ["Vicini al cantiere", num(near, 0), "priorità Alta o Media"],
      ["Già nel radar curato", num(sum.nel_radar_curato, 0), "progetti verificati sopra"],
      ["Dalle Regioni", num(sum.da_regioni || 0, 0), `più ${num(sum.regionali_gia_al_mase || 0, 0)} già presenti al MASE · ${num(sum.archiviati_esclusi, 0)} archiviati esclusi`],
    ];
    document.getElementById("wcKpis").innerHTML = items.map(([l, v, s]) =>
      `<div class="wc-kpi"><span>${esc(l)}</span><strong>${esc(v)}</strong><small>${esc(s)}</small></div>`).join("");
  }

  function exportCsv() {
    const head = ["Progetto", "Proponente", "Comuni", "Provincia", "Regione", "Tipo", "MW", "Fase", "Procedura", "Stato procedure",
                  "Radar curato", "FER X", "Vicini al cantiere", "Fase", "EPC", "Scheda", "Fonte"];
    const q = v => `"${String(v == null ? "" : v).replace(/"/g, '""')}"`;
    const lines = filtered().map(r => [shortName(r) + (r.secondaria ? " (procedura secondaria)" : ""), r.proponent, r.municipalities, r.province, r.region, kind(r),
      r.power_mw, phase(r), r.procedure, r.procedure_status, r.curato ? r.curato.nome : "", (r.ferx || []).map(x => "#" + x.posizione).join(" "),
      lv(r) || "", (ready[r.project_key] || {}).phase_label || "", epc(r) || "", cards[r.project_key] ? "sì" : "", r.url].map(q).join(";"));
    const blob = new Blob(["﻿" + [head.map(q).join(";")].concat(lines).join("\n")], {type: "text/csv;charset=utf-8"});
    const a = document.createElement("a");
    a.href = URL.createObjectURL(blob); a.download = "censimento_eolico.csv"; a.click();
    setTimeout(() => URL.revokeObjectURL(a.href), 2000);
  }

  function mount() {
    const anchor = document.querySelector("section.contractor-panel") || document.querySelector("section.method-strip");
    if (!anchor || document.getElementById("windCensus")) return;
    const sec = document.createElement("section");
    sec.className = "panel wc-panel"; sec.id = "windCensus";
    sec.innerHTML = `<div class="panel-heading"><div><h2>Censimento eolico</h2>
        <p>Tutti i progetti eolici vivi sul portale VIA del MASE (onshore oltre 30 MW e offshore), le procedure eoliche dei portali regionali e gli impianti FER X.
        Letti come il radar fotovoltaico: filtri, segnali e scheda documentale. Il radar curato sopra resta il livello verificato.</p></div>
        <button type="button" class="wc-btn ghost" id="wcCsv">Esporta CSV</button></div>
      <div class="wc-kpis" id="wcKpis">Caricamento…</div>
      <div class="wc-filters">
        <label class="wide"><span>Cerca</span><input id="wcQ" type="search" placeholder="Progetto, proponente, comune…"></label>
        <label><span>Regione</span><select id="wcRegion"><option value="">Tutte</option></select></label>
        <label><span>Fonte</span><select id="wcSrc"><option value="">Tutte</option><option value="mase">MASE (VIA nazionale)</option>
          <option value="regione">Regioni</option><option value="gse">GSE FER X</option></select></label>
        <label><span>Tipo</span><select id="wcKind"><option value="">Tutti</option><option>Onshore</option><option>Offshore</option></select></label>
        <label><span>Fase</span><select id="wcPhase"><option value="">Tutte</option>${["VIA in istruttoria", "VIA conclusa (esito da verificare)",
          "Post-VIA: ottemperanza", "Scoping (pre-VIA)", "Screening", "Procedura regionale in corso", "Procedura regionale conclusa",
          "Procedura regionale (da individuare)"].map(x => `<option>${x}</option>`).join("")}</select></label>
        <label><span>Mostra</span><select id="wcShow"><option value="">Tutti i progetti</option><option value="card">Solo con scheda</option>
          <option value="ferx">Solo con incentivo FER X</option><option value="ready">Solo vicini al cantiere</option>
          <option value="epc">Solo con evidenza EPC</option><option value="live">Solo con procedura in corso</option>
          <option value="new">Non nel radar curato</option><option value="lead">FER X senza procedura nazionale</option></select></label>
        <label class="wc-check"><input id="wcSide" type="checkbox"><span>Includi modifiche e varianti</span></label>
        <label><span>Ordina</span><select id="wcSort"><option value="ready">Priorità cantiere, poi MW</option><option value="mw">MW decrescenti</option><option value="date">Procedura più recente</option></select></label>
      </div>
      <div class="wc-count" id="wcCount"></div>
      <div class="wc-scroll"><table class="wc-table"><thead><tr><th>Progetto</th><th>Luogo</th><th class="num">MW</th><th>Fase e procedura</th><th>Segnali</th><th></th></tr></thead>
        <tbody id="wcBody"></tbody></table></div>
      <button type="button" class="wc-more" id="wcMore" hidden></button>
      <p class="wc-note" id="wcNote"></p>`;
    anchor.parentNode.insertBefore(sec, anchor);
    ["wcQ", "wcRegion", "wcSrc", "wcKind", "wcPhase", "wcShow", "wcSide"].forEach(id => document.getElementById(id).addEventListener("input", () => { shown = PAGE; render(); }));
    document.getElementById("wcSort").addEventListener("change", e => { sortKey = e.target.value; shown = PAGE; render(); });
    document.getElementById("wcMore").addEventListener("click", () => { shown += PAGE; render(); });
    document.getElementById("wcCsv").addEventListener("click", exportCsv);
    sec.addEventListener("click", e => {
      const b = e.target.closest("[data-wcard]");
      if (!b || !window.PVScheda) return;
      getJson(RAW + "schede/" + encodeURIComponent(b.dataset.wcard)).then(c => { if (c && c.project_key) window.PVScheda.open(c); });
    });
    Promise.all([getJson(RAW + "wind/census.json"), getJson(RAW + "schede/live_index.json"), getJson(RAW + "readiness/wind_index.json")])
      .then(([census, idx, rd]) => {
        rows = census.records || [];
        cards = idx || {};
        ready = (rd && rd.projects) || {};
        window.PVReady = Object.assign({}, window.PVReady || {}, ready);
        window.PVFerx = Object.assign({}, window.PVFerx || {}, Object.fromEntries(rows.filter(r => (r.ferx || []).length).map(r => [r.project_key, r.ferx])));
        if (!rows.length) { document.getElementById("wcKpis").textContent = "Censimento non ancora disponibile."; return; }
        const regs = [...new Set(rows.map(r => r.region).filter(Boolean))].sort();
        document.getElementById("wcRegion").insertAdjacentHTML("beforeend", regs.map(r => `<option>${esc(r)}</option>`).join(""));
        kpis(census.summary || {});
        document.getElementById("wcNote").textContent = `Censimento aggiornato il ${(census.updated || "").split("-").reverse().join("/")}. ` +
          "Fonti: portale VIA MASE (tipologie «Impianti eolici onshore/offshore»), portali VIA regionali (Puglia, Basilicata, Campania, Calabria, Sardegna, Sicilia), graduatoria GSE FER X eolico. " +
          "Esclusi i progetti con tutte le procedure archiviate; modifiche, varianti, ottemperanze e proroghe " +
          `(${num(census.summary && census.summary.procedure_secondarie, 0)}) sono nascoste di default e non entrano nei totali. ` +
          "«Conclusa» indica una procedura chiusa con decreto, positivo o negativo: l'esito si legge nella scheda o sul portale. " +
          "Le schede si aggiungono ogni notte, dai progetti più grandi.";
        render();
      });
  }
  if (document.readyState === "loading") document.addEventListener("DOMContentLoaded", mount); else mount();
})();
