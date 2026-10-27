/* v0.8 – GSE FER X wind ranking (PC_EOL_2025_01) against the wind radar.
   Data: ferx/wind_matches.json on the doc-intel-state branch (written by doc-intel-ferx). Information only. */
(function () {
  const SRC = "https://raw.githubusercontent.com/EmAnzi3/pv_agent_mvp/doc-intel-state/ferx/wind_matches.json";
  const esc = s => String(s == null ? "" : s).replace(/[&<>"]/g, c => ({"&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;"}[c]));
  const mw = kw => (Number(kw || 0) / 1000).toLocaleString("it-IT", {maximumFractionDigits: 2});
  function render(d) {
    const rows = d.impianti || [], el = document.getElementById("ferxWindBody");
    const inR = rows.filter(r => r.progetto), out = rows.filter(r => !r.progetto);
    const tot = a => mw(a.reduce((s, r) => s + (r.kw || 0), 0));
    document.getElementById("ferxWindSummary").innerHTML =
      `<span><b>${rows.length}</b> impianti ammessi · ${tot(rows)} MW</span>` +
      `<span><b>${inR.length}</b> già nel radar · ${tot(inR)} MW</span>` +
      `<span><b>${out.length}</b> non presenti nel radar · ${tot(out)} MW</span>`;
    el.innerHTML = rows.map(r => `<tr class="${r.progetto ? "in" : "out"}">
      <td>${esc(r.posizione)}</td><td>${esc(r.ragione_sociale)}</td>
      <td>${esc(r.comune)}${r.provincia ? " (" + esc(r.provincia) + ")" : ""}</td>
      <td class="num">${mw(r.kw)}</td><td class="num">${mw(r.kw_ammessi)}</td>
      <td>${r.progetto ? `<b>${esc(r.progetto.nome)}</b><small>${esc(r.progetto.fase || "")} · ${esc(r.progetto.criterio || "")}</small>`
                       : `<span class="ferx-new">Non nel radar</span>`}</td></tr>`).join("");
    document.getElementById("ferxWindNote").innerHTML =
      `${esc(d.procedura)}, graduatoria pubblicata il ${esc(d.pubblicata)} (<a href="${esc(d.url)}" target="_blank" rel="noopener">GSE</a>). ` +
      `Abbinamento al radar solo con stesso comune, stessa società e potenza compatibile; aggiornato ${esc(d.updated)}.`;
  }
  function mount() {
    const anchor = document.querySelector("section.method-strip");
    if (!anchor || document.getElementById("ferxWindPanel")) return;
    const sec = document.createElement("section");
    sec.className = "panel ferx-wind-panel"; sec.id = "ferxWindPanel";
    sec.innerHTML = `<div class="panel-heading"><div><h2>Graduatoria FER X eolico (GSE)</h2>
      <p>Impianti eolici con incentivo FER X assegnato: quali sono già nel radar e quali no.</p></div></div>
      <div class="ferx-wind-summary" id="ferxWindSummary">Caricamento…</div>
      <div class="ferx-wind-scroll"><table class="ferx-wind-table"><thead><tr><th>Pos.</th><th>Società</th><th>Comune</th>
      <th class="num">MW</th><th class="num">MW ammessi</th><th>Nel radar</th></tr></thead><tbody id="ferxWindBody"></tbody></table></div>
      <p class="ferx-wind-note" id="ferxWindNote"></p>`;
    anchor.parentNode.insertBefore(sec, anchor);
    const st = document.createElement("style");
    st.textContent = `.ferx-wind-summary{display:flex;flex-wrap:wrap;gap:8px 18px;font-size:12.5px;margin-bottom:10px;color:var(--muted)}
      .ferx-wind-summary b{color:var(--text);font-size:15px}.ferx-wind-scroll{overflow-x:auto}
      .ferx-wind-table{width:100%;border-collapse:collapse;font-size:12px;min-width:640px}
      .ferx-wind-table th{text-align:left;font-size:10px;text-transform:uppercase;letter-spacing:.05em;color:var(--muted);border-bottom:1px solid var(--border);padding:6px 8px}
      .ferx-wind-table td{border-bottom:1px solid var(--border);padding:6px 8px;vertical-align:top}
      .ferx-wind-table .num{text-align:right;font-variant-numeric:tabular-nums}
      .ferx-wind-table tr.in td{background:var(--green-soft)}.ferx-wind-table small{display:block;color:var(--muted);font-size:10.5px}
      .ferx-new{color:var(--amber);font-weight:700}.ferx-wind-note{font-size:11px;color:var(--muted);margin:8px 0 0}`;
    document.head.appendChild(st);
    fetch(SRC, {cache: "no-store"}).then(r => r.ok ? r.json() : Promise.reject(r.status)).then(render)
      .catch(() => { document.getElementById("ferxWindSummary").textContent = "Graduatoria non disponibile al momento."; });
  }
  if (document.readyState === "loading") document.addEventListener("DOMContentLoaded", mount); else mount();
})();
