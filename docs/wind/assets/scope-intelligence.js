(()=>{'use strict';
const $=s=>document.querySelector(s), all=s=>[...document.querySelectorAll(s)];
const esc=v=>String(v??'').replace(/[&<>"']/g,m=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#039;'}[m]));
const fmt=v=>new Intl.NumberFormat('it-IT',{maximumFractionDigits:1}).format(+v||0);
let baseMeta={},overlay={},auxOverlays=[],projects=[],byId=new Map(),contractorChosen='',contractorView=null,contractorSummary=null,contractorSelect=null,renderLock=false;
async function load(){
  const manifest=await fetch('data/projects.json',{cache:'no-store'}).then(r=>r.json());
  const [meta,...chunks]=await Promise.all([fetch('data/'+manifest.meta,{cache:'no-store'}).then(r=>r.json()),...manifest.chunks.map(x=>fetch('data/'+x,{cache:'no-store'}).then(r=>r.json()))]);
  baseMeta=meta;
  try{overlay=await fetch('data/enrichment-2026-09-04.json',{cache:'no-store'}).then(r=>r.json())}catch(err){console.warn('scope-intelligence: enrichment non disponibile',err);overlay={projects:{},method:{core_scopes:[],scope_role_map:{}}}}
  auxOverlays=await Promise.all([
    fetch('data/enrichment-docpass2-2026-09-04.json',{cache:'no-store'}).then(r=>r.json()).catch(err=>{console.warn('scope-intelligence: docpass2 non disponibile',err);return{projects:{}}}),
    fetch('data/contractor-leads-2026-09-05.json',{cache:'no-store'}).then(r=>r.json()).catch(err=>{console.warn('scope-intelligence: contractor leads non disponibili',err);return{projects:{}}})
  ]);
  projects=chunks.flat().map(p=>{
    let merged=mergeProject(structuredClone(p),overlay.projects?.[p.id],true);
    auxOverlays.forEach(a=>{merged=mergeProject(merged,a.projects?.[p.id],false)});
    return merged
  });
  byId=new Map(projects.map(p=>[p.id,p]));
}
function mergeProject(p,o,primary=false){
  if(!o)return p;
  if(primary)p._intel=o;
  p.sources=p.sources||[]; p.relations=p.relations||[];
  (o.sources||[]).forEach(s=>{if(!p.sources.some(x=>x.id===s.id))p.sources.push(s)});
  (o.relations||[]).forEach(spec=>{
    const clean={...spec}; delete clean.action; delete clean.match;
    let found=null;
    if(spec.action==='upgrade_or_add'&&spec.match){found=p.relations.find(r=>r.company===spec.match.company&&(!spec.match.role_contains||String(r.role).includes(spec.match.role_contains)))}
    if(found)Object.assign(found,clean); else if(!p.relations.some(r=>r.company===clean.company&&r.role===clean.role&&r.source_id===clean.source_id))p.relations.push(clean);
  });
  return p;
}
function applicableScopes(p){return (overlay.method?.core_scopes||[]).filter(s=>s.applicable==='all'||(s.applicable==='repowering'&&String(p.type).toLowerCase().includes('repowering')))}
function relCovers(r,scope){const map=overlay.method?.scope_role_map?.[scope.id]||[];return map.includes(r.role)}
function confirmed(r){return r.status==='confirmed'&&['A1','A2'].includes(r.confidence)}
function scopeState(p,scope){
  const rels=(p.relations||[]).filter(r=>relCovers(r,scope));
  const yes=rels.filter(confirmed);
  if(yes.length)return{status:'covered',rels:yes};
  const hints=(p.relations||[]).filter(r=>r.scope_hint===scope.id&&['B','C'].includes(r.confidence));
  if(hints.length)return{status:'signal',rels:hints};
  const weak=rels.filter(r=>!confirmed(r));
  if(weak.length)return{status:'signal',rels:weak};
  return{status:'open',rels:[]};
}
function projectCoverage(p){const scopes=applicableScopes(p),states=scopes.map(s=>({scope:s,state:scopeState(p,s)}));return{covered:states.filter(x=>x.state.status==='covered').length,total:states.length,states}}
function visibleProjects(){const rows=all('#opportunityRows [data-project-id]');if(rows.length){const ids=new Set(rows.map(x=>x.dataset.projectId));return projects.filter(p=>ids.has(p.id))}const txt=$('#filterCount')?.textContent||'';return /^0\s+progetti/i.test(txt)?[]:projects}
function refreshKpis(){const cards=all('#kpis .kpi');if(cards.length<6||!projects.length)return;const ps=visibleProjects();let coveredSlots=0,totalSlots=0;const withAny=ps.filter(p=>{const c=projectCoverage(p);coveredSlots+=c.covered;totalSlots+=c.total;return c.covered>0});const mwAny=withAny.reduce((s,p)=>s+(+p.mw||0),0),pct=totalSlots?coveredSlots/totalSlots*100:0;
  cards[4].querySelector('.label').textContent='MW con ≥1 scope esecutivo';cards[4].querySelector('strong').textContent=fmt(mwAny)+' MW';cards[4].querySelector('small').textContent='almeno uno scope A1/A2; non implica BoP completo';cards[4].classList.add('positive');
  cards[5].querySelector('.label').textContent='Scope esecutivi coperti';cards[5].querySelector('strong').textContent=`${coveredSlots} / ${totalSlots}`;cards[5].querySelector('small').textContent=`${fmt(pct)}% · A1/A2 sui core scope applicabili`;cards[5].classList.remove('alert');
}
function decorateOpportunities(){all('#opportunityRows [data-project-id]').forEach(row=>{const p=byId.get(row.dataset.projectId);if(!p)return;const c=projectCoverage(p),gap=row.querySelector('.gap-list');if(gap&&!gap.querySelector('.scope-coverage-chip'))gap.insertAdjacentHTML('beforeend',`<span class="scope-coverage-chip ${c.covered?'has':'open'}">${c.covered}/${c.total} scope A1/A2</span>`)})}
function completeness(p){const checks=[!!p.name,Number.isFinite(+p.mw),!!(p.region&&p.province),!!p.developer,!!p.stage,Number.isFinite(+p.wtg),!!(p.timing||[]).length,!!(p.sources||[]).some(s=>['A1','A2'].includes(s.grade))];const n=checks.filter(Boolean).length;return{n,total:checks.length,pct:Math.round(n/checks.length*100)}}
function windowClass(code){return String(code||'').toLowerCase().replace(/[^a-z]/g,'')}
function relationLine(r){return `<div class="intel-rel"><div><b>${esc(r.company)}</b><span>${esc(r.role)}</span></div><span class="confidence ${esc(r.confidence)}">${esc(r.confidence)}</span></div>`}
function injectDrawer(){const body=$('#detailBody'),title=$('#detailTitle');if(!body||!title||body.querySelector('.scope-intel-section'))return;const p=projects.find(x=>x.name===title.textContent.trim());if(!p)return;const intel=p._intel||{},cov=projectCoverage(p),comp=completeness(p),win=intel.commercial_window||{};
  const scopeHtml=cov.states.map(({scope,state})=>{let detail='OPEN';if(state.status==='covered')detail=state.rels.map(r=>`${esc(r.company)} ${esc(r.confidence)}`).join(' · ');if(state.status==='signal')detail='SIGNAL '+state.rels.map(r=>`${esc(r.company)} ${esc(r.confidence)}`).join(' · ');return `<div class="scope-item ${state.status}"><span>${esc(scope.label)}</span><b>${detail}</b></div>`}).join('');
  const inv=(intel.investigation||[]).map(x=>`<li>${esc(x)}</li>`).join('')||'<li>Nessuna azione investigativa specifica censita.</li>';
  const docs=(intel.documents||[]).map(d=>`<div class="doc-intel"><div><b>${esc(d.title)}</b><span>${esc(d.note||'')}</span></div><span class="doc-status ${esc(d.status)}">${esc(d.status)}</span></div>`).join('');
  const primaryRels=(intel.relations||[]).map(spec=>{const r=(p.relations||[]).find(x=>x.company===spec.company&&x.source_id===spec.source_id&&x.role===spec.role)||spec;return relationLine(r)});
  const auxSpecs=auxOverlays.flatMap(a=>a.projects?.[p.id]?.relations||[]);
  const auxRels=auxSpecs.map(spec=>{const r=(p.relations||[]).find(x=>x.company===spec.company&&x.source_id===spec.source_id&&x.role===spec.role)||spec;return relationLine(r)});
  const overlayRels=[...primaryRels,...auxRels].join('');
  const extraInv=auxOverlays.flatMap(a=>a.projects?.[p.id]?.investigation||[]);
  const allInv=[...(intel.investigation||[]),...extraInv].filter((x,i,a)=>a.indexOf(x)===i).map(x=>`<li>${esc(x)}</li>`).join('')||'<li>Nessuna azione investigativa specifica censita.</li>';
  const html=`<section class="detail-section scope-intel-section"><h3>Commercial & scope intelligence</h3><div class="intel-head"><span class="commercial-window ${windowClass(win.code)}">${esc(win.code||'N.D.')}</span><span class="intel-score">Completezza intelligence ${comp.n}/${comp.total} · ${comp.pct}%</span><span class="intel-score">Scope A1/A2 ${cov.covered}/${cov.total}</span></div>${win.reason?`<div class="detail-note">${esc(win.reason)}</div>`:''}<div class="scope-grid">${scopeHtml}</div>${overlayRels?`<h4>Nuove evidenze / segnali</h4><div class="intel-rel-list">${overlayRels}</div>`:''}<h4>Investigation queue</h4><ul class="investigation-list">${allInv}</ul>${docs?`<h4>Document intelligence</h4><div class="doc-intel-list">${docs}</div>`:''}</section>`;
  const first=body.querySelector('.detail-section');if(first)first.insertAdjacentHTML('beforebegin',html);else body.insertAdjacentHTML('beforeend',html)
}
function waitFor(sel,ms=2500){return new Promise(resolve=>{const hit=$(sel);if(hit)return resolve(hit);const start=Date.now(),t=setInterval(()=>{const el=$(sel);if(el||Date.now()-start>ms){clearInterval(t);resolve(el)}},40)})}
function contractorNames(ps){return [...new Set(ps.flatMap(p=>(p.relations||[]).map(r=>r.company)).filter(Boolean))].sort((a,b)=>a.localeCompare(b,'it',{sensitivity:'base'}))}
function takeoverContractor(){const panel=$('.contractor-panel');if(!panel||renderLock)return false;if($('#contractorFocusV03')){renderContractor();return true}const oldLabel=panel.querySelector('.contractor-search'),oldView=$('#contractorView'),oldSum=$('#contractorSummary');if(!oldLabel||!oldView||!oldSum)return false;renderLock=true;
  const label=document.createElement('label');label.className='contractor-search contractor-search-v03';label.innerHTML='<span>Approfondisci azienda</span><select id="contractorFocusV03" aria-label="Approfondisci azienda"></select>';oldLabel.replaceWith(label);contractorSelect=label.querySelector('select');
  contractorSummary=document.createElement('div');contractorSummary.className='contractor-summary';contractorSummary.id='contractorSummary';oldSum.replaceWith(contractorSummary);
  contractorView=document.createElement('div');contractorView.className='contractor-view contractor-view-v03';contractorView.id='contractorView';oldView.replaceWith(contractorView);
  contractorSelect.addEventListener('change',()=>{contractorChosen=contractorSelect.value;renderContractor()});renderLock=false;renderContractor();return true
}
function renderContractor(){if(!contractorView||!contractorSummary||!contractorSelect)return;const ps=visibleProjects(),names=contractorNames(ps);if(!contractorChosen||!names.includes(contractorChosen))contractorChosen=names[0]||'';const current=[...contractorSelect.options].map(o=>o.value);if(current.length!==names.length||current.some((v,i)=>v!==names[i]))contractorSelect.replaceChildren(...names.map(n=>new Option(n,n)));contractorSelect.value=contractorChosen;
  if(!contractorChosen){contractorSummary.innerHTML='';contractorView.innerHTML='<div class="contractor-empty">Nessuna azienda disponibile nel filtro.</div>';return}
  const linked=ps.map(p=>({p,rels:(p.relations||[]).filter(r=>r.company===contractorChosen)})).filter(x=>x.rels.length).sort((a,b)=>(b.p.score||0)-(a.p.score||0));const mw=linked.reduce((s,x)=>s+(+x.p.mw||0),0);const strictCompanies=new Set(ps.flatMap(p=>(p.relations||[]).filter(r=>confirmed(r)&&(baseMeta.execution_roles||[]).includes(r.role)).map(r=>r.company)));const linkedCount=new Set(ps.filter(p=>(p.relations||[]).length).map(p=>p.id)).size;
  contractorSummary.innerHTML=`<span class="summary-chip"><b>${names.length}</b> aziende/nodi nel filtro</span><span class="summary-chip"><b>${strictCompanies.size}</b> con scope esecutivo A1/A2</span><span class="summary-chip"><b>${linkedCount}</b> progetti collegati</span>`;
  const roles=[...new Set(linked.flatMap(x=>x.rels.map(r=>r.role)))].sort((a,b)=>a.localeCompare(b,'it')).map(r=>`<span class="role-pill">${esc(r)}</span>`).join('');const rows=linked.map(({p,rels})=>`<div class="contractor-project" data-intel-project="${esc(p.id)}"><div><b>${esc(p.name)}</b><small>${rels.map(r=>`${esc(r.role)} · ${esc(r.status)} · <span class="confidence ${esc(r.confidence)}">${esc(r.confidence)}</span>`).join('<br>')}<br>${esc(p.stage)} · next ${esc(p.next?.label||'n.d.')}</small></div><span class="project-mw">${fmt(p.mw)} MW</span></div>`).join('');contractorView.innerHTML=`<article class="contractor-card direct-contractor-card"><div class="contractor-card-head"><h3>${esc(contractorChosen)}</h3><div class="contractor-metric"><b>${fmt(mw)}</b><span>MW collegati · ${linked.length} progetti</span></div></div><div class="contractor-roles">${roles}</div>${rows||'<div class="contractor-empty">Nessun progetto collegato.</div>'}</article>`;
  contractorView.querySelectorAll('[data-intel-project]').forEach(x=>x.onclick=()=>{const id=x.dataset.intelProject,target=document.querySelector(`#opportunityRows [data-project-id="${CSS.escape(id)}"]`)||document.querySelector(`#mapMarkers [data-id="${CSS.escape(id)}"]`);target?.click()})
}
function schedule(){clearTimeout(schedule.t);schedule.t=setTimeout(()=>{refreshKpis();decorateOpportunities();renderContractor()},80)}
function watch(){const opp=$('#opportunityRows'),body=$('#detailBody');if(opp)new MutationObserver(schedule).observe(opp,{childList:true,subtree:true});if(body)new MutationObserver(injectDrawer).observe(body,{childList:true,subtree:true});['region','stage','type','developer','contractor','window'].forEach(id=>document.getElementById(id)?.addEventListener('change',schedule));['mwMin','mwMax','q'].forEach(id=>document.getElementById(id)?.addEventListener('input',schedule));document.getElementById('resetFilters')?.addEventListener('click',schedule)}
async function init(){try{await load();watch();await waitFor('#contractorFocusDirect');takeoverContractor();refreshKpis();decorateOpportunities();setTimeout(()=>{takeoverContractor();refreshKpis();decorateOpportunities()},700)}catch(err){console.error('scope-intelligence init failed',err)}}
if(document.readyState==='loading')document.addEventListener('DOMContentLoaded',init);else init();
})();
