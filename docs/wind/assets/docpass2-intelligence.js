(()=>{'use strict';
const $=s=>document.querySelector(s),all=s=>[...document.querySelectorAll(s)];
const esc=v=>String(v??'').replace(/[&<>"']/g,m=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#039;'}[m]));
let overlay={},publicRegister={},localRegister={},industryRegister={},byName=new Map(),body=null,timer=null;
async function load(){
  const manifest=await fetch('data/projects.json',{cache:'no-store'}).then(r=>r.json());
  const chunks=await Promise.all(manifest.chunks.map(x=>fetch('data/'+x,{cache:'no-store'}).then(r=>r.json())));
  byName=new Map(chunks.flat().map(p=>[p.name,p.id]));
  [overlay,publicRegister,localRegister,industryRegister]=await Promise.all([
    fetch('data/enrichment-docpass2-2026-09-04.json',{cache:'no-store'}).then(r=>r.json()),
    fetch('data/public-entity-sources-2026-09-04.json',{cache:'no-store'}).then(r=>r.json()).catch(()=>({projects:{}})),
    fetch('data/local-entity-sources-2026-09-04.json',{cache:'no-store'}).then(r=>r.json()).catch(()=>({projects:{}})),
    fetch('data/industry-press-intelligence-2026-09-05.json',{cache:'no-store'}).then(r=>r.json()).catch(()=>({projects:{}}))
  ]);
}
function docCard(d){return `<div class="doc2-card"><div><b>${esc(d.title)}</b><span>${esc(d.note||'')}</span></div><span class="doc-status ${esc(d.status||'')}">${esc(d.status||'')}</span></div>`}
function relationCard(r){return `<div class="doc2-rel"><div><b>${esc(r.company)}</b><span>${esc(r.role)} · ${esc(r.scope||'')}</span></div><span class="confidence ${esc(r.confidence||'D')}">${esc(r.confidence||'D')}</span></div>`}
function schedule(intel){const s=intel?.schedule_intelligence;if(!s)return'';const items=(s.items||[]).map(x=>`<div class="doc2-schedule-row"><span>${esc(x.label)}</span><b>${esc(x.when)}</b></div>`).join('');return `<h4>Schedule intelligence</h4><div class="doc2-note">${esc(s.note||'')}</div><div class="doc2-schedule">${items}</div>`}
function localTargets(intel){return (intel?.public_sources||[]).map(x=>({entity:x.entity,use:x.use,url:x.url,kind:x.level||'target'}))}
function registerSources(reg){return (reg?.sources||[]).map(x=>({entity:x.entity,title:x.title,use:x.use,url:x.url,kind:x.kind,date:x.date}))}
function publicSources(intel,reg,local){
 const a=[...localTargets(intel),...registerSources(reg),...registerSources(local)];if(!a.length)return'';
 const identity=reg?.identity_status?`<div class="doc2-note"><b>Identity check:</b> ${esc(reg.identity_status)}${reg.note?` · ${esc(reg.note)}`:''}</div>`:'';
 return `<h4>Fonti pubbliche territoriali</h4>${identity}<div class="doc2-public">${a.map(x=>`<div class="doc2-public-row"><span class="doc2-level">${esc(x.kind||'ENTE')}</span><div><b>${esc(x.title||x.entity)}</b>${x.entity&&x.title?`<span>${esc(x.entity)}</span>`:''}${x.date?`<span>${esc(x.date)}</span>`:''}<span>${esc(x.use||'')}</span>${x.url?`<a href="${esc(x.url)}" target="_blank" rel="noopener">Apri fonte</a>`:''}</div></div>`).join('')}</div>`
}
function industryPress(industry){
 if(!industry)return'';
 const signals=(industry.signals||[]).map(x=>`<div class="doc2-public-row"><span class="doc2-level">${esc(x.grade||'B')}</span><div><b>${esc(x.title)}</b>${x.date?`<span>${esc(x.date)}</span>`:''}<span>${esc(x.note||'')}</span></div></div>`).join('');
 const sources=(industry.sources||[]).map(x=>`<div class="doc2-public-row"><span class="doc2-level">${esc(x.grade||'B')}</span><div><b>${esc(x.publisher||'Fonte di settore')}</b><span>${esc(x.title||'')}</span>${x.date?`<span>${esc(x.date)}</span>`:''}${x.url?`<a href="${esc(x.url)}" target="_blank" rel="noopener">Apri fonte</a>`:''}</div></div>`).join('');
 if(!signals&&!sources)return'';
 return `<h4>Industry & market intelligence</h4><div class="doc2-note">Stampa di settore e market intelligence: utile per lead, timing, financing e collisioni; non chiude scope esecutivi senza evidenza A1/A2 di ruolo.</div>${signals?`<div class="doc2-public">${signals}</div>`:''}${sources?`<div class="doc2-public">${sources}</div>`:''}`
}
function inject(){
 if(!body)return;const old=body.querySelector('.docpass2-section');if(old)old.remove();
 const name=$('#detailTitle')?.textContent.trim(),id=byName.get(name),intel=id?overlay.projects?.[id]:null,reg=id?publicRegister.projects?.[id]:null,local=id?localRegister.projects?.[id]:null,industry=id?industryRegister.projects?.[id]:null;if(!intel&&!reg&&!local&&!industry)return;
 const docs=(intel?.documents||[]).map(docCard).join(''),rels=(intel?.relations||[]).map(relationCard).join('');
 const html=`<section class="detail-section docpass2-section"><h3>Deep document & public-source pass</h3>${docs?`<h4>Documenti acquisiti</h4><div class="doc2-list">${docs}</div>`:''}${rels?`<h4>Soggetti tecnici emersi</h4><div class="doc2-list">${rels}</div>`:''}${schedule(intel)}${publicSources(intel,reg,local)}${industryPress(industry)}</section>`;
 const scope=body.querySelector('.scope-intel-section');if(scope)scope.insertAdjacentHTML('afterend',html);else body.insertAdjacentHTML('afterbegin',html)
}
function scheduleInject(){clearTimeout(timer);timer=setTimeout(inject,60)}
async function init(){try{await load();body=$('#detailBody');if(!body)return;new MutationObserver(scheduleInject).observe(body,{childList:true,subtree:true});$('#drawer')&&new MutationObserver(scheduleInject).observe($('#drawer'),{attributes:true,attributeFilter:['class','aria-hidden']});scheduleInject()}catch(err){console.error('docpass2 intelligence init failed',err)}}
if(document.readyState==='loading')document.addEventListener('DOMContentLoaded',init);else init();
})();