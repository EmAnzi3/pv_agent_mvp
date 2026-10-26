(()=>{'use strict';
const esc=v=>String(v??'').replace(/[&<>"']/g,m=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#039;'}[m]));
let glossary=null,modal=null,query=null,list=null,count=null,lastFocus=null;
function normalize(v){return String(v||'').normalize('NFD').replace(/[\u0300-\u036f]/g,'').toLowerCase()}
function countLabel(n,filtered){if(n===1)return filtered?'1 termine trovato':'1 termine';return filtered?`${n} termini trovati`:`${n} termini`}
function render(){if(!glossary||!list)return;const q=normalize(query?.value);const terms=(glossary.terms||[]).filter(x=>!q||normalize([x.term,x.full,x.category,x.definition].join(' ')).includes(q));count.textContent=countLabel(terms.length,!!q);
 if(!terms.length){list.innerHTML='<div class="glossary-empty">Nessun termine trovato. Prova con una sigla o una parola più semplice.</div>';return}
 const groups=new Map();terms.forEach(x=>{const k=x.category||'Altro';if(!groups.has(k))groups.set(k,[]);groups.get(k).push(x)});
 list.innerHTML=[...groups.entries()].map(([category,items])=>`<section class="glossary-group"><h3>${esc(category)}</h3><div class="glossary-items">${items.map(x=>`<article class="glossary-item"><div class="glossary-term"><b>${esc(x.term)}</b>${x.full?`<span>${esc(x.full)}</span>`:''}</div><p>${esc(x.definition)}</p></article>`).join('')}</div></section>`).join('')}
function openGlossary(){if(!modal)return;lastFocus=document.activeElement;modal.classList.add('on');modal.setAttribute('aria-hidden','false');document.body.classList.add('modal-open');query.value='';render();setTimeout(()=>query.focus(),30)}
function closeGlossary(){if(!modal)return;modal.classList.remove('on');modal.setAttribute('aria-hidden','true');document.body.classList.remove('modal-open');lastFocus?.focus?.()}
function build(){const strip=document.querySelector('.method-strip');if(!strip)return;
 const methodBtn=document.getElementById('openMethod');const button=document.createElement('button');button.type='button';button.id='openGlossary';button.className='text-btn';button.textContent='Glossario';button.setAttribute('aria-haspopup','dialog');if(methodBtn)methodBtn.insertAdjacentElement('afterend',button);else strip.appendChild(button);
 document.body.insertAdjacentHTML('beforeend',`<div aria-hidden="true" class="modal glossary-modal" id="glossaryModal"><button aria-label="Chiudi glossario" class="modal-backdrop" id="glossaryBackdrop"></button><section aria-labelledby="glossaryTitle" aria-modal="true" class="modal-card glossary-card" role="dialog"><div class="drawer-head"><div><div class="eyebrow dark">Per leggere il Radar</div><h2 id="glossaryTitle">Glossario</h2><p class="glossary-intro">${esc(glossary.intro||'')}</p></div><button aria-label="Chiudi glossario" class="icon-btn" id="closeGlossary">×</button></div><div class="glossary-tools"><label><span class="sr-only">Cerca nel glossario</span><input id="glossaryQuery" type="search" placeholder="Cerca: BoP, WTG, autorizzazione…" autocomplete="off"/></label><span id="glossaryCount"></span></div><div class="glossary-list" id="glossaryList"></div></section></div>`);
 modal=document.getElementById('glossaryModal');query=document.getElementById('glossaryQuery');list=document.getElementById('glossaryList');count=document.getElementById('glossaryCount');
 button.addEventListener('click',openGlossary);document.getElementById('closeGlossary').addEventListener('click',closeGlossary);document.getElementById('glossaryBackdrop').addEventListener('click',closeGlossary);query.addEventListener('input',render);
 document.addEventListener('keydown',e=>{if(e.key==='Escape'&&modal?.classList.contains('on'))closeGlossary()});render()}
async function init(){try{glossary=await fetch('data/glossary.json',{cache:'no-store'}).then(r=>{if(!r.ok)throw new Error(`HTTP ${r.status}`);return r.json()});build()}catch(err){console.error('glossary init failed',err)}}
if(document.readyState==='loading')document.addEventListener('DOMContentLoaded',init);else init();
})();
